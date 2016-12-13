# -*- coding: utf-8 -*-
# Monkey patch the several parts of standard library that can block the
# interpreter with parts provided by gevent.
from gevent import monkey; monkey.patch_all()

from collections import OrderedDict
from math import sqrt, floor
from time import sleep
import itertools

from docker.errors import APIError as DockerAPIError
from mininet.clean import cleanup as mininet_cleanup
from mininet.cli import CLI
from mininet.net import Mininet
from mininet.node import UserSwitch, RemoteController, OVSSwitch
import docker
import gevent


client = docker.from_env()
DOCKER_HOST = '127.0.0.1'
ORIG_PORT_CONTROLLER = 6653
ORIG_PORT_REST_API = 8080


switches = []


class Controller(object):
    """A floodlight controller that runs on a Docker container."""

    __slots__ = ('name', 'container', 'mn_controller',
                 'port_controller', 'port_rest_api',
                 'cpu_percentage', 'state',
                 'greenlet_cpu_percentage')

    # Enumeration for controller's state.
    STATE_ACTIVE = 0
    STATE_PENDING = 1
    STATE_INACTIVE = 2

    counter = itertools.count()
    """Atomic counter in Python world for the index number of controller.
    Since multiple greenlets (lightweight threads) may create controllers at
    the same time, we need to guarantee the atomicity of counter.
    """

    #: Global list of controllers created.
    controllers = []

    def __init__(self, activate=True):
        """Create a new controller.

        If activate is False, the Docker container for running this controller
        will not be created and started. You can explicitly activate this
        controller later by calling activate().
        """
        self.cpu_percentage = 0.0
        self.state = Controller.STATE_INACTIVE
        # Get current value of counter and increment atomically.
        i = Controller.counter.next()
        self.name = "floodlight{}".format(i)
        self.port_controller = ORIG_PORT_CONTROLLER + i
        self.port_rest_api = ORIG_PORT_REST_API + i
        #: Docker container on which this controller runs.
        self.container = None
        #: A greenlet which monitors the container's cpu usage in percentage.
        self.greenlet_cpu_percentage = None
        if activate:
            self.activate()
        Controller.controllers.append(self)

    def activate(self):
        """Activate this controller.

        After calling this function, the controller's state will be ACTIVE.
        """
        current_state = self.state
        if current_state is Controller.STATE_ACTIVE:
            return
        if self.container is None:
            # This is the first time this controller is activated, or
            # the Docker container has been removed.
            self.create_container()
        if current_state is Controller.STATE_INACTIVE:
            # The Docker container must be running
            # if the current state is PENDING.
            client.start(self.container)
        self.state = Controller.STATE_ACTIVE
        # Spawn a greenlet which monitors the container's cpu usage in percentage.
        # This greenlet will keep updating `self.cpu_percentage` until it is killed.
        self.greenlet_cpu_percentage = gevent.spawn(monitor_cpu_percentage, self)

    def create_container(self):
        """Create a Docker container for this controller."""
        port_controller = self.port_controller
        port_rest_api = self.port_rest_api
        host_config = client.create_host_config(port_bindings={
            ORIG_PORT_CONTROLLER: port_controller,
            ORIG_PORT_REST_API: port_rest_api
        })
        container = client.create_container(image="pierrecdn/floodlight",
                                            detach=True,
                                            name=self.name,
                                            ports=[port_controller,
                                                   port_rest_api],
                                            host_config=host_config)
        self.container = container

    def deactivate(self):
        """Deactivate this controller.

        After calling this function, this controller's state will be INACTIVE
        and the container where this controller runs will stop.
        """
        if self.state is Controller.STATE_INACTIVE:
            return
        self.stop_monitoring()
        client.stop(self.container)
        self.state = Controller.STATE_INACTIVE

    def set_pending(self):
        """Set the state of controller to PENDING.

        The Docker container where this controller runs will not stop.
        """
        if self.state is Controller.STATE_PENDING:
            return
        self.stop_monitoring()
        self.state = Controller.STATE_PENDING

    def remove_container(self):
        """Remove the Docker container where this controller runs.

        Since this function kills the Docker container, make sure you are not
        doing something important with the container when you call this
        function.
        """
        if self.container is None:
            return
        if self.state is Controller.STATE_ACTIVE:
            self.stop_monitoring()
        container = self.container
        client.kill(container)
        try:
            client.remove_container(container)
        except DockerAPIError as e:
            client.remove_container(container, force=True)
        self.container = None

    def stop_monitoring(self):
        """Stop monitoring the CPU usage of the Docker container where this
        controller runs."""
        g = self.greenlet_cpu_percentage
        if not g.dead:
            g.kill()

    @classmethod
    def remove_containers(cls):
        for c in cls.controllers:
            # TODO: print this message using logger.
            #print("removing container {name}...".format(name=c.name))
            c.remove_container()


class LoadAdapter(object):
    """Load adapter module for multiple controllers.

    Currently, it works like ElastiCon's one. To run this module, you should
    create an instance of this class and explicitly call run() on the instance.
    """

    __slots__ = ('running',
                 'controller_to_switch_on', 'controller_to_switch_off',
                 'assignment', 'cpu_percentages')

    HIGH_UTIL_THRESH = 70.0
    LOW_UTIL_THRESH = 10.0
    STDDEV_THRESH = 0.25

    def __init__(self):
        self.running = False
        self.controller_to_switch_on = None
        self.controller_to_switch_off = None
        self.assignment = OrderedDict()
        """Controller assignment of the switches. A key-value pair has
        a controller as the key, and the set of switches assigned to the
        controller as the value."""

    def assign(self, switch, controller):
        """Assign a switch which is not assigned to any controller yet to a
        controller.

        :param mininet.node.OVSSwitch switch: A switch. This switch should not
            be assigned to a controller yet.
        :param Controller controller: A controller to assign the switch."""
        assignment = self.assignment
        if controller not in assignment:
            assignment[controller] = set()
        switches = assignment[controller]
        switches.add(switch)

    def run(self):
        """Run this module."""
        self.running = True
        while self.running:
            self.cpu_percentages = OrderedDict((c, c.cpu_percentage)
                                               for c
                                               in self.assignment.iterkeys())
            migration_set = self.do_rebalancing()
            if migration_set is None:
                if self.do_resizing():
                    if self.check_resizing():
                        migration_set = self.do_rebalancing()
                    else:
                        self.revert_resizing()
            self.execute_plan(migration_set)
            sleep(3)

    def stop(self):
        """Gracefully stop this module."""
        self.running = False

    def do_rebalancing(self):
        migration_set = None
        # Copy CPU percentages to avoid inconsistency issue.
        stddev, mean = LoadAdapter.stddev((percentage
                                           for _, percentage
                                           in self.cpu_percentages.iteritems()),
                                          with_mean=True)
        if mean >= LoadAdapter.HIGH_UTIL_THRESH:
            # Migration not available.
            return None
        migration_set, new_cpu_percentages = \
            self.get_best_migration(self.cpu_percentages)
        new_stddev = LoadAdapter.stddev((percentage
                                         for _, percentage
                                         in new_cpu_percentages.iteritems()))
        if stddev - new_stddev < LoadAdapter.STDDEV_THRESH:
            return None
        else:
            return migration_set

    def get_best_migration(self, cpu_percentages):
        # [(switch, old_controller, new_controller), (switch_, old_controller_, new_controller_)]
        pass

    @staticmethod
    def stddev(nums, with_mean=False):
        m_of_sq = LoadAdapter.mean(x * x for x in nums)
        m = LoadAdapter.mean(x for x in nums)
        ret = sqrt(m_of_sq - m * m)
        if with_mean:
            return ret, m
        else:
            return m_of_sq - m * m

    @staticmethod
    def mean(nums):
        return float(sum(nums)) / float(len(nums))

    def do_resizing(self):
        for c in Controller.controllers:
            if c.cpu_percentage >= self.HIGH_UTIL_THRESH:
                self.switch_on_controller()
                return True
        counter = 0
        most_free_controller = None
        min_cpu_percentage = 0.0
        for c in Controller.controllers:
            cpu_percentage = c.cpu_percentage
            if cpu_percentage <= self.LOW_UTIL_THRESH:
                counter += 1
                if most_free_controller is None or cpu_percentage <= min_cpu_percentage:
                    most_free_controller = c
                    min_cpu_percentage = cpu_percentage
        if counter >= 2:
            self.switch_off_controller(most_free_controller)
            return True
        else:
            return False

    def switch_on_controller(self):
        self.controller_to_switch_on = Controller(activate=False)

    def switch_off_controller(self, controller):
        self.controller_to_switch_off = controller

    def check_resizing(self):
        if self.controller_to_switch_on is not None:
            # A new controller will be switched on.
            return True
        new_cpu_percentages = OrderedDict(self.cpu_percentages)
        controller_off = self.controller_to_switch_off
        p_off = new_cpu_percentages.pop(controller_off)
        n_switches = len(self.assignment[controller_off])
        p_switch = p_off / n_switches
        for p in new_cpu_percentages.itervalues():
            n_moves = int(floor((LoadAdapter.HIGH_UTIL_THRESH - p) / p_switch))
            n_switches -= n_moves
            if n_switches <= 0:
                break
        if n_switches > 0:
            return False
        else:
            return True

    def revert_resizing(self):
        self.controller_to_switch_on = None
        self.controller_to_switch_off = None

    def execute_plan(self, migration_set):
        self.execute_power_on_controller()
        self.execute_migrations(migration_set)
        self.execute_power_off_controller()

    def execute_power_on_controller(self):
        self.controller_to_switch_on.activate()

    def execute_migrations(self, migration_set):
        """Execute the migrations.

        :param migration_set: A set of migrations. Each migration should be
            a tuple of switch, the old controller, and the new controller.
        :type migration_set: collections.Iterable[tuple(mininet.node.OVSSwitch, Controller, Controller)]
        """
        for switch, old_controller, new_controller in migration_set:
            switch.start([new_controller])
            switches_old = self.assignment[old_controller]
            switches_old.remove(switch)
            self.assign(switch, new_controller)

    def execute_power_off_controller(self):
        self.controller_to_switch_off.deactivate()


def addHost(net, N):
    name = 'h%d' % N
    ip = '10.0.0.%d' % N
    return net.addHost(name, ip=ip)


def MultiControllerNet(controller1, controller2):
    "Create a network with multiple controllers."

    net = Mininet(controller=RemoteController, switch=UserSwitch)

    print "Creating controllers"
    c1 = net.addController(name='RemoteFloodlight1',
                           controller=RemoteController,
                           defaultIP=DOCKER_HOST,
                           port=controller1.port_controller)
    c2 = net.addController(name='RemoteFloodlight2',
                           controller=RemoteController,
                           defaultIP=DOCKER_HOST,
                           port=controller2.port_controller)
    controller1.mn_controller = c1
    controller2.mn_controller = c2

    print "*** Creating switches"
    s1 = net.addSwitch('s1', cls=OVSSwitch)
    s2 = net.addSwitch('s2', cls=OVSSwitch)
    s3 = net.addSwitch('s3', cls=OVSSwitch)
    s4 = net.addSwitch('s4', cls=OVSSwitch)
    switches.append(s1)
    switches.append(s2)
    switches.append(s3)
    switches.append(s4)

    print "*** Creating hosts"
    hosts1 = [addHost(net, n) for n in 3, 4]
    hosts2 = [addHost(net, n) for n in 5, 6]
    hosts3 = [addHost(net, n) for n in 7, 8]
    hosts4 = [addHost(net, n) for n in 9, 10]

    print "*** Creating links"
    for h in hosts1:
        s1.linkTo(h)
    for h in hosts2:
        s2.linkTo(h)
    for h in hosts3:
        s3.linkTo(h)
    for h in hosts4:
        s4.linkTo(h)

    s1.linkTo(s2)
    s2.linkTo(s3)
    s4.linkTo(s2)

    print "*** Building network"
    net.build()

    # In theory this doesn't do anything
    c1.start()
    c2.start()

    #print "*** Starting Switches"
    s1.start([c1])
    s2.start([c2])
    s3.start([c1])
    s4.start([c1])

    return net


def create_containers():
    for i in range(2):
        Controller(activate=False)


def simulate():
    net = MultiControllerNet(*Controller.controllers)
    # Wait for switches to be turned on.
    sleep(10)
    net.pingAll()
    # Keep calm and check the Web UI.
    _ = raw_input("Press return to continue...")
    # Assign switches to other controller
    s1, s2, s3, s4 = switches
    c1, c2 = (c.mn_controller for c in Controller.controllers)
    s1.start([c2])
    s3.start([c2])
    s4.start([c2])
    # Wait for switches to be turned on.
    sleep(10)
    net.pingAll()
    # TODO: run the actual load adaption here, by creating a LoadAdapter object, assign the controller to switches, and run the adapter.
    CLI(net)
    net.stop()


def monitor_cpu_percentage(controller):
    container = controller.container
    for stat in client.stats(container, decode=True, stream=True):
        controller.cpu_percentage = get_cpu_percentage(stat)


def get_cpu_percentage(stat):
    cpu_percentage = 0.0
    precpu_stats = stat[u'precpu_stats']
    cpu_stats = stat[u'cpu_stats']
    cpu_delta = float(cpu_stats[u'cpu_usage'][u'total_usage']) - float(precpu_stats[u'cpu_usage'][u'total_usage'])
    system_cpu_delta = float(cpu_stats[u'system_cpu_usage']) - float(precpu_stats[u'system_cpu_usage'])
    if cpu_delta > 0.0 and system_cpu_delta > 0.0:
        cpu_percentage = (cpu_delta / system_cpu_delta) * len(cpu_stats[u'cpu_usage'][u'percpu_usage']) * 100.0
    return cpu_percentage


def main():
    create_containers()
    for c in Controller.controllers:
        print("starting container {name}...".format(name=c.name))
        c.activate()
    simulate()
    Controller.remove_containers()
    mininet_cleanup()


if __name__ == '__main__':
    main()
