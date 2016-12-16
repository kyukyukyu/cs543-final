# -*- coding: utf-8 -*-
# Monkey patch the several parts of standard library that can block the
# interpreter with parts provided by gevent.
from gevent import monkey; monkey.patch_all()

from collections import Iterator, OrderedDict
from math import sqrt, floor
from time import sleep
from datetime import datetime
import itertools
import logging
import random

from docker.errors import APIError as DockerAPIError
from mininet.clean import cleanup as mininet_cleanup
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import UserSwitch, RemoteController, OVSSwitch
import docker
import gevent


client = docker.from_env()
DOCKER_HOST = '127.0.0.1'
ORIG_PORT_CONTROLLER = 6653
ORIG_PORT_REST_API = 8080


logging.basicConfig(level=logging.DEBUG)
logger_adapter = logging.getLogger('adapter')
logger_container = logging.getLogger('container')
logger_response = logging.getLogger('response')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-10s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger_adapter.addHandler(console)
logger_container.addHandler(console)
logger_response.addHandler(console)

num_packets = 0
rtt_sum = 0


switches = []
list_hosts = []


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

    #: Mininet network.
    mininet = Mininet(controller=RemoteController, switch=UserSwitch)

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
        self.mn_controller = Controller.mininet.addController(name=self.name,
                                                              controller=RemoteController,
                                                              defaultIP=DOCKER_HOST,
                                                              port=self.port_controller)
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
        if self.state is Controller.STATE_ACTIVE:
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
            logger_container.info("removing container {name}..."
                                  .format(name=c.name))
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
        logger_adapter.info("running the load adapter module...")
        while self.running:
            self.controller_to_switch_on = None
            self.controller_to_switch_off = None
            self.cpu_percentages = OrderedDict((c, c.cpu_percentage)
                                               for c
                                               in self.assignment.iterkeys())
            migration_set = self.do_rebalancing()
            if 0 == len(migration_set):
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
        logger_adapter.info("rebalancing...")
        migration_set = set()
        if self.controller_to_switch_on is not None:
            self.assignment[self.controller_to_switch_on] = set()
        if self.controller_to_switch_off is not None:
            return self.do_rebalancing_for_switch_off()
        utilizations = OrderedDict((c, (p, len(self.assignment[c])))
                                   for c, p
                                   in self.cpu_percentages.iteritems())
        while True:
            best_migration, stddev_improv =\
                self.get_best_migration(utilizations)
            if stddev_improv >= LoadAdapter.STDDEV_THRESH:
                migration_set.add(best_migration)
            else:
                logger_adapter.info("returning migration set...")
                return migration_set

    def do_rebalancing_for_switch_off(self):
        migration_set = set()
        controller_to_switch_off = self.controller_to_switch_off
        switches_off = self.assignment[controller_to_switch_off]
        n_switch_to_assign = len(switches_off) / (len(self.assignment) - 1)
        for controller in self.assignment.iterkeys():
            if controller is controller_to_switch_off:
                continue
            i = n_switch_to_assign
            n_switches_off = len(switches_off)
            if n_switches_off - n_switch_to_assign < n_switch_to_assign:
                i = n_switches_off
            for switch in switches_off:
                if i == 0:
                    break
                migration_set.add((switch, controller_to_switch_off, controller))
                i -= 1
            switches_off.clear()
        return migration_set

    def get_best_migration(self, utilizations):
        logger_adapter.info("finding best migration...")
        best_migration = None
        best_stddev_improv = 0.0
        stddev = LoadAdapter.stddev(p for p, _ in utilizations.itervalues())
        for c, util in utilizations.iteritems():
            p, n_switches = util
            if 0 == n_switches:
                continue
            for c_, util_ in utilizations.iteritems():
                if c is c_ or c is self.controller_to_switch_off:
                    continue
                p_, n_switches_ = util_
                delta = p / n_switches
                utilizations[c] = (p - delta, n_switches - 1)
                utilizations[c_] = (p_ + delta, n_switches_ + 1)
                new_stddev = LoadAdapter.stddev(p for p, _
                                                  in utilizations.itervalues())
                stddev_improv = stddev - new_stddev
                if best_stddev_improv < stddev_improv:
                    best_migration = (None, c, c_)
                    best_stddev_improv = stddev_improv
                utilizations[c] = util
                utilizations[c_] = util_
        if best_migration is not None:
            _, c, c_ = best_migration
            best_migration = (self.assignment[c].pop(), c, c_)
            p, n_switches = utilizations[c]
            p_, n_switches_ = utilizations[c_]
            delta = p / n_switches
            utilizations[c] = (p - delta, n_switches - 1)
            utilizations[c_] = (p_ + delta, n_switches_ + 1)
        return best_migration, best_stddev_improv

    @staticmethod
    def stddev(nums, with_mean=False):
        if isinstance(nums, Iterator):
            _nums = list(nums)
        else:
            _nums = nums
        m_of_sq = LoadAdapter.mean(x * x for x in _nums)
        m = LoadAdapter.mean(x for x in _nums)
        ret = sqrt(m_of_sq - m * m)
        if with_mean:
            return ret, m
        else:
            return m_of_sq - m * m

    @staticmethod
    def mean(nums):
        if isinstance(nums, Iterator):
            _nums = list(nums)
        else:
            _nums = nums
        return float(sum(_nums)) / float(len(_nums))

    def do_resizing(self):
        for c in self.assignment.iterkeys():
            if c.cpu_percentage >= self.HIGH_UTIL_THRESH:
                self.switch_on_controller()
                return True
        counter = 0
        most_free_controller = None
        min_cpu_percentage = 0.0
        for c in self.assignment.iterkeys():
            if c.state is not Controller.STATE_ACTIVE:
                continue
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
        logger_adapter.info("let's switch on a controller!")
        new_controller = Controller(activate=False)
        self.controller_to_switch_on = new_controller

    def switch_off_controller(self, controller):
        logger_adapter.info("let's switch off {name}!"
                            .format(name=controller.name))
        self.controller_to_switch_off = controller

    def check_resizing(self):
        if self.controller_to_switch_on is not None:
            # A new controller will be switched on.
            return True
        new_cpu_percentages = OrderedDict(self.cpu_percentages)
        controller_off = self.controller_to_switch_off
        p_off = new_cpu_percentages.pop(controller_off)
        n_switches = len(self.assignment[controller_off])
        if n_switches == 0:
            return True
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
        logger_adapter.info("reverts resizing...")
        if self.controller_to_switch_on is not None:
            del self.assignment[self.controller_to_switch_on]
        self.controller_to_switch_on = None
        self.controller_to_switch_off = None

    def execute_plan(self, migration_set):
        self.execute_power_on_controller()
        self.execute_migrations(migration_set)
        self.execute_power_off_controller()

    def execute_power_on_controller(self):
        if self.controller_to_switch_on is not None:
            logger_adapter.info("activate the new controller {name}..."
                                .format(name=self.controller_to_switch_on.name))
            self.controller_to_switch_on.activate()

    def execute_migrations(self, migration_set):
        """Execute the migrations.

        :param migration_set: A set of migrations. Each migration should be
            a tuple of switch, the old controller, and the new controller.
        :type migration_set: collections.Iterable[tuple(mininet.node.OVSSwitch, Controller, Controller)]
        """
        for switch, old_controller, new_controller in migration_set:
            switch.start([new_controller.mn_controller])
            switches_old = self.assignment[old_controller]
            switches_old.remove(switch)
            self.assign(switch, new_controller)
            logger_adapter.info("migrating switch {switch_name} from {old_ctrl_name} to {new_ctrl_name}..."
                                .format(switch_name=switch.name,
                                        old_ctrl_name=old_controller.name,
                                        new_ctrl_name=new_controller.name))

    def execute_power_off_controller(self):
        if self.controller_to_switch_off is not None:
            logger_adapter.info("power off controller {name}..."
                                .format(name=self.controller_to_switch_off.name))
            del self.assignment[self.controller_to_switch_off]
            self.controller_to_switch_off.deactivate()


def addHost(net, N):
    name = 'h%d' % N
    ip = '10.0.0.%d' % N
    return net.addHost(name, ip=ip)


def MultiControllerNet(controllers):
    "Create a network with multiple controllers."
    net = Controller.mininet

    print "*** Creating switches"
    s1 = net.addSwitch('s1', cls=OVSSwitch)
    s2 = net.addSwitch('s2', cls=OVSSwitch)
    s3 = net.addSwitch('s3', cls=OVSSwitch)
    s4 = net.addSwitch('s4', cls=OVSSwitch)
    s5 = net.addSwitch('s5', cls=OVSSwitch)
    s6 = net.addSwitch('s6', cls=OVSSwitch)
    switches.append(s1)
    switches.append(s2)
    switches.append(s3)
    switches.append(s4)
    switches.append(s5)
    switches.append(s6)

    print "*** Creating hosts"
    hosts1 = [addHost(net, n) for n in 3, 4]
    hosts2 = [addHost(net, n) for n in 5, 6, 7]
    hosts3 = [addHost(net, n) for n in 8, 9]
    hosts4 = [addHost(net, n) for n in 10, 11, 12]
    hosts5 = [addHost(net, n) for n in 13, 14, 15]
    hosts6 = [addHost(net, n) for n in 16, 17, 18]
    host_lists = [hosts1, hosts2, hosts3, hosts4, hosts5, hosts6]

    print "*** Creating links"
    for s, hosts in zip(switches, host_lists):
        for h in hosts:
            s.linkTo(h)
            list_hosts.append(h)

    s1.linkTo(s2)
    s2.linkTo(s3)
    s3.linkTo(s4)
    s4.linkTo(s5)
    s5.linkTo(s6)
    s6.linkTo(s1)
    s1.linkTo(s4)

    print "*** Building network"
    net.build()

    c1, c2, c3 = (c.mn_controller for c in controllers)

    # In theory this doesn't do anything
    c1.start()
    c2.start()
    c3.start()

    #print "*** Starting Switches"
    s1.start([c1])
    s2.start([c1])
    s3.start([c1])
    s4.start([c2])
    s5.start([c2])
    s6.start([c2])

    return net


def create_containers():
    for i in range(3):
        Controller(activate=False)


def simulate():
    MultiControllerNet(Controller.controllers)
    # Wait for switches to be turned on.
    Controller.mininet.waitConnected()
    logger_adapter.info("nodes in mininet are connected!")
    s1, s2, s3, s4, s5, s6 = switches
    c1, c2, c3 = Controller.controllers
    adapter = LoadAdapter()
    adapter.assign(s1, c1)
    adapter.assign(s2, c1)
    adapter.assign(s3, c1)
    adapter.assign(s4, c2)
    adapter.assign(s5, c2)
    adapter.assign(s6, c2)
    # Run the load adapter module.
    g_load = gevent.spawn(adapter.run)
    # Keep running for a while.
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        # Stop the tasks.
        adapter.stop()
        g_load.kill()
        Controller.mininet.stop()


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
        logger_container.info("starting container {name}..."
                              .format(name=c.name))
        c.activate()
    simulate()
    Controller.remove_containers()
    mininet_cleanup()


if __name__ == '__main__':
    random.seed(datetime.now())
    setLogLevel('error')
    main()
