# -*- coding: utf-8 -*-
from gevent import monkey; monkey.patch_all()

from time import sleep

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


# Keep in mind that the list of controllers may contain None object.
# This is to simplify the management of controller indices.
controllers = []
switches = []


class Controller(object):
    __slots__ = ('name', 'container', 'mn_controller',
                 'port_controller', 'port_rest_api',
                 'greenlet_cpu_percentage')
    pass


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
    logmsg_create = "creating container {name} with controller port {port_controller} and REST API port {port_rest_api}..."
    for i in range(2):
        port_controller = ORIG_PORT_CONTROLLER + i
        port_rest_api = ORIG_PORT_REST_API + i
        name = "floodlight{}".format(i)
        host_config = client.create_host_config(port_bindings={
            ORIG_PORT_CONTROLLER: port_controller,
            ORIG_PORT_REST_API: port_rest_api
        })
        print(logmsg_create.format(name=name,
                                   port_controller=port_controller,
                                   port_rest_api=port_rest_api))
        container = client.create_container(image="pierrecdn/floodlight",
                                            detach=True,
                                            name=name,
                                            ports=[port_controller,
                                                   port_rest_api],
                                            host_config=host_config)
        controller = Controller()
        controller.container = container
        controller.name = name
        controller.port_controller = port_controller
        controller.port_rest_api = port_rest_api
        controllers.append(controller)


def simulate():
    net = MultiControllerNet(*controllers)
    # Wait for switches to be turned on.
    sleep(10)
    net.pingAll()
    # Keep calm and check the Web UI.
    _ = raw_input("Press return to continue...")
    # Assign switches to other controller
    s1, s2, s3, s4 = switches
    c1, c2 = (c.mn_controller for c in controllers)
    s1.start([c2])
    s3.start([c2])
    s4.start([c2])
    # Wait for switches to be turned on.
    sleep(10)
    net.pingAll()
    CLI(net)
    net.stop()


def destroy_containers():
    for c in controllers:
        container = c.container
        print("stopping container {name}...".format(name=c.name))
        client.kill(container)
        try:
            client.remove_container(container)
        except DockerAPIError as e:
            client.remove_container(container, force=True)


def start_monitoring():
    for c in controllers:
        g = gevent.spawn(monitor_cpu_percentage, c)
        c.greenlet_cpu_percentage = g


def stop_monitoring():
    for c in controllers:
        g = c.greenlet_cpu_percentage
        if not g.dead:
            g.kill()


def monitor_cpu_percentage(controller):
    container = controller.container
    for stat in client.stats(container, decode=True, stream=True):
        percentage = get_cpu_percentage(stat)
        print("container {name}: {percentage}%".format(name=controller.name, percentage=percentage))


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
    for c in controllers:
        container = c.container
        print("starting container {name}...".format(name=c.name))
        client.start(container)
    start_monitoring()
    simulate()
    stop_monitoring()
    destroy_containers()
    mininet_cleanup()


if __name__ == '__main__':
    main()
