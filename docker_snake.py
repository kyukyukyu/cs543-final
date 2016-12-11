# -*- coding: utf-8 -*-
from time import sleep

from docker.errors import APIError as DockerAPIError
from mininet.cli import CLI
from mininet.net import Mininet
from mininet.node import UserSwitch, RemoteController, OVSSwitch
import docker


client = docker.from_env()
ORIG_PORT_CONTROLLER = 6653
ORIG_PORT_REST_API = 8080


controllers = []
switches = []


def addHost(net, N):
    name = 'h%d' % N
    ip = '10.0.0.%d' % N
    return net.addHost(name, ip=ip)


def MultiControllerNet(c1ip, c1port, c2ip, c2port):
    "Create a network with multiple controllers."

    net = Mininet(controller=RemoteController, switch=UserSwitch)

    print "Creating controllers"
    c1 = net.addController(name = 'RemoteFloodlight1', controller = RemoteController, defaultIP=c1ip, port=c1port)
    c2 = net.addController(name = 'RemoteFloodlight2', controller = RemoteController, defaultIP=c2ip, port=c2port)
    controllers.append(c1)
    controllers.append(c2)

    print "*** Creating switches"
    s1 = net.addSwitch( 's1', cls=OVSSwitch )
    s2 = net.addSwitch( 's2', cls=OVSSwitch )
    s3 = net.addSwitch( 's3', cls=OVSSwitch )
    s4 = net.addSwitch( 's4', cls=OVSSwitch )
    switches.append(s1)
    switches.append(s2)
    switches.append(s3)
    switches.append(s4)

    print "*** Creating hosts"
    hosts1 = [ addHost( net, n ) for n in 3, 4 ]
    hosts2 = [ addHost( net, n ) for n in 5, 6 ]
    hosts3 = [ addHost( net, n ) for n in 7, 8 ]
    hosts4 = [ addHost( net, n ) for n in 9, 10 ]

    print "*** Creating links"
    for h in hosts1:
        s1.linkTo( h )
    for h in hosts2:
        s2.linkTo( h )
    for h in hosts3:
        s3.linkTo( h )
    for h in hosts4:
        s4.linkTo( h )

    s1.linkTo( s2 )
    s2.linkTo( s3 )
    s4.linkTo( s2 )

    print "*** Building network"
    net.build()

    # In theory this doesn't do anything
    c1.start()
    c2.start()

    #print "*** Starting Switches"
    s1.start( [c1] )
    s2.start( [c2] )
    s3.start( [c1] )
    s4.start( [c1] )

    return net


def create_containers(containers):
    logmsg_create = "creating container {name} with controller port {port_controller} and REST API port {port_rest_api}..."
    for i in range(2):
        port_controller = ORIG_PORT_CONTROLLER + i
        port_rest_api = ORIG_PORT_REST_API + i
        container_name = "floodlight{}".format(i)
        host_config = client.create_host_config(port_bindings={
            ORIG_PORT_CONTROLLER: port_controller,
            ORIG_PORT_REST_API: port_rest_api
        })
        print(logmsg_create.format(name=container_name,
                                   port_controller=port_controller,
                                   port_rest_api=port_rest_api))
        container = client.create_container(image="pierrecdn/floodlight",
                                            detach=True,
                                            name=container_name,
                                            ports=[port_controller,
                                                   port_rest_api],
                                            host_config=host_config)
        containers.append(container)


def simulate(containers):
    cont_addr = "127.0.0.1"
    cont_ports = [ORIG_PORT_CONTROLLER, ORIG_PORT_CONTROLLER + 1]

    print "ip1:%s:%s  ip2:%s:%s" % (cont_addr, cont_ports[0],
                                    cont_addr, cont_ports[1])
    net = MultiControllerNet(cont_addr, cont_ports[0],
                             cont_addr, cont_ports[1])
    # Wait for switches to be turned on.
    sleep(10)
    net.pingAll()
    ctn1, ctn2 = containers
    print("c1: {}%, c2: {}%".format(get_cpu_percentage(ctn1), get_cpu_percentage(ctn2)))
    # Keep calm and check the Web UI.
    _ = raw_input("Press return to continue...")
    # Assign switches to other controller
    s1, s2, s3, s4 = switches
    c1, c2 = controllers
    s1.start([c2])
    s3.start([c2])
    s4.start([c2])
    # Wait for switches to be turned on.
    sleep(10)
    net.pingAll()
    print("c1: {}%, c2: {}%".format(get_cpu_percentage(ctn1), get_cpu_percentage(ctn2)))
    CLI(net)


def destroy_containers(containers):
    for container in containers:
        container_id = container.get('Id')
        print("stopping container {container_id}".format(container_id=container_id))
        client.kill(container_id)
        try:
            client.remove_container(container=container_id)
        except DockerAPIError as e:
            client.remove_container(container=container_id, force=True)


def get_cpu_percentage(container):
    cpu_percentage = 0.0
    stat = client.stats(container, stream=False)
    precpu_stats = stat[u'precpu_stats']
    cpu_stats = stat[u'cpu_stats']
    cpu_delta = float(cpu_stats[u'cpu_usage'][u'total_usage']) - float(precpu_stats[u'cpu_usage'][u'total_usage'])
    system_cpu_delta = float(cpu_stats[u'system_cpu_usage']) - float(precpu_stats[u'system_cpu_usage'])
    if cpu_delta > 0.0 and system_cpu_delta > 0.0:
        cpu_percentage = (cpu_delta / system_cpu_delta) * len(cpu_stats[u'cpu_usage'][u'percpu_usage']) * 100.0
    return cpu_percentage


def main():
    containers = []
    create_containers(containers)
    for container in containers:
        container_id = container.get('Id')
        print("starting container {container_id}".format(container_id=container_id))
        client.start(container=container_id)
    simulate(containers)
    destroy_containers(containers)


if __name__ == '__main__':
    main()
