#! python
#
# MIT License
#
# (C) Copyright 2024-2025 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# pylint: disable='consider-using-f-string'
"""Internal script intended to be run on a Virtual Blade by the Ubuntu
flavor of the vTDS Cluster Layer. This creates Virtual Networks and
Virtual Nodes as well as setting up DHCP and booting the Virtual Nodes
based on a configuration file provided as the second argument on the
command line. The first agument is the blade class of the blade it is
running on.

"""
import sys
from socket import (
    socket,
    SOCK_STREAM,
    AF_INET
)
from subprocess import (
    Popen,
    PIPE
)
from tempfile import (
    NamedTemporaryFile
)
from uuid import uuid4
from time import sleep
import json

from node_builder import pick_node_builder

from cluster_common import (
    ContextualError,
    UsageError,
    usage,
    error_msg,
    info_msg,
    run_cmd,
    read_config,
    if_network,
    net_name,
    blade_ipv4_ifname,
    network_blade_connected,
    is_nat_router,
    is_dhcp_server,
    network_connected,
    node_ipv4_addrs,
    node_mac_addrs,
    find_address_family,
    find_blade_cidr,
    network_layer_2_name,
    network_bridge_name,
    node_connected_networks,
    instance_range,
    install_blade_ssh_keys,
    get_blade_interface_data,
    prepare_nat,
    install_nat_rule,
    compute_hostname,
    compute_node_name,
    node_ipv4
)


class NetworkInstaller:
    """A class to handle declarative creation of virtual networks on a
    blade.

    """
    @staticmethod
    def _get_interfaces():
        """Retrieve information about existing interfaces structured for
        easy inspection to determine what is already in place.

        """
        if_data = get_blade_interface_data()
        with Popen(
                ['bridge', '--json', 'fdb'],
                stdout=PIPE,
                stderr=PIPE
        ) as cmd:
            fdb_data = json.loads(cmd.stdout.read())
        interfaces = {iface['ifname']: iface for iface in if_data}
        dsts = [fdb_entry for fdb_entry in fdb_data if 'dst' in fdb_entry]
        for dst in dsts:
            if 'dst' in dst:
                iface = interfaces[dst['ifname']]
                iface['fdb_dsts'] = (
                    [dst['dst']] if 'fdb_dsts' not in iface else
                    iface['fdb_dsts'] + [dst['dst']]
                )
        return interfaces

    @staticmethod
    def _get_virtual_networks():
        """Retrieve information about existing interfaces structured for
        easy inspection to determine what is already in place.

        """
        with Popen(
                ['virsh', 'net-list', '--name'],
                stdout=PIPE,
                stderr=PIPE
        ) as cmd:
            vnets = [
                line[:-1].decode('UTF-8') for line in cmd.stdout.readlines()
                if line[:-1].decode('UTF-8')
            ]
        return vnets

    def __init__(self):
        """Constructor

        """
        self.interfaces = self._get_interfaces()
        self.veths = {
            key: val for key, val in self.interfaces.items()
            if 'linkinfo' in val and val['linkinfo']['info_kind'] == 'veth'
        }
        self.virtuals = {
            key: val for key, val in self.interfaces.items()
            if 'linkinfo' in val and val['linkinfo']['info_kind'] == 'vxlan'
            or 'linkinfo' in val and val['linkinfo']['info_kind'] == 'veth'
        }
        self.bridges = {
            key: val for key, val in self.interfaces.items()
            if 'linkinfo' in val and val['linkinfo']['info_kind'] == 'bridge'
        }
        self.vnets = self._get_virtual_networks()

    def _check_conflict(self, name, bridge_name):
        """Look for conflicting existing interfaces for the named
        layer_2 and bridge and error if they are found.

        """
        if name in self.interfaces and name not in self.virtuals:
            raise ContextualError(
                "attempting to create virtual network '%s' but conflicting "
                "non-virtual network interface already exists on blade" % name
            )
        if bridge_name in self.interfaces and bridge_name not in self.bridges:
            raise ContextualError(
                "attempting to create bridge for virtual network '%s' [%s] "
                "but conflicting non-bridge network interface already "
                "exists on blade" % (name, bridge_name)
            )

    def _find_underlay(self, endpoint_ips):
        """All non-blade-local virtual networks have a tunnel endpoint
        on the blades where they are used, so they all have a network
        device used as the point of access to the underlay network
        which determines that tunnel endpoint. This function finds the
        device and endpoint IP on the current blade that will be used
        to connect to the virtual network.

        """
        for intf, if_desc in self.interfaces.items():
            addr_info = if_desc.get('addr_info', [])
            for info in addr_info:
                if 'local' in info and info['local'] in endpoint_ips:
                    return (intf, info['local'])
        raise ContextualError(
            "no network device was found with an IP address matching any of "
            "the following endpoint IPs: %s" % (str(endpoint_ips))
        )

    @staticmethod
    def remove_link(if_name):
        """Remove an interface (link) specified by the interface name.

        """
        run_cmd('ip', ['link', 'del', if_name])

    @staticmethod
    def add_new_network(layer_2_name, bridge_name, vxlan_id, device):
        """Set up a the blade-level network infrastructure including a
        layer 2 device, which can be either a blade-local virtual
        ethernet or a VxLAN tunnel ingress using the supplied VxLAN
        ID, and set up the bridge interface mastering the layer_2
        deviceonto which IPs and VMs can be bound. If 'vxlan_id' is
        None, assume this is a blade-local network.

        """
        # Make the L2 Endpoint
        args = (
            [
                'link', 'add', layer_2_name,
                'type', 'vxlan',
                'id', vxlan_id,
                'dev', device,
                'dstport', '4789',
            ] if vxlan_id is not None
            else [
                'link', 'add', layer_2_name,
                'type', 'veth',
            ]
        )
        run_cmd('ip', args)
        # Make the bridge device
        run_cmd(
            'ip',
            ['link', 'add', bridge_name, 'type', 'bridge']
        )
        # Master the layer 2 device under the bridge
        run_cmd(
            'ip',
            ['link', 'set', layer_2_name, 'master', bridge_name]
        )
        # Turn the bridge on
        run_cmd(
            'ip',
            ['link', 'set', bridge_name, 'up']
        )
        # Turn the layer 2 device on
        run_cmd(
            'ip',
            ['link', 'set', layer_2_name, 'up']
        )

    @staticmethod
    def add_blade_interface(peer_name, ifname, bridge_name, blade_cidr):
        """Set up local connectivity to a Virtual Network on a blade
        using a peer and paired interface to allow the bridge to join
        in the Virtual Network.

        """
        if peer_name is None or ifname is None:
            return
        # Create the interface / peer name
        run_cmd(
            'ip',
            [
                'link', 'add', ifname,
                'type', 'veth',
                'peer', 'name', peer_name,
            ]
        )
        # Master the peer under the bridge
        run_cmd(
            'ip',
            ['link', 'set', peer_name, 'master', bridge_name]
        )
        # Turn on the peer
        run_cmd(
            'ip',
            ['link', 'set', peer_name, 'up']
        )
        # Turn on the interface
        run_cmd(
            'ip',
            ['link', 'set', ifname, 'up']
        )
        if blade_cidr:
            # Add IP address to the interface
            run_cmd(
                'ip',
                ['addr', 'add', blade_cidr, 'dev', ifname]
            )

    @staticmethod
    def connect_endpoints(tunnel_name, endpoint_ips, local_ip_addr):
        """Create the static mesh interconnect between tunnel
        endpoints (blades) for the named network.

        """
        remote_ips = [
            ip_addr for ip_addr in endpoint_ips
            if ip_addr != local_ip_addr
        ]
        for ip_addr in remote_ips:
            run_cmd(
                'bridge',
                [
                    'fdb', 'append', 'to', '00:00:00:00:00:00',
                    'dst', ip_addr,
                    'dev', tunnel_name,
                ],
            )

    def add_virtual_network(self, network_name, bridge_name):
        """Add a network to libvirt that is bound onto the bridge that
        is mastering the layer 2 devicefor that network.

        """
        net_desc = """
<network>
  <name>%s</name>
  <forward mode="bridge" />
  <bridge name="%s" />
</network>
        """ % (network_name, bridge_name)

        with NamedTemporaryFile(mode='w+', encoding='UTF-8') as tmpfile:
            tmpfile.write(net_desc)
            tmpfile.flush()
            run_cmd('virsh', ['net-define', tmpfile.name])
        run_cmd('virsh', ['net-start', network_name])
        run_cmd('virsh', ['net-autostart', network_name])
        self.vnets.append(network_name)

    def remove_virtual_network(self, network_name):
        """Remove a network from libvirt

        """
        if network_name not in self.vnets:
            # Don't remove it if it isn't there
            return
        run_cmd('virsh', ['net-destroy', network_name])
        run_cmd('virsh', ['net-undefine', network_name])
        self.vnets.remove(network_name)

    def construct_virtual_network(self, network, blade_cidr):
        """Create a layer 2 device (VxLAN Tunnel or 'veth' device) and
        bridge for a virtual network, populate its layer2 mesh (among
        the blades where it can be seen) and add it to the libvirt
        list of networks on the blade.

        """
        blade_local = network.get('blade_local', False)
        network_name = net_name(network)
        layer_2_name = network_layer_2_name(network)
        bridge_name = network_bridge_name(network)
        blade_peer_name = None
        blade_ifname = None
        if (
                isinstance(network.get('devices', None), dict) and
                isinstance(network['devices'].get('local', None), dict)
        ):
            blade_peer_name = network['devices']['local'].get('peer', None)
            blade_ifname = network['devices']['local'].get(
                'interface', None
            )
            if blade_peer_name is None:
                raise ContextualError(
                    "Virtual Network '%s' local block is missing 'peer' "
                    "element" % network_name
                )
            if blade_ifname is None:
                raise ContextualError(
                    "Virtual Network '%s' local block is missing "
                    "'interface' element" % network_name
                )
        vxlan_id = str(network.get('tunnel_id', None))
        if not blade_local and vxlan_id is None:
            raise ContextualError(
                "Non-blade local virtual network '%s' has no tunnel ID" %
                network_name
            )
        # Drop any configured VxLAN ID if the network is blade-local
        if blade_local:
            vxlan_id = None
        endpoint_ips = network.get(
            'endpoint_ips', []
        ) if not blade_local else []
        self._check_conflict(layer_2_name, bridge_name)
        if layer_2_name in self.interfaces:
            self.remove_link(layer_2_name)
        if bridge_name in self.interfaces:
            self.remove_link(bridge_name)
        if blade_peer_name in self.interfaces:
            self.remove_link(blade_peer_name)
        device, local_ip_addr = self._find_underlay(
            endpoint_ips
        ) if not blade_local else (None, None)
        self.add_new_network(layer_2_name, bridge_name, vxlan_id, device)
        # Blade local networks will have no endpoints, so calling this
        # is harmless in that case.
        self.connect_endpoints(layer_2_name, endpoint_ips, local_ip_addr)
        self.add_blade_interface(
            blade_peer_name, blade_ifname, bridge_name, blade_cidr
        )
        self.remove_virtual_network(network_name)
        self.add_virtual_network(network_name, bridge_name)


class VirtualNode:
    """A class for composing, creating and managing Virtual Nodes.

    """
    def __init__(self, config, node_class, node_instance):
        """Constructor: 'node_class' is the node class configuration
        for the Virtual node, 'networks' is a filtered dictionary
        indexed by network name of the networks that are connected to
        the Virtual Node, 'node_instance' is the instance number
        within its node class of the Virtual Node.

        """
        self.node_class = node_class
        self.node_instance = node_instance
        self.networks = node_connected_networks(node_class, config['networks'])
        class_name = node_class['class_name']
        if self.node_class.get('virtual_machine', None) is None:
            raise ContextualError(
                "configuration error: node class '%s' does not define "
                "a 'virtual_machine' section: %s" % (
                    class_name, str(self.node_class)
                )
            )
        self.hostname = compute_hostname(node_class, node_instance)
        self.node_name = compute_node_name(node_class, node_instance)
        root_passwd = str(uuid4())
        self.node_builder = pick_node_builder(
            config, node_class, node_instance, root_passwd
        )

    def __host_blade_ipv4(self):
        """Find the IP address of this Virtual Node on the Host Blade
        network (the blade local network used by the Virtual Blade
        hosting a Virtual Node to talk to the node).

        """
        # This is kind of wrong, since 'non_cluster' could be used for
        # something else entirely, but it is the best I have right now
        # without knowing the whole config, so it will have to do.
        candidates = [
            node_ipv4(self.node_class, self.node_instance, network)
            for network, network_settings in self.networks.items()
            if network_settings.get('non_cluster', False)
        ]
        return candidates[0] if candidates else None

    def wait_for_ssh(self):
        """Wait for the node to be up and listening on the SSH port
        (port 22). This is a good indication that the node has fully
        booted. Do this using simple TCP connect operations to reduce
        overhead and speed up the operation. If we can connect to the
        SSH port, that indicates that the server is running which
        should be enough.

        """
        last_err = None
        retries = 100
        while retries > 0:
            with socket(AF_INET, SOCK_STREAM) as tmp:
                try:
                    # Got a connection, we are finished. Let the
                    # conection close with the return.
                    tmp.connect((self.__host_blade_ipv4(), 22))
                    return
                except TimeoutError as err:
                    # The connect attempt timed out. This means that
                    # the node has not yet started handling network
                    # traffic, but it takes quite a while to happen,
                    # so don't sleep, just let the loop try again.
                    info_msg(
                        "timed out waiting for '%s' to be "
                        "ready, trying again" % self.hostname
                    )
                    last_err = err
                except ConnectionRefusedError as err:
                    # Connect Refused means that networking is up but
                    # SSH is not being served just yet. Give it 5
                    # seconds and it will probably be ready.
                    info_msg(
                        "connection refused waiting for '%s' to be "
                        "ready, trying again" % self.hostname
                    )
                    sleep(5)
                    last_err = err
                except OSError as err:
                    # An OSError here (other than COnnectionRefused)
                    # usually indicates a failure to ARP on the host,
                    # meaning that the network is there but not yet up
                    # on the node.
                    info_msg(
                        "%s occurred waiting for '%s' to be "
                        "ready, trying again" % (str(err), self.hostname)
                    )
                    sleep(10)
                    last_err = err
                finally:
                    retries -= 1
        # If we got to here, we failed and the exception information
        # is in 'last_err'. Raise an exception to report the error.
        raise ContextualError(
            "failed to connect to '%s' on port 22 to verify "
            "boot success - %s" % (self.hostname, str(last_err))
        )

    def create(self):
        """Create the Virtual Node on the host blade

        """
        self.node_builder.build()

    def stop(self):
        """Stop but do not undefine the Virtual Node.
        """
        run_cmd('virsh', ['destroy', self.node_name], check=False)

    def remove(self):
        """Stop and undefine the Virtual Node.

        """
        self.stop()
        run_cmd('virsh', ['undefine', self.node_name], check=False)


class KeaDHCP4:
    """A class used to compose Kea DHCP4 configuration.

    """
    def __init__(self, config, blade_class, blade_instance):
        """Constructor

        """
        # Get a dictionary of networks indexed by name for which this
        # blade is the DHCP server.
        self.nets_by_name = {
            net_name(network): network
            for _, network in config.get('networks', {}).items()
            if is_dhcp_server(network, blade_class, blade_instance)
        }
        # Get a list of Virtual Node network interfaces that are
        # connected to one of the networks for which this blade is a
        # DHCP server.
        self.network_interfaces = [
            interface
            for _, node_class in config.get('node_classes', {}).items()
            for _, interface in node_class.get(
                    'network_interfaces', {}
            ).items()
            if if_network(interface) in self.nets_by_name
        ]
        self.dhcp4_config = self.__compose_config()

    def __compose_reservations(self, interfaces):
        """Compose Kea DHCP4 reservations for network interfaces that have
        reserved configuration on the specified network and return the
        host entry list.

        """
        reservations = []
        for interface in interfaces:
            mac_addrs = node_mac_addrs(interface)
            ip_addrs = node_ipv4_addrs(interface)
            reservations += [
                {
                    'hw-address': mac_addrs[i],
                    'ip-address': ip_addrs[i],
                }
                for i in range(
                    0,
                    len(mac_addrs)
                    if len(mac_addrs) <= len(ip_addrs) else len(ip_addrs)
                )
            ]
        return reservations

    def __compose_subnet(self, blade_if, address_family, interfaces):
        """Based on a network's l3 configuration block, compose the
        DCP4 subnet configuration for Kea.

        """
        pools = [
            {'pool': "%s - %s" % (pool['start'], pool['end'])}
            for pool in address_family['dhcp'].get('pools', [])
        ]
        try:
            cidr = address_family['cidr']
        except KeyError as err:
            raise ContextualError(
                "configuration error: network address_family %s "
                "has no 'cidr' element" % str(address_family)
            ) from err
        subnet = {
            'pools': pools,
            'subnet': cidr,
            'interface': blade_if,
            'reservations': self.__compose_reservations(interfaces),
            'option-data': []
        }
        gateway = address_family.get('gateway', None)
        if gateway:
            subnet['option-data'].append(
                {
                    'name': 'routers',
                    'data': gateway,
                },
            )
        nameservers = address_family.get('name_servers', [])
        if nameservers:
            subnet['option-data'].append(
                {
                    'name': 'domain-name-servers',
                    'data': ','.join(nameservers),
                },
            )
        return subnet

    def __compose_network(self, network):
        """Compose the base Kea DHCP4 configuration for the provided
        network and return it. A network may be a set of subnets,
        based on 'address_family' blocks, so treat each AF_INET
        'address_family' block as its own subnet.

        """
        # Further filter network interfaces to get only those that
        # apply to this network itself.
        interfaces = [
            interface
            for interface in self.network_interfaces
            if if_network(interface) == net_name(network)
        ]
        blade_if = blade_ipv4_ifname(network)
        address_family = find_address_family(network, 'AF_INET')
        subnet = (
            self.__compose_subnet(blade_if, address_family, interfaces)
            if address_family.get('dhcp', {}) else None
        )
        return [subnet] if subnet is not None else []

    def __compose_subnets(self):
        """Compose the list of subnets and reservations for DHCP4

        """
        return [
            netconf
            for _, network in self.nets_by_name.items()
            for netconf in self.__compose_network(network)
        ]

    def __compose_config(self):
        """Compose a global DHCP4 configuration into which subnets and
        reservations will be dropped and return it.

        """
        # Get the list of blade level interface names (i.e. interfaces
        # through which the blade can access the Virtual Network)
        # associated with all of the networks this blade serves. These
        # will be the interfaces this instance of DHCP4 listens on. We
        # do this using a set comprehension because there are likely
        # duplicates in the list
        if_names = {
            self.nets_by_name[
                if_network(network_interface)
            ]['devices']['local']['interface']
            for network_interface in self.network_interfaces
            if (
                blade_ipv4_ifname(
                    self.nets_by_name[if_network(network_interface)]
                )
                is not None
            )
        }
        return {
            'Dhcp4': {
                'valid-lifetime': 4000,
                'renew-timer': 1000,
                'rebind-timer': 2000,
                'interfaces-config': {
                    'interfaces': list(if_names),  # Make the set a list
                },
                'lease-database': {
                    'type': 'memfile',
                    'persist': True,
                    'name': '/var/lib/kea/kea-leases4.csv',
                    'lfc-interval': 1800,
                },
                'subnet4': self.__compose_subnets(),
            }
        }

    def write_config(self, filename):
        """Write out the configuration into the specified filname.

        """
        try:
            with open(filename, 'w', encoding='UTF-8') as config_file:
                json.dump(self.dhcp4_config, config_file, indent=4)
        except OSError as err:
            raise ContextualError(
                "error creating Kea DHCP4configuration "
                "['%s'] - %s" % (str(err), filename)
            ) from err

    def restart_server(self):
        """Restart the Kea DHCP4 servers

        """
        run_cmd('systemctl', ['restart', 'kea-dhcp4-server'])
        # Wait for the service to report itself active. If it doesn't
        # do so in 30 seconds, something is wrong, raise an error.
        timeout = 30
        while timeout > 0:
            with Popen(
                    ['systemctl', '--quiet', 'is-active', 'kea-dhcp4-server'],
            ) as cmd:
                if cmd.wait() == 0:
                    # It's active we are done here...
                    return
                sleep(1)
        # The server never became active. Run a systemctl status
        # capturing the output and then raise an error reporting the
        # failure and the status.
        with Popen(
            ['systemctl', 'status', 'kea-dhcp4-server'],
            stdout=PIPE
        ) as cmd:
            status = cmd.stdout.read()
            raise ContextualError(
                "when restarting kea-dhcp4-server the service timed out "
                "while waiting to become active. "
                "Reported status:\n%s" % status
            )


def main(argv):
    """Main function...

    """
    # Arguments are 'blade_class' the name of the blade class to which
    # this blade belongs and 'config_path' the path to the
    # configuration file used for this deployment.
    if not argv:
        raise UsageError("no arguments provided")
    if len(argv) < 4:
        raise UsageError("too few arguments")
    if len(argv) > 4:
        raise UsageError("too many arguments")
    blade_class = argv[0]
    try:
        blade_instance = int(argv[1])
    except ValueError as err:
        raise UsageError(
            "invalid 'blade-instance' value ('%s') should be an "
            "integer value" % argv[1]
        ) from err
    config = read_config(argv[2])
    key_dir = argv[3]
    install_blade_ssh_keys(key_dir)
    network_installer = NetworkInstaller()
    network_installer.remove_virtual_network("default")
    # Only work with node classes that are hosted on our blade
    # class. Turn the map into a list and filter out any irrelevant
    # node classes.
    node_classes = config.get('node_classes', {})
    # Stuff class names (the keys used to look up the node classes in
    # the config) into the node classes we pulled from the
    # config. That will let us use the class name when we see a node
    # class but not have to keep everything in a dictionary.
    for class_name, node_class in node_classes.items():
        node_class['class_name'] = class_name
    node_classes = [
        node_class for _, node_class in node_classes.items()
        if (
            node_class
            .get('host_blade', {})
            .get('blade_class', None)
        ) == blade_class
    ]
    # Only work with networks that are connected to our blade class.
    # Turn the map into a list and filter out any irrelevant networks.
    networks = config.get('networks', {})
    networks = [
        network
        for _, network in networks.items()
        if network_connected(network, node_classes)
        or network_blade_connected(network, blade_class, blade_instance)
    ]
    # Set up to install fresh NAT rules...
    prepare_nat()
    # Build the virtual networks for the cluster.
    for network in networks:
        network_installer.construct_virtual_network(
            network, find_blade_cidr(network, blade_class, blade_instance)
        )
        if is_nat_router(network, blade_class, blade_instance):
            install_nat_rule(network)

    # Configure Kea on this blade to serve DHCP4 for the networks
    # served by this blade.
    kea_dhcp4 = KeaDHCP4(config, blade_class, blade_instance)
    kea_dhcp4.write_config("/etc/kea/kea-dhcp4.conf")
    kea_dhcp4.restart_server()

    # Deploy the Virtual Nodes to this blade
    #
    # First construct a bunch of VirtualNode objects, one for each
    # Virtual Node to be created. Each node class specifies a blade
    # capacity for nodes of that class, so only create the instances
    # of that class that belong on this blade (i.e. spread them across
    # the blades).
    nodes = [
        VirtualNode(
            config,
            node_class,
            instance
        )
        for node_class in node_classes
        for instance in range(
            *instance_range(node_class, blade_instance)
        )
    ]
    print("nodes: %s" % str(nodes))
    # Now remove any Virtual Nodes that are in our list and are
    # currently deployed.
    for node in nodes:
        node.remove()
    # Now create all the Virtual Nodes in the list
    for node in nodes:
        node.create()
    # Now wait for the Virtual Nodes to be up and running (listening
    # on the SSH port)
    for node in nodes:
        node.wait_for_ssh()


def entrypoint(usage_msg, main_func):
    """Generic entrypoint function. This sets up command line
    arguments for the invocation of a 'main' function and takes care
    of handling any vTDS exceptions that are raised to report
    errors. Other exceptions are allowed to pass to the caller for
    handling.

    """
    try:
        main_func(sys.argv[1:])
    except ContextualError as err:
        error_msg(str(err))
        sys.exit(1)
    except UsageError as err:
        usage(usage_msg, str(err))


if __name__ == '__main__':
    USAGE_MSG = """
usage: deploy_to_blade blade_class blade_instance config_path ssh_key_dir

Where:

    blade_class is the name of the Virtual Blade class to which this
                Virtual Blade belongs.
    blade_instance is the instance number of the blade within the
                   list of blades of this class
    config_path is the path to a YAML file containing the blade
                configuration to apply.
    ssh_key_dir is the path to a directory containing the SSH key pair
                for blades and nodes to use
"""[1:-1]
    entrypoint(USAGE_MSG, main)
