#! python
#
# MIT License
#
# (C) Copyright [2024] Hewlett Packard Enterprise Development LP
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
import os
from os import (
    remove as remove_file,
    makedirs
)
from os.path import (
    exists,
    join as path_join
)
from socket import (
    socket,
    SOCK_STREAM,
    AF_INET
)
from subprocess import (
    Popen,
    TimeoutExpired,
    PIPE
)
from tempfile import (
    NamedTemporaryFile
)
from uuid import uuid4
from time import sleep
import json
from jinja2 import (
    Template,
    TemplateError
)
import yaml


class ContextualError(Exception):
    """Exception to report failures seen and contextualized within the
    application.

    """


class UsageError(Exception):  # pylint: disable=too-few-public-methods
    """Exception to report usage errors

    """


def write_out(string):
    """Write an arbitrary string on stdout and make sure it is
    flushed.

    """
    sys.stdout.write(string)
    sys.stdout.flush()


def write_err(string):
    """Write an arbitrary string on stderr and make sure it is
    flushed.

    """
    sys.stderr.write(string)
    sys.stderr.flush()


def usage(usage_msg, err=None):
    """Print a usage message and exit with an error status.

    """
    if err:
        write_err("ERROR: %s\n" % err)
    write_err("%s\n" % usage_msg)
    sys.exit(1)


def error_msg(msg):
    """Format an error message and print it to stderr.

    """
    write_err("ERROR: %s\n" % msg)


def warning_msg(msg):
    """Format a warning and print it to stderr.

    """
    write_err("WARNING: %s\n" % msg)


def info_msg(msg):
    """Format an informational message and print it to stderr.

    """
    write_err("INFO: %s\n" % msg)


def run_cmd(cmd, args, stdin=sys.stdin, check=True, timeout=None):
    """Run a command with output on stdout and errors on stderr

    """
    exitval = 0
    try:
        with Popen(
                [cmd, *args],
                stdin=stdin, stdout=sys.stdout, stderr=sys.stderr
        ) as command:
            time = 0
            signaled = False
            while True:
                try:
                    exitval = command.wait(timeout=5)
                except TimeoutExpired:
                    time += 5
                    if timeout and time > timeout:
                        if not signaled:
                            # First try to terminate the process
                            command.terminate()
                            continue
                        command.kill()
                        print()
                        # pylint: disable=raise-missing-from
                        raise ContextualError(
                            "'%s' timed out and did not terminate "
                            "as expected after %d seconds" % (
                                " ".join([cmd, *args]),
                                time
                            )
                        )
                    continue
                # Didn't time out, so the wait is done.
                break
            print()
    except OSError as err:
        raise ContextualError(
            "executing '%s' failed - %s" % (
                " ".join([cmd, *args]),
                str(err)
            )
        ) from err
    if exitval != 0 and check:
        fmt = (
            "command '%s' failed"
            if not signaled
            else "command '%s' timed out and was killed"
        )
        raise ContextualError(fmt % " ".join([cmd, *args]))
    return exitval


def read_config(config_file):
    """Read in the specified YAML configuration file for this blade
    and return the parsed data.

    """
    try:
        with open(config_file, 'r', encoding='UTF-8') as config:
            return yaml.safe_load(config)
    except OSError as err:
        raise ContextualError(
            "failed to load blade configuration file '%s' - %s" % (
                config_file,
                str(err)
            )
        ) from err


def if_network(interface):
    """Retrieve the network name attached to an interface, raise an
    exception if there is none.

    """
    try:
        return interface['cluster_network']
    except KeyError as err:
        raise ContextualError(
            "configuration error: interface '%s' doesn't "
            "identify its connected Virtual Network" % str(interface)
        ) from err


def net_name(network):
    """Retrieve the network name of a network, raise an exception if
    there is none.

    """
    try:
        return network['network_name']
    except KeyError as err:
        raise ContextualError(
            "configuration error: network %s has no "
            "network name" % str(network)
        ) from err


def blade_ipv4_ifname(network):
    """Get the name of the interface on the blade where DHCP is
    served for this interface (if any) and return it. Return None
    if there is nothing configured for that.

    """
    return network.get('devices', {}).get('local', {}).get('interface', None)


def blade_ipv4_cidr(network):
    """Get the name of the interface on the blade where DHCP is
    served for this interface (if any) and return it. Return None
    if there is nothing configured for that.

    """
    return network.get('devices', {}).get('local', {}).get('interface', None)


def connected_blade_instances(network, blade_class):
    """Get the list of conencted blade instance numbers for a given
    network and blade class.

    """
    return [
        int(blade_instance)
        for blade in network.get('connected_blades', [])
        if blade.get('blade_class', None) == blade_class
        for blade_instance in blade.get('blade_instances', [])
    ]


def connected_blade_ipv4s(network, blade_class):
    """Get the list of conencted blade IP addresses for a given
    network and blade class.

    """
    address_family = find_address_family(network, 'AF_INET')
    return [
        ipv4_addr
        for blade in address_family.get('connected_blades', [])
        if blade.get('blade_class', None) == blade_class
        for ipv4_addr in blade.get('addresses', [])
    ]


def connected_blade_macs(network, blade_class):
    """Get the list of conencted blade IP addresses for a given
    network and blade class.

    """
    address_family = find_address_family(network, 'AF_LINK')
    return [
        mac
        for blade in address_family.get('connected_blades', [])
        if blade.get('blade_class', None) == blade_class
        for mac in blade.get('blade_macs', [])
    ]


def network_blade_connected(network, blade_class, blade_instance):
    """Determine whether the specified network is connected to the
    specified instance of the specified blade class. If it is, return
    True otherwise False.

    """
    return blade_instance in connected_blade_instances(network, blade_class)


def network_blade_ipv4(network, blade_class, blade_instance):
    """Return the IPv4 address (if any) of the given instance of the
    given blade class on the given network. If no such address is
    found, return None.

    """
    instances = connected_blade_instances(network, blade_class)
    ipv4s = connected_blade_ipv4s(network, blade_class)
    count = len(instances) if len(instances) <= len(ipv4s) else len(ipv4s)
    candidates = [
        ipv4s[i] for i in range(0, count)
        if blade_instance == instances[i]
    ]
    return candidates[0] if candidates else None


def network_ipv4_gateway(network):
    """Return the network IPv4 gateway address if there is one for the
    specified network. If no gateway is configured, return None.

    """
    return find_address_family(network, 'AF_INET').get('gateway', None)


def is_nat_router(network, blade_class, blade_instance):
    """Determine whether the specified instance of the specified
    virtual blade class is the NAT router for the specified virtual
    network (the NAT router for a virtual network is whatever blade
    hosts the gateway for that network).

    """
    blade_ipv4 = network_blade_ipv4(network, blade_class, blade_instance)
    gateway = network_ipv4_gateway(network)
    return blade_ipv4 and gateway and blade_ipv4 == gateway


def is_dhcp_server(network, blade_class, blade_instance):
    """Determine whether the specified instance of the specified
    virtual blade class is the NAT router for the specified virtual
    network (the NAT router for a virtual network is whatever blade
    hosts the gateway for that network).

    """
    address_family = find_address_family(network, 'AF_INET')
    candidates = [
        blade
        for blade in address_family.get('connected_blades', [])
        if blade.get('blade_class', None) == blade_class and
        blade.get('dhcp_server_instance', None) == blade_instance
    ]
    return len(candidates) > 0


def network_connected(network, node_classes):
    """Determine whether the specified network is connected to an
    interface in any of the specified node classes. If it is return
    True otherwise False.

    """
    interface_connections = [
        interface['cluster_network']
        for node_class in node_classes
        if 'network_interfaces' in node_class
        for _, interface in node_class['network_interfaces'].items()
        if 'cluster_network' in interface
    ]
    return net_name(network) in interface_connections


def node_addrs(network_interface, address_family):
    """Get the list of addresses configured for the named address
    family in the supplied netowrk interface taken from a node class

    """
    try:
        addr_info = network_interface['addr_info']
    except KeyError as err:
        raise ContextualError(
            "cofiguration error: network interface %s has no 'addr_info' "
            "section" % str(network_interface)
        ) from err
    addrs = []
    for _, info in addr_info.items():
        if info.get('family', None) == address_family:
            if addrs:
                raise ContextualError(
                    "configuration error: more than one '%s' addr_info "
                    "block found in "
                    "network interface %s" % (
                        address_family, str(network_interface)
                    )
                )
            addrs += info.get('addresses', [])
    return addrs


def node_mac_addrs(network_interface):
    """Get the list of node MAC addresses from the provided network
    interface information taken from a node class.

    """
    return node_addrs(network_interface, 'AF_PACKET')


def node_ipv4_addrs(network_interface):
    """Get the list of node IPv4 addresses from the provided network
    interface information taken from a node class.

    """
    return node_addrs(network_interface, 'AF_INET')


def find_addr_info(interface, family):
    """Find the address information for the specified address family
    ('family') in the provided node class interface configuration
    ('interface').

    """
    addr_infos = [
        addr_info
        for _, addr_info in interface.get('addr_info', {}).items()
        if addr_info.get('family', None) == family
    ]
    if len(addr_infos) > 1:
        netname = interface['cluster_network']
        raise ContextualError(
            "configuration error: the interface for network '%s' in a "
            "node class has more than one '%s' 'addr_info' block: %s" % (
                netname,
                family,
                str(interface)
            )
        )
    if not addr_infos:
        raise ContextualError(
            "configuration error: the interface for network '%s' in the "
            "node class has no '%s' 'addr_info' block: %s" % (
                netname,
                family,
                str(interface)
            )
        )
    return addr_infos[0]


def find_address_family(network, family):
    """Find the L3 configuration for the specified address family
    ('family') in the provided network configuration ('network').

    """
    netname = net_name(network)
    # There should be exactly one 'address_family' block in the network
    # with the specified family.
    address_families = [
        address_family
        for _, address_family in network.get('address_families', {}).items()
        if address_family.get('family', None) == family
    ]
    if len(address_families) > 1:
        raise ContextualError(
            "configuration error: the Virtual Network named '%s' has more "
            "than one %s 'address_family' block." % (netname, family)
        )
    if not address_families:
        raise ContextualError(
            "configuration error: the Virtual Network named '%s' has "
            "no %s 'address_family' block." % (netname, family)
        )
    return address_families[0]


def network_length(address_family, netname):
    """Given an address_family ('address_family') from a network named
    'netname' return the network length from its 'cidr' element.

    """
    if 'cidr' not in address_family:
        raise ContextualError(
            "configuration error: the AF_INET 'address_family' block for the "
            "network named '%s' has no 'cidr' configured" % netname
        )
    if '/' not in address_family['cidr']:
        raise ContextualError(
            "configuration error: the AF_INET 'cidr' value '%s' for the "
            "network named '%s' is malformed" % (
                address_family['cidr'], netname
            )
        )
    return address_family['cidr'].split('/')[1]


def find_blade_cidr(network, blade_class, blade_instance):
    """Find the IPv4 address/CIDR to use on the network interface for
    a specified network. The 'network' argument describes the network
    of interest, 'blade_class' and 'blade_instance' identify the blade
    we are running on. If there is no IPv4 address for this blade,
    then return None.

    """
    address_family = find_address_family(network, "AF_INET")
    blade_ip = network_blade_ipv4(network, blade_class, blade_instance)
    return (
        '/'.join((blade_ip, network_length(address_family, net_name(network))))
        if blade_ip is not None else None
    )


def network_layer_2_name(network):
    """Get or construct the layer 2 interface name for the network
    configuration found in 'network'.

    """
    network_name = net_name(network)
    return network.get('devices', {}).get('layer_2', network_name)


def network_bridge_name(network):
    """Get or construct the bridge name for the network configuration
    found in 'network'.

    """
    layer_2_name = network_layer_2_name(network)
    return network.get('devices', {}).get(
        'bridge_name', "br-%s" % layer_2_name
    )


def node_connected_networks(node_class, networks):
    """Given a node class and a list of networks return a dictionary
    of networks that are connected to that node class indexed by
    network name.

    """
    if_nets = [
        interface.get('cluster_network', "")
        for _, interface in node_class.get('network_interfaces', {}).items()
    ]
    return {
        net_name(network): network
        for network in networks
        if net_name(network) in if_nets
    }


def instance_range(node_class, blade_instance):
    """Compute a range of Virtual Nodes of a given node class
    ('node_class') that belong on a given Virtual Blade instance
    ('blade_instance') based on the number of Virtual Nodes of that
    class to be deployed and the number of Virtual Nodes of that class
    that fit on each blade. Return the range as a tuple.

    """
    blade_instance = int(blade_instance)  # coerce to int for local use
    node_count = int(node_class.get('node_count'))
    capacity = int(
        node_class
        .get('host_blade', {})
        .get('instance_capacity', 1)
    )
    start = blade_instance * capacity
    start = start if start < node_count else node_count
    end = (blade_instance * capacity) + capacity
    end = end if end < node_count else node_count
    return (start, end)


def open_safe(path, flags):
    """Safely open a file with a mode that only permits reads and
    writes by owner. This is used as an 'opener' in cases where the
    file needs protecting.

    """
    return os.open(path, flags, 0o600)


def install_blade_ssh_keys(key_dir):
    """Copy the blade SSH keys into place from the uploaded key
    directory. The public key is already authorized, so no need to do
    anything to authorized keys.

    """
    priv = 'id_rsa'
    pub = 'id_rsa.pub'
    with open(path_join(key_dir, priv), 'r', encoding='UTF-8') as key_in, \
         open(path_join(os.sep, 'root', '.ssh', priv), 'w',
              encoding='UTF-8', opener=open_safe) as key_out:
        key_out.write(key_in.read())
    with open(path_join(key_dir, pub), 'r', encoding='UTF-8') as key_in, \
         open(path_join(os.sep, 'root', '.ssh', pub), 'w',
              encoding='UTF-8') as key_out:
        key_out.write(key_in.read())


def get_blade_interface_data():
    """Collect the interface data from the blade as a data structure.

    """
    with Popen(
        ['ip', '-d', '--json', 'addr'],
        stdout=PIPE,
        stderr=PIPE
    ) as cmd:
        return json.loads(cmd.stdout.read())


def find_interconnect_interface():
    """Find the interface that connects to the blade interconnect on
    this blade (i.e. not a tunnel, not a bridge, not a peer, but a
    straight up interface and return its name).

    """
    if_data = get_blade_interface_data()
    # Look for interfaces that are 'ether' and not qualified by any other
    # stuff like bridging or tunelling or whatever.
    candidates = [
        interface['ifname']
        for interface in if_data
        if interface.get('link_type', '') == 'ether' and
        interface.get('linkinfo', None) is None
    ]
    if len(candidates) > 1:
        # We should only have one candidate, catch the case where there
        # are more and error out on them. If this is not a valid assumption
        # we may, some day, need to go further with this, but for now it
        # should suffice.
        raise ContextualError(
            "internal error: there appears to be more than one pure ethernet "
            "interface on this Virtual Blade: %s" % str(candidates)
        )
    if not candidates:
        # We should have a candidate. If not there is a problem.
        raise ContextualError(
            "internal error: there does not appear to be any pure ethernet "
            "interface on this Virtual Blade: %s" % str(if_data)
        )
    return candidates[0]


def find_mtu(link_name):
    """Given the name of a network link (interface) return the MTU of
    that link.

    """
    if_data = get_blade_interface_data()
    candidates = [
        link
        for link in if_data
        if link.get('ifname', "") == link_name
    ]
    if not candidates:
        raise ContextualError(
            "internal error: no link named '%s' found in blade interfaces "
            "cannot retrieve the MTU - %s" % (link_name, if_data)
        )
    if len(candidates) > 1:
        raise ContextualError(
            "internal error: more than one link named '%s' found in blade "
            "interfaces cannot discern the MTU - %s" % (
                link_name, str(if_data)
            )
        )
    mtu = candidates[0].get('mtu', None)
    if mtu is None:
        raise ContextualError(
            "internal error: link '%s' has no MTU "
            "specified - %s" % str(candidates[0])
        )
    return mtu


def prepare_nat():
    """Prepare the blade for installation of NAT rules (load kernel
    modules and clear out any stale NAT rules).

    """
    # We are going to add NAT, so Load the canonically necessary
    # kernel modules...
    run_cmd('modprobe', ['ip_tables'])
    run_cmd('modprobe', ['ip_conntrack'])
    run_cmd('modprobe', ['ip_conntrack_irc'])
    run_cmd('modprobe', ['ip_conntrack_ftp'])
    # Clear out any old NAT rules...
    run_cmd('iptables', ['-t', 'nat', '-F'])


def install_nat_rule(network):
    """Run through the networks for which this blade is a DHCP server
    and, if this blade is also the configured gateway for the network,
    add a NAT rule for this network on the blade to masquerade traffic
    from that network to external IPs.

    """
    dest_if = find_interconnect_interface()
    cidr = find_address_family(network, 'AF_INET')['cidr']
    run_cmd(
        'iptables',
        [
            '-t', 'nat', '-A', 'POSTROUTING', '-s', cidr,
            '-o', dest_if, '-j', 'MASQUERADE'
        ]
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
    def __init__(self, node_class, networks, node_instance):
        """Constructor: 'node_class' is the node class configuration
        for the Virtual node, 'networks' is a filtered dictionary
        indexed by network name of the networks that are connected to
        the Virtual Node, 'node_instance' is the instance number
        within its node class of the Virtual Node.

        """
        self.class_name = node_class['class_name']
        self.node_class = node_class
        self.networks = networks
        self.node_instance = node_instance
        self.hostname = self.__compute_hostname()
        self.node_name = self.__compute_node_name()
        self.nodeclass_dir = path_join(
            os.sep, 'var', 'local', 'vtds', self.class_name
        )
        self.host_dir = path_join(self.nodeclass_dir, self.node_name)
        makedirs(self.host_dir, mode=0o755, exist_ok=True)
        self.boot_disk_name = None
        try:
            self.virtual_machine = self.node_class['virtual_machine']
        except KeyError as err:
            raise ContextualError(
                "configuration error: node class '%s' does not define "
                "a 'virtual_machine' section: %s" % (
                    self.class_name, str(self.node_class)
                )
            ) from err
        self.context = self.__compose()

    def __compute_node_name(self):
        """Based on the naming information in the node_class compose a
        node name for this instance of the node_class and return it as
        a string.

        Since all of the magic to make sure node names are set up has
        already been done by the preparation of the config, just use
        the hostnames that are there. No need to be fancy about it.

        """
        return self.node_class['node_naming']['node_names'][self.node_instance]

    def __compute_hostname(self):
        """Based on the naming information in the node_class compose a
        host name for this instance of the node_class and return it as
        a string.

        Since all of the magic to make sure hostnames are set up has
        already been done by the preparation of the config, just use
        the hostnames that are there. No need to be fancy about it.

        """
        return self.node_class['host_naming']['hostnames'][self.node_instance]

    def __node_ipv4(self, network):
        """Get the IPv4 address of this node on the named network if
        this node's instance of its node class has a static or
        reserved address on that network. Otherwise return None.

        """
        interface_candidates = [
            interface
            for _, interface in self.node_class
            .get('network_interfaces', {}).items()
            if interface.get('cluster_network', None) == network
        ]
        if not interface_candidates:
            raise ContextualError(
                "there is no network interface for network '%s' in %s" % (
                    network, self.node_name
                )
            )
        if len(interface_candidates) > 1:
            raise ContextualError(
                "there is more than one network interface for "
                "network '%s' in %s" % (
                    network, self.node_name
                )
            )
        interface = interface_candidates[0]
        addr_info = find_addr_info(interface, 'AF_INET')
        addresses = addr_info.get('addresses', [])
        return (
            addresses[self.node_instance]
            if len(addresses) > self.node_instance
            else None
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
            self.__node_ipv4(network)
            for network, network_settings in self.networks.items()
            if network_settings.get('non_cluster', False)
        ]
        return candidates[0] if candidates else None

    @staticmethod
    def __retrieve_image(url, dest):
        """Retrieve a disk image from a URL ('url') and write it the
        file named in 'dest'.

        """
        # Using curl here instead of writing a requests library
        # operation because it is simpler and just about as fast and
        # the error handling is covered. If the destination file
        # already exists, simply return. If retrieval fails, remove
        # any partial destination file that might have been created.
        if not exists(dest):
            try:
                run_cmd('curl', ['-o', dest, '-s', url])
            except Exception as err:
                if exists(dest):
                    remove_file(dest)
                raise err

    @staticmethod
    # Take this out when we start using partitions
    #
    # pylint: disable=unused-argument
    def __make_disk_image(name, size, source_image_name, partitions):
        """Build a disk image in a file named 'name'. If 'size' is not
        None make sure the resulting disk image is 'size' bytes
        long. If 'source_image_name' is not None, use the source image
        in the named file. If 'partitions' is specified, partition the
        disk accordingly.

        """
        run_cmd(
            'rm',
            ['-f', name]
        )
        # pylint: disable=fixme
        # TODO implement partitioning
        source_options = (
            ['-b', source_image_name, '-F', 'qcow2']
            if source_image_name
            else []
        )
        size_args = ['%sM' % size] if size else []
        run_cmd(
            'qemu-img',
            ['create', *source_options, '-f', 'qcow2', name, *size_args]
        )
        run_cmd(
            'chown',
            ['libvirt-qemu:kvm', name]
        )

    def __make_disk(self, name, disk_config, source_image_name=None):
        """Given an the filename ('name') to store the boot disk file,
        the size in megabytes ('size' -- multiples of 1,000,000) and
        the image URL ('image_url') or a set of partition descriptions
        ('partitions'), construct a disk for use with the Virtual Node
        and return its description.

        """
        image_url = disk_config.get('source_image', None)
        partitions = disk_config.get('partitions', {})
        size = disk_config.get('disk_size_mb', None)
        target_dev = disk_config.get('target_device', None)
        if not target_dev:
            raise ContextualError(
                "configuration error: disk '%s' has no target device "
                "configured: %s" % (name, str(disk_config))
            )
        if partitions and image_url:
            raise ContextualError(
                "configuration error: Virtual Node class '%s' "
                "disk configuration "
                "declares both a non-empty 'source_image' "
                "URL ('%s') and a non-empty partition list, "
                "must choose one or the other: %s" % (
                    self.class_name,
                    image_url,
                    str(disk_config)
                )
            )
        if not image_url and not partitions and not size:
            raise ContextualError(
                "configuration error: Virtual Node class '%s' disk "
                "configuration must declare at "
                "at least one of 'disk_size_mb', 'source_image' "
                "or 'partitions': %s" % (
                    self.class_name,
                    str(disk_config)
                )
            )
        if image_url and not source_image_name:
            raise ContextualError(
                "internal error: no source image name supplied when making "
                "a disk with a  source image URL"
            )
        if image_url:
            self.__retrieve_image(image_url, source_image_name)
        self.__make_disk_image(name, size, source_image_name, partitions)
        return {
            'file_name': name,
            'target_device': target_dev,
        }

    def __make_boot_disk(self):
        """Create a boot disk image for this Virtual Node using the
        information from the Node Class and return the template
        description of the disk.

        """
        try:
            disk_config = self.virtual_machine['boot_disk']
        except KeyError as err:
            raise ContextualError(
                "configuration error: Virtual Node class '%s' "
                "'virtual_machine' section "
                "does not contain a 'boot_disk' "
                "section: %s" % (
                    self.class_name,
                    str(self.node_class)
                )
            ) from err
        self.boot_disk_name = path_join(self.host_dir, "boot_disk.img")
        source_image_name = path_join(
            self.nodeclass_dir, 'boot-img-source.qcow'
        )
        return self.__make_disk(
            self.boot_disk_name, disk_config, source_image_name
        )

    def __make_extra_disks(self):
        """Create all of the extra disk images for this Virtual Node
        using the information in from the Node Class and return the
        template description of the list of disks.

        """
        extra_disks = self.virtual_machine.get('additional_disks', {})
        return [
            self.__make_disk(
                path_join(self.host_dir, "%s.img" % disk_name),
                extra_disk,
                (
                    path_join(self.nodeclass_dir, "%s.qcow" % disk_name)
                    if extra_disk.get('source_image', None) else
                    None
                )
            )
            for disk_name, extra_disk in extra_disks.items()
        ]

    def __configure_netplan(self, context):
        """Given the network interfaces portion of a VM template
        context ('context') configure the netplan on this instance's
        boot disk image to bring up all of the network interfaces as
        configured.

        """
        if not self.boot_disk_name or not exists(self.boot_disk_name):
            raise ContextualError(
                "internal error: __configure_netplan run before the "
                "boot disk image was created"
            )
        netplan = {
            'network': {
                'version': "2",
                'renderer': 'networkd',
                'ethernets': {
                    interface['ifname']: {
                        'addresses': (
                            [
                                '/'.join(
                                    [
                                        interface['ipv4_addr'],
                                        interface['ipv4_netlength']
                                    ]
                                )
                            ]
                            if interface['ipv4_addr'] is not None
                            else []
                        ),
                        'dhcp6': False,
                        'dhcp4': interface['dhcp4'],
                        'mtu': interface['mtu'],
                        'match': {
                            'macaddress': interface['mac_addr']
                        }
                    }
                    for interface in context
                }
            }
        }
        with NamedTemporaryFile(mode='w', encoding='UTF-8') as tmpfile:
            yaml.safe_dump(netplan, tmpfile)
            tmpfile.flush()
            run_cmd(
                'virt-customize',
                [
                    '-a', self.boot_disk_name,
                    '--upload',
                    "%s:/etc/netplan/10-vtds-ethernets.yaml" % tmpfile.name
                ]
            )

    def __reconfigure_ssh(self):
        """Run 'dpkg-recofigure openssh-server' on the root disk so
        that the SSH servers will have host keys. Also, install root
        SSH keys and authorizations.

        """
        run_cmd(
            'virt-customize',
            [
                '-a', self.boot_disk_name,
                '--run-command', 'dpkg-reconfigure openssh-server',
                '--copy-in', '/root/.ssh:/root',
                '--hostname', self.hostname,
            ]
        )

    def __make_network_interface(self, interface, network):
        """Given an interface configuration ('interface') taken from a
        node class and the matching network configuration ('network')
        taken from self.networks, return a context for rendering XML
        and for composing a netplan configuration for a single
        interface in this Virtual Node.

        """
        node_instance = int(self.node_instance)
        netname = interface['cluster_network']
        bridge_name = network_bridge_name(network)
        mtu = find_mtu(bridge_name)
        ipv4_info = find_addr_info(interface, "AF_INET")
        mac_addrs = node_mac_addrs(interface)
        addresses = ipv4_info.get('addresses', [])
        address_family = find_address_family(network, "AF_INET")
        net_length = network_length(address_family, netname)
        try:
            mode = ipv4_info['mode']
        except KeyError as err:
            raise ContextualError(
                "configuration error: AF_INET addr_info in interface "
                "for network '%s' in node class has no 'mode' value: %s" % (
                    netname, str(self.node_class)
                )
            ) from err
        dhcp4 = (
            mode in ['dynamic', 'reserved'] or
            node_instance >= len(addresses)
        )
        ipv4_addr = (
            addresses[self.node_instance]
            if node_instance < len(addresses)
            else None
        )
        ipv4_netlen = (
            net_length
            if node_instance < len(addresses)
            else None
        )
        return {
            'ifname': netname,
            'dhcp4': dhcp4,
            'ipv4_addr': ipv4_addr,
            'ipv4_netlength': ipv4_netlen,
            'name_servers': [
                "",
                ...
            ],
            'netname': netname,
            'source_if': bridge_name,
            'mac_addr': mac_addrs[node_instance],
            'mtu': mtu,
        }

    def __make_network_interfaces(self):
        """Configure the network interfaces on the boot disk image and
        return the template description of the network interfaces.

        """
        context = [
            self.__make_network_interface(
                interface, self.networks[interface['cluster_network']]
            )
            for _, interface in self.node_class.get(
                'network_interfaces', {}
            ).items()
        ]
        self.__configure_netplan(context)
        return context

    def __configure_root_passwd(self):
        """Configure the root password on the boot disk image for the
        Virtual Node.

        """
        if not self.boot_disk_name or not exists(self.boot_disk_name):
            raise ContextualError(
                "internal error: __configure_root_passwd run before the "
                "boot disk image was created"
            )
        # pylint: disable=fixme
        #
        # For now use uuid to generate a root password so that I have
        # something and I can access the Virtual Nodes.
        #
        # TODO: figure out how to do this using a Google Secret so
        #       the user can configure the root password for nodes,
        #       node classes or whatever.
        root_passwd = str(uuid4())
        run_cmd(
            'virt-customize',
            [
                '-a', self.boot_disk_name,
                '--root-password', 'password:%s' % root_passwd,
            ]
        )
        # Toss the root password for the node in a root-owned readable
        # only by owner file so we can use it later.
        filename = "%s-passwd.txt" % self.hostname
        with open(
            filename, mode='w', opener=open_safe, encoding='UTF-8'
        ) as pw_file:
            pw_file.write("%s\n" % root_passwd)

    def __compose(self):
        """Compose the template data that will be used to fill out the
        XML template that will be used to create the Virtual Node
        using 'virsh create <filename>'

        """
        try:
            memsize = str(int(self.virtual_machine['memory_size_mib']) * 1024)
        except KeyError as err:
            raise ContextualError(
                "configuration error: no 'memory_size_mib' found in "
                "Virtual Machine configuration for Virtual Node class '%s': "
                " %s " % (self.class_name, str(self.node_class))
            ) from err
        except ValueError as err:
            raise ContextualError(
                "configuration error: the value of 'memory_size_mib' ('%s') "
                "must be an integer value in Virtual Machine configuration "
                "for Virtual Node class '%s': %s " % (
                    self.virtual_machine['memory_size_mib'],
                    self.class_name,
                    str(self.node_class)
                )
            ) from err
        try:
            cpus = self.virtual_machine['cpu_count']
        except KeyError as err:
            raise ContextualError(
                "configuration error: no 'cpu_count' found in "
                "Virtual Machine configuration for Virtual Node "
                "class '%s': %s " % (self.class_name, str(self.node_class))
            ) from err
        return {
            'hostname': self.node_name,
            'uuid': str(uuid4()),
            'memsize_kib': memsize,
            'cpus': cpus,
            'boot_disk': self.__make_boot_disk(),
            'extra_disks': self.__make_extra_disks(),
            'interfaces': self.__make_network_interfaces(),
        }

    def create(self):
        """Compose an XML definition of the Virtual Node and create
        it on the current blade.

        """
        self.__reconfigure_ssh()
        try:
            vm_template = self.node_class['vm_xml_template']
        except KeyError as err:
            raise ContextualError(
                "internal configuration error: Virtual Node class '%s' does "
                "not have a VM XML template stored in it. This may be some "
                "kind of module version mismatch."
            ) from err
        self.__configure_root_passwd()
        template = Template(vm_template)
        try:
            vm_xml = template.render(**self.context)
        except TemplateError as err:
            raise ContextualError(
                "internal error: error rendering VM XML file from context and "
                "XML template - %s" % str(err)
            ) from err
        with NamedTemporaryFile(mode='w', encoding='UTF-8') as tmpfile:
            tmpfile.write(vm_xml)
            tmpfile.flush()
            run_cmd('virsh', ['define', tmpfile.name])
            run_cmd('virsh', ['start', self.node_name])

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
            node_class,
            node_connected_networks(node_class, networks),
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
