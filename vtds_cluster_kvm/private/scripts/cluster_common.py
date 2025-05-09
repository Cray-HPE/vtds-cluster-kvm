#! python
#
# MIT License
#
# (C) Copyright [2025] Hewlett Packard Enterprise Development LP
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
"""Common routines and classes used to support cluster deployment.

"""
import os
from os.path import join as path_join
import sys
from subprocess import (
    Popen,
    TimeoutExpired,
    PIPE
)
import json
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
    """Get the IPv4 CIDR of the interface on the blade where DHCP is
    served for this interface (if any) and return it. Return None if
    there is nothing configured for that.

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


def node_ipv4(node_class, node_instance, network):
    """Get the IPv4 address of this node on the named network if
       this node's instance of its node class has a static or
       reserved address on that network. Otherwise return None.

        """
    node_name = compute_node_name(node_class, node_instance)
    interface_candidates = [
        interface
        for _, interface in node_class
        .get('network_interfaces', {}).items()
        if interface.get('cluster_network', None) == network
    ]
    if not interface_candidates:
        raise ContextualError(
            "there is no network interface for network '%s' in %s" % (
                network, node_name
            )
        )
    if len(interface_candidates) > 1:
        raise ContextualError(
            "there is more than one network interface for "
            "network '%s' in %s" % (
                network, node_name
            )
        )
    interface = interface_candidates[0]
    addr_info = find_addr_info(interface, 'AF_INET')
    addresses = addr_info.get('addresses', [])
    return (
        addresses[node_instance]
        if len(addresses) > node_instance
        else None
    )


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
        for _, network in networks.items()
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
    ssh_dir = path_join(os.sep, 'root', '.ssh')
    with open(path_join(key_dir, priv), 'r', encoding='UTF-8') as key_in, \
         open(path_join(ssh_dir, priv), 'w',
              encoding='UTF-8', opener=open_safe) as key_out:
        key_out.write(key_in.read())
    with open(path_join(key_dir, pub), 'r', encoding='UTF-8') as key_in, \
         open(path_join(ssh_dir, pub), 'w', encoding='UTF-8') as key_out:
        key_out.write(key_in.read())
    # Remove the known_hosts file, since it shouldn't have anything
    # useful in it anyway and it is going to have the wrong keys for
    # the Virtual Nodes if it has anything.
    run_cmd('rm', ['-f', path_join(ssh_dir, 'known_hosts')])


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


def compute_node_name(node_class, node_instance):
    """Based on the naming information in the node_class compose a
       node name for this instance of the node_class and return it as
       a string.

       Since all of the magic to make sure node names are set up has
       already been done by the preparation of the config, just use
       the hostnames that are there. No need to be fancy about it.

    """
    return node_class['node_naming']['node_names'][node_instance]


def compute_hostname(node_class, node_instance):
    """Based on the naming information in the node_class compose a
       host name for this instance of the node_class and return it as
       a string.

       Since all of the magic to make sure hostnames are set up has
       already been done by the preparation of the config, just use
       the hostnames that are there. No need to be fancy about it.

    """
    return node_class['host_naming']['hostnames'][node_instance]
