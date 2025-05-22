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
"""Private layer implementation module for the kvm cluster.

"""

from os.path import (
    join as path_join,
    dirname
)
from random import randint
from ipaddress import IPv4Network
from yaml import safe_dump

from vtds_base import (
    ContextualError,
    info_msg,
    expand_inheritance
)
from vtds_base.layers.cluster import (
    ClusterAPI
)

from . import (
    DEPLOY_SCRIPT_PATH,
    DEPLOY_SCRIPT_NAME,
    CLUSTER_SCRIPT_LIBS,
    VM_XML_PATH
)
from .common import Common
from .api_objects import (
    VirtualNodes,
    VirtualNetworks
)


class Cluster(ClusterAPI):
    """Cluster class, implements the kvm cluster layer
    accessed through the python Cluster API.

    """
    def __init__(self, stack, config, build_dir):
        """Constructor, stash the root of the platfform tree and the
        digested and finalized cluster configuration provided by the
        caller that will drive all activities at all layers.

        """
        self.__doc__ = ClusterAPI.__doc__
        self.config = config.get('cluster', None)
        if self.config is None:
            raise ContextualError(
                "no cluster configuration found in top level configuration"
            )
        self.provider_api = None
        self.platform_api = None
        self.stack = stack
        self.build_dir = build_dir
        self.blade_config_path = path_join(
            self.build_dir, 'blade_core_config.yaml'
        )
        self.prepared = False
        self.common = Common(self.config, self.stack, self.build_dir)

    def __add_endpoint_ips(self, network):
        """Go through the list of connected blade classes for a
        network and use the list of endpoint IPs represented by all of
        the blades in each of those classes to compose a comprehensive
        list of endpoint IPs for the overlay network we are going to
        build for the network. Add that list under the 'endpoint_ips'
        key in the network and return the modified network to the
        caller.

        """
        virtual_blades = self.provider_api.get_virtual_blades()
        try:
            interconnect = network['blade_interconnect']
        except KeyError as err:
            raise ContextualError(
                "network configuration '%s' does not specify "
                "'blade_interconnect'" % str(network)
            ) from err
        blade_classes = network.get('connected_blade_classes', None)
        blade_classes = (
            virtual_blades.blade_classes()
            if blade_classes is None
            else blade_classes
        )
        network['endpoint_ips'] = [
            virtual_blades.blade_ip(blade_class, instance, interconnect)
            for blade_class in blade_classes
            for instance in range(0, virtual_blades.blade_count(blade_class))
        ] if interconnect is not None else []
        return network

    @staticmethod
    def __clean_deleted_interfaces(node_class_config):
        """Go through the network interfaces in a node class
        configuration and remove any that have the 'deleted' flag
        set. Return the resulting config.

        """
        net_interfaces = {
            key: interface
            for key, interface in node_class_config.get(
                    'network_interfaces', {}
            ).items()
            if not interface.get('delete', False)
        }
        node_class_config['network_interfaces'] = net_interfaces
        return node_class_config

    @staticmethod
    def __clean_deleted_partitions(disk):
        """Go through any partitions that might be defined on a disk
        and remove any that have been deleted.

        """
        partitions = {
            key: partition
            for key, partition in disk.get('partitions', {}).items()
            if not partition.get('delete', False)
        }
        disk['partitions'] = partitions
        return disk

    def __clean_deleted_disks(self, node_class_config):
        """Go through the additional disks in a node class
        configuration and remove any that have the 'deleted' flag
        set. Return the resulting config.

        """
        virtual_machine = node_class_config.get('virtual_machine', {})
        additional_disks = {
            key: self.__clean_deleted_partitions(disk)
            for key, disk in virtual_machine.get(
                    'additional_disks', {}
            ).items()
            if not disk.get('delete', False)
        }
        virtual_machine['additional_disks'] = additional_disks
        node_class_config['virtual_machine'] = virtual_machine
        return node_class_config

    @staticmethod
    def __get_node_classes(config):
        """Extract the node classes section from a cluster config and
        return it.

        """
        node_classes = (
            config.get('node_classes', None)
        )
        if node_classes is None:
            raise ContextualError(
                "configuration error - cluster configuration has no "
                "'node_classes' defined: %s" % (str(config))
            )
        return node_classes

    @staticmethod
    def __net_name(network):
        """Return the network name of a network and error if there is
        none.

        """
        netname = network.get('network_name', None)
        if netname is None:
            raise ContextualError(
                "configuration error: network has no network name: %s" %
                str(network)
            )
        return netname

    def __get_address_family(self, network, family):
        """Look up the L3 configuration for the specified address
        family in the specified network.

        """
        address_families = network.get('address_families', None)
        if address_families is None:
            raise ContextualError(
                "configuration error: network '%s' has no "
                "'address_families' section" % self.__net_name(network)
            )
        candidates = [
            address_family
            for _, address_family in address_families.items()
            if address_family.get('family', None) == family
        ]
        if not candidates:
            raise ContextualError(
                "configuration error: network '%s' has no "
                "%s L3 configuration" % (self.__net_name(network), family)
            )
        if len(candidates) > 1:
            raise ContextualError(
                "configuration error: network '%s' has more than one "
                "%s L3 configuration" % (self.__net_name(network), family)
            )
        return candidates[0]

    def __get_ipv4_cidr(self, network):
        """Return the IPv4 CIDR for the specified network.  Error if
        there is none.

        """
        address_family = self.__get_address_family(network, 'AF_INET')
        cidr = address_family.get('cidr', None)
        if cidr is None:
            raise ContextualError(
                "configuration error: AF_INET L3 configuration for "
                "network '%s' has no 'cidr' specified" %
                self.__net_name(network)
            )
        return cidr

    def __cluster_node_count(self):
        """Return the total number of Virtual Nodes in the cluster

        """
        node_classes = self.__get_node_classes(self.config)
        counts = [
            int(node_class.get('node_count', 0))
            for _, node_class in node_classes.items()
        ]
        return sum(counts)

    def __add_host_blade_net(self, config):
        """Merge the blade host networks into the config making sure
        every Virtual Node instance is connected to a blade host
        network and has a static IP address, and making sure that each
        Virtual Blade is connected to its blade host network and has
        an IP address.

        """
        virtual_blades = self.provider_api.get_virtual_blades()
        node_classes = config.get('node_classes', {})
        networks = config.get('networks', {})
        host_blade_network = config.get('host_blade_network', None)
        netname = self.__net_name(host_blade_network)
        hosts = [
            *IPv4Network(self.__get_ipv4_cidr(host_blade_network)).hosts()
        ][:self.__cluster_node_count() + 1]
        hosts.reverse()  # We are going to pop from this list, so reverse it
        # The blade IP for every conencted blade on the host blade
        # network is the same. It is the '.1' IP in the CIDR block for
        # that network.
        blade_ip = str(hosts.pop())
        if host_blade_network is None:
            raise ContextualError(
                "configuration error: no 'host_blade_network' defined in "
                "the cluster configuration"
            )
        # Connect the host_blade_network to all blades of all classes.
        blade_classes = virtual_blades.blade_classes()
        host_blade_network['connected_blades'] = [
            {
                'blade_class': blade_class,
                'blade_instances': [
                    *range(0, virtual_blades.blade_count(blade_class))
                ],
            }
            for blade_class in blade_classes
        ]
        address_family = self.__get_address_family(
            host_blade_network, 'AF_INET'
        )
        address_family['connected_blades'] = [
            {
                # All blade IPs are the '.1' address of the
                # network. We need one copy of that value per blade
                # instance.
                'blade_class': blade_class,
                'addresses': [blade_ip] * virtual_blades.blade_count(
                    blade_class
                ),
                'dhcp_server_instance': None,
            }
            for blade_class in blade_classes
        ]
        # Add the host blade network to the set of Virtual Networks so
        # it will be used.
        networks[netname] = host_blade_network
        # Connect all the Virtual Node classes of all classes to the
        # host_blade_network
        for _, node_class in node_classes.items():
            if node_class.get('pure_base_class', False):
                # Skip inheritance and installation for pure base
                # classes since they have no parents, and they aren't
                # used for deployment.
                continue
            host_blade_interface = {
                'delete': False,
                'cluster_network': netname,
                'addr_info': {
                    'ipv4': {
                        'family': 'AF_INET',
                        'mode': 'static',
                        'addresses': [
                            str(hosts.pop())
                            for i in range(
                                0, int(node_class.get('node_count', 0))
                            )
                        ],
                        'hostname_suffix': '-host-blade'
                    }
                }
            }
            node_class['network_interfaces'][netname] = host_blade_interface
        config['networks'] = networks
        config['node_classes'] = node_classes

    def __cache_connected_blades(self, config):
        """For each network in the configuration comb through the IPv4
        address families and find out the list of connected blade types
        and instances for that network. Add that information at the top
        level of the network (<network>.connected_blades) to give
        the deployment script easier access to it.

        """
        for _, network in config['networks'].items():
            ipv4 = self.__get_address_family(network, 'AF_INET')
            network['connected_blades'] = [
                {
                    'blade_class': connected_blade['blade_class'],
                    'blade_instances': list(
                        range(0, len(connected_blade.get('addresses', [])))
                    ),
                }
                for connected_blade in ipv4.get('connected_blades', [])
            ]

    def __expand_node_classes(self, blade_config):
        """Expand the node class inheritance tree found in the
        provided blade_config data and replace the node classes found
        there with their expanded versions.

        """
        node_classes = self.__get_node_classes(blade_config)
        for key, node_class in node_classes.items():
            # Expand the inheritance tree for Virtual Node classes and put
            # the expanded result back into the configuration. That way,
            # when we write out the configuration we have the full
            # expansion there.
            if node_class.get('pure_base_class', False):
                # Skip inheritance and installation for pure base
                # classes since they have no parents, and they aren't
                # used for deployment.
                continue
            expanded_config = expand_inheritance(node_classes, key)
            expanded_config = self.__clean_deleted_interfaces(expanded_config)
            expanded_config = self.__clean_deleted_disks(expanded_config)
            node_classes[key] = expanded_config

    @staticmethod
    def __random_mac(prefix=None):
        """Generate a MAC address using a specified prefix specified
        as a string containing colon separated hexadecimal octet
        values for the length of the desired prefix. By default use
        the KVM reserved, locally administered, unicast prefix
        '52:54:00'.

        """
        prefix = prefix if prefix is not None else "52:54:00"
        try:
            prefix_octets = [
                int(octet, base=16) for octet in prefix.split(':')
            ]
        except Exception as err:
            raise ContextualError(
                "internal error: parsing MAC prefix '%s' failed - %s" % (
                    prefix, str(err)
                )
            ) from err
        if len(prefix_octets) > 6:
            raise ContextualError(
                "internal error: MAC address prefix '%s' has too "
                "many octets" % prefix
            )
        mac_binary = prefix_octets + [
            randint(0x00, 0xff) for i in range(0, 6 - len(prefix_octets))
        ]
        return ":".join(["%2.2x" % octet for octet in mac_binary])

    @staticmethod
    def __find_address_family(addr_info, family):
        """Return a key / value pair identifying the information for
        the specified address family in the addr_info dictionary
        provided.

        """
        candidates = [
            (key, family)
            for key, addr in addr_info.items()
            if addr.get('family', None) == family
        ]
        if len(candidates) > 1:
            raise ContextualError(
                "address information block: %s has more than one '%s' "
                "address block in it" % (str(addr_info), family)
            )
        return candidates[0] if candidates else (None, None)

    def __add_mac_addresses(self, node_class, prefix=None):
        """Compute MAC address for every Virtual Node interface and
        overlay an AF_PACKET entry list with an updated list of MAC
        addresses in it. If there is already an AF_PACKET entry block
        present, then just make sure there are enough MAC addresses in
        it, and supplement as needed.

        """
        node_count = int(node_class.get('node_count', 0))
        interfaces = node_class.get('network_interfaces', {})
        for if_key, interface in interfaces.items():
            addr_info = interface.get('addr_info', {})
            af_key, addr = self.__find_address_family(addr_info, 'AF_PACKET')
            if af_key is None:
                af_key = 'layer_2'
            if addr is None:
                addr = {
                    'family': 'AF_PACKET',
                    'addresses': [],
                }
            existing_macs = addr.get('addresses', [])[0:node_count]
            existing_count = len(existing_macs)
            addr['addresses'] = existing_macs + [
                self.__random_mac(prefix)
                for i in range(0, node_count - existing_count)
            ]
            addr_info[af_key] = addr
            interface['addr_info'] = addr_info
            # While there is no reason to believe that adjusting 'interface'
            # made it into a new object, it doesn't hurt to be explicit here
            # and put the modified 'interface' back into interfaces.
            interfaces[if_key] = interface

    def __add_xml_template(self, node_class):
        """Add the contents of the libvirt XML template for
        configuring a node class to the node class. This is done on a
        per-node class basis because it will be more flexible in the
        long run. For now it is the same data in every node class,
        which is a bit wasteful, but no big deal.

        """
        with open(VM_XML_PATH, 'r', encoding='UTF-8') as xml_template:
            node_class['vm_xml_template'] = xml_template.read()

    def __set_node_mac_addresses(self, blade_config):
        """Compute and inject MAC addresses for every Virtual Node
        interface in all of the node classes.

        """
        node_classes = self.__get_node_classes(blade_config)
        for _, node_class in node_classes.items():
            self.__add_mac_addresses(node_class)

    @classmethod
    def __underlay_mac_addrs(cls, addrs, length, prefix=None):
        """Fill in each missing (i.e. None or unpopulated) entry in
        'addrs' (up to length entries) with a generated MAC address
        and return the resulting list.

        """
        return [
            addrs[i]
            if i < len(addrs) and addrs[i] is not None
            else cls.__random_mac(prefix)
            for i in range(0, length)
        ]

    def __set_connected_blade_macs(self, network, prefix=None):
        """Compute and inject MAC addresses for the connected blades on a
        given virtual network as part of a newly generated 'AF_PACKET'
        address family within the network.

        """
        # Get a map of blade classes to their corresponding blade
        # instance lists to use since that is less cumbersome than the
        # connected_blades list.
        blade_map = {
            blade['blade_class']: blade['blade_instances']
            for blade in network.get('connected_blades', [])
            if 'blade_class' in blade and 'blade_instances' in blade
        }
        # Find the AF_PACKET address family (if there is one) in the
        # network so we can underlay whatever it has with the MAC
        # addresses we are going to generate.
        address_families = network.get('address_families', {})
        packet_families = [
            (key, family)
            for key, family in address_families.items()
            if family.get('family', None) == 'AF_PACKET'
        ]
        if len(packet_families) > 1:
            network_name = network.get('network_name', "<unspecified name>")
            raise ContextualError(
                "Virtual Network '%s' is configured with more than one "
                "address family using AF_PACKET" % network_name
            )
        key, family = (
            packet_families[0] if len(packet_families) > 0
            else (
                'link',
                {
                    'family': 'AF_PACKET',
                    'connected_blades': [],
                }
            )
        )
        # Now get a map of connected blade MAC address lists whose
        # blade_classes match a connected blade in the other map
        # indexed blade_class from what is already configured in the
        # address family (if any)
        family_populated_map = {
            blade['blade_class']: blade
            for blade in family.get('connected_blades', [])
            if 'blade_class' in blade and blade['blade_class'] in blade_map
        }
        # Now, go through what we found in the address family (if
        # anything) and underlay it with generated MAC addresses. This
        # will leave only unpopulated connected blades in the
        # 'blade_map'...
        populated_classes = [
            {
                'blade_class': blade_class,
                'addresses': self.__underlay_mac_addrs(
                    blade.get('addresses', []),
                    len(blade_map.pop(blade_class)),
                    prefix
                )
            }
            for blade_class, blade in family_populated_map.items()
        ]
        # ... and make up a list of newly populated connected blade classes.
        new_classes = [
            {
                'blade_class': blade_class,
                'addresses': self.__underlay_mac_addrs([], len(instances))
            }
            for blade_class, instances in blade_map.items()
        ]
        family['connected_blades'] = populated_classes + new_classes
        # Put it all back just to be sure we are getting it right. If
        # there was nothing there before, this will make sure it is
        # there now.
        address_families[key] = family
        network['address_families'] = address_families

    def __set_all_connected_blade_macs(
            self, blade_config, prefix=None
    ):
        """Compute and inject MAC addresses for connected blades on
        each of the virtual networks as part of a, possibly newly
        generated, 'AF_PACKET' address family within each network. The
        MAC addresses are underlaid, so, if the configuration already
        contains some or all of them pre-configured, only the missing
        ones will be filled in.

        """
        networks = blade_config.get('networks', {})
        for _, network in networks.items():
            self.__set_connected_blade_macs(network, prefix)

    def consolidate(self):
        self.provider_api = self.stack.get_provider_api()
        self.platform_api = self.stack.get_platform_api()
        self.__expand_node_classes(self.config)
        self.__add_host_blade_net(self.config)
        self.__cache_connected_blades(self.config)
        self.__set_node_mac_addresses(self.config)
        self.__set_all_connected_blade_macs(self.config)
        networks = self.config.get('networks', {})
        updated_config = self.config
        updated_config['networks'] = {
            key: self.__add_endpoint_ips(network)
            for key, network in networks.items()
            if not network.get('delete', False)
        }
        self.config = updated_config

    def prepare(self):
        blade_config = self.config
        for _, node_class in self.__get_node_classes(blade_config).items():
            self.__add_xml_template(node_class)
        with open(self.blade_config_path, 'w', encoding='UTF-8') as conf:
            safe_dump(blade_config, stream=conf)
        self.prepared = True

    def validate(self):
        if not self.prepared:
            raise ContextualError(
                "cannot validate an unprepared cluster, call prepare() first"
            )
        print("Validating vtds-cluster-kvm")

    def deploy(self):
        if not self.prepared:
            raise ContextualError(
                "cannot deploy an unprepared cluster, call prepare() first"
            )
        # Open up connections to all of the vTDS Virtual Blades so I can
        # reach SSH (port 22) on each of them to copy in files and run
        # the deployment script.
        virtual_blades = self.provider_api.get_virtual_blades()
        with virtual_blades.ssh_connect_blades() as connections:
            # Copy the blade SSH keys out to the virtual blades so we
            # can use them. Since each virtual blade class may have
            # its own SSH key, we need to do this one at a time. It
            # should be quick though.
            info_msg("copying SSH keys to the blades")
            for connection in connections.list_connections():
                blade_class = connection.blade_class()
                _, priv_path = virtual_blades.blade_ssh_key_paths(blade_class)
                key_dir = dirname(priv_path)
                connection.copy_to(
                    key_dir, '/root/ssh_keys',
                    recurse=True, logname='copy-ssh-keys-to'
                )
            info_msg(
                "copying '%s' to all Virtual Blades at "
                "'/root/blade_cluster_config.yaml'" % (
                    self.blade_config_path
                )
            )
            connections.copy_to(
                self.blade_config_path, "/root/blade_cluster_config.yaml",
                recurse=False, logname="upload-cluster-config-to"
            )
            info_msg(
                "copying '%s' to all Virtual Blades at '/root/%s'" % (
                    DEPLOY_SCRIPT_PATH, DEPLOY_SCRIPT_NAME
                )
            )
            for (source, dest, name) in CLUSTER_SCRIPT_LIBS:
                connections.copy_to(
                    source, dest, False, "upload-%s-library-to" % name
                )
            connections.copy_to(
                DEPLOY_SCRIPT_PATH, "/root/%s" % DEPLOY_SCRIPT_NAME,
                False, "upload-cluster-deploy-script-to"
            )
            python3 = self.platform_api.get_blade_python_executable()
            cmd = (
                "chmod 755 ./%s;" % DEPLOY_SCRIPT_NAME +
                "%s " % python3 +
                "./%s {{ blade_class }} {{ instance }} " % DEPLOY_SCRIPT_NAME +
                "blade_cluster_config.yaml "
                "/root/ssh_keys"
            )
            info_msg("running '%s' on all Virtual Blades" % cmd)
            connections.run_command(cmd, "run-cluster-deploy-script-on")

    def remove(self):
        if not self.prepared:
            raise ContextualError(
                "cannot remove an unprepared cluster, call prepare() first"
            )

    def get_virtual_nodes(self):
        return VirtualNodes(self.common)

    def get_virtual_networks(self):
        return VirtualNetworks(self.common)
