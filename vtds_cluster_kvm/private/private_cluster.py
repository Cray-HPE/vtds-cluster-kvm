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
"""Private layer implementation module for the kvm cluster.

"""

from os.path import (
    join as path_join,
    dirname
)
from random import randint
from yaml import safe_dump

from vtds_base import (
    ContextualError,
    info_msg,
    expand_inheritance
)

from . import (
    DEPLOY_SCRIPT_PATH,
    DEPLOY_SCRIPT_NAME,
    VM_XML_PATH
)


class PrivateCluster:
    """PrivateCluster class, implements the kvm cluster layer
    accessed through the python Cluster API.

    """
    def __init__(self, stack, config, build_dir):
        """Constructor, stash the root of the platfform tree and the
        digested and finalized cluster configuration provided by the
        caller that will drive all activities at all layers.

        """
        self.config = config
        self.provider_api = None
        self.stack = stack
        self.build_dir = build_dir
        self.blade_config_path = path_join(
            self.build_dir, 'blade_core_config.yaml'
        )
        self.prepared = False

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
            virtual_blades.blade_types()
            if blade_classes is None
            else blade_classes
        )
        network['endpoint_ips'] = [
            virtual_blades.blade_ip(blade_class, instance, interconnect)
            for blade_class in blade_classes
            for instance in range(0, virtual_blades.blade_count(blade_class))
        ]
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
    def __random_mac(prefix="52:54:00"):
        """Generate a MAC address using a specified prefix specified
        as a string containing colon separated hexadecimal octet
        values for the length of the desired prefix. By default use
        the KVM reserved prefix '52:54:00'.

        """
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

    def __add_mac_addresses(self, node_class):
        """Compute MAC address for every Virtual Node interface and
        overlay an 'addr_info.layer_2' that has AF_PACKET as its
        address family, and a list of MAC addresses in it. If that
        block already exists, then just make sure there are enough MAC
        addresses in it, and supplement as needed.

        """
        node_count = node_class.get('node_count', 0)
        interfaces = node_class.get('network_interfaces', {})
        for key, interface in interfaces.items():
            layer_2 = interface.get(
                "layer_2",
                {
                    'family': 'AF_PACKET',
                    'configuration': {
                        'addresses': []
                    }
                }
            )
            existing_macs = (
                layer_2
                .get('configuration')
                .get('addresses', [])[0:node_count]
            )
            existing_count = len(existing_macs)
            layer_2['configuration']['addresses'] = existing_macs + [
                self.__random_mac()
                for i in range(0, node_count - existing_count)
            ]
            interface['addr_info'] = (
                interface['addr_info'] if 'addr_info' in interface else
                {}
            )
            interface['addr_info']['layer_2'] = layer_2
            interfaces[key] = interface

    def __add_xml_template(self, node_class):
        """Add the contents of the libvirt XML template for
        configuring a node class to the node class. This is done on a
        per-node class basis because it will be more flexible in the
        long run. For now it is the same data in ever node class,
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

    def prepare(self):
        """Prepare operation. This drives creation of the cluster
        layer definition and any configuration that need to be driven
        down into the cluster layer to be ready for deployment.

        """
        self.provider_api = self.stack.get_provider_api()
        blade_config = self.config
        self.__expand_node_classes(blade_config)
        self.__set_node_mac_addresses(blade_config)
        networks = self.config.get('networks', {})
        blade_config['networks'] = {
            key: self.__add_endpoint_ips(network)
            for key, network in networks.items()
            if not network.get('delete', False)
        }
        for _, node_class in self.__get_node_classes(blade_config).items():
            self.__add_xml_template(node_class)
        with open(self.blade_config_path, 'w', encoding='UTF-8') as conf:
            safe_dump(blade_config, stream=conf)
        self.prepared = True

    def validate(self):
        """Run the terragrunt plan operation on a prepared kvm
        cluster layer to make sure that the configuration produces a
        useful result.

        """
        if not self.prepared:
            raise ContextualError(
                "cannot validate an unprepared cluster, call prepare() first"
            )
        print("Validating vtds-cluster-kvm")

    def deploy(self):
        """Deploy operation. This drives the deployment of cluster
        layer resources based on the layer definition. It can only be
        called after the prepare operation (prepare()) completes.

        """
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
                blade_type = connection.blade_type()
                _, priv_path = virtual_blades.blade_ssh_key_paths(blade_type)
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
            connections.copy_to(
                DEPLOY_SCRIPT_PATH, "/root/%s" % DEPLOY_SCRIPT_NAME,
                False, "upload-cluster-deploy-script-to"
            )
            cmd = (
                "chmod 755 ./%s;" % DEPLOY_SCRIPT_NAME +
                "python3 " +
                "./%s {{ blade_type }} {{ instance }} " % DEPLOY_SCRIPT_NAME +
                "blade_cluster_config.yaml "
                "/root/ssh_keys"
            )
            info_msg("running '%s' on all Virtual Blades" % cmd)
            connections.run_command(cmd, "run-cluster-deploy-script-on")

    def remove(self):
        """Remove operation. This will remove all resources
        provisioned for the cluster layer.

        """
        if not self.prepared:
            raise ContextualError(
                "cannot remove an unprepared cluster, call prepare() first"
            )
