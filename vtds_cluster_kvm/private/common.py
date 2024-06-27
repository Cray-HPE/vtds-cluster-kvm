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
"""A class that provides common tools based on configuration
and so forth that relate to the GCP vTDS provider.

"""
from vtds_base import (
    ContextualError
)


class Common:
    """A class that provides common tools based on configuration and
    so forth that relate to the vTDS KVM Cluster.

    """
    def __init__(self, config, stack, build_dir):
        """Constructor.

        """
        self.config = config
        self.stack = stack
        self.build_directory = build_dir

    def __get_node(self, node_type):
        """class private: retrieve the node type deascription for the
        named type.

        """
        virtual_nodes = (
            self.get('node_classes', {})
        )
        node = virtual_nodes.get(node_type, None)
        if node is None:
            raise ContextualError(
                "cannot find the virtual node type '%s'" % node_type
            )
        if node.get('pure_base_class', False):
            raise ContextualError(
                "node type '%s' is a pure pure base class" % node_type
            )
        return node

    def __get_node_interface(self, node_type, network_name):
        """class private: Get the named virtual network interface
        information from the specified Virtual Node type. Return None
        if there is no such interface.

        """
        node = self.__get_node(node_type)
        network_interfaces = node.get('network_interfaces', None)
        if network_interfaces is None:
            raise ContextualError(
                "provider config error: Virtual Node type '%s' has no "
                "Virtual Network interfaces configured" % node_type
            )
        candidates = [
            network_interface
            for _, network_interface in network_interfaces.items()
            if not network_interface.get("delete", False)
            and network_interface.get('cluster_network', None) == network_name
        ]
        if len(candidates) > 1:
            raise ContextualError(
                "virtual node class '%s' defines more than one network "
                "interface connected to "
                "network '%s'" % (node_type, network_name)
            )
        return candidates[0] if candidates else None

    def __check_node_instance(self, node_type, instance):
        """class private: Ensure that the specified instance number
        for a given blade type (blades) is legal.

        """
        if not isinstance(instance, int):
            raise ContextualError(
                "Virtual Blade instance number must be integer not '%s'" %
                type(instance)
            )
        node = self.__get_node(node_type)
        count = int(node.get('count', 0))
        if instance < 0 or instance >= count:
            raise ContextualError(
                "instance number %d out of range for Virtual Node "
                "type '%s' which has a count of %d" %
                (instance, node_type, count)
            )

    def __addr_info(self, node_type, network_name, family):
        """Search the 'addr_info' blocks in a network interface
        configuration structure and return the one with the specified
        address family (e.g. 'AF_INET'). Return an empty dictionary if
        no match is found.

        """
        network_interface = self.__get_node_interface(node_type, network_name)
        addr_info = network_interface.get('addr_info', {})
        candidates = [
            info
            for info in addr_info
            if info.get('family', None) == family
        ]
        if len(candidates > 1):
            raise ContextualError(
                "network interface for virtual network '%s' in virtual node "
                "type '%s' has more than one addr_info block for the '%s' "
                "address family" % (network_name, node_type, family)
            )
        return candidates[0] if candidates else {}

    def __hostname_net_suffix(self, node_type, network_name):
        """Get the configured host name network suffix (if any) for
        the spefified node_type and named network. If no suffix is
        configured or if the network name is None, return an empty
        string.

        """
        addr_info = self.__addr_info(node_type, network_name, 'AF_INET')
        return addr_info.get("hostname_suffix", "")

    def __host_blade_class(self, node_type):
        """Determine the blade class hosting instances of the
        specified Virtual Node class.

        """
        node = self.__get_node(node_type)
        host_blade_class = node.get('host_blade', {}).get('blade_class', None)
        if host_blade_class is None:
            raise ContextualError(
                "unable to find the host blade class for "
                "node class '%s'" % node_type
            )
        return host_blade_class

    def get_config(self):
        """Get the full config data stored here.

        """
        return self.config

    def get(self, key, default):
        """Perform a 'get' operation on the top level 'config' object
        returning the value of 'default' if 'key' is not found.

        """
        return self.config.get(key, default)

    def build_dir(self):
        """Return the 'build_dir' provided at creation.

        """
        return self.build_directory

    def node_hostname(self, node_type, instance, network_name=None):
        """Get the hostname of a given instance of the specified type
        of Virtual Node on the specified network. If the network is
        None or unspecified, return just the computed hostname with no
        local network suffix.

        """
        self.__check_node_instance(node_type, instance)
        node = self.__get_node(node_type)
        try:
            node_naming = node['node_naming']
        except KeyError as err:
            raise ContextualError(
                "virtual node class '%s' has no 'node_naming' "
                "section." % node_type
            ) from err
        try:
            base_name = node_naming['base_name']
        except KeyError as err:
            raise ContextualError(
                "virtual node class '%s' has no 'base_name' in its "
                "'node_naming' section" % node_type
            ) from err
        node_names = node_naming.get('node_names', [])
        return (
            node_names[instance]
            if instance < len(node_names)
            else "%s-%3.3d" % (base_name, instance + 1)
        ) + self.__hostname_net_suffix(node_type, network_name)

    def node_count(self, node_type):
        """Get the number of Virtual Blade instances of the specified
        type.

        """
        node = self.__get_node(node_type)
        return int(node.get('count', 0))

    def node_networks(self, node_type):
        """Return the list of names of Virtual Networks connected to
        nodes of the specified type.

        """
        node = self.__get_node(node_type)
        return [
            network_interface.get['cluster_network']
            for _, network_interface in node.get('network_interfaces', {})
            if not network_interface.get('delete', False)
            and 'cluster_network' in network_interface
        ]

    def node_ssh_key_secret(self, node_type):
        """Return the name of the secret used to store the SSH key
        pair used to reach nodes of the specified type through a
        tunneled SSH connection.

        """
        # In the KVM Cluster SSH keys  for Virtual Nodes are the
        # same as for the Virtual Blades that host them. So, find out
        # what class of blade hosts the named Virtual Node class and
        # then get the blade SSH key secret name from there.
        host_blade_class = self.__host_blade_class(node_type)
        virtual_blades = self.stack.get_provider_api().get_virtual_blades()
        return virtual_blades.blade_ssh_key_secret(host_blade_class)

    def ssh_key_paths(self, node_type):
        """Return a tuple of paths to files containing the public and
        private SSH keys used to to authenticate with Virtual Nodes of the
        specified node class. The tuple is in the form '(public_path,
        private_path)' The value of 'private_path' is suitable for use
        with the '-i' option of 'ssh'. If 'ignore_missing' is set, to
        True, the path names will be generated, but no check will be
        done to verify that the files exist. By default, or if
        'ignore_missing' is set to False, this function will verify
        that the files can be opened for reading and raise a
        ContextualError if they cannot.

        """
        # In the KVM Cluster SSH keys for Virtual Nodes are the
        # same as for the Virtual Blades that host them. So, find out
        # what class of blade hosts the named Virtual Node class and
        # then get the blade SSH key path from there.
        host_blade_class = self.__host_blade_class(node_type)
        virtual_blades = self.stack.get_provider_api().get_virtual_blades()
        return virtual_blades.blade_ssh_key_paths(host_blade_class)

    def node_host_blade(self, node_type, instance):
        """Get a tuple containing the the blade class and instance
        number of the Virtual Blade that hosts the Virtual Node
        instance 'instance' of the given node type.

        """
        if instance < 0:
            raise ContextualError(
                "internal error: requesting the node host blade for a "
                "negative instance number (%d) of node class '%s'" % (
                    instance, node_type
                )
            )
        host_blade_class = self.__host_blade_class(node_type)
        instance_capacity = int(
            host_blade_class.get('host_blade', {}).get('instance_capacity', 1)
        )
        return (host_blade_class, instance / instance_capacity)
