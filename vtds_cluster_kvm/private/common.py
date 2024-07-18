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

    def __get_node(self, node_class):
        """class private: retrieve the node class deascription for the
        named class.

        """
        virtual_nodes = (
            self.get('node_classes', {})
        )
        node = virtual_nodes.get(node_class, None)
        if node is None:
            raise ContextualError(
                "cannot find the virtual node class '%s'" % node_class
            )
        if node.get('pure_base_class', False):
            raise ContextualError(
                "node class '%s' is a pure pure base class" % node_class
            )
        return node

    def __get_node_interface(self, node_class, network_name):
        """class private: Get the named virtual network interface
        information from the specified Virtual Node class. Return None
        if there is no such interface.

        """
        node = self.__get_node(node_class)
        network_interfaces = node.get('network_interfaces', None)
        if network_interfaces is None:
            raise ContextualError(
                "provider config error: Virtual Node class '%s' has no "
                "Virtual Network interfaces configured" % node_class
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
                "network '%s'" % (node_class, network_name)
            )
        return candidates[0] if candidates else None

    def __check_node_instance(self, node_class, instance):
        """class private: Ensure that the specified instance number
        for a given blade class (blades) is legal.

        """
        if not isinstance(instance, int):
            raise ContextualError(
                "Virtual Node instance number must be integer not '%s'" %
                type(instance)
            )
        node = self.__get_node(node_class)
        count = int(node.get('node_count', 0))
        if instance < 0 or instance >= count:
            raise ContextualError(
                "instance number %d out of range for Virtual Node "
                "class '%s' which has a count of %d" %
                (instance, node_class, count)
            )

    def __addr_info(self, node_class, network_name, family):
        """Search the 'addr_info' blocks in a network interface
        configuration structure and return the one with the specified
        address family (e.g. 'AF_INET'). Return an empty dictionary if
        no match is found.

        """
        network_interface = self.__get_node_interface(node_class, network_name)
        addr_info = network_interface.get('addr_info', {})
        candidates = [
            info
            for _, info in addr_info.items()
            if info.get('family', None) == family
        ]
        if len(candidates) > 1:
            raise ContextualError(
                "network interface for virtual network '%s' in virtual node "
                "class '%s' has more than one addr_info block for the '%s' "
                "address family" % (network_name, node_class, family)
            )
        return candidates[0] if candidates else {}

    def __hostname_net_suffix(self, node_class, network_name):
        """Get the configured host name network suffix (if any) for
        the spefified node_class and named network. If no suffix is
        configured or if the network name is None, return an empty
        string.

        """
        if network_name is None:
            return ""
        addr_info = self.__addr_info(node_class, network_name, 'AF_INET')
        return addr_info.get("hostname_suffix", "")

    def __host_blade_class(self, node_class):
        """Determine the blade class hosting instances of the
        specified Virtual Node class.

        """
        node = self.__get_node(node_class)
        host_blade_class = node.get('host_blade', {}).get('blade_class', None)
        if host_blade_class is None:
            raise ContextualError(
                "unable to find the host blade class for "
                "node class '%s'" % node_class
            )
        return host_blade_class

    def __host_blade_instance_capacity(self, node_class):
        """Determine the blade class hosting instances of the
        specified Virtual Node class.

        """
        node = self.__get_node(node_class)
        instance_capacity = int(
            node.get('host_blade', {}).get('instance_capacity', 1)
        )
        return instance_capacity

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

    def node_hostname(self, node_class, instance, network_name=None):
        """Get the hostname of a given instance of the specified class
        of Virtual Node on the specified network. If the network is
        None or unspecified, return just the computed hostname with no
        local network suffix.

        """
        self.__check_node_instance(node_class, instance)
        node = self.__get_node(node_class)
        try:
            node_naming = node['node_naming']
        except KeyError as err:
            raise ContextualError(
                "virtual node class '%s' has no 'node_naming' "
                "section." % node_class
            ) from err
        try:
            base_name = node_naming['base_name']
        except KeyError as err:
            raise ContextualError(
                "virtual node class '%s' has no 'base_name' in its "
                "'node_naming' section" % node_class
            ) from err
        node_names = node_naming.get('node_names', [])
        return (
            node_names[instance]
            if instance < len(node_names)
            else "%s-%3.3d" % (base_name, instance + 1)
        ) + self.__hostname_net_suffix(node_class, network_name)

    def node_count(self, node_class):
        """Get the number of Virtual Blade instances of the specified
        class.

        """
        node = self.__get_node(node_class)
        return int(node.get('node_count', 0))

    def node_networks(self, node_class):
        """Return the list of names of Virtual Networks connected to
        nodes of the specified class.

        """
        node = self.__get_node(node_class)
        return [
            network_interface.get['cluster_network']
            for _, network_interface in node.get('network_interfaces', {})
            if not network_interface.get('delete', False)
            and 'cluster_network' in network_interface
        ]

    def node_ssh_key_secret(self, node_class):
        """Return the name of the secret used to store the SSH key
        pair used to reach nodes of the specified class through a
        tunneled SSH connection.

        """
        # In the KVM Cluster SSH keys  for Virtual Nodes are the
        # same as for the Virtual Blades that host them. So, find out
        # what class of blade hosts the named Virtual Node class and
        # then get the blade SSH key secret name from there.
        host_blade_class = self.__host_blade_class(node_class)
        virtual_blades = self.stack.get_provider_api().get_virtual_blades()
        return virtual_blades.blade_ssh_key_secret(host_blade_class)

    def ssh_key_paths(self, node_class):
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
        host_blade_class = self.__host_blade_class(node_class)
        virtual_blades = self.stack.get_provider_api().get_virtual_blades()
        return virtual_blades.blade_ssh_key_paths(host_blade_class)

    def node_host_blade(self, node_class, instance):
        """Get a tuple containing the the blade class and instance
        number of the Virtual Blade that hosts the Virtual Node
        instance 'instance' of the given node class.

        """
        if instance < 0:
            raise ContextualError(
                "internal error: requesting the node host blade for a "
                "negative instance number (%d) of node class '%s'" % (
                    instance, node_class
                )
            )
        host_blade_class = self.__host_blade_class(node_class)
        instance_capacity = self.__host_blade_instance_capacity(node_class)
        return (host_blade_class, int(instance / instance_capacity))

    def node_host_blade_ip(self, node_class, node_instance):
        """Given a node class and instance number return the IP
        address on the host blade network for that node.

        """
        host_blade_net = self.get('host_blade_network', {})
        netname = host_blade_net.get('network_name', None)
        if netname is None:
            raise ContextualError(
                "configuration error: the cluster configuration does not "
                "contain a 'network_name' for the 'host_blade_network'"
            )
        addr_info = self.__addr_info(node_class, netname, 'AF_INET')
        addrs = addr_info.get('addresses', None)
        if addrs is None:
            raise ContextualError(
                "internal error: no IP addresses defined for node class '%s' "
                "on the host blade network" % node_class
            )
        if len(addrs) < (node_instance + 1):
            raise ContextualError(
                "internal error: instance '%d' of node class '%s' has no "
                "IP address defined on the host blade network" % (
                    node_instance, node_class
                )
            )
        return addrs[node_instance]

    def node_host_blade_connection(
            self, node_class, node_instance, remote_port
    ):
        """Given the node class and node instance of a cluster node,
        establish a tunneled connection to the specified remote port
        on the host blade for that cluster node and return the
        BladeConnection object for that connection to the caller.

        """
        blade_class, blade_instance = self.node_host_blade(
            node_class, node_instance
        )
        virtual_blades = self.stack.get_provider_api().get_virtual_blades()
        return virtual_blades.connect_blade(
            blade_class, blade_instance, remote_port
        )
