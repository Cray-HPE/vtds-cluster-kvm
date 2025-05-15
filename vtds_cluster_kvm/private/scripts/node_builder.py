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
"""Base class for Node Builder classes. Node Builders implement a
particular method of building a Virtual Node on a Virtual Blade.

"""
from abc import (
    ABCMeta,
    abstractmethod
)
from tempfile import (
    NamedTemporaryFile
)
from uuid import uuid4

from jinja2 import (
    Template,
    TemplateError
)

from cluster_common import (
    ContextualError,
    compute_node_name,
    compute_hostname,
    run_cmd,
    node_mac_addrs,
    find_addr_info,
    find_address_family,
    network_length,
    network_bridge_name,
    find_mtu,
    node_connected_networks
)

from disk_builder import pick_disk_builder


# We want a bunch of information harvested for easy use by inheritors,
# shut lint up about our large number of instance attributes
#
# pylint: disable=too-many-instance-attributes
class NodeBuilder(metaclass=ABCMeta):
    """Base class for a node-builder class. These classes are used for
    different mechanisms for building Virtual Nodes on a Virtual
    Blade.

    """
    def __init__(self, config, node_class, node_instance, root_passwd):
        """Constructor

        """
        self.config = config
        self.node_class = node_class
        self.node_instance = node_instance
        self.root_passwd = root_passwd
        self.class_name = node_class['class_name']
        self.networks = node_connected_networks(node_class, config['networks'])
        self.node_name = compute_node_name(node_class, node_instance)
        self.host_name = compute_hostname(node_class, node_instance)
        self.virtual_machine = node_class.get('virtual_machine', {})
        self.disk_builder = pick_disk_builder(
            config, node_class, node_instance, root_passwd
        )

        # Get the CPU and Memory parameters...
        try:
            self.memsize = str(
                int(self.virtual_machine['memory_size_mib']) * 1024
            )
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
            self.cpus = self.virtual_machine['cpu_count']
        except KeyError as err:
            raise ContextualError(
                "configuration error: no 'cpu_count' found in "
                "Virtual Machine configuration for Virtual Node "
                "class '%s': %s " % (self.class_name, str(self.node_class))
            ) from err

    @abstractmethod
    def build(self):
        """Build the Virtual Node

        """


class DebianQCOWNode(NodeBuilder):
    """NodeBuilder Class for building Virtual Nodes using an XML
    description of the virtual machine and the 'virsh create'
    command. This also assumes we are creating a boot disk image from
    a qcow2 image and directly customizing it using virt-customize.

    """
    def __init__(self, config, node_class, node_instance, root_passwd):
        """Constructor

        """
        self.__doc__ = NodeBuilder.__doc__
        NodeBuilder.__init__(
            self, config, node_class, node_instance, root_passwd
        )

    def __make_net_if(self, interface, network):
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

    def __make_net_ifs(self):
        """Configure the network interfaces on the boot disk image and
        return the template description of the network interfaces.

        """
        context = [
            self.__make_net_if(
                interface, self.networks[interface['cluster_network']]
            )
            for _, interface in self.node_class.get(
                'network_interfaces', {}
            ).items()
        ]
        return context

    def __compose(self):
        """Compose the template data that will be used to fill out the
        XML template that will be used to create the Virtual Node
        using 'virsh create <filename>'

        """
        return {
            'hostname': self.node_name,
            'uuid': str(uuid4()),
            'memsize_kib': self.memsize,
            'cpus': self.cpus,
            'boot_disk': self.disk_builder.build_boot_disk(),
            'extra_disks': self.disk_builder.build_extra_disks(),
            'interfaces': self.__make_net_ifs(),
        }

    def __create(self):
        """Compose an XML definition of the Virtual Node and create
        it on the current blade.

        """
        context = self.__compose()
        try:
            vm_template = self.node_class['vm_xml_template']
        except KeyError as err:
            raise ContextualError(
                "internal configuration error: Virtual Node class '%s' does "
                "not have a VM XML template stored in it. This may be some "
                "kind of module version mismatch."
            ) from err
        template = Template(vm_template)
        try:
            vm_xml = template.render(**context)
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

    def build(self):
        self.__create()


class RedHatISONode(NodeBuilder):
    """Node builder class for building RedHat style nodes using ISO
    installation media.

    """

    def __init__(self, config, node_class, node_instance, root_passwd):
        """Constructor

        """
        self.__doc__ = NodeBuilder.__doc__
        NodeBuilder.__init__(
            self, config, node_class, node_instance, root_passwd
        )

    def __make_net_opt(self, interface):
        """Using a network configuration for the Virtual Node, compose
        the '--network' option to use in the virt-install command for
        that network.

        """
        network_name = 'network=' + interface['cluster_network']
        mac_addrs = node_mac_addrs(interface)
        mac = (
            ',mac=' + mac_addrs[self.node_instance]
            if self.node_instance < len(mac_addrs)
            else ""
        )
        return "--network=" + network_name + mac

    def __make_virt_install_args(self, boot_disk_info, extra_disks):
        """Compose the arguments to pass to the virt-install command
        to install the Virtual Node.

        """
        # Get the boot disk size in GB from the disk size in MB,
        # defaulting to 100GB
        boot_size = str(int(
            self.virtual_machine
            .get('boot_disk', {})
            .get('disk_size_mb', '100000')
        ) / 1000)
        base_opts = [
            '--name', self.node_name,
            '--wait=-1',
            '--location', boot_disk_info['iso_path'],
            '--disk', 'path=%s,size=%s' % (
                boot_disk_info['file_path'], boot_size
            ),
            '--os-variant', 'rocky8',
            '--ram', str(self.virtual_machine.get('memory_size_mib', '4096')),
            '--vcpus', str(self.virtual_machine.get('cpu_count', '1')),
            '--graphics', 'none',
            '--console', 'pty,target_type=virtio',
            '--noautoconsole',
            '--extra-args', 'inst.ks=cdrom:/install_files/ks.cfg'
        ]
        extra_disk_opts = [
            '--disk=path=%s' % disk_info['file_path']
            for disk_info in extra_disks
        ]
        net_opts = [
            self.__make_net_opt(interface)
            for _, interface in self.node_class.get(
                'network_interfaces', {}
            ).items()
        ]
        return (
            base_opts +
            extra_disk_opts +
            net_opts
        )

    def build(self):
        boot_disk_info = self.disk_builder.build_boot_disk()
        extra_disks = self.disk_builder.build_extra_disks()
        inst_args = self.__make_virt_install_args(boot_disk_info, extra_disks)
        run_cmd('virt-install', inst_args)


def pick_node_builder(config, node_class, node_instance, root_passwd):
    """Based on the supplied node_class configuration pick the right
    NodeBuilder object type, instantiate it and return the instance
    allowing the caller to get a NodeBuilder without knowing the
    details of how the selection is made.

    """
    node_builders = {
        ("Debian", "qcow2"): DebianQCOWNode,
        ("RedHat", "iso"): RedHatISONode,
    }
    # Figure out what node builder to use and get one...
    distro_family = node_class.get('distro', {}).get('family', "Debian")
    boot_disk_medium = (
        node_class
        .get('virtual_machine', {})
        .get('boot_disk', {})
        .get('media_type', 'qcow2')
    )
    key = (distro_family, boot_disk_medium)
    if key not in node_builders:
        raise ContextualError(
            "the combination of a '%s' Linux distro family and a '%s' "
            "root image medium is unsupported at this time - make sure "
            "your configuration identifies both the Virtual Node distro "
            "family and boot image medium and that they fit within these "
            "supported combinations: %s" % (
                key[0], key[1], str(node_builders.keys())
            )
        )
    # Create and return the node builder
    return node_builders[key](config, node_class, node_instance, root_passwd)
