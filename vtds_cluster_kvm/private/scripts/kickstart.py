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
"""Library file containing the definition of a class used to generate
a kickstart config for virtual nodes based on a cluster configuration.

"""
from ipaddress import IPv4Network
from cluster_common import (
    ContextualError,
    find_addr_info,
    find_address_family,
    network_bridge_name,
    find_mtu,
    node_connected_networks
)


class KickstartConfig:
    """Class to construct and generate a kickstart configuration file
    based on settings found in a cluster configuration.

    """
    def __init__(self, config, node_class, node_instance, password):
        """Constructor

        """
        self.config = config
        self.node_class = node_class
        self.node_instance = node_instance
        self.class_name = node_class['class_name']
        self.networks = node_connected_networks(node_class, config['networks'])
        # The configuration has been filled out with all of the
        # computed and specified node names and host names before we
        # get here, so just use them.
        self.node_name = (
            node_class['node_naming']['node_names'][node_instance]
        )
        self.host_name = (
            node_class['host_naming']['hostnames'][node_instance]
        )
        self.password = password

    def __install_mode(self):
        """Return a string containing the installation mode (for now
        this is always 'text')
        """
        return "text"

    def __repos(self):
        """Return an array of 'repo' command strings indicating the
        repositories to be used in the install.

        """
        # For now, there is only this one repo string...
        #
        # In the future, generate repo strings for any repos specified
        # in the config.
        return ['repo --name="minimal" --baseurl=file:///run/install/sources/mount-0000-cdrom/minimal']

    def __packages(self):
        """Return an array of package designators to be installed by
        kickstart.

        """
        # For now, the following packages are hard coded and there is
        # no way to specify them. When there is, this is the base set,
        # others can be added to the end.
        return [
            "@^custom-environment",
            "@headless-management",
            "@legacy-unix",
            "@network-server",
            "@standard",
            "@system-tools",
            "kexec-tools",
        ]

    def __language(self):
        """Return the language statement to be used in the kickstart
        config.

        """
        # For now, the language will be hard coded, but, when it gets
        # added to the configuration use it here.
        language = "en_US.UTF-8"
        return "lang %s" % language

    # pylint: disable=fixme
    #
    # XXX - From here to __network() is all here to generate the
    #       'network' command for a given network. While I like the
    #       way this is being done, it really needs to be refactored
    #       out into its own class. All of the commands that we use
    #       should be, and they should all use the same approach.
    # pylint: disable=unused-argument
    def __net_activate(self, interface, network):
        """Network command option generator for --activate

        """
        # Not conditional at this time, the network will be
        # activated if it exists.
        return ""

    # pylint: disable=unused-argument
    def __net_no_activate(self, interface, network):
        """Network command option generator for --no-activate

        """
        # Not conditional at this time, the network will be
        # activated if it exists.
        return None

    # pylint: disable=unused-argument
    def __net_bootproto(self, interface, network):
        """Network command option generator for --bootproto

        """
        addr_info = find_addr_info(interface, "AF_INET")
        # DHCP is used if it is configured that way or for any node
        # instances that do not have IP addresses configured for them.
        addresses = addr_info.get('addresses', [])
        base_mode = (
            'static' if addr_info.get('mode', 'dynamic') == 'static'
            else 'dhcp'
        )
        return (
            ("=%s" % base_mode) if self.node_instance < len(addresses)
            else '=dhcp'
        )

    # pylint: disable=unused-argument
    def __net_device(self, interface, network):
        """Network command option generator for --device

        """
        # We are going to use the --device=<MAC> syntax here so that
        # we don't need to know or guess the device name in the VM.
        # This will then line up with the MAC address configured for
        # the interface when we create the VM.
        addr_info = find_addr_info(interface, "AF_PACKET")
        addresses = addr_info.get('addresses', [])
        return "=%s" % addresses[self.node_instance]

    # pylint: disable=unused-argument
    def __net_ipv4_dns_search(self, interface, network):
        """Network command option generator for --ipv4-dns-search

        """
        # Not supported as yet.
        return None

    # pylint: disable=unused-argument
    def __net_ipv6_dns_search(self, interface, network):
        """Network command option generator for --ipv6-dns-search

        """
        # No ipv6 implemented at this time. When it is, we will put
        # the DNS info here...
        return None

    # pylint: disable=unused-argument
    def __net_ipv4_ignore_auto_dns(self, interface, network):
        """Network command option generator for --ipv4-ignore-auto-dns

        """
        # Not supported as yet.
        return None

    # pylint: disable=unused-argument
    def __net_ipv6_ignore_auto_dns(self, interface, network):
        """Network command option generator for --ipv6-ignore-auto-dns

        """
        # Not supported as yet.
        return None

    # pylint: disable=unused-argument
    def __net_ip(self, interface, network):
        """Network command option generator for --ip

        """
        addr_info = find_addr_info(interface, "AF_INET")
        # DHCP is used if it is configured that way or for any node
        # instances that do not have IP addresses configured for
        # them. Don't assign an IP to a DHCP configured node.
        addresses = addr_info.get('addresses', [])
        return (
            ("=%s" % addresses[self.node_instance])
            if self.node_instance < len(addresses)
            else None
        )

    def __net_netmask(self, interface, network):
        """Network command option generator for --netmask

        """
        address_family = find_address_family(network, "AF_INET")
        addr_info = find_addr_info(interface, "AF_INET")
        addresses = addr_info.get('addresses', [])
        # DHCP is used if it is configured that way or for any node
        # instances that do not have IP addresses configured for
        # them. Don't assign an IP to a DHCP configured node.
        return (
            (
                "=%s" % str(
                    IPv4Network(address_family['cidr'], strict=False).netmask
                )
            )
            if self.node_instance < len(addresses)
            else None
        )

    # pylint: disable=unused-argument
    def __net_ipv6(self, interface, network):
        """Network command option generator for --ipv6

        """
        # No ipv6 implemented at this time. When it is, we will put
        # the info here...
        return None

    # pylint: disable=unused-argument
    def __net_gateway(self, interface, network):
        """Network command option generator for --gateway

        """
        address_family = find_address_family(network, "AF_INET")
        gateway = address_family.get('gateway', None)
        return (
            ("=%s" % gateway) if gateway is not None else None
        )

    # pylint: disable=unused-argument
    def __net_ipv6gateway(self, interface, network):
        """Network command option generator for --ipv6gateway

        """
        # No ipv6 implemented at this time. When it is, we will put
        # the info here...
        return None

    # pylint: disable=unused-argument
    def __net_nodefroute(self, interface, network):
        """Network command option generator for --nodefroute

        """
        address_family = find_address_family(network, "AF_INET")
        gateway = address_family.get('gateway', None)
        # If there is no gateway configured, it gets the nodefroute
        # setting. Not sure this is the right way to do this forwver
        # but it should work for now.
        return None if gateway is not None else ""

    # pylint: disable=unused-argument
    def __net_nameserver(self, interface, network):
        """Network command option generator for --nameserver

        """
        address_family = find_address_family(network, "AF_INET")
        nameservers = ','.join(address_family.get('nameservers', []))
        return (
            ("=%s" % nameservers) if nameservers else None
        )

    # pylint: disable=unused-argument
    def __net_hostname(self, interface, network):
        """Network command option generator for --hostname

        """
        return "=%s" % self.host_name

    # pylint: disable=unused-argument
    def __net_onboot(self, interface, network):
        """Network command option generator for --onboot

        """
        # Not conditional at this time. Interfaces always come up on
        # boot.
        return "=yes"

    # pylint: disable=unused-argument
    def __net_dhcpclass(self, interface, network):
        """Network command option generator for --dhcpclass

        """
        # Not implemented at this time. Might need for vShasta though.
        return None

    # pylint: disable=unused-argument
    def __net_mtu(self, interface, network):
        """Network command option generator for --mtu

        """
        bridge_name = network_bridge_name(network)
        return "=%s" % find_mtu(bridge_name)

    # pylint: disable=unused-argument
    def __net_noipv4(self, interface, network):
        """Network command option generator for --noipv4

        """
        # Not conditional at this time, all interfaces have ipv4
        return None

    # pylint: disable=unused-argument
    def __net_noipv6(self, interface, network):
        """Network command option generator for --noipv6

        """
        # Not conditional at this time, ipv6 is not implemented so no
        # interfaces get ipv6
        return ""

    # pylint: disable=unused-argument
    def __net_bondslaves(self, interface, network):
        """Network command option generator for --bondslaves

        """
        # No bonding implemented at this time. When it is, we will put
        # the info here...
        return None

    # pylint: disable=unused-argument
    def __net_bondopts(self, interface, network):
        """Network command option generator for --bondopts

        """
        # No bonding implemented at this time. When it is, we will put
        # the info here...
        return None

    # pylint: disable=unused-argument
    def __net_vlanid(self, interface, network):
        """Network command option generator for --vlanid

        """
        # No VLANs implemented at this time. When it is, we will put
        # the info here...
        return None

    # pylint: disable=unused-argument
    def __net_interfacename(self, interface, network):
        """Network command option generator for --interfacename

        """
        # No interfacenames (no VLANs) implemented at this time. When
        # it is, we will put the info here...
        return None

    # pylint: disable=unused-argument
    def __net_teamslaves(self, interface, network):
        """Network command option generator for --teamslaves

        """
        # No teams implemented at this time. When it is, we will put
        # the info here...
        return None

    # pylint: disable=unused-argument
    def __net_teamconfig(self, interface, network):
        """Network command option generator for --teamconfig

        """
        # No teams implemented at this time. When it is, we will put
        # the info here...
        return None

    # pylint: disable=unused-argument
    def __net_bridgeslaves(self, interface, network):
        """Network command option generator for --bridgeslaves

        """
        # No bridging implemented at this time. When it is, we will put
        # the info here...
        return None

    # pylint: disable=unused-argument
    def __net_bridgeopts(self, interface, network):
        """Network command option generator for --bridgeopts

        """
        # No bridging implemented at this time. When it is, we will put
        # the info here...
        return None

    # pylint: disable=unused-argument
    def __net_bindto(self, interface, network):
        """Network command option generator for --bindto

        """
        return "=mac"

    # pylint: disable=unused-argument
    def __net_ethtool(self, interface, network):
        """Network command option generator for --ethtool

        """
        # No ethtool implemented at this time. When it is, we will put
        # the info here...
        return None

    def __net_options(self):
        """Return a dictionary of option to bound method references
        that can be used to construct a 'network' command. Each method
        will return an empty string if the option is active and has no
        value, a non-empty string (formatted as '=<value>') if the
        option is active and has a value, and None if the option is
        not active for the specified network name ('netname') when
        called.

        """
        return {
            '--activate': self.__net_activate,
            '--no-activate': self.__net_no_activate,
            '--bootproto': self.__net_bootproto,
            '--device': self.__net_device,
            '--ipv4-dns-search': self.__net_ipv4_dns_search,
            '--ipv6-dns-search': self.__net_ipv6_dns_search,
            '--ipv4-ignore-auto-dns': self.__net_ipv4_ignore_auto_dns,
            '--ipv6-ignore-auto-dns': self.__net_ipv6_ignore_auto_dns,
            '--ip': self.__net_ip,
            '--netmask': self.__net_netmask,
            '--ipv6': self.__net_ipv6,
            '--gateway': self.__net_gateway,
            '--ipv6gateway': self.__net_ipv6gateway,
            '--nodefroute': self.__net_nodefroute,
            '--nameserver': self.__net_nameserver,
            '--hostname': self.__net_hostname,
            '--ethtool': self.__net_ethtool,
            '--onboot': self.__net_onboot,
            '--dhcpclass': self.__net_dhcpclass,
            '--mtu': self.__net_mtu,
            '--noipv4': self.__net_noipv4,
            '--noipv6': self.__net_noipv6,
            '--bondslaves': self.__net_bondslaves,
            '--bondoppts': self.__net_bondopts,
            '--vlanid': self.__net_vlanid,
            '--interfacename': self.__net_interfacename,
            '--teamslaves': self.__net_teamslaves,
            '--teamconfig': self.__net_teamconfig,
            '--bridgeslaves': self.__net_bridgeslaves,
            '--bridgeopts': self.__net_bridgeopts,
            '--bindto': self.__net_bindto,
        }

    def __network(self, interface, network):
        """Generate the network statement to be used to configure a
        named network from the configuration.

        """
        options = self.__net_options()
        args = [
            "%s%s" % (option, func(interface, network))
            for option, func in options.items()
            if func(interface, network) is not None
        ]
        return "network %s" % ' '.join(args)

    def __find_network(self, network_name):
        """Find the named network in the set of networks supplied for
        this node.

        """
        networks = [
            network
            for _, network in self.networks.items()
            if network.get('network_name', "") == network_name
        ]
        if len(networks) < 1:
            raise ContextualError(
                "no network named '%s' in the "
                "cluster configuration" % network_name
            )
        if len(networks) > 1:
            raise ContextualError(
                "more than one network named '%s' in the "
                "cluster configuration" % network_name
            )
        return networks[0]

    def __networks(self):
        """Return the list of network statements to be used to set up
        networks on the Virtual Node.

        """
        return [
            self.__network(
                interface,
                self.__find_network(interface['cluster_network'])
            )
            for _, interface in self.node_class.get(
                'network_interfaces', {}
            ).items()
        ]

    def __install_medium(self):
        """Return the statement indicating what installation medium to
        use for installation.

        """
        # For now, the only installation medium we will use is an ISO
        # mounted as a cdrom, so, 'cdrom'
        return "cdrom"

    def __firstboot(self):
        """Return the 'firstboot' statement indicating whether
        firstboot is to be enabled or not.

        """
        states = {
            True: "--enable",
            False: "--disable",
        }
        # For now this is not configurable.
        firstboot = True
        return "firstboot %s" % states[firstboot]

    def __reboot(self):
        """Construct and return a string containing the 'reboot'
        statement controlling what to do once installation is
        complete.

        """
        options = "--eject"
        return "reboot %s" % options

    def __xwindows(self):
        """Return a string containing the instruction for what to do
        about X Windows (generally we just want to skip X Windows)
        during the install.

        """
        return "skipx"

    def __ignore_disks(self):
        """Return a string with the instruction indicating what disks
        to ignore during install.

        """
        options = "--only-use=vda"
        return "ignoredisk %s" % options

    def __bootloader(self):
        """Return a string with the bootloader instructions for the
        installed system.

        """
        options = '--append="crashkernel=auto" --location=mbr --boot-drive=vda'
        return "bootloader %s" % options

    def __partitioning(self):
        """Return a string containing the instructions for
        partitioning disks.

        """
        # For now there is no configuration for this. Just use plain
        # auto partitioning.
        return "autopart --type=plain"

    def __partition_clearing(self):
        """Return the partition clearing strategy for the install

        """
        # For now this is hard-coded
        options = "--all --initlabel --drives=vda"
        return "clearpart %s" % options

    def __timezone(self):
        """Return the timezone setting instruction for the install

        """
        # For now this is hard-coded
        timezone = "US/Central"
        options = "--isUtc"
        return "timezone %s %s" % (timezone, options)

    def __root_password(self):
        """Return the root password setting instruction

        """
        # pylint: disable=fixme
        #
        # For now, crypt() is deprecated in Python and I have not been
        # able to find a viable generally applicable solution for
        # encrypted password generation. I can make the generated
        # kickstart file root-only readable, or delete the rootpw line
        # altogether from it in the post script if needed.
        #
        # XXX - this should use an encrypted password and --iscrypted
        options = "--plaintext"
        return "rootpw %s %s" % (options, self.password)

    def __kdump_enable(self):
        """Return the add-on instruction to enable kdump on the
        Virtual Node

        """
        section = [
            "%addon com_redhat_kdump --enable --reserve-mb='auto'",
            '%end'
        ]
        return '\n'.join(section)

    def __password_policies(self):
        """Return the list of password policy instructions for the
        Virtual Node

        """
        # For now these are hard-coded
        policies = {
            'root': "--minlen=6 --minquality=1 --notstrict --nochanges --notempty",
            'user': "--minlen=6 --minquality=1 --notstrict --nochanges --emptyok",
            'luks': "--minlen=6 --minquality=1 --notstrict --nochanges --notempty",
        }
        return [
            "pwpolicy %s %s" % (key, value) for key, value in policies.items()
        ]

    def __post_install(self):
        """ Return the lines of a post-install script as an array of strings

        """
        # For now, these are hard-coded
        return [
            "mkdir -p /mnt/cdrom",
            "mount -o ro /dev/cdrom /mnt/cdrom",
            "cp -r /mnt/cdrom/install_files/.ssh /root",
            "umount /mnt/cdrom",
        ]

    def __sect_pre(self, options=""):
        """Compose the %pre section as a string.

        """
        # For now this is hard-coded and non-existent
        return ""

    def __sect_packages(self, options=""):
        """Compose the packages section and return it as a string.

        """
        section = [
            "%packages " + options,
            *self.__packages(),
            "%end"
        ]
        return '\n'.join(section)

    def __sect_anaconda(self, options=""):
        """Compose the %anaconda section and return it as a string.

        """
        section = [
            "%anaconda " + options,
            *self.__password_policies(),
            "%end"
        ]
        return '\n'.join(section)

    def __sect_post(self, options=""):
        """Compose the %post section and return it as a string.

        """
        section = [
            "%post " + options,
            *self.__post_install(),
            "%end"
        ]
        return '\n'.join(section)

    def __sect_onerr(self, options=""):
        """Compose the %onerr section and retiurn it as a string

        """
        # For now this is hard-coded and non-existent
        return ""

    def __sect_command(self):
        """Compose the command section of the kickstart file and
        return it as a string. This is basically the whole kickstart
        file, since it includes all the other sections.

        """
        return '\n\n'.join(
            [
                self.__sect_pre(),
                self.__install_mode(),
                *self.__repos(),
                self.__sect_packages(),
                self.__language(),
                *self.__networks(),
                self.__install_medium(),
                self.__firstboot(),
                self.__reboot(),
                self.__xwindows(),
                self.__ignore_disks(),
                self.__bootloader(),
                self.__partitioning(),
                self.__partition_clearing(),
                self.__timezone(),
                self.__root_password(),
                self.__kdump_enable(),
                self.__sect_anaconda(),
                self.__sect_post(),
                self.__sect_onerr(),
                "",
            ]
        )

    def compose(self, path):
        """Compose the overall Kickstart file and write it out as a
        file at 'path'

        """
        with open(path, 'w', encoding='UTF-8') as ks_file:
            # For now, make the Kickstart file be an RHEL8 Kickstart file...
            ks_file.write("\n".join(["#version=RHEL9", self.__sect_command()]))
