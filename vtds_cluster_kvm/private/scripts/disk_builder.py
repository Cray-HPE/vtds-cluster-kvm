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
"""Base class and implementations of Disk Builder classes to support
deploying various combinations of Linux distributiuon and Disk Media
types.

"""
from abc import (
    ABCMeta,
    abstractmethod
)
import os
from os import (
    makedirs,
    remove as remove_file,
    rmdir
)
from os.path import (
    exists,
    join as path_join
)
from shutil import rmtree
from tempfile import (
    NamedTemporaryFile,
    mkdtemp
)
from time import sleep
import yaml

from cluster_common import (
    ContextualError,
    run_cmd,
    node_mac_addrs,
    compute_node_name,
    compute_hostname,
    find_addr_info,
    find_address_family,
    network_length,
    network_bridge_name,
    open_safe,
    find_mtu,
    node_connected_networks,
    info_msg
)

from kickstart import KickstartConfig


class DiskBuilder(metaclass=ABCMeta):
    """Base class for Disk Builder classes

    """
    def __init__(self, config, node_class, node_instance, root_passwd):
        """Constructor

        """
        self.config = config
        self.node_class = node_class
        self.node_instance = node_instance
        self.root_passwd = root_passwd
        self.networks = node_connected_networks(node_class, config['networks'])
        self.host_name = compute_hostname(node_class, node_instance)
        # Set up to be ready to make the boot disk
        self.nodeclass_dir = path_join(
            os.sep, 'var', 'local', 'vtds', self.node_class['class_name']
        )
        node_name = compute_node_name(node_class, node_instance)
        self.host_dir = path_join(self.nodeclass_dir, node_name)
        makedirs(self.host_dir, mode=0o755, exist_ok=True)
        virtual_machine = node_class.get('virtual_machine', {})
        try:
            self.disk_config = virtual_machine['boot_disk']
        except KeyError as err:
            raise ContextualError(
                "configuration error: Virtual Node class '%s' "
                "'virtual_machine' section "
                "does not contain a 'boot_disk' "
                "section: %s" % (
                    self.node_class['class_name'],
                    str(self.node_class)
                )
            ) from err
        self.boot_disk_path = path_join(self.host_dir, "boot_disk.img")

    @staticmethod
    def _retrieve_image(url, dest):
        """Retrieve a disk image from a URL ('url') and write it the
        file named in 'dest'.

        """
        # Using curl here instead of writing a requests library
        # operation because it is simpler and just about as fast and
        # the error handling is covered. If the destination file
        # already exists, simply return. If retrieval fails, remove
        # any partial destination file that might have been created.
        dest = dest.strip()
        url = url.strip()
        retry = 0
        while not exists(dest):
            try:
                run_cmd('curl', ['-o', dest, '-s', url])
            except Exception as err:
                if exists(dest):
                    remove_file(dest)
                # This does not always work on the first try...
                retry += 1
                if retry < 20:
                    info_msg(
                        "retrieving '%s' failed, retrying [%d]..." % (
                            url, retry,
                        )
                    )
                    sleep(30)
                    continue
                raise err

    def _make_extra_disks(self):
        """Create all of the extra disk images for this Virtual Node
        using the information in from the Node Class and return the
        template description of the list of disks.

        Only qcow2 images are supported for extra disk content
        images. If no content is needed, then there will be no image
        specified and we make an empty disk.

        """
        virtual_machine = self.node_class.get('virtual_machine', {})
        extra_disks = virtual_machine.get('additional_disks', {})
        return [
            self._make_qcow_disk(
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

    @staticmethod
    # Take the pylint line out when we start using partitions
    #
    # pylint: disable=unused-argument
    def _make_qcow_image(name, size, source_image_name, partitions):
        """Build a qcow2 disk image in a file named 'name'. If 'size'
        is not None make sure the resulting disk image is 'size' bytes
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

    def _make_qcow_disk(self, name, disk_config, source_image_name=None):
        """Given the filename ('name') to store the boot disk file,
        the disk configuration ('disk_config') and the path to the
        place to store the source image ('source_image_name'),
        construct a disk for use with the Virtual Node and return its
        description.

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
                    self.node_class['class_name'],
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
                    self.node_class['class_name'],
                    str(disk_config)
                )
            )
        if image_url and not source_image_name:
            raise ContextualError(
                "internal error: no source image name supplied when making "
                "a disk with a source image URL"
            )
        if image_url:
            self._retrieve_image(image_url, source_image_name)
        self._make_qcow_image(name, size, source_image_name, partitions)
        return {
            'file_path': name,
            'target_device': target_dev,
        }

    @abstractmethod
    def build_boot_disk(self):
        """Build the boot disk for the node class this disk builder
        builds for.

        """

    def build_extra_disks(self):
        """Build the extra disks for the node class.

        """
        return self._make_extra_disks()


class DebianQCOWDisk(DiskBuilder):
    """Implementation of a disk builder for building Debian root file
    systems using QCOW2 images.

    """
    def __init__(self, config, node_class, node_instance, root_passwd):
        """Constructor

        """
        self.__doc__ = DiskBuilder.__doc__
        DiskBuilder.__init__(
            self, config, node_class, node_instance, root_passwd
        )

    def __make_boot_disk(self):
        """Create a boot disk image for this Virtual Node using the
        information from the Node Class and return the template
        description of the disk.

        """
        source_image_name = path_join(
            self.nodeclass_dir, 'boot-img-source.qcow'
        )
        return self._make_qcow_disk(
            self.boot_disk_path, self.disk_config, source_image_name
        )

    def __netplan_if(self, interface):
        """Compose an interface block for the netplan configuration

        """
        network = self.networks[interface['cluster_network']]
        node_instance = int(self.node_instance)
        ipv4_info = find_addr_info(interface, "AF_INET")
        address_family = find_address_family(network, "AF_INET")
        addresses = ipv4_info.get('addresses', [])
        static_addr = node_instance < len(addresses)
        net_length = network_length(
            address_family, interface['cluster_network']
        )
        return {
            'addresses': (
                [
                    '/'.join([addresses[self.node_instance], net_length])
                ]
                if static_addr else []
            ),
            'dhcp6': False,
            'dhcp4': (
                ipv4_info['mode'] in ['dynamic', 'reserved'] or
                node_instance >= len(addresses)
            ),
            'mtu': find_mtu(network_bridge_name(network)),
            'match': {
                'macaddress': node_mac_addrs(interface)[node_instance]
            }
        }

    def __configure_netplan(self):
        """Configure netplan for the Virtual Node with all of the
        available network interfaces that are defined for the node.

        """
        if not self.boot_disk_path or not exists(self.boot_disk_path):
            raise ContextualError(
                "internal error: __configure_netplan run before the "
                "boot disk image was created"
            )
        netplan = {
            'network': {
                'version': "2",
                'renderer': 'networkd',
                'ethernets': {
                    interface['cluster_network']: self.__netplan_if(interface)
                    for _, interface in self.node_class.get(
                            'network_interfaces', {}
                    ).items()
                }
            }
        }
        with NamedTemporaryFile(mode='w', encoding='UTF-8') as tmpfile:
            yaml.safe_dump(netplan, tmpfile)
            tmpfile.flush()
            run_cmd(
                'virt-customize',
                [
                    '-a', self.boot_disk_path,
                    '--upload',
                    "%s:/etc/netplan/10-vtds-ethernets.yaml" % tmpfile.name
                ]
            )

    def __reconfigure_ssh(self):
        """Run 'dpkg-recofigure openssh-server' on the root disk so
        that the SSH servers will have host keys. Also, install root
        SSH keys and authorizations.

        """
        actions = [
            '--run-command', 'dpkg-reconfigure openssh-server',
            '--copy-in', '/root/.ssh:/root',
            '--hostname', self.host_name,
        ]
        run_cmd(
            'virt-customize',
            [
                '-a', self.boot_disk_path,
                *actions,
            ]
        )

    def __configure_root_passwd(self):
        """Configure the root password on the boot disk image for the
        Virtual Node.

        """
        if not self.boot_disk_path or not exists(self.boot_disk_path):
            raise ContextualError(
                "internal error: __configure_root_passwd run before the "
                "boot disk image was created"
            )
        run_cmd(
            'virt-customize',
            [
                '-a', self.boot_disk_path,
                '--root-password', 'password:%s' % self.root_passwd,
            ]
        )
        # Toss the root password for the node in a root-owned readable
        # only by owner file so we can use it later.
        filename = "%s-passwd.txt" % self.host_name
        with open(
            filename, mode='w', opener=open_safe, encoding='UTF-8'
        ) as pw_file:
            pw_file.write("%s\n" % self.root_passwd)

    def build_boot_disk(self):
        boot_image_name = self.__make_boot_disk()
        self.__configure_root_passwd()
        self.__reconfigure_ssh()
        self.__configure_netplan()
        return boot_image_name


class RedHatISODisk(DiskBuilder):
    """Implementation of a disk builder for building RedHat root file
    systems using ISO images. This will construct an image for
    installation that has a Kickstart file in it and installs whatever
    RedHat flavor the ISO image provides.

    """
    def __init__(self, config, node_class, node_instance, root_passwd):
        """Constructor

        """
        self.__doc__ = DiskBuilder.__doc__
        DiskBuilder.__init__(
            self, config, node_class, node_instance, root_passwd
        )
        self.boot_iso_path = path_join(self.host_dir, "install_disk.iso")

    def __populate_install_media(self, mount_point, install_root):
        """Populate the directory under 'install_root' with the
        generated files needed to carry out the OS install on the
        Virtual Node.

        """
        run_cmd('cp', ['-a', path_join(mount_point, '.'), install_root])
        iso_install_files_dir = path_join(install_root, 'install_files')
        makedirs(iso_install_files_dir)
        run_cmd(
            'cp',
            ['-v', '-a', path_join('/', 'root', '.ssh'), iso_install_files_dir]
        )
        kickstart = KickstartConfig(
            self.config, self.node_class, self.node_instance,
            self.root_passwd
        )
        kickstart.compose(path_join(iso_install_files_dir, 'ks.cfg'))
        # pylint: disable=fixme
        #
        # XXX - Add metadata or something for the ISO class that
        #       allows the configuration to specify the 'boot
        #       binary path' (-b option value) and the 'boot
        #       catalogue path' (-c option value). For now these
        #       are hard-coded. These are paths within the ISO
        #       image relative to the mount point of the image.
        run_cmd(
            'mkisofs',
            [
                '-b', 'isolinux/isolinux.bin',
                '-c', 'isolinux/boot.cat',
                '-R', '-J', '-pad',
                '-no-emul-boot',
                '-boot-load-size', '4',
                '-V', "%s Kickstart Disk" % self.host_name,
                '-o', self.boot_iso_path,
                install_root
            ]
        )

    def build_boot_disk(self):
        # The source image will be an ISO that we need to retrieve,
        # unpack, modify (add a kickstart file and ssh public key for
        # root to use), then make into a new ISO.
        image_url = self.disk_config.get('source_image', None)
        source_image_name = path_join(
            self.nodeclass_dir, 'boot-img-source.iso'
        )
        self._retrieve_image(image_url, source_image_name)
        # pylint: disable=fixme
        #
        # Mount the ISO and copy it into a temporary directory
        # tree. Ideally the mount would be context managed. For now we
        # can live without that using finally.
        #
        # XXX - make the mount context managed
        install_root = None
        mount_point = None
        try:
            mount_point = mkdtemp(dir="/mnt")
            install_root = mkdtemp(dir="/tmp")
            run_cmd(
                'mount',
                [
                    '-o', 'loop,ro', '-t', 'iso9660',
                    source_image_name, mount_point
                ]
            )
            self.__populate_install_media(mount_point, install_root)
        except Exception as err:
            raise ContextualError(
                "failed to construct new ISO image '%s' from '%s' - %s'" % (
                    self.boot_disk_path, source_image_name, str(err)
                )
            ) from err
        finally:
            if mount_point:
                run_cmd('umount', [mount_point], check=False)
                rmdir(mount_point)
            if install_root:
                rmtree(install_root)
        target_dev = self.disk_config.get('target_device', None)
        return {
            'file_path': self.boot_disk_path,
            'iso_path': self.boot_iso_path,
            'target_device': target_dev,
        }


def pick_disk_builder(config, node_class, node_instance, root_passwd):
    """Based on the supplied node_class configuration pick the right
    DiskBuilder object type, instantiate it and return the instance
    allowing the caller to get a DiskBuilder without knowing the
    details of how the selection is made.

    """
    disk_builders = {
        ("Debian", "qcow2"): DebianQCOWDisk,
        ("RedHat", "iso"): RedHatISODisk,
    }
    # Figure out what disk builder to use and get one...
    distro_family = node_class.get('distro', {}).get('family', "Debian")
    boot_disk_medium = (
        node_class
        .get('virtual_machine', {})
        .get('boot_disk', {})
        .get('media_type', 'qcow2')
    )
    key = (distro_family, boot_disk_medium)
    if key not in disk_builders:
        raise ContextualError(
            "the combination of a '%s' Linux distro family and a '%s' "
            "root image medium is unsupported at this time - make sure "
            "your configuration identifies both the Virtual Node distro "
            "family and boot image medium and that they fit within these "
            "supported combinations: %s" % (
                key[0], key[1], str(disk_builders.keys())
            )
        )
    # Create and return the disk builder
    return disk_builders[key](config, node_class, node_instance, root_passwd)
