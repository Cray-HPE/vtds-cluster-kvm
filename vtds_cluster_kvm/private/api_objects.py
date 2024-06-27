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
"""Objects presented on the Layer API containing public information
and operations in the provider layer.

"""
from contextlib import contextmanager

from ..api_objects import (
    VirtualNodes,
    VirtualNetworks,
    NodeConnection,
    NodeConnectionSet,
    NodeSSHConnection,
    NodeSSHConnectionSet
)


class PrivateVirtualNodes(VirtualNodes):
    """Private implementation of the VirtualNodes Cluster Layer API
    Class.

    """
    def __init__(self):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = VirtualNodes.__doc__

    def node_types(self):
        return None

    def node_count(self, node_type):
        return None

    def network_names(self, node_type):
        return None

    def node_hostname(self, node_type, instance, network_name=None):
        return None

    def node_ip(self, node_type, instance, network_name):
        return None

    def node_ssh_key_secret(self, node_type):
        return None

    def node_ssh_key_paths(self, node_type):
        return None

    @contextmanager
    def connect_node(self, node_type, instance, remote_port):
        return None

    @contextmanager
    def connect_nodes(self, remote_port, node_types=None):
        return None

    @contextmanager
    def ssh_connect_node(self, node_type, instance, remote_port):
        return None

    @contextmanager
    def ssh_connect_nodes(self, remote_port=22, node_types=None):
        return None


class PrivateVirtualNetworks(VirtualNetworks):
    """Private implementation of the VirtualNetworks Cluster Layer API
    Class.

    """
    def __init__(self):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = VirtualNetworks.__doc__

    def network_names(self):
        return None

    def ipv4_cidr(self, network_name):
        return None


class PrivateNodeConnection(NodeConnection):
    """Private implementation of the NodeConnection Cluster Layer API
    Class.

    """
    def __init__(self):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = NodeConnection.__doc__

    def node_type(self):
        return None

    def node_hostname(self, network_name=None):
        return None

    def local_ip(self):
        return None

    def local_port(self):
        return None

    def remote_port(self):
        return None


class PrivateNodeConnectionSet(NodeConnectionSet):
    """Private implementation of the NodeConnectionSet Cluster Layer API
    Class.

    """
    def __init__(self):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = NodeConnectionSet.__doc__

    def list_connections(self, node_type=None):
        return None

    def get_connection(self, hostname):
        return None


class PrivateNodeSSHConnection(NodeSSHConnection, PrivateNodeConnection):
    """Private implementation of the NodeSSHConnection Cluster Layer API
    Class.

    """
    def __init__(self):
        "Constructor"
        PrivateNodeConnection.__init__(self)
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = NodeSSHConnection.__doc__

    def copy_to(
        self, source, destination,
        recurse=False, blocking=True, logname=None, **kwargs
    ):
        return None

    def copy_from(
        self, source, destination,
        recurse=False, blocking=True, logname=None, **kwargs
    ):
        return None

    def run_command(self, cmd, blocking=True, logfiles=None, **kwargs):
        return None


class PrivateNodeSSHConnectionSet(
        NodeSSHConnectionSet, PrivateNodeConnectionSet
):
    """Private implementation of the NodeSSHConnectionSet Cluster Layer API
    Class.

    """
    def __init__(self):
        "Constructor"
        PrivateNodeConnectionSet.__init__(self)
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = NodeSSHConnectionSet.__doc__

    def copy_to(
        self, source, destination, recurse=False, logname=None, node_type=None
    ):
        return None

    def run_command(self, cmd, logname=None, node_type=None):
        return None
