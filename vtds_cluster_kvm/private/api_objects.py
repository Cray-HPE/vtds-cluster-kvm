#
# MIT License
#
# (C) Copyright 2025 Hewlett Packard Enterprise Development LP
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
from subprocess import (
    Popen,
    TimeoutExpired
)
from socketserver import TCPServer
from socket import (
    socket,
    AF_INET,
    SOCK_STREAM
)
from time import sleep

from vtds_base import (
    ContextualError,
    log_paths,
    logfile,
    info_msg,
    render_command_string
)
from vtds_base.layers.cluster import (
    VirtualNodesBase,
    VirtualNetworksBase,
    NodeConnectionBase,
    NodeConnectionSetBase,
    NodeSSHConnectionBase,
    NodeSSHConnectionSetBase,
    AddressingBase
)


class VirtualNodes(VirtualNodesBase):
    """Implementation of the VirtualNodes Cluster Layer API Class.

    """
    def __init__(self, common):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = VirtualNodes.__doc__
        self.common = common

    def node_classes(self):
        node_classes = self.common.get('node_classes', {})
        return [
            name for name, node_class in node_classes.items()
            if not node_class.get('pure_base_class', False)
        ]

    def application_metadata(self, node_class):
        return self.common.node_application_metadata(node_class)

    def node_count(self, node_class):
        return self.common.node_count(node_class)

    def set_node_node_name(self, node_class, instance, name):
        self.common.set_node_node_name(node_class, instance, name)

    def node_node_name(self, node_class, instance):
        return self.common.node_node_name(node_class, instance)

    def network_names(self, node_class):
        return self.common.node_networks(node_class)

    def set_node_hostname(self, node_class, instance, name):
        self.common.set_node_hostname(node_class, instance, name)

    def node_hostname(self, node_class, instance, network_name=None):
        return self.common.node_hostname(
            node_class, instance, network_name
        )

    def node_host_blade_info(self, node_class):
        return self.common.node_host_blade_info(node_class)

    def node_ipv4_addr(self, node_class, instance, network_name):
        return self.common.node_ipv4_addr(node_class, instance, network_name)

    def node_class_addressing(self, node_class, network_name):
        # The connected instances for a node class on a given network
        # are just all of the instances in the node count if that
        # network is connected. Otherwise, there are none.
        connected_instances = list(range(0, self.node_count(node_class)))
        address_families = self.common.node_address_families(
            node_class, network_name
        )
        return (
            Addressing(connected_instances, address_families)
            if address_families is not None else None
        )

    def node_ssh_key_secret(self, node_class):
        return self.common.node_ssh_key_secret(node_class)

    def node_ssh_key_paths(self, node_class):
        return self.common.node_ssh_key_paths(node_class)

    def connect_node(self, node_class, instance, remote_port):
        return NodeConnection(
            self.common, node_class, instance, remote_port
        )

    def connect_nodes(self, remote_port, node_classes=None):
        node_classes = (
            self.node_classes() if node_classes is None else node_classes
        )
        connections = [
            NodeConnection(
                self.common, node_class, instance, remote_port
            )
            for node_class in node_classes
            for instance in range(0, self.node_count(node_class))
        ]
        return NodeConnectionSet(self.common, connections)

    def ssh_connect_node(self, node_class, instance, remote_port=22):
        return NodeSSHConnection(
            self.common, node_class, instance, remote_port
        )

    def ssh_connect_nodes(self, node_classes=None, remote_port=22):
        node_classes = (
            self.node_classes() if node_classes is None else node_classes
        )
        connections = [
            NodeSSHConnection(
                self.common, node_class, instance, remote_port
            )
            for node_class in node_classes
            for instance in range(0, self.node_count(node_class))
        ]
        return NodeSSHConnectionSet(self.common, connections)


class VirtualNetworks(VirtualNetworksBase):
    """Implementation of the VirtualNetworks Cluster Layer API Class.

    """
    def __init__(self, common):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = VirtualNetworks.__doc__
        self.common = common
        self.networks_by_name = self.__networks_by_name()

    def __networks_by_name(self):
        """Return a dictionary of non-deleted Virtual Networks
        indexed by 'network_name'

        """
        networks = self.common.get("networks", {})
        try:
            return {
                network['network_name']: network
                for _, network in networks.items()
                if not network.get('delete', False)
            }
        except KeyError as err:
            # Since we are going to error out anyway, build a list of
            # interconnects without network names so we can give a
            # more useful error message.
            missing_names = [
                key for key, network in networks.items()
                if 'network_name' not in network
            ]
            raise ContextualError(
                "provider config error: 'network_name' not specified in "
                "the following Virtual Networks: %s" % str(missing_names)
            ) from err

    def __network_by_name(self, network_name):
        """Return the network configuration for the named network.
        """
        if network_name not in self.networks_by_name:
            raise ContextualError(
                "the Virtual Network named '%s' does not exist" % network_name
            )
        return self.networks_by_name[network_name]

    def __address_family(self, network_name, family):
        """Get the address_family block for the specified address family from
        the network of the specified name.  If the network doesn't
        exist raise an exception. If there is no matching address_family,
        return None.

        """
        network = self.__network_by_name(network_name)
        candidates = [
            address_family
            for _, address_family in network.get(
                    'address_families', {}
            ).items()
            if address_family.get('family', None) == family
        ]
        return candidates[0] if candidates else None

    def network_names(self):
        return self.networks_by_name.keys()

    def application_metadata(self, network_name):
        network = self.__network_by_name(network_name)
        return network.get('application_metadata', {})

    def ipv4_cidr(self, network_name):
        address_family = self.__address_family(network_name, 'AF_INET')
        if address_family is None:
            return None
        return address_family.get('cidr', None)

    def non_cluster_network(self, network_name):
        network = self.__network_by_name(network_name)
        return network.get('non_cluster', False)

    def blade_class_addressing(self, blade_class, network_name):
        network = self.__network_by_name(network_name)
        candidates = [
            candidate.get('blade_instances', [])
            for candidate in network.get('connected_blades', [])
            if candidate.get('blade_class', None) == blade_class
        ]
        if len(candidates > 1):
            raise ContextualError(
                "the vTDS cluster network '%s' configuration is populated "
                "with more than one list of connected blades for blade "
                "class '%s'" % (
                    network_name, blade_class
                )
            )
        connected_instances = candidates[0] if len(candidates) > 0 else []
        address_families = [
            {
                'family': family['family'],
                'addresses': family['addresses']
            }
            for family in network.get('address_families', [])
            if 'family' in family and 'addresses' in family
        ]
        return Addressing(connected_instances, address_families)


# pylint: disable=too-many-instance-attributes
class NodeConnection(NodeConnectionBase):
    """Implementation of the NodeConnection Cluster Layer API Class.

    """
    def __init__(self, common, node_class, instance, remote_port):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = NodeConnection.__doc__
        self.common = common
        self.n_class = node_class
        self.instance = instance
        self.rem_port = int(remote_port)
        self.loc_ip = "127.0.0.1"
        self.loc_port = None
        self.subprocess = None
        self.blade_connection = None
        self.out_log = None
        self.err_log = None
        self.options = [
            '-o', 'NoHostAuthenticationForLocalhost=yes',
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'ServerAliveInterval=30',
            '-o', 'ServerAliveCountMax=3',
        ]
        self.hostname = self.common.node_hostname(node_class, instance)
        self._connect()

    def __enter__(self):
        return self

    def __exit__(
            self,
            exception_class=None,
            exception_value=None,
            traceback=None
    ):
        # Tear down the connection here and return
        if self.blade_connection:
            self.blade_connection.__exit__()
        self.blade_connection = None
        if self.subprocess:
            self.subprocess.kill()
        self.subprocess = None
        if self.out_log is not None:
            self.out_log.close()
        self.out_log = None
        if self.err_log is not None:
            self.err_log.close()
        self.err_log = None

    def _connect(self):
        """Set up the port forwarding connection to the node.

        """
        self.blade_connection = self.common.node_host_blade_connection(
            self.n_class, self.instance, 22
        )
        blade_hostname = self.blade_connection.blade_hostname()
        ssh_ip = self.blade_connection.local_ip()
        ssh_port = self.blade_connection.local_port()
        _, ssh_key_path = self.common.ssh_key_paths(self.n_class)
        node_ip = self.common.node_host_blade_ip(self.n_class, self.instance)
        out_path, err_path = log_paths(
            self.common.build_dir(),
            "node_connection-%s-port-%d" % (self.hostname, self.rem_port)
        )
        reconnects = 10
        while reconnects > 0:
            # Get a "free" port to use for the connection by briefly
            # binding a TCP server and then destroying it before it
            # listens on anything.
            with TCPServer((self.loc_ip, 0), None) as tmp:
                self.loc_port = tmp.server_address[1]

            # Not using with for these files because they want to
            # survive the return from the function.
            #
            # pylint: disable=consider-using-with
            self.out_log = open(out_path, 'w', encoding='UTF-8')
            # pylint: disable=consider-using-with
            self.err_log = open(err_path, 'w', encoding='UTF-8')

            # Not using 'with' for the Popen because the Popen
            # object becomes part of this class instance for the
            # duration of the class instance's life cycle. The
            # instance itself is a context manager which will
            # disconnect and destroy the Popen object when the
            # context ends.
            #
            # pylint: disable=consider-using-with
            cmd = [
                'ssh',
                '-L', "%s:%s:%s:%s" % (
                    self.loc_ip, str(self.loc_port),
                    node_ip, str(self.rem_port)
                ),
                *self.options,
                '-N',
                '-T',
                '-vvv',
                '-p', str(ssh_port),
                '-i', ssh_key_path,
                "root@%s" % ssh_ip
            ]
            self.subprocess = Popen(
                cmd,
                stdout=self.out_log, stderr=self.err_log,
                text=True, encoding='UTF-8'
            )

            # Wait for the tunnel to be established before returning.
            retries = 60
            while retries > 0:
                # If the connection command fails, then break out of
                # the loop, since there is no point trying to connect
                # to a port that will never be there.
                exit_status = self.subprocess.poll()
                if exit_status is not None:
                    info_msg(
                        "SSH port forwarding connection to '%s' on port %d "
                        "terminated with exit status %d [%s%s]" % (
                            blade_hostname, ssh_port, exit_status,
                            "retrying" if reconnects > 1 else "failing",
                            " - details in '%s'" % err_path if reconnects <= 1
                            else ""
                        )
                    )
                    break
                with socket(AF_INET, SOCK_STREAM) as tmp:
                    try:
                        tmp.connect((self.loc_ip, self.loc_port))
                        return
                    except ConnectionRefusedError:
                        sleep(1)
                        retries -= 1
                    except Exception as err:
                        self.__exit__(type(err), err, err.__traceback__)
                        raise ContextualError(
                            "internal error: failed attempt to connect to "
                            "service on SSH port forwarding tunnel "
                            "to node '%s' port %d "
                            "(local port = %d, local IP = %s) "
                            "connect cmd was %s - %s" % (
                                self.hostname, self.rem_port,
                                self.loc_port, self.loc_ip,
                                str(cmd),
                                str(err)
                            ),
                            out_path, err_path
                        ) from err
            # If we got out of the loop either the connection command
            # terminated or we timed out trying to connect, keep
            # trying the connection from scratch a few times.
            reconnects -= 1
            self.subprocess.kill()
            self.subprocess = None
            self.loc_port = None
            # If we timed out, we have waited long enough to reconnect
            # immediately. If not, give it some time to get better
            # then reconnect.
            if retries > 0:
                sleep(10)
        # The reconnect loop ended without a successful connection,
        # report the error and bail out...
        raise ContextualError(
            "internal error: timeout waiting for SSH port forwarding "
            "connection to '%s' "
            "port %d to be ready (local port = %d, local IP = %s) "
            "- connect command was %s" % (
                self.hostname, self.rem_port,
                self.loc_port, self.loc_ip,
                str(cmd)
            ),
            out_path, err_path
        )

    def node_class(self):
        return self.n_class

    def node_hostname(self, network_name=None):
        return self.hostname

    def local_ip(self):
        return self.loc_ip

    def local_port(self):
        return self.loc_port

    def remote_port(self):
        return self.rem_port


class NodeConnectionSet(NodeConnectionSetBase):
    """Implementation of the NodeConnectionSet Cluster Layer API
    Class.

    """
    def __init__(self, common, connections):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = NodeConnectionSet.__doc__
        self.common = common
        self.connections = connections

    def __enter__(self):
        return self

    def __exit__(
            self,
            exception_type=None,
            exception_value=None,
            traceback=None
    ):
        for connection in self.connections:
            connection.__exit__(exception_type, exception_value, traceback)

    def list_connections(self, node_class=None):
        return [
            node_connection for node_connection in self.connections
            if node_class is None or
            node_connection.node_class() == node_class
        ]

    def get_connection(self, hostname):
        for node_connection in self.connections:
            if node_connection.node_hostname() == hostname:
                return node_connection
        return None


# The following is shared by NodeSSHConnection and
# NodeSSHConnectionSet. This should be treaded as private to
# this file. It is pulled out of both classes for easy sharing.
def wait_for_popen(subprocess, cmd, logpaths, timeout=None, **kwargs):
    """Wait for a Popen() object to reach completion and return
    the exit value.

    If 'check' is either omitted from the keyword arguments or is
    supplied in the keyword aguments and True, raise a
    ContextualError if the command exists with a non-zero exit
    value, otherwise simply return the exit value.

    If 'timeout' is supplied (in seconds) and exceeded kill the
    Popen() object and then raise a ContextualError indicating the
    timeout and reporting where the command logs can be found.

    """
    info_msg(
        "waiting for popen: "
        "subproc='%s', cmd='%s', logpaths='%s', timeout='%s', kwargs='%s'" % (
            str(subprocess), str(cmd), str(logpaths), str(timeout), str(kwargs)
        )
    )
    check = kwargs.get('check', True)
    time = timeout if timeout is not None else 0
    signaled = False
    while True:
        try:
            exitval = subprocess.wait(timeout=5)
            break
        except TimeoutExpired:
            time -= 5 if timeout is not None else 0
            if timeout is not None and time <= 0:
                if not signaled:
                    # First try to terminate the process
                    subprocess.terminate()
                    continue
                subprocess.kill()
                # pylint: disable=raise-missing-from
                raise ContextualError(
                    "SSH command '%s' timed out and did not terminate "
                    "as expected after %d seconds" % (str(cmd), time),
                    *logpaths
                )
            continue
    if check and exitval != 0:
        raise ContextualError(
            "SSH command '%s' terminated with a non-zero "
            "exit status '%d'" % (str(cmd), exitval),
            *logpaths
        )
    return exitval


class NodeSSHConnection(NodeSSHConnectionBase, NodeConnection):
    """Implementation of the NodeSSHConnection Cluster Layer API
    Class.

    """
    def __init__(
            self, common, node_class, node_instance, remote_port, **kwargs
    ):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = NodeSSHConnection.__doc__
        NodeConnection.__init__(
            self, common, node_class, node_instance, remote_port
        )
        default_opts = [
            '-o', 'BatchMode=yes',
            '-o', 'NoHostAuthenticationForLocalhost=yes',
            '-o', 'StrictHostKeyChecking=no',
        ]
        port_opt = [
            '-o', 'Port=%s' % str(self.loc_port),
        ]
        self.options = kwargs.get('options', default_opts)
        self.options += port_opt
        _, self.private_key_path = self.common.ssh_key_paths(node_class)

    def __enter__(self):
        return self

    def __exit__(
            self,
            exception_type=None,
            exception_value=None,
            traceback=None
    ):
        NodeConnection.__exit__(
            self, exception_type, exception_value, traceback
        )

    def __run(
        self, cmd, blocking=True, out_path=None, err_path=None,  **kwargs
    ):
        """Run an arbitrary command under Popen() either synchronously
        or asynchronously letting exceptions bubble up to the caller.

        """
        with logfile(out_path) as out_file, logfile(err_path) as err_file:
            if blocking:
                with Popen(
                        cmd,
                        stdout=out_file, stderr=err_file,
                        **kwargs
                ) as subprocess:
                    return wait_for_popen(
                        subprocess, cmd, (out_path, err_path), None, **kwargs
                    )
            else:
                return Popen(
                    cmd,
                    stdout=out_file, stderr=err_file,
                    **kwargs
                )

    def _render_cmd(self, cmd):
        """Layer private: render the specified command string with
        Jinja to fill in the BladeSSHConnection specific data in a
        templated command.

        """
        jinja_values = {
            'node_class': self.n_class,
            'instance': self.instance,
            'node_hostname': self.hostname,
            'remote_port': self.rem_port,
            'local_ip': self.loc_ip,
            'local_port': self.loc_port
        }
        return render_command_string(cmd, jinja_values)

    def copy_to(
            self, source, destination,
            recurse=False, blocking=True, logname=None, **kwargs
    ):
        logname = (
            logname if logname is not None else
            "copy-to-%s-%s" % (source, destination)
        )
        logfiles = log_paths(
            self.common.build_dir(),
            "%s-%s" % (logname, self.node_hostname())
        )
        recurse_option = ['-r'] if recurse else []
        cmd = [
            'scp', '-i', self.private_key_path,
            '-P', str(self.local_port()), *recurse_option, *self.options,
            source, 'root@%s:%s' % (self.loc_ip, destination),
        ]
        try:
            return self.__run(cmd, blocking, *logfiles, **kwargs)
        except ContextualError:
            # If it is one of ours just send it on its way to be handled
            raise
        except Exception as err:
            # Not one of ours, turn it into one of ours
            raise ContextualError(
                "failed to copy file '%s' to 'root@%s:%s' "
                "using command: %s - %s" % (
                    source, self.hostname, destination, str(cmd), str(err)
                ),
                *logfiles
            ) from err

    def copy_from(
        self, source, destination,
            recurse=False, blocking=True, logname=None, **kwargs
    ):
        logname = (
            logname if logname is not None else
            "copy-from-%s-%s" % (source, destination)
        )
        logfiles = log_paths(
            self.common.build_dir(),
            "%s-%s" % (logname, self.node_hostname())
        )
        recurse_option = ['-r'] if recurse else []
        cmd = [
            'scp', '-i', self.private_key_path,
            '-P', str(self.local_port()), *recurse_option, *self.options,
            'root@%s:%s' % (self.loc_ip, source), destination,
        ]
        try:
            return self.__run(cmd, blocking, *logfiles, **kwargs)
        except ContextualError:
            # If it is one of ours just send it on its way to be handled
            raise
        except Exception as err:
            # Not one of ours, turn it into one of ours
            raise ContextualError(
                "failed to copy file '%s' from 'root@%s:%s' "
                "using command: %s - %s" % (
                    destination, self.hostname, source, str(cmd), str(err)
                ),
                *logfiles,
            ) from err

    def run_command(self, cmd, blocking=True, logfiles=None, **kwargs):
        cmd = self._render_cmd(cmd)
        logfiles = logfiles if logfiles is not None else (None, None)
        ssh_cmd = [
            'ssh', '-i', self.private_key_path, *self.options,
            'root@%s' % (self.loc_ip), cmd
        ]
        try:
            return self.__run(ssh_cmd, blocking, *logfiles, **kwargs)
        except ContextualError:
            # If it is one of ours just send it on its way to be handled
            raise
        except Exception as err:
            # Not one of ours, turn it into one of ours
            raise ContextualError(
                "failed to run command '%s' on '%s' - %s" % (
                    cmd, self.hostname, str(err)
                ),
                *logfiles
            ) from err


class NodeSSHConnectionSet(NodeSSHConnectionSetBase, NodeConnectionSet):
    """Implementation of the NodeSSHConnectionSet Cluster Layer API
    Class.

    """
    def __init__(self, common, connections):
        "Constructor"
        # Make sure instances get a good Doc string, even though the
        # class doesn't
        self.__doc__ = NodeSSHConnectionSet.__doc__
        NodeConnectionSet.__init__(self, common, connections)

    def __enter__(self):
        return self

    def __exit__(
            self,
            exception_type=None,
            exception_value=None,
            traceback=None
    ):
        for connection in self.connections:
            connection.__exit__(exception_type, exception_value, traceback)

    def copy_to(
        self, source, destination, recurse=False, logname=None, node_class=None
    ):
        logname = (
            logname if logname is not None else
            "parallel-copy-to-node-%s-%s" % (source, destination)
        )
        # Okay, this is big and weird. It composes the arguments to
        # pass to wait_for_popen() for each copy operation. Note
        # that, normally, the 'cmd' argument in wait_for_popen() is
        # the Popen() 'cmd' argument (i.e. a list of command
        # compoinents. Here it is simply a descriptive string. This is
        # okay because wait_for_popen() only uses that information
        # for error generation.
        wait_args_list = [
            (
                node_connection.copy_to(
                    source, destination, recurse=recurse, blocking=False,
                    logname=logname
                ),
                "scp %s to root@%s:%s" % (
                    source,
                    node_connection.node_hostname(),
                    destination
                ),
                log_paths(
                    self.common.build_dir(),
                    "%s-%s" % (logname, node_connection.node_hostname())
                )
            )
            for node_connection in self.connections
            if node_class is None or
            node_connection.node_class() == node_class
        ]
        # Go through all of the copy operations and collect (if
        # needed) any errors that are raised by
        # wait_for_popen(). This acts as a barrier, so when we are
        # done, we know all of the copies have completed.
        errors = []
        for wait_args in wait_args_list:
            try:
                wait_for_popen(*wait_args)
            # pylint: disable=broad-exception-caught
            except Exception as err:
                errors.append(str(err))
        if errors:
            raise ContextualError(
                "errors reported while copying '%s' to '%s' on %s\n"
                "    %s" % (
                    source,
                    destination,
                    "all Virtual Nodes" if node_class is None else
                    "Virtual Nodes of class %s" % node_class,
                    "\n\n    ".join(errors)
                )
            )

    def run_command(self, cmd, logname=None, node_class=None):
        logname = (
            logname if logname is not None else
            "parallel-run-on-node-%s" % (cmd.split()[0])
        )
        # Okay, this is big and weird. It composes the arguments to
        # pass to wait_for_popen() for each copy operation. Note
        # that, normally, the 'cmd' argument in wait_for_popen() is
        # the Popen() 'cmd' argument. Here is is simply the shell
        # command being run under SSH. This is okay because
        # wait_for_popen() only uses that information for error
        # generation.
        wait_args_list = [
            (
                node_connection.run_command(
                    cmd, False,
                    log_paths(
                        self.common.build_dir(),
                        "%s-%s" % (logname, node_connection.node_hostname())
                    )
                ),
                cmd,
                log_paths(
                    self.common.build_dir(),
                    "%s-%s" % (logname, node_connection.node_hostname())
                )
            )
            for node_connection in self.connections
            if node_class is None or
            node_connection.node_class() == node_class
        ]
        # Go through all of the copy operations and collect (if
        # needed) any errors that are raised by
        # wait_for_popen(). This acts as a barrier, so when we are
        # done, we know all of the copies have completed.
        errors = []
        for wait_args in wait_args_list:
            try:
                wait_for_popen(*wait_args)
            # pylint: disable=broad-exception-caught
            except Exception as err:
                errors.append(str(err))
        if errors:
            raise ContextualError(
                "errors reported running command '%s' on %s\n"
                "    %s" % (
                    cmd,
                    "all Virtual Nodes" if node_class is None else
                    "Virtual Nodes of class %s" % node_class,
                    "\n\n    ".join(errors)
                )
            )


class Addressing(AddressingBase):
    """Addressing information for node and blade classes. This
    contains all addressing by address family for instances of node
    class or blade classes as assigned at the cluster level.

    """
    def __init__(self, connected_instances, address_families):
        """Constructor
        """
        self.connected_instances = connected_instances.copy()
        # Our local address_families contains, for each address
        # family, an expanded list of addresses indexed by instance
        # number. The incoming address_families contains a compressed
        # list of addresses that lines up with the list of instances
        # in the object. Expand the list here, filling in None for any
        # instance numbers that are not in the list of instances.
        tmp = self.connected_instances.copy()
        address_families = address_families.copy()  # So we can muck with it...
        tmp.sort()  # So the highest numbered instance is in [-1]
        top_instance = tmp[-1]
        self.families = {
            family['family']: [
                family['addresses'].pop(0)
                if instance in self.connected_instances else None
                for instance in range(0, top_instance + 1)
            ]
            for family in address_families
            if 'family' in family and 'addresses' in family
        }

    def address(self, family, instance):
        return (
            self.addresses(family)[instance]
            if instance in self.connected_instances
            else None
        )

    def addresses(self, family):
        return self.families.get(family, [])

    def address_families(self):
        return [
            family
            for family, _ in self.families.items()
        ]

    def instances(self):
        return self.connected_instances.copy()
