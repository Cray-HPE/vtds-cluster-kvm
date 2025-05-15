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
"""Module initialization

"""

from os.path import (
    join as path_join,
    dirname
)
CONFIG_DIR = path_join(dirname(__file__), 'config')
DEPLOY_SCRIPT_NAME = 'deploy_cluster_to_blade.py'
SCRIPTS_DIR = path_join(dirname(__file__), 'scripts')

DEPLOY_SCRIPT_PATH = path_join(SCRIPTS_DIR, DEPLOY_SCRIPT_NAME)
CLUSTER_SCRIPT_LIBS = [
    (
        path_join(SCRIPTS_DIR, "cluster_common.py"),
        "/root/cluster_common.py",
        "cluster_common"
    ),
    (
        path_join(SCRIPTS_DIR, "node_builder.py"),
        "/root/node_builder.py",
        "node_builder"
    ),
    (
        path_join(SCRIPTS_DIR, "disk_builder.py"),
        "/root/disk_builder.py",
        "disk_builder"
    ),
    (
        path_join(SCRIPTS_DIR, "kickstart.py"),
        "/root/kickstart.py",
        "kickstart"
    ),
]
VM_XML_PATH = path_join(
    CONFIG_DIR,
    'virtual_node_template.xml'
)
