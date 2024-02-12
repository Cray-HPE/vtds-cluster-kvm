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

from vtds_base import (
    ContextualError,
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
        self.stack = stack
        self.build_dir = build_dir
        self.prepared = False

    def prepare(self):
        """Prepare operation. This drives creation of the cluster
        layer definition and any configuration that need to be driven
        down into the cluster layer to be ready for deployment.

        """
        self.prepared = True
        print("Preparing vtds-cluster-kvm")

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
        print("Deploying vtds-cluster-kvm")

    def remove(self):
        """Remove operation. This will remove all resources
        provisioned for the cluster layer.

        """
        if not self.prepared:
            raise ContextualError(
                "cannot deploy an unprepared cluster, call prepare() first"
            )
        print("Removing vtds-cluster-kvm")
