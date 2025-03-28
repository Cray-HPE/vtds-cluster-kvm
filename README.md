# vtds-cluster-kvm

The kvm vTDS Cluster layer plugin implementation

## Description

This repository contains the implementation of a vTDS Cluster layer
plugin that should be usable by any vTDS configuration to create a
vTDS cluster. The plugin includes an implementation of the vTDS
Cluster layer API and a base configuration. The API implementation can
be used on top of any combination of vTDS Provider and vTDS Platform
implementations to manage a vTDS system at the cluster level. The base
configuration supplied here. The base configuration defines a single
node class (`ubuntu_24_04_node`) with zero node instances. To get a
maningful result, therefore, it is necessary to overlay the
the value in `cluster.node_classes.ubuntu_24_04_node.node_count` with
a non-zero value. With that overlay, the base configuration will
deploy the requested number of KVM Linux VM Virtual Nodes running
on a Virtual Node Interconnect using a VxLAN overlay
over the top of the configured set of provider and
platform supplied Blade Interconnect network underlays.

The core driver mechanism and a brief introduction to the vTDS
architecture and concepts can be found in the [vTDS Core Project
Repository](https://github.com/Cray-HPE/vtds-core/tree/main).
