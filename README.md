# vtds-cluster-common
The common vTDS Cluster layer plugin implementation
## Description
This repository contains the implementation of a vTDS Cluster layer plugin that should
be usable by any vTDS configuration to create a vTDS cluster. The plugin includes an
implementation of the vTDS Cluster layer API and a base configuration. The API
implementation can be used on top of any combination of vTDS Provider and vTDS Platform
implementations to manage a vTDS system at the cluster level. The base configuration
supplied here, if used unchanged, will create a cluster of 5 Ubuntu Linux VM Virtual Nodes
running on a Virtual Node Interconnect using a VxLAN overlay over the top of a provider and
platform supplied Blade Interconnect network underlay. Application specific configuration
like IP Addresses on the Virtual Node Interconnect, routing into the Virtual Node Interconnect,
virtual IP addresses, external network connectivity, host-names, and so forth are driven from
the Application layer of the vTDS architecture.

The core driver mechanism and a brief introduction to the vTDS architecture and concepts can be
found in the [vTDS Core Project Repository](https://github.com/Cray-HPE/vtds-core/tree/main).
