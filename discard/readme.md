Drop Everything Tungsten Applier Plugin
=======================================

This Applier plugin discards every event it receives.  It keeps track of the
location in the input stream so that the replicator can startup at the
appropriate location afterwards.

# Dependencies

The following libraries are needed to build the DiscardApplier:
 * log4j-1.2.16.jar
 * tungsten-fsm.jar
 * tungsten-replicator.jar

Eventually, we will use Maven to manage these dependencies.

# Building the Plugin

    ant package

Creates the file `DiscardApplier.jar`.

# Deploying the Plugin

Copy the `DiscardApplier.jar` file to the tungsten `lib/` directory.

Potentially copy the contents of `lib/` to the tungsten `lib/` directory.

# Warnings

Do not use this plugin if you want tungsten to actually do replication!

The _ONLY_ reason to use this plugin is if you do not want replication events
to propagate further.
