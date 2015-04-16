These plugins have been useful for us, and hopefully will be useful
for others.

discard is an applier that we use to keep track of the location in the
replication stream when we are not using Tungsten for actual replication.

pkpublish is a filter that publishes the primary keys of selected rows
and/or transactions to a message queue where they can be consumed in an
event-driven manner.
