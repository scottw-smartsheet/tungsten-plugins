Primary Key to Message Queue
============================

A Tungsten Replicator Filter plugin to send primary keys of replicated rows
to a RabbitMQ message queue.

__NOTE: this filter plugin is currently under relatively heavy churn, and may not work corectly.__

# Dependencies

The plugin currently supports Tungsten 2.2.1.  It may work with other
versions of the Tungsten Replicator, but it has not been tested with them.

The following libraries are needed to build the plugin:

 * jackson-annotations-2.3.3.jar
 * jackson-core-2.3.4.jar
 * jackson-databind-2.3.4.jar
 * javametrics.jar
 * javametrics-json.jar
 * log4j-1.2.16.jar
 * mysql-connector-java-5.1.6WithEscapeProcessingFix-bin.jar
 * rabbitmq-client.jar
 * slf4j-api-1.7.7.jar
 * slf4j-simple-1.7.7.jar
 * tungsten-fsm.jar
 * tungsten-fsm-src.jar
 * tungsten-replicator.jar

That list of libraries might be overkill.  Eventually, we will use Maven
to manage the build and dependencies.

# Building the Plugin

    ant package

Creates the file PKPublish.jar.  The libaries it depends on are expected to
be found in the `./lib` directory.

# Deploying the Plugin

Copy the `PKPublish.jar` file to the tungsten `lib/` directory.  It might
work to instead copy the JAR file to the tungsten `lib-ext/` directory,
but I've had some issues with that in the past and need to test it again
-- since that is actually the *right* thing to do.

In addition to the `PKPublish.jar` file, there are a number of dependency JAR
files in the `lib/` directory.  Those need to be copied to the tungsten `lib/`
directory as well.

# Plugin Configuration

The replicator plugin is configured using the tungsten properties file and
a JSON file specifying rules for which events the filter will match.

    replicator.filter.pkpublish=com.smartsheet.tin.filters.pkpublish.PKPublish
    replicator.filter.pkpublish.ruleFile=/tmp/pkpublish.rules.json
    replicator.filter.pkpublish.dbUrl=jdbc:mysql:thin://${replicator.global.extract.db.host}:${replicator.global.extract.db.port}/
    replicator.filter.pkpublish.dbUser= ${replicator.global.extract.db.user}
    replicator.filter.pkpublish.dbPassword= ${replicator.global.extract.db.password}
    replicator.filter.pkpublish.exchangeName=db_events
    replicator.filter.pkpublish.exchangeType=topic
    replicator.filter.pkpublish.messageQueueServerName=mq.gic.smartsheet.com
    replicator.filter.pkpublish.messaqeQueueVHost=event
    replicator.filter.pkpublish.statusMessageInterval=60
    
The name given to the filter is the section just after "`replicator.filter.`".
In the case of this example, the name is "`pkpublish`".

The `.ruleFile` value specifies the JSON file the plugin should load its
filtering rules from.

The `.ruleFileCheckInterval` is used to specify how frequently, in seconds,
the rule file should be checked for modifications.  The "clock" used is the
timestamp from each of the `DBMSReplEvent`s.

The `.dbUrl` value specifies the URL used to connect to the DB that has the
table and schema info for the events that are being processed.  Generally,
this will be the master (the DB that events are being 'extract'ed from).  In
the example, we are connecting to whatever DB is defined as the master in
the properties file.

The `.dbUser` value specifies the user to connect to `.dbUrl` as.  In the
example, this is the same as the globally configured extracting user, (which
will typically be 'tungsten').  However, since this user's permissions needs
are lower (`SELECT on *.*`) it is probably prudent to use a separate DB user.

The `.dbPassword` value is the password for the `.dbUser`.  In the example,
we are using the globally configured extracting user's password.

The `.exchangeName` is the name of the message queue exchange the filtered
events should be published to.

The `.exchangeType` is the RabbiteMQ type.  In most cases this should be
`topic` or `direct`, or, in some cases, `fanout`.  Default is `direct`.

The `.messageQueueServerName` is the name of the message queue in DNS.
An IP address is probably also valid here.

If you run RabbitMQ on a nonstandard port, the property
`.messageQueueServerPort` can be used to specify the port.

The `.messageQueueVHost` is the name of the virtual host to use.  If not
specified, uses the default virtual host `/`.

If you wish to lie to yourself about the actual durability of the events
written to the message queue, you can do so by setting
`.messageQueueIsDurable` to true.  You won't acutally get truly durable
events, but they'll frequently be durable.  Probably just often enough to
begin to encourage bad designs that assumes the message are durable.

Finally, when the plugin shuts down, it can wait for a while to finish
delivering messages to the message queue.  This wait time, in seconds,
is specified with the `.messageQueueCloseTimeout` parameter.

The frequency with which internal filter metrics are reported (by log file
and message queue) is given with `.statusMessageInterval` in seconds.

## Loading the Plugin

In addition to configuring the plugin, the properties file is how the
plugin is loaded and has it's order-of-call defined.  The plugin must be
added to the desired *stage*.  In the example below, the plugin is added
to the `thl-to-q` stage, after the `logger` plugin.


    replicator.stage.thl-to-q.filters=logger,pkpublish


# Filtering Rules Specification

The filter rules allow the stream of `ReplDBMSEvents` to be partioned out in
several useful ways.  The filter rules specify the schema/database name,
table name, and type of change (`INSERT`, `UPDATE`, or `DELETE`) that the
filter should send to the message queue.

The schema and table name values can also specify the `"*"` wild card,
which will match any schema or table name.  In addition, the change
type can be specified as a list, `["INSERT", "UPDATE"]` or using the
`"*"` wild card.

TODO: These example filters need help.
```
{
    "transaction_filters": [
        {
            "name": "Add User",
            "must_match_all_filters": true,
            "must_match_all_rows": true,
            "actions": [
                "publish": true,
                "routing_key": "add user",
                "message": "My hardcoded message"
            ],
            "row_filters": [
                {
                    "row_pattern": {
                        "schema": "videostore",
                        "table": "user",
                        "change_types": ["INSERT"]
                    },
                    "include": true,
                    "actions": [
                        {
                            "type": "publish",
                        }
                    ]
                },
                {
                    "include": False,
                    "row_pattern": {
                        "schema": "videostore",
                        "table": "paymentInfo",
                        "change_types": ["INSERT", "UPDATE"]
                    },
                },
                {
                    "row_pattern": {
                        "schema": "videostore",
                        "table": "contactInfo",
                        "change_types": ["INSERT"]
                    },
                    "actions": [
                        {
                            "type": "publish",
                            "routing_key": "user.contact",
                            "message:", "A user set their contact info."
                        }
                    ]
                }
            ]
        },

        {
            "name": "Delete Things",
            "filter_match_type": "ANY",
            "row_match_type": "ANY"

        }
    ],
    "row_filters": [
    ]
}
```

If a `routing_key` for a filter rule is not specified, then it will be
constructed as: `<schema name>.<table name>.<change type>`.  Each changed
row results in a message published with the computed or specified
routing key.


# Logging

Logging for the filter plugin is configured in the `log4j.properties` file
in the `conf/` directory.  The block below will cause the filter to log
`DEBUG` and up messages to `log/pkpublish.log`.  The metrics that are logged
are written as JSON, for hopefully easy ingesting by `logstash`.

    log4j.appender.pkpublishlog=org.apache.log4j.RollingFileAppender
    log4j.appender.pkpublishlog.File=${replicator.log.dir}/pkpublish.log
    log4j.appender.pkpublishlog.MaxFileSize=10MB
    log4j.appender.pkpublishlog.layout=org.apache.log4j.PatternLayout
    log4j.appender.pkpublishlog.layout.ConversionPattern=%d [%t] %-5p %c{1} %m\n
    # Turn on debugging for PKPublish.
    log4j.logger.com.smartsheet.tin.filters.pkpublish=DEBUG,pkpublishlog

# Metrics

Metrics are captured for a variety of internal states and events, some that
are errors, and others that are just informational.  Metrics are published
periodically to the `PKPublish` log, at level `INFO`.  They are also
published to the message queue with the routing key `pkpublish.stats`.

Some of the more relevant metrics are:

    totalEventCount - # of transactions seen (assuming no fragmentation)
    errorCount - # of errors (of any type) detected
    publishingErrorCount - # of errors publishing to the message queue
    dbConnectErrorCount - # of failures connecting to the DB
    ruleFileReloadErrorCount - # of failures reading the rule file
    totalEventsThisReport - # of events since the last report was generated
    totalErrorsThisReport - # of errors since the last report was generated
    reportStartTime - Timestamp (in milliseconds) of start of covered time
    reportEndTime - Timestamp (in milliseconds) of end of covered time


# Example message consumer

The script below uses the pika library (`pip install pika`) to get messages
from a message queue.  With the PKPublish set to publish messages to the
same exchange using the same routing key, this script will write the
received messages to stdout.

```
#!/usr/bin/env python
import pika
import json

creds = pika.PlainCredentials('username', 'password')

conn = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost', credentials=creds))

channel = conn.channel()
channel.exchange_declare(exchange='my_exchange', exchange_type='topic',
    durable=True)

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

def cb(ch, method, properties, body):
    print "ch:", ch, "method:", method, "properties:", properties, "body:", body;
    try:
        obj = json.loads(body)
        print "    Loaded body as JSON!"
        print obj
    except Exception, e:
        print "    Error loading body as JSON.", str(e)

channel.queue_bind(exchange='my_exchange', queue=queue_name, routing_key='test.foo')
channel.basic_consume(cb, queue=queue_name, no_ack=True)
channel.start_consuming()
```

# TODO 

 * Revisit TransactionFilter's match requirments
   * Make the difference between ALL or ANY more clear in docs and code
 * Internally, actions should probably implement some sort of visitor pattern.
 * Add the ability to define the messages using a template.
   * Include ability to reference fields from the transaction/rows and filter
 * The ORCFormatter class probably needs to be renamed.
 * Document the rest of the metrics.
 * Determine which metrics should be captured that aren't.
 * Identify extraneous metrics that don't need to be captured.




