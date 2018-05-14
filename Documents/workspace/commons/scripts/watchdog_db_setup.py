import sys
from moengage.commons.connections import ConnectionUtils
from moengage.commons.utils.common import CommonUtils


def setup_influx_database(database):
    # Create database
    print "Setting up influx database: ", database
    influx_client = ConnectionUtils.getInfluxClient(database)
    print "Creating influx database: ", database
    try:
        influx_client.create_database(database)
        print "Successfully created influx database: ", database
    except Exception, e:
        print "Failed to create influx database: %s due to exception: %r" % (database, e)
        sys.exit(1)

    # Create retention policies
    print "Creating retention policies on database"
    retention_policy = "seven_days"
    try:
        print "Retention Policy: ", retention_policy
        influx_client.create_retention_policy(retention_policy, "7d", "1 SHARD DURATION 1h", default=True, database=database)
    except Exception, e:
        print CommonUtils.view_traceback()
        print "Failed to create retention policy: %s on database: %s due to exception: %r" % (retention_policy, database, e)
        sys.exit(1)

    retention_policy = "one_year"
    try:
        print "Retention Policy: ", retention_policy
        influx_client.create_retention_policy(retention_policy, "52w", "1 SHARD DURATION 24h", database=database)
    except Exception, e:
        print "Failed to create retention policy: %s on database: %s due to exception: %r" % (retention_policy, database, e)
        sys.exit(1)

    # Create continuous queries
    source_rp = "seven_days"
    destination_rp = "one_year"
    query = 'CREATE CONTINUOUS QUERY "cq_basic_br" ON {database} BEGIN SELECT mean(*), sum(*), count(*), min(*), max(*) ' \
            'INTO "{database}"."{destination_rp}".:MEASUREMENT FROM {database}.{source_rp}./.*/ ' \
            'GROUP BY time(1h),* END'.format(database=database, source_rp=source_rp, destination_rp=destination_rp)
    print "*****************************Creating continuous query************************************"
    print query
    print "*****************************Creating continuous query************************************"
    try:
        influx_client.query(query)
    except Exception, e:
        print "Failed to create continuous query on database: %s due to exception: %r" % (database, e)
        sys.exit(1)
