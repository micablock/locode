import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


with open('demo.env') as f:
  d = dict(x.rstrip().split(':', 1) for x in f)
  secure_connect_bundle = d.get('secure_connect_bundle')
  userId = d.get('userId')
  passWord = d.get('passWord')
  keySpace = d.get('keySpace')


cloud_config= {
        'secure_connect_bundle': secure_connect_bundle
}
tblCQL = "SELECT keyspace_name, table_name, writetime(id) AS created_on \
            FROM system_schema.tables \
          WHERE keyspace_name = '%s'" %(keySpace)
auth_provider = PlainTextAuthProvider(userId, passWord)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

rows_tbl = session.execute(tblCQL)
for tbl in rows_tbl:
    print ('table:', tbl.table_name, '\tcreated on:',
        time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(tbl.created_on/1000000.0)))
cluster.shutdown()
