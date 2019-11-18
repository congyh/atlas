# Config loading strategy

1. If config file (`atlas-application.properties`) can be find in local path, use it directly;
2. Else pull from remote path set on `ApplicationProperties.java`.

For test use, you should always put `atlas-application-test.properties` in the same path of test script and rename it to `atlas-application.properties` to avoid pulling config from remote. Because so far the config path is hard coded in `ApplicationProperties.java`.

## Example for testing/debugging

Say your script locate in `~/congyihao/data-lineage/`. Then the config file should also in this directory.

Add following lines to the header of test sql:

```sql
ADD jar hdfs://ns1018/user/jd_ad/ads_app/data_lineage/hive/test/atlas-plugin-classloader-2.0.0.jar;
ADD jar hdfs://ns1018/user/jd_ad/ads_app/data_lineage/hive/test/hive-bridge-shim-2.0.0.jar;
ADD jar hdfs://ns1018/user/jd_ad/ads_app/data_lineage/hive/test/atlas-hive-plugin-impl;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.atlas.hive.hook.HiveHook;
```

Then fire test like this:

`hive -f <script_name>.sql`

Or fire debug like this:

`hive --debug -f <script_name>.sql`

Note:

1. You should update jar in remote before testing or debugging.
1. In addition, you should build a ssh tunnel before debugging:

    `ssh congyihao@jps.jd.com -p 80 -L localhost:8000:10.198.47.106:8000`