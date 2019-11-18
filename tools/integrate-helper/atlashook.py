#!/usr/bin/env python3
"""
AtlasHook for Integrating Hive with Apache Atlas.

WARNING: This script is only used for adapting common script in JD company like:

- HiveTask.py
- SparkTask.py
- ads_hive.py

You should refer to the origin script to check the usage for specific method.
"""
import os,sys

sys.path.append(os.getenv('HIVE_TASK'))
sys.path.append(os.getenv("CODELIB")+"/mart_szad")
import ads_hive as adshive
from HiveTask import HiveTask as ht
from SparkTask import SparkTask as st

__all__ = ['HiveTask', 'SparkTask', 'ads_hive']


sql_head = """
    ADD jar hdfs://ns1018/user/jd_ad/ads_app/data_lineage/hive/atlas-plugin-classloader-2.0.0.jar;
    ADD jar hdfs://ns1018/user/jd_ad/ads_app/data_lineage/hive/hive-bridge-shim-2.0.0.jar;
    ADD jar hdfs://ns1018/user/jd_ad/ads_app/data_lineage/hive/atlas-hive-plugin-impl;

    SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.atlas.hive.hook.HiveHook;

    """


class HiveTask(ht):
    def exec_sql(self, schema_name, sql, *args, **kwargs):
        """Add atlas hive hook before exec_sql in HiveTask"""
        sql = sql_head + sql
        super().exec_sql(schema_name, sql, *args, **kwargs)


class SparkTask(st):
    def exec_sql(self, schema_name, sql, *args, **kwargs):
        """Add atlas hive hook before exec_sql in SparkTask"""
        sql = sql_head + sql
        super().exec_sql(schema_name, sql, *args, **kwargs)


class Process(adshive.Process):
    def execute(self, schema_name, table_name,hql, *args, **kwargs):
        """Add atlas hive hook before execute in ads_hive.Process"""
        hql = sql_head + hql
        super().execute(schema_name, table_name,hql, *args, **kwargs)


adshive.Process = Process
ads_hive = adshive