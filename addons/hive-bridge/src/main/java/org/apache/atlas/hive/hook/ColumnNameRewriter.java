/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive.hook;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is response for loading lineage related config file.
 * Then rewrite the qualified column name.
 *
 * Note that only the lineage related partition info should be config.
 *
 * Eg:
 *
 * If a table has two partition keys: dt and dp, only dp should appear in
 * config file.
 *
 * WARNING: Only support one level of lineage related partition
 */
public class ColumnNameRewriter {

    private static final Logger logger = LoggerFactory.getLogger(ColumnNameRewriter.class);

    private Map<String, HiveTableEntity> hiveTableEntityMap;

    public ColumnNameRewriter(Map<String, HiveTableEntity> hiveTableEntityMap) {
        this.hiveTableEntityMap = hiveTableEntityMap;
    }

    public ColumnNameRewriter() {
        init();
    }

    public List<String> getLineagePartitionValues(Table table) {
        return getLineagePartitionValues(getTableFullName(table));
    }

    public List<String> getLineagePartitionValues(org.apache.hadoop.hive.metastore.api.Table table) {
        return getLineagePartitionValues(getTableFullName(table));
    }

    public List<String> getLineagePartitionValues(String hiveTableName) {
        return hiveTableEntityMap.get(hiveTableName).getLineagePartitionValues();
    }

    public String getLineagePartitionName(Table table) {
        return getLineagePartitionName(getTableFullName(table));
    }

    public String getLineagePartitionName(org.apache.hadoop.hive.metastore.api.Table table) {
        return getLineagePartitionName(getTableFullName(table));
    }

    public String getLineagePartitionName(String hiveTableName) {
        return hiveTableEntityMap.get(hiveTableName).getLineagePartition();
    }

    public boolean isLineagePartitioned(Table table) {
        return isLineagePartitioned(getTableFullName(table));
    }

    public boolean isLineagePartitioned(org.apache.hadoop.hive.metastore.api.Table table) {
        return isLineagePartitioned(getTableFullName(table));
    }

    /**
     * Returns if the hive table has lineage related partition.
     */
    public boolean isLineagePartitioned(String hiveTableName) {
        return hiveTableEntityMap.containsKey(hiveTableName);
    }

    public String getTableFullName(Table table) {
        return getTableFullName(table.getDbName(), table.getTableName());
    }

    public String getTableFullName(org.apache.hadoop.hive.metastore.api.Table table) {
        return getTableFullName(table.getDbName(), table.getTableName());
    }

    public String getTableFullName(String dbName, String tableName) {
        return dbName + "." + tableName;
    }

    public void init() {
        // TODO: 第一版先从hdfs中读入信息了.
        Configuration conf = new Configuration();

        conf.set("dfs.replication", "3");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFS", "hdfs://ns1018");//指定hdfs的nameservice为cluster1,是NameNode的URI
        conf.set("dfs.nameservices", "ns1018");//指定hdfs的nameservice为cluster1
        conf.set("dfs.ha.namenodes.ns1018", "nna,nns");//cluster1下面有两个NameNode，分别是nna，nns
        conf.set("dfs.namenode.rpc-address.ns1018.nna", "10.198.37.194:8020");//nna的RPC通信地址
        conf.set("dfs.namenode.rpc-address.ns1018.nns", "10.198.35.107:8020");//nns的RPC通信地址
        conf.set("dfs.client.failover.proxy.provider.ns1018",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        Path path = new Path("file:///Users/congyihao/DevelopmentTools/apache-atlas-1.1.0/apache-atlas-1.1.0-hive-hook/apache-atlas-hive-hook-1.1.0/hook/hive/atlas_hive_hook_config.json");
        FileSystem fs = null;
        FSDataInputStream inputStream = null;
        String hiveTableEntitiesStr = null;
        try {
            fs = path.getFileSystem(conf);
            inputStream = fs.open(path);
            hiveTableEntitiesStr = IOUtils.toString(inputStream, "UTF-8");
            logger.info(hiveTableEntitiesStr);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        List<HiveTableEntity> hiveTableEntities = JSON.parseArray(hiveTableEntitiesStr, HiveTableEntity.class);

        hiveTableEntityMap = new HashMap<>();
        for (HiveTableEntity entity: hiveTableEntities) {
            hiveTableEntityMap.put(entity.getTableName(), entity);
        }
    }

    public Map<String, HiveTableEntity> getHiveTableEntityMap() {
        return hiveTableEntityMap;
    }
}
