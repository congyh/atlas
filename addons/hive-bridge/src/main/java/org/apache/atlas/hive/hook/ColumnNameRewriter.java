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
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
        // TODO: 第一版先直接内置测试代码了, 后面要通过文件解析的方式进行.
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

    private void init() {
        String hiveTableEntitiesStr = "[{\"table_name\": \"dim.dim_test_table_with_dp_level1\", \"lineage_partition\": \"dp\", \"lineage_partition_values\": [\"RTB\", \"GDT\", \"CPS\"]}, {\"table_name\": \"dim.dim_test_table_with_dp_level2\", \"lineage_partition\": \"dp\", \"lineage_partition_values\": [\"RTB\", \"GDT\", \"CPS\"]}, {\"table_name\": \"dim.dim_test_table_with_dp_level3\", \"lineage_partition\": \"dp\", \"lineage_partition_values\": [\"RTB\", \"GDT\", \"CPS\"]},{\"table_name\": \"dim.dim_test_table_with_pt_level1\", \"lineage_partition\": \"pt\", \"lineage_partition_values\": [\"RTB\", \"GDT\", \"CPS\"]}, {\"table_name\": \"dim.dim_test_table_with_pt_level2\", \"lineage_partition\": \"pt\", \"lineage_partition_values\": [\"RTB\", \"GDT\", \"CPS\"]}, {\"table_name\": \"dim.dim_test_table_with_pt_level3\", \"lineage_partition\": \"pt\", \"lineage_partition_values\": [\"RTB\", \"GDT\", \"CPS\"]}]";

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
