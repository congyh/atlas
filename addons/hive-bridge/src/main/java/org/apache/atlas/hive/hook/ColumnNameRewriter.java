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

//import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.map.HashedMap;
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
 *
 * TODO: 第一版首先假装能够拿到dp的具体值.
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

    public List<String> getPartitionValueForHiveTable(String hiveTableName) {
        List<String> partitionValueList = new ArrayList<>();
        if (hiveTableName.equals("dim.dim_test_table_with_dp_level1")) {
            partitionValueList.add("RTB");
        } else if (hiveTableName.equals("dim.dim_test_table_with_dp_level2")) {
            partitionValueList.add("GDT");
        } else if (hiveTableName.equals("dim.dim_test_table_with_dp_level3")) {
            partitionValueList.add("GDT");
        } else {
            throw new RuntimeException("Unsupported table!");
        }

        return partitionValueList;
    }

    public List<String> getLineagePartitionValues(String hiveTableName) {
        return hiveTableEntityMap.get(hiveTableName).getLineagePartitionValues();
    }


    public Set<String> getPartitionsForHiveTable(String hiveTableName) {
        HiveTableEntity hiveTableEntity = hiveTableEntityMap.get(hiveTableName);
        return new HashSet<>(hiveTableEntity.getPartitions());
    }

    public String getPartitionName(String hiveTableName) {
        // TODO: 暂时仅支持一个
        return hiveTableEntityMap.get(hiveTableName).getLineagePartition();
    }

    private void init() {
        // TODO: 暂时移除fastjson依赖
        String hiveTableEntitiesStr = "[{\"hive_table_name\": \"dim.dim_test_table_with_dp_level1\", \"hive_lineage_partitions\": \"dp\"}, {\"hive_table_name\": \"dim.dim_test_table_with_dp_level2\", \"hive_lineage_partitions\": \"dp\"}, {\"hive_table_name\": \"dim.dim_test_table_with_dp_level3\", \"hive_lineage_partition\": \"dp\"}]";
        HiveTableEntity hiveTableEntity1 = new HiveTableEntity(
                "dim.dim_test_table_with_dp_level1",
                "dp",
                Arrays.asList("dt", "dp"),
                Arrays.asList("RTB", "GDT", "CPS"));
        HiveTableEntity hiveTableEntity2 = new HiveTableEntity("dim.dim_test_table_with_dp_level2",
                "dp",
                Arrays.asList("dt", "dp"),
                Arrays.asList("RTB", "GDT", "CPS"));
        HiveTableEntity hiveTableEntity3 = new HiveTableEntity(
                "dim.dim_test_table_with_dp_level3",
                "dp",
                Arrays.asList("dt", "dp"),
                Arrays.asList("RTB", "GDT", "CPS"));
//        List<HiveTableEntity> hiveTableEntities = JSON.parseArray(hiveTableEntitiesStr, HiveTableEntity.class);
        List<HiveTableEntity> hiveTableEntities = new ArrayList<>();
        hiveTableEntities.add(hiveTableEntity1);
        hiveTableEntities.add(hiveTableEntity2);
        hiveTableEntities.add(hiveTableEntity3);
        hiveTableEntityMap = new HashMap<>();
        for (HiveTableEntity entity: hiveTableEntities) {
            hiveTableEntityMap.put(entity.getTableName(), entity);
        }
    }
}
