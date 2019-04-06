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

import org.apache.hadoop.hive.ql.hooks.ReadEntity;

import java.util.*;

public class LineageInfoParser {

    private Map<String, HiveTableEntity> hiveTableEntityMap;

    public LineageInfoParser(Map<String, HiveTableEntity> hiveTableEntityMap) {
        this.hiveTableEntityMap = hiveTableEntityMap;
    }

    /**
     * Returns if the hive table has lineage related partition.
     */
    public boolean isLineagePartitioned(String hiveTableName) {
        return hiveTableEntityMap.containsKey(hiveTableName);
    }

    /**
     * 解析sql中的input表里面带血统分区列的表的血统分区具体值
     *
     * 注意: 这个方法在一个sql执行中被调用一次就可以了.
     *
     * @param inputs input表
     * @return 以待血统分区列的表名为key, 血统分区具体值list为value的map
     */
    public Map<String, List<String>> getInputLineagePartitionValues(Set<ReadEntity> inputs) {
        Map<String, List<String>> ret = new HashMap<>();

        for (ReadEntity input: inputs) {
            String tableFullName = getTableFullNameFromInput(input);
            if (isLineagePartitioned(tableFullName)) {
                String lineagePartitionName = hiveTableEntityMap.get(tableFullName).getLineagePartition();
                // 如果在lineage table map中, 那么进行partition value值的获取.
                String actualLineagePartitionValue =
                        getActualLineagePartionValue(tableFullName, lineagePartitionName, input);

                if (ret.containsKey(tableFullName)) {
                    ret.get(tableFullName).add(actualLineagePartitionValue);
                } else {
                    List<String> actualLineagePartitionValues = new ArrayList<>();
                    actualLineagePartitionValues.add(actualLineagePartitionValue);
                    ret.put(tableFullName, actualLineagePartitionValues);
                }
            }
        }

        return ret;
    }

    private String getActualLineagePartionValue(String tableFullName, String lineagePartitionName, ReadEntity input) {
        // TODO: fake first
        return "";
    }

    private String getTableFullNameFromInput(ReadEntity inputs) {

        // TODO: fake first
        return "";
    }
}
