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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Partition;

import java.util.*;

public class LineageInfoParser {


    private HiveLineageTableInfo rewriter;

    public LineageInfoParser(HiveLineageTableInfo rewriter) {
        this.rewriter = rewriter;
    }

    /**
     * Returns if the hive table has lineage related partition.
     */
    private boolean isLineagePartitioned(String hiveTableName) {
        return rewriter.isLineagePartitioned(hiveTableName);
    }

    /**
     * 解析sql中的input表里面带血统分区列的表的血统分区具体值
     * <p>
     * 注意: 这个方法在一个sql执行中被调用一次就可以了.
     *
     * @param inputs input表
     * @return 以待血统分区列的表名为key, 血统分区具体值list为value的map
     */
    public Map<String, List<String>> getInputLineagePartitionValues(Set<ReadEntity> inputs) {
        Map<String, List<String>> ret = new HashMap<>();
        Set<String> knownPartitionValues = new HashSet<>();

        for (ReadEntity input : inputs) {
            if (input.getType() == Entity.Type.PARTITION) {
                String tableFullName = getTableFullNameFromInput(input);
                if (isLineagePartitioned(tableFullName)) {
                    String lineagePartitionName = rewriter.getLineagePartitionName(tableFullName);
                    // 如果在lineage table map中, 那么进行partition value值的获取.
                    String actualLineagePartitionValue =
                            getActualLineagePartitionValue(input, lineagePartitionName);

                    // 多层分区情况下去重
                    if (knownPartitionValues.add(tableFullName + actualLineagePartitionValue)) {
                        if (ret.containsKey(tableFullName)) {
                            ret.get(tableFullName).add(actualLineagePartitionValue);
                        } else {
                            List<String> actualLineagePartitionValues = new ArrayList<>();
                            actualLineagePartitionValues.add(actualLineagePartitionValue);
                            ret.put(tableFullName, actualLineagePartitionValues);
                        }
                    }
                }
            }
        }

        return ret;
    }

    /**
     * 从input partition中获取血统分区列的值
     * <p>
     * 注意: 本调用需要由调用方保证传入的ReadEntity是partition类型的.
     *
     * @param input                input table
     * @param lineagePartitionName 血统分区列列名
     * @return 血统分区列值
     */
    private String getActualLineagePartitionValue(ReadEntity input, String lineagePartitionName) {
        List<FieldSchema> partCols = input.getPartition().getTable().getPartCols();
        Partition part = input.getPartition();

        String ret = null;

        for (int i = 0; i < partCols.size(); i++) {
            if (partCols.get(i).getName().equals(lineagePartitionName)) {
                ret = part.getValues().get(i);
                break;
            }
        }

        return ret;
    }

    /**
     * 从input table/partition中获取表名
     * <p>
     * 注意: 本调用需由调用方保证传入的ReadEntity是table/partition
     *
     * @param input input table/partition
     * @return 表全名
     */
    private String getTableFullNameFromInput(ReadEntity input) {
        return rewriter.getTableFullName(input.getTable());
    }
}
