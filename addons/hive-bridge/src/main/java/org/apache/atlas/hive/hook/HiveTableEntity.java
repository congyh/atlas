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

public class HiveTableEntity {

    public static final String HIVE_TABLE_NAME = "hive_table_name";
    public static final String HIVE_TABLE_LINEAGE_PARTITIONS = "hive_lineage_partitions";

    private String tableName;
    private String lineagePartition;

    public HiveTableEntity(String tableName, String lineagePartition) {
        this.tableName = tableName;
        this.lineagePartition = lineagePartition;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getLineagePartition() {
        return lineagePartition;
    }

    public void setLineagePartition(String lineagePartition) {
        this.lineagePartition = lineagePartition;
    }
}
