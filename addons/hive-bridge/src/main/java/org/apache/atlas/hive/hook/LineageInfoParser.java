package org.apache.atlas.hive.hook;

import kafka.security.auth.Read;
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
