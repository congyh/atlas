package org.apache.atlas.hive.hook;

import java.util.List;
import java.util.Map;

public class LineageParseContext {
    private Map<String, List<String>> lineagePartitionValues;

    // TODO: 需要包含表名信息
    // TODO: lineagePartiton信息表.

    public LineageParseContext(Map<String, List<String>> map) {
        this.lineagePartitionValues = map;
    }

    public Map<String, List<String>> getLineagePartitionValues() {
        return lineagePartitionValues;
    }

    public void setLineagePartitionValues(Map<String, List<String>> lineagePartitionValues) {
        this.lineagePartitionValues = lineagePartitionValues;
    }
}
