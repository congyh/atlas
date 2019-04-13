package org.apache.atlas.hive.hook;

import java.util.List;
import java.util.Map;

public class LineageParseContext {

    private HiveLineageTableInfo hiveLineageTableInfo;

    private Map<String, List<String>> actualLineagePartVals;

    public LineageParseContext(HiveLineageTableInfo tableInfo, Map<String, List<String>> map) {
        this.hiveLineageTableInfo = tableInfo;
        this.actualLineagePartVals = map;
    }

    public HiveLineageTableInfo getHiveLineageTableInfo() {
        return hiveLineageTableInfo;
    }

    public void setHiveLineageTableInfo(HiveLineageTableInfo hiveLineageTableInfo) {
        this.hiveLineageTableInfo = hiveLineageTableInfo;
    }

    public Map<String, List<String>> getActualLineagePartVals() {
        return actualLineagePartVals;
    }

    public void setActualLineagePartVals(Map<String, List<String>> actualLineagePartVals) {
        this.actualLineagePartVals = actualLineagePartVals;
    }
}
