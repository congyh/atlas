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

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.JdbcConstants;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;

import java.util.*;

// TODO: 需要通过一个实际的sql运行来看实际的where情况有多么复杂.
public class LineageParser {

    private HiveLineageTableInfo hiveLineageTableInfo;

    private Map<String, String> inputTableDbNames= new HashMap<>();

    private Map<String, Set<String>> actualLineagePartVals = new HashMap<>();

    private Set<ReadEntity> inputs;

    public LineageParser(HiveLineageTableInfo hiveLineageTableInfo, Set<ReadEntity> inputs) {
        this.hiveLineageTableInfo = hiveLineageTableInfo;
        this.inputs = inputs;

        generateTableDbNameFromInputs();
    }

    public LineageParser(HiveLineageTableInfo hiveLineageTableInfo,
                         Set<ReadEntity> inputs,
                         Map<String, String> inputTableDbNames,
                         Map<String, Set<String>> actualLineagePartVals) {
        this.hiveLineageTableInfo = hiveLineageTableInfo;
        this.inputs = inputs;
        this.inputTableDbNames = inputTableDbNames;
        this.actualLineagePartVals = actualLineagePartVals;

        generateTableDbNameFromInputs();
    }

    public static void main(String[] args) throws JSQLParserException {
        final String dbType = JdbcConstants.HIVE;
        List<SQLExpr> exprs = new ArrayList<>();
        // expect RTB, GDT
        exprs.add(SQLUtils.toSQLExpr("(dim_test_table_with_pt_level1.pt IN ('RTB', 'GDT') AND dim_test_table_with_pt_level1.dt = '2019-04-08') OR 'CPS' = dim_a.pt", dbType));
        // expect GDT
        exprs.add(SQLUtils.toSQLExpr("dim_test_table_with_pt_level1.pt not in ('RTB', 'CPS')"));
        // expect GDT, CPS
        exprs.add(SQLUtils.toSQLExpr("dim_test_table_with_pt_level1.pt <> 'RTB'"));
        // expect GDT, CPS
        exprs.add(SQLUtils.toSQLExpr("(dim_test_table_with_pt_level1.pt != 'RTB' AND 1 = 1) OR 2 = 2"));
        // expect RTB
        exprs.add(SQLUtils.toSQLExpr("dim_test_table_with_pt_level1.pt = 'RTB'"));
        // expect {}
        exprs.add(SQLUtils.toSQLExpr("(dim_a.pt != 'RTB' AND 1 = 1) OR 2 = 2"));

        HiveLineageTableInfo lineageTableInfo = new HiveLineageTableInfo();

        // TODO: fake first
        Set<ReadEntity> inputs = new HashSet<>();
        Map<String, String> inputTableDbNames = new HashMap<>();
        inputTableDbNames.put("dim_test_table_with_pt_level1", "dim");

        for (SQLExpr expr : exprs) {
            LineageParser parser = new LineageParser(lineageTableInfo, inputs, inputTableDbNames, new HashMap<>());
            parser.getColumnValuePair(expr);
            System.out.println(parser.getActualLineagePartVals());
        }
    }

    private void generateTableDbNameFromInputs() {
        if (inputTableDbNames == null || inputTableDbNames.isEmpty()) {
            inputTableDbNames = new HashMap<>();
            for (ReadEntity input : inputs) {
                if (input.getType() == Entity.Type.TABLE) {
                    inputTableDbNames.put(input.getTable().getTableName(), input.getTable().getDbName());
                }
            }
        }
    }

    private String getTableFullNameFromInputs(String tableName) {
        return inputTableDbNames.get(tableName) + "." + tableName;
    }

    public void getColumnValuePair(SQLExpr expr) {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr boExpr = (SQLBinaryOpExpr) expr;
            SQLExpr leftExpr = boExpr.getLeft();
            SQLExpr rightExpr = boExpr.getRight();

            if (leftExpr instanceof SQLBinaryOpExpr || leftExpr instanceof SQLInListExpr) {
                getColumnValuePair(leftExpr);
            }

            if (rightExpr instanceof SQLBinaryOpExpr || leftExpr instanceof SQLInListExpr) {
                getColumnValuePair(rightExpr);
            }

            if (leftExpr instanceof SQLPropertyExpr && rightExpr instanceof SQLCharExpr) {
                putValues((SQLPropertyExpr) leftExpr, (SQLCharExpr) rightExpr);
            } else if (leftExpr instanceof SQLCharExpr && rightExpr instanceof SQLPropertyExpr) {
                putValues((SQLPropertyExpr) rightExpr, (SQLCharExpr) leftExpr);
            }
        } else if (expr instanceof SQLInListExpr) {
            Map.Entry<String, List<String>> values = getValues((SQLInListExpr) expr);
            String fieldName = getFieldName((SQLInListExpr) expr);

            if (((SQLInListExpr) expr).isNot()) {
                List<String> otherValues = getOtherValues(values.getKey(), values.getValue());
                values.setValue(otherValues);
            }

            putValues(values, fieldName);
        }
    }

    private String getFieldName(SQLInListExpr expr) {
        return ((SQLPropertyExpr) expr.getExpr()).getName();
    }

    private boolean isLineagePartitioned(String tableName) {
        String tableFullName = getTableFullNameFromInputs(tableName);
        return hiveLineageTableInfo.isLineagePartitioned(tableFullName);
    }

    private String getLineagePartitionName(String tableName) {
        String tableFullName = getTableFullNameFromInputs(tableName);
        return hiveLineageTableInfo.getLineagePartitionName(tableFullName);
    }

    private Map.Entry<String, String> getValue(SQLPropertyExpr property, SQLCharExpr value) {
        String name = ((SQLIdentifierExpr) (property.getOwner())).getName();
        return new KeyValue<>(name, value.getText());
    }

    private Map.Entry<String, List<String>> getValues(SQLInListExpr expr) {
        List<String> values = new ArrayList<>();
        Map.Entry<String, List<String>> ret = null;
        SQLExpr leftExpr = expr.getExpr();
        List<SQLExpr> rightExprs = expr.getTargetList();

        Map.Entry<String, String> value = null;
        if (leftExpr instanceof SQLPropertyExpr && rightExprs.get(0) instanceof SQLCharExpr) {
            for (SQLExpr rightExpr : rightExprs) {
                value = getValue((SQLPropertyExpr) leftExpr, (SQLCharExpr) rightExpr);
                values.add(value.getValue());
            }

            ret = new KeyValue<>(value.getKey(), values);
        }

        return ret;
    }

    private List<String> getOtherValues(String tableName, List<String> values) {
        List<String> ret = null;
        String tableFullName = getTableFullNameFromInputs(tableName);
        if (isLineagePartitioned(tableName)) {
            List<String> lineagePartitionValues =
                    hiveLineageTableInfo.getLineagePartitionValues(tableFullName);
            ret = new ArrayList<>(lineagePartitionValues);
            ret.removeAll(values);
        }

        return ret;
    }

    private void putValue(Map.Entry<String, String> value, String fieldName) {
        putValues(value.getKey(), Arrays.asList(value.getValue()), fieldName);
    }

    private void putValues(SQLPropertyExpr property, SQLCharExpr value) {
        Map.Entry<String, String> entry = getValue(property, value);
        SQLBinaryOpExpr boExpr = (SQLBinaryOpExpr) property.getParent();
        String fieldName = property.getName();
        if (boExpr.getOperator() == SQLBinaryOperator.NotEqual
                || boExpr.getOperator() == SQLBinaryOperator.LessThanOrGreater) {
            List<String> otherValues = getOtherValues(entry.getKey(), Arrays.asList(entry.getValue()));
            putValues(new KeyValue<>(entry.getKey(), otherValues), fieldName);
        } else {
            putValue(entry, fieldName);
        }
    }

    private void putValues(Map.Entry<String, List<String>> values, String fieldName) {
        putValues(values.getKey(), values.getValue(), fieldName);
    }

    private void putValues(String key, List<String> values, String fieldName) {
        if (isLineagePartitioned(key) && fieldName.equals(getLineagePartitionName(key))) {
            if (actualLineagePartVals.containsKey(key)) {
                actualLineagePartVals.get(key).addAll(values);
            } else {
                actualLineagePartVals.put(key, new HashSet<>(values));
            }
        }
    }

    @Deprecated
    private void putValue(SQLPropertyExpr property, SQLCharExpr value) {
        String name = ((SQLIdentifierExpr) (property.getOwner())).getName();
        String fieldName = property.getName();
        if (isLineagePartitioned(name) && fieldName.equals(getLineagePartitionName(name))) {
            if (actualLineagePartVals.containsKey(name)) {
                actualLineagePartVals.get(name).add(value.getText());
            } else {
                Set<String> values = new HashSet<>();
                values.add(value.getText());
                actualLineagePartVals.put(name, values);
            }
        }
    }

    public HiveLineageTableInfo getHiveLineageTableInfo() {
        return hiveLineageTableInfo;
    }

    public void setHiveLineageTableInfo(HiveLineageTableInfo hiveLineageTableInfo) {
        this.hiveLineageTableInfo = hiveLineageTableInfo;
    }

    public Map<String, String> getInputTableDbNames() {
        return inputTableDbNames;
    }

    public void setInputTableDbNames(Map<String, String> inputTableDbNames) {
        this.inputTableDbNames = inputTableDbNames;
    }

    public Map<String, Set<String>> getActualLineagePartVals() {
        return actualLineagePartVals;
    }

    public void setActualLineagePartVals(Map<String, Set<String>> actualLineagePartVals) {
        this.actualLineagePartVals = actualLineagePartVals;
    }

    private static void druidExample1() {
        final String dbType = JdbcConstants.HIVE;
        List<String> sqls = new ArrayList<>();
        sqls.add("select * from t where dim_a.pt in ('RTB', 'CPS')");
        sqls.add("select * from t where dim_a.pt not in ('RTB', 'CPS')");
        sqls.add("select * from t where dim_a.pt <> 'RTB'");
        sqls.add("select * from t where (dim_a.pt != 'RTB' AND 1 = 1) OR 2 = 2");

        List<List<SQLStatement>> stmtLists = new ArrayList<>();

        for (String sql : sqls) {
            stmtLists.add(SQLUtils.parseStatements(sql, dbType));
        }

        for (List<SQLStatement> stmtList : stmtLists) {
//            System.out.println(SQLUtils.toSQLString(stmtList, dbType));

            for (SQLStatement stmt : stmtList) {
                if (stmt instanceof SQLSelectStatement) {
                    SQLSelectStatement sstmt = (SQLSelectStatement) stmt;
                    SQLSelect sqlSelect = sstmt.getSelect();
                    SQLSelectQueryBlock query = (SQLSelectQueryBlock) sqlSelect.getQuery();
                    StringBuffer where = new StringBuffer();
                    SQLASTOutputVisitor whereVisitor = SQLUtils.createFormatOutputVisitor(where, stmtList, JdbcConstants.HIVE);

                    query.getWhere().accept(whereVisitor);
                    System.out.println(where);
                }
            }
        }
    }
}
