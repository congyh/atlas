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
import org.apache.hadoop.hive.ql.hooks.ReadEntity;

import java.util.*;

// TODO: 需要通过一个实际的sql运行来看实际的where情况有多么复杂.
public class LineageParser {

    private HiveLineageTableInfo hiveLineageTableInfo;

    public LineageParser(HiveLineageTableInfo hiveLineageTableInfo) {
        this.hiveLineageTableInfo = hiveLineageTableInfo;
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
        LineageParser parser = new LineageParser(lineageTableInfo);

        for (SQLExpr expr: exprs) {
            LineageParseContext context = new LineageParseContext(lineageTableInfo, new HashMap<>());
            parser.getColumnValuePair(expr, context);
            System.out.println(context.getActualLineagePartVals());
        }
    }

//    /**
//     * 从input table/partition中获取表名
//     * <p>
//     * 注意: 本调用需由调用方保证传入的ReadEntity是table/partition
//     *
//     * @param input input table/partition
//     * @return 表全名
//     */
//    public String getTableFullNameFromInput(ReadEntity input) {
//        return hiveLineageTableInfo.getTableFullName(input.getTable());
//    }

    public String getTableFullNameFromInput(String tableName) {
        // TODO: fake first.
        return "dim." + tableName;
    }

    private void getColumnValuePair(SQLExpr expr, LineageParseContext context) {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr boExpr = (SQLBinaryOpExpr) expr;
            SQLExpr leftExpr = boExpr.getLeft();
            SQLExpr rightExpr = boExpr.getRight();

            if (leftExpr instanceof SQLBinaryOpExpr || leftExpr instanceof SQLInListExpr) {
                getColumnValuePair(leftExpr, context);
            }

            if (rightExpr instanceof SQLBinaryOpExpr || leftExpr instanceof SQLInListExpr) {
                getColumnValuePair(rightExpr, context);
            }

            if (leftExpr instanceof SQLPropertyExpr && rightExpr instanceof SQLCharExpr) {
                putValues((SQLPropertyExpr) leftExpr, (SQLCharExpr) rightExpr, context);
            } else if (leftExpr instanceof SQLCharExpr && rightExpr instanceof SQLPropertyExpr) {
                putValues((SQLPropertyExpr) rightExpr, (SQLCharExpr) leftExpr, context);
            }
        } else if (expr instanceof SQLInListExpr) {
            Map.Entry<String, List<String>> values = getValues((SQLInListExpr) expr);
            String fieldName = getFieldName((SQLInListExpr) expr);

            if (((SQLInListExpr) expr).isNot()) {
                List<String> otherValues = getOtherValues(values.getKey(), values.getValue());
                values.setValue(otherValues);
            }

            putValues(values, fieldName, context);
        }
    }

    private String getFieldName(SQLInListExpr expr) {
        return ((SQLPropertyExpr)expr.getExpr()).getName();
    }

    private boolean isLineagePartitioned(String tableName) {
        String tableFullName = getTableFullNameFromInput(tableName);
        return hiveLineageTableInfo.isLineagePartitioned(tableFullName);
    }

    private String getLineagePartitionName(String tableName) {
        String tableFullName = getTableFullNameFromInput(tableName);
        return hiveLineageTableInfo.getLineagePartitionName(tableFullName);
    }

    private Map.Entry<String, String> getValue(SQLPropertyExpr property, SQLCharExpr value) {
        String name = ((SQLIdentifierExpr)(property.getOwner())).getName();
        return new KeyValue<>(name, value.getText());
    }

    private Map.Entry<String, List<String>> getValues(SQLInListExpr expr) {
        List<String> values = new ArrayList<>();
        Map.Entry<String, List<String>> ret = null;
        SQLExpr leftExpr =  expr.getExpr();
        List<SQLExpr> rightExprs = expr.getTargetList();

        Map.Entry<String, String> value = null;
        if (leftExpr instanceof SQLPropertyExpr && rightExprs.get(0) instanceof SQLCharExpr) {
            for (SQLExpr rightExpr: rightExprs) {
                value = getValue((SQLPropertyExpr) leftExpr, (SQLCharExpr) rightExpr);
                values.add(value.getValue());
            }

            ret = new KeyValue<>(value.getKey(), values);
        }

        return ret;
    }

    private List<String> getOtherValues(String tableName, List<String> values) {
        List<String> ret = null;
        String tableFullName = getTableFullNameFromInput(tableName);
        if (isLineagePartitioned(tableName)) {
            List<String> lineagePartitionValues =
                    hiveLineageTableInfo.getLineagePartitionValues(tableFullName);
            ret = new ArrayList<>(lineagePartitionValues);
            ret.removeAll(values);
        }

        return ret;
    }

    private void putValue(Map.Entry<String, String> value, String fieldName, LineageParseContext context) {
        putValues(value.getKey(), Arrays.asList(value.getValue()), fieldName, context);
    }

    private void putValues(SQLPropertyExpr property, SQLCharExpr value, LineageParseContext context) {
        Map.Entry<String, String> entry = getValue(property, value);
        SQLBinaryOpExpr boExpr = (SQLBinaryOpExpr) property.getParent();
        String fieldName = property.getName();
        if (boExpr.getOperator() == SQLBinaryOperator.NotEqual
                || boExpr.getOperator() == SQLBinaryOperator.LessThanOrGreater) {
            List<String> otherValues = getOtherValues(entry.getKey(), Arrays.asList(entry.getValue()));
            putValues(new KeyValue<>(entry.getKey(), otherValues), fieldName, context);
        } else {
            putValue(entry, fieldName, context);
        }
    }

    private void putValues(Map.Entry<String, List<String>> values, String fieldName, LineageParseContext context) {
        putValues(values.getKey(), values.getValue(), fieldName, context);
    }

    private void putValues(String key, List<String> values, String fieldName, LineageParseContext context) {
        if (isLineagePartitioned(key) && fieldName.equals(getLineagePartitionName(key))) {
            Map<String, List<String>> lineageMap =  context.getActualLineagePartVals();
            if (lineageMap.containsKey(key)) {
                lineageMap.get(key).addAll(values);
            } else {
                lineageMap.put(key, values);
            }
        }
    }

    @Deprecated
    private void putValue(SQLPropertyExpr property, SQLCharExpr value, LineageParseContext context) {
        String name = ((SQLIdentifierExpr)(property.getOwner())).getName();
        String fieldName = property.getName();
        if (isLineagePartitioned(name) && fieldName.equals(getLineagePartitionName(name))) {
            Map<String, List<String>> lineageMap =  context.getActualLineagePartVals();
            if (lineageMap.containsKey(name)) {
                lineageMap.get(name).add(value.getText());
            } else {
                List<String> values = new ArrayList<>();
                values.add(value.getText());
                lineageMap.put(name, values);
            }
        }
    }

    private static void druidExample1() {
        final String dbType = JdbcConstants.HIVE;
        List<String> sqls = new ArrayList<>();
        sqls.add("select * from t where dim_a.pt in ('RTB', 'CPS')");
        sqls.add("select * from t where dim_a.pt not in ('RTB', 'CPS')");
        sqls.add("select * from t where dim_a.pt <> 'RTB'");
        sqls.add("select * from t where (dim_a.pt != 'RTB' AND 1 = 1) OR 2 = 2");

        List<List<SQLStatement>> stmtLists = new ArrayList<>();

        for (String sql: sqls) {
            stmtLists.add(SQLUtils.parseStatements(sql, dbType));
        }

        for (List<SQLStatement> stmtList: stmtLists) {
//            System.out.println(SQLUtils.toSQLString(stmtList, dbType));

            for (SQLStatement stmt: stmtList) {
                if (stmt instanceof SQLSelectStatement) {
                    SQLSelectStatement sstmt = (SQLSelectStatement)stmt;
                    SQLSelect sqlSelect = sstmt.getSelect();
                    SQLSelectQueryBlock query = (SQLSelectQueryBlock)sqlSelect.getQuery();
                    StringBuffer where = new StringBuffer();
                    SQLASTOutputVisitor whereVisitor = SQLUtils.createFormatOutputVisitor(where, stmtList, JdbcConstants.HIVE);

                    query.getWhere().accept(whereVisitor);
                    System.out.println(where);
                }
            }
        }
    }
}
