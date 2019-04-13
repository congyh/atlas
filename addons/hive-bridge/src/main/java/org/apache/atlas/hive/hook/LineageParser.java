package org.apache.atlas.hive.hook;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.JdbcConstants;
import net.sf.jsqlparser.JSQLParserException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: 需要通过一个实际的sql运行来看实际的where情况有多么复杂.
public class LineageParser {

    public static void main(String[] args) throws JSQLParserException {
        final String dbType = JdbcConstants.HIVE;
        List<SQLExpr> exprs = new ArrayList<>();
        exprs.add(SQLUtils.toSQLExpr("(dim_a.pt IN ('RTB', 'GDT') AND 'CPA' = 'CPA') OR 'CPS' = dim_a.pt", dbType));
        exprs.add(SQLUtils.toSQLExpr("dim_a.pt not in ('RTB', 'CPS')"));
        exprs.add(SQLUtils.toSQLExpr("dim_a.pt <> 'RTB'"));
        exprs.add(SQLUtils.toSQLExpr("(dim_a.pt != 'RTB' AND 1 = 1) OR 2 = 2"));

        for (SQLExpr expr: exprs) {
            LineageParseContext context = new LineageParseContext(new HashMap<>());
            getColumnValuePair(expr, context);
            System.out.println(context.getLineagePartitionValues());
        }
    }

    private static void getColumnValuePair(SQLExpr expr, LineageParseContext context) {
        // TODO: 需要能够鉴别是否是lineagePartition.
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
            // TODO: 判断是否是not, 还需要与parseContext交互全部的分区值信息.
           SQLExpr leftExpr = ((SQLInListExpr) expr).getExpr();
           List<SQLExpr> rightExprs = ((SQLInListExpr) expr).getTargetList();
           if (leftExpr instanceof SQLPropertyExpr && rightExprs.get(0) instanceof SQLCharExpr) {
               for (SQLExpr rightExpr: rightExprs) {
                   putValues((SQLPropertyExpr) leftExpr, (SQLCharExpr) rightExpr, context);
               }
           }
        }
    }

    private static void putValues(SQLPropertyExpr property, SQLCharExpr value, LineageParseContext context) {
        Map<String, List<String>> lineageMap =  context.getLineagePartitionValues();
        String name = property.getName();
        if (lineageMap.containsKey(name)) {
            lineageMap.get(name).add(value.getText());
        } else {
            List<String> values = new ArrayList<>();
            values.add(value.getText());
            lineageMap.put(name, values);
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
