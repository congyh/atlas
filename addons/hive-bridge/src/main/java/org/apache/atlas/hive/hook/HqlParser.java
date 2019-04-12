package org.apache.atlas.hive.hook;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.visitor.ExportParameterVisitor;
import com.alibaba.druid.util.JdbcConstants;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.List;

public class HqlParser {

    public static void main(String[] args) throws JSQLParserException {
//        final String dbType = JdbcConstants.HIVE;
//        String sql = "select * from t where dim.dim_a.pt in ('RTB', 'CPS')";
//        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
//
//        System.out.println(SQLUtils.toSQLString(stmtList, dbType));
//
//        for (SQLStatement stmt: stmtList) {
//            System.out.println(stmt);
//        }
//
//        SQLBinaryOpExpr bo = new SQLBinaryOpExpr();

        Statement statement = CCJSqlParserUtil.parse("select * from t where dim.dim_a.pt in ('RTB', 'CPS')");
        Select selectStatement = (Select) statement;
        statement.get
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);

        for (String table: tableList) {
            System.out.println(table);
        }

    }
}
