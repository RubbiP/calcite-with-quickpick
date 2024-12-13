import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class SQLParserExampleTest {

    SQLParserExample sqlParserExample=new SQLParserExample();

    @Test
    public void testRun() throws Exception {
        sqlParserExample.run();
    }

    @Test
    public void testExtractTables() throws JSQLParserException {

        String sql=sqlParserExample.genSQL();
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();


        List<String> tablesList = new ArrayList<>();
        Map<String,String> aliasToTableMap=new HashMap<>();
        Map<String,String> tableToAliasMap=new HashMap<>();
        // Extract tables
        sqlParserExample.extractTables(plainSelect, tablesList,aliasToTableMap,tableToAliasMap);
        System.out.println("----------------------tablenames----------------------");
        tablesList.forEach(tablename->{
            System.out.println(tablename);
        });

        System.out.println("----------------------alias : tablenames----------------------");
        aliasToTableMap.forEach((alias,tablename)->{
            System.out.println(alias+" : "+tablename);
        });

        System.out.println("----------------------tablenames : alias----------------------");
        tableToAliasMap.forEach((tablename,alias)->{
            System.out.println(tablename+" : "+alias);
        });

    }

    @Test
    public void testExtractPredicates() throws JSQLParserException {

        String sql = sqlParserExample.genSQL();
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        Expression where = plainSelect.getWhere();

        List<Expression> joinPredicatesExpression = new ArrayList<>();
        List<Expression> nonJoinPredicatesExpression = new ArrayList<>();
        List<String> joinPredicates=new ArrayList<>();
        List<String> nonJoinPredicates=new ArrayList<>();

        sqlParserExample.extractPredicates(where, joinPredicatesExpression, nonJoinPredicatesExpression);

        // Print results
        System.out.println("----------------------Join Predicates----------------------");
        joinPredicatesExpression.forEach(jp->{
            joinPredicates.add(jp.toString());
            System.out.println(jp);
        });

        System.out.println();

        System.out.println("----------------------NonJoin Predicates----------------------");
        nonJoinPredicatesExpression.forEach(njp->{
            nonJoinPredicates.add(njp.toString());
            System.out.println(njp);
        });
    }

    @Test
    public void testBuildPredicate() throws JSQLParserException, ClassNotFoundException, SQLException {

        String sql = sqlParserExample.genSQL();
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        Expression where = plainSelect.getWhere();

        List<Expression> joinPredicatesExpression = new ArrayList<>();
        List<Expression> nonJoinPredicatesExpression = new ArrayList<>();
        List<String> joinPredicates=new ArrayList<>();
        List<String> nonJoinPredicates=new ArrayList<>();

        sqlParserExample.extractPredicates(where, joinPredicatesExpression, nonJoinPredicatesExpression);
        nonJoinPredicatesExpression.forEach(njp->{
            nonJoinPredicates.add(njp.toString());
        });

        RelBuilder relBuilder = sqlParserExample.getRelBuilder();


        List<String> tablesList = new ArrayList<>();
        Map<String,String> aliasToTableMap=new HashMap<>();
        Map<String,String> tableToAliasMap=new HashMap<>();
        // Extract tables
        sqlParserExample.extractTables(plainSelect, tablesList,aliasToTableMap,tableToAliasMap);

        // 映射表名称到 RelNode
        Map<String, RelNode> tableNodes = new HashMap<>();
        for (String table : tablesList) {
            RelNode tableNode = relBuilder.scan(table).build();
            tableNodes.put(table, tableNode);
        }

        // 构建非连接谓词的 RelNode
        System.out.println("----------------------ScanAndFilterNode----------------------");
        Map<String, RelNode> predicateNodes = new HashMap<>();
        for (String predicate : nonJoinPredicates) {
            // 提取表名并构建谓词 RexNode
            String tableName;
            if(aliasToTableMap.get(sqlParserExample.extractTableName(predicate))==null){
                tableName=sqlParserExample.extractTableName(predicate);
            }else{
                tableName=aliasToTableMap.get(sqlParserExample.extractTableName(predicate));
            }
            RelNode tableNode = tableNodes.get(tableName);
            if (tableNode == null) {
                System.out.println("Table not found for predicate: " + predicate);
                continue;
            }
            RexNode rexPredicate = sqlParserExample.buildPredicate(relBuilder.getCluster(), tableNode, predicate);

            // 构建过滤节点
            RelNode filterNode = relBuilder.scan(tableName).filter(rexPredicate).build();
            System.out.println(filterNode.toString());
            predicateNodes.put(predicate, filterNode);
        }
    }

    @Test
    public void testGetStringRelNodeMap() throws SQLException, ClassNotFoundException, JSQLParserException {

        RelBuilder relBuilder = sqlParserExample.getRelBuilder();

        String sql = sqlParserExample.genSQL();

        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();


        List<String> tablesList = new ArrayList<>();
        Map<String,String> aliasToTableMap=new HashMap<>();
        Map<String,String> tableToAliasMap=new HashMap<>();
        // Extract tables
        sqlParserExample.extractTables(plainSelect, tablesList,aliasToTableMap,tableToAliasMap);

        // Extract where clause
        Expression where = plainSelect.getWhere();
        List<Expression> joinPredicatesExpression = new ArrayList<>();
        List<Expression> nonJoinPredicatesExpression = new ArrayList<>();
        List<String> joinPredicates=new ArrayList<>();
        List<String> nonJoinPredicates=new ArrayList<>();

        sqlParserExample.extractPredicates(where, joinPredicatesExpression, nonJoinPredicatesExpression);

        joinPredicatesExpression.forEach(jp->{
            joinPredicates.add(jp.toString());
        });

        nonJoinPredicatesExpression.forEach(njp->{
            nonJoinPredicates.add(njp.toString());
        });

        Deque<String> predicateStack = new ArrayDeque<>();
        Collections.shuffle(joinPredicatesExpression);

        // Use Deque as a stack
        for (String predicate : joinPredicates) {
            predicateStack.push(predicate); // Push each predicate onto the stack
        }

        // 映射表名称到 RelNode
        Map<String, RelNode> tableNodes = new HashMap<>();
        for (String table : tablesList) {
            RelNode tableNode = relBuilder.scan(table).build();
            tableNodes.put(table, tableNode);
        }

        sqlParserExample.getStringRelNodeMap(relBuilder, aliasToTableMap, nonJoinPredicates, tableNodes);
//        System.out.println("----------------------predicate : node----------------------");
//        predicateNodes.forEach((predicate,node)->{
//            System.out.println(predicate+" : "+node);
//        });
    }

    @Test
    public void testCombineTablesAndNonJoinPredicates() throws SQLException, ClassNotFoundException, JSQLParserException {
        RelBuilder relBuilder = sqlParserExample.getRelBuilder();

        String sql = sqlParserExample.genSQL();

        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();


        List<String> tablesList = new ArrayList<>();
        Map<String,String> aliasToTableMap=new HashMap<>();
        Map<String,String> tableToAliasMap=new HashMap<>();
        // Extract tables
        sqlParserExample.extractTables(plainSelect, tablesList,aliasToTableMap,tableToAliasMap);

        // Extract where clause
        Expression where = plainSelect.getWhere();
        List<Expression> joinPredicatesExpression = new ArrayList<>();
        List<Expression> nonJoinPredicatesExpression = new ArrayList<>();
        List<String> joinPredicates=new ArrayList<>();
        List<String> nonJoinPredicates=new ArrayList<>();

        sqlParserExample.extractPredicates(where, joinPredicatesExpression, nonJoinPredicatesExpression);

        joinPredicatesExpression.forEach(jp->{
            joinPredicates.add(jp.toString());
        });

        nonJoinPredicatesExpression.forEach(njp->{
            nonJoinPredicates.add(njp.toString());
        });

        Deque<String> predicateStack = new ArrayDeque<>();
        Collections.shuffle(joinPredicatesExpression);

        // Use Deque as a stack
        for (String predicate : joinPredicates) {
            predicateStack.push(predicate); // Push each predicate onto the stack
        }

        // 映射表名称到 RelNode
        Map<String, RelNode> tableNodes = new HashMap<>();
        for (String table : tablesList) {
            RelNode tableNode = relBuilder.scan(table).build();
            tableNodes.put(table, tableNode);
        }

        sqlParserExample.getStringRelNodeMap(relBuilder, aliasToTableMap, nonJoinPredicates, tableNodes);

//        sqlParserExample.combineTablesAndNonJoinPredicates(relBuilder, tableToAliasMap, tableNodes, predicateNodes);
//
//
//        for (Map.Entry<String, RelNode> tableEntry : tableNodes.entrySet()) {
//            String tableName = tableEntry.getKey();
//            RelNode tableNode = tableEntry.getValue();
//
//            /*
//              @method: combineTablesAndNonJoinPredicates
//             */
//            for (Map.Entry<String, RelNode> predicateEntry : predicateNodes.entrySet()) {
//                String predicate = predicateEntry.getKey();
//                RelNode filterNode = predicateEntry.getValue();
//                if (predicate.contains(tableName) ||
//                        tableToAliasMap.get(tableName)!=null &&
//                                predicate.contains(tableToAliasMap.get(tableName))) {
//                    // 将表与其对应的非连接谓词结合
//                    RelNode combinedNode = relBuilder.push(tableNode).push(filterNode).build();
//                    System.out.println("Combined Node for table " + tableName + ": " + combinedNode);
//                }
//            }
//        }
    }
}
