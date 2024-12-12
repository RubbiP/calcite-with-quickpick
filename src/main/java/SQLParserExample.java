import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.BinaryExpression;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.postgresql.jdbc2.optional.SimpleDataSource;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.*;

public class SQLParserExample {
    public static void main(String[] args) throws Exception {
        SQLParserExample sqlParserExample=new SQLParserExample();
        sqlParserExample.run();
    }

    public  void run() throws Exception {

        RelBuilder relBuilder = getRelBuilder();

        String sql = genSQL();

        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();


        List<String> tablesList = new ArrayList<>();
        Map<String,String> aliasToTableMap=new HashMap<>();
        Map<String,String> tableToAliasMap=new HashMap<>();
        // Extract tables
        extractTables(plainSelect, tablesList,aliasToTableMap,tableToAliasMap);

        // Extract where clause
        Expression where = plainSelect.getWhere();
        List<Expression> joinPredicatesExpression = new ArrayList<>();
        List<Expression> nonJoinPredicatesExpression = new ArrayList<>();
        List<String> joinPredicates=new ArrayList<>();
        List<String> nonJoinPredicates=new ArrayList<>();

        extractPredicates(where, joinPredicatesExpression, nonJoinPredicatesExpression);

        joinPredicatesExpression.forEach(jp->{
            joinPredicates.add(jp.toString());
        });

        nonJoinPredicatesExpression.forEach(njp->{
            nonJoinPredicates.add(njp.toString());
        });

        Deque<String> predicateStack = new ArrayDeque<>();
        Collections.shuffle(joinPredicates);

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


        Map<String, RelNode> predicateNodes = getStringRelNodeMap(relBuilder, aliasToTableMap, nonJoinPredicates, tableNodes);

        combineTablesAndNonJoinPredicates(relBuilder, tableToAliasMap, tableNodes, predicateNodes);

        RelNode rootNode=buildRelTree(tablesList, joinPredicates,nonJoinPredicates,
                tableNodes,relBuilder,aliasToTableMap);

        executeRelNode(rootNode);

    }

    public RelBuilder getRelBuilder() throws ClassNotFoundException, SQLException {
        String url = "jdbc:postgresql://192.168.5.135:5432/imdbload";
        String username = "guest";
        String password = "guest";
        String driverClassName = "org.postgresql.Driver";
        // 注册 PostgreSQL 驱动
        Class.forName(driverClassName);

        // 使用 Calcite 连接 PostgreSQL 数据库
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, "imdbload", JdbcSchema.dataSource(url, driverClassName, username, password),
                null, null);
        rootSchema.add("imdbload", jdbcSchema);
        calciteConnection.setSchema("imdbload");

        //省略设置表关系
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                .defaultSchema(rootSchema.getSubSchema("imdbload"))
                .build();

        RelBuilder relBuilder = RelBuilder.create(config);
        return relBuilder;
    }

    public void combineTablesAndNonJoinPredicates(RelBuilder relBuilder, Map<String, String> tableToAliasMap, Map<String, RelNode> tableNodes, Map<String, RelNode> predicateNodes) {
        // 遍历表和非连接谓词，将它们结合
        for (Map.Entry<String, RelNode> tableEntry : tableNodes.entrySet()) {
            String tableName = tableEntry.getKey();
            RelNode tableNode = tableEntry.getValue();

            for (Map.Entry<String, RelNode> predicateEntry : predicateNodes.entrySet()) {
                String predicate = predicateEntry.getKey();
                RelNode filterNode = predicateEntry.getValue();
                if (predicate.contains(tableName) ||
                        tableToAliasMap.get(tableName)!=null &&
                                predicate.contains(tableToAliasMap.get(tableName))) {
                    // 将表与其对应的非连接谓词结合
                    RelNode combinedNode = relBuilder.push(tableNode).push(filterNode).build();
                    // System.out.println("Combined Node for table " + tableName + ": " + combinedNode);
                }
            }
        }
    }

    public Map<String, RelNode> getStringRelNodeMap(RelBuilder relBuilder, Map<String, String> aliasToTableMap, List<String> nonJoinPredicates, Map<String, RelNode> tableNodes) {
        // 构建非连接谓词的 RelNode
        Map<String, RelNode> predicateNodes = new HashMap<>();
        for (String predicate : nonJoinPredicates) {
            // 提取表名并构建谓词 RexNode
            String tableName = extractTableName(predicate);
            if(aliasToTableMap.get(tableName)!=null){
                tableName= aliasToTableMap.get(tableName);
            }
            RelNode tableNode = tableNodes.get(tableName);
            if (tableNode == null) {
                System.out.println("Table not found for predicate: " + predicate);
                continue;
            }
            RexNode rexPredicate = buildPredicate(relBuilder.getCluster(), tableNode, predicate);

            // 构建过滤节点
            RelNode filterNode = relBuilder.scan(tableName).filter(rexPredicate).build();
            predicateNodes.put(predicate, filterNode);
        }
        return predicateNodes;
    }

    public  void extractTables(PlainSelect plainSelect, List<String> tablesList, Map<String,String> aliasToTableMap,
                               Map<String,String> tableToAliasMap) {
        FromItem fromItem = plainSelect.getFromItem();
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            if (table.getAlias() != null) {
                tablesList.add(table.getName());
                aliasToTableMap.put(table.getAlias().getName(),table.getName());
                tableToAliasMap.put(table.getName(),table.getAlias().getName());
            }else{
                tablesList.add(table.getName());
            }
        }

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                FromItem joinItem = join.getRightItem();
                if (joinItem instanceof Table) {
                    Table joinTable = (Table) joinItem;
                    if (joinTable.getAlias() != null) {
                        tablesList.add(joinTable.getName());
                        aliasToTableMap.put(joinTable.getAlias().getName(),joinTable.getName());
                        tableToAliasMap.put(joinTable.getName(),joinTable.getAlias().getName());
                    }else{
                        tablesList.add(joinTable.getName());
                    }
                }
            }
        }
    }

    public void extractPredicates(Expression expression, List<Expression> joinPredicatesExpression, List<Expression> nonJoinPredicates) {
        if (expression == null) {
            return;
        }

        if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            extractPredicates(andExpression.getLeftExpression(), joinPredicatesExpression, nonJoinPredicates);
            extractPredicates(andExpression.getRightExpression(), joinPredicatesExpression, nonJoinPredicates);
        } else if (expression instanceof BinaryExpression) {
            if (isJoinPredicate(expression)) {
                joinPredicatesExpression.add(expression);
            } else {
                nonJoinPredicates.add(expression);
            }
        } else {
            nonJoinPredicates.add(expression);
        }
    }

    public boolean isJoinPredicate(Expression expression) {
        if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            return involvesMultipleTables(binaryExpression.getLeftExpression(), binaryExpression.getRightExpression());
        }
        return false;
    }

    public boolean involvesMultipleTables(Expression left, Expression right) {
        if (left instanceof Column && right instanceof Column) {
            Column leftColumn = (Column) left;
            Column rightColumn = (Column) right;
            return leftColumn.getTable() != null && rightColumn.getTable() != null &&
                    !leftColumn.getTable().getName().equalsIgnoreCase(rightColumn.getTable().getName());
        }
        return false;
    }

    // 提取表名的工具方法
    public String extractTableName(String predicate) {
        // 简单提取表名，假设谓词格式为 table.col = 'value'
        return predicate.split("\\.")[0];
    }

    // 构建谓词的工具方法
    public RexNode buildPredicate(RelOptCluster cluster, RelNode tableNode, String predicate) {
        // 假设谓词格式为 table.col = 'value'
        String[] parts = predicate.split("=");
        String column = parts[0].trim().split("\\.")[1]; // 提取列名
        String value = parts[1].trim().replace("'", ""); // 提取常量值

        RelDataType rowType = tableNode.getRowType(); // 获取表的行类型
        int columnIndex = rowType.getFieldNames().indexOf(column); // 获取列的索引
        if (columnIndex == -1) {
            throw new RuntimeException("Column " + column + " not found in table schema");
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode columnRef = rexBuilder.makeInputRef(rowType, columnIndex); // 构建列引用
        RexNode constant = rexBuilder.makeLiteral(value); // 构建常量值

        // 构建谓词 (列 = 常量)
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, columnRef, constant);
    }

    public RexNode convertToRexNode(Expression expression, RelBuilder relBuilder) {
        // 使用 RelBuilder 的 toRex 方法将 JSqlParser 的 Expression 转换为 RexNode
        return relBuilder.getRexBuilder().makeLiteral(expression.toString());
    }

    public String genSQL() {
        String sql = "SELECT MIN(cn.name) AS producing_company, "
                + "MIN(miidx.info) AS rating, "
                + "MIN(t.title) AS movie "
                + "FROM company_name AS cn, "
                + "company_type AS ct, "
                + "info_type AS it, "
                + "info_type AS it2, "
                + "kind_type AS kt, "
                + "movie_companies AS mc, "
                + "movie_info AS mi, "
                + "movie_info_idx AS miidx, "
                + "title AS t "
                + "WHERE cn.country_code ='[us]' "
                + "AND ct.kind ='production companies' "
                + "AND it.info ='rating' "
                + "AND it2.info ='release dates' "
                + "AND kt.kind ='movie' "
                + "AND mi.movie_id = t.id "
                + "AND it2.id = mi.info_type_id "
                + "AND kt.id = t.kind_id "
                + "AND mc.movie_id = t.id "
                + "AND cn.id = mc.company_id "
                + "AND ct.id = mc.company_type_id "
                + "AND miidx.movie_id = t.id "
                + "AND it.id = miidx.info_type_id "
                + "AND mi.movie_id = miidx.movie_id "
                + "AND mi.movie_id = mc.movie_id "
                + "AND miidx.movie_id = mc.movie_id;";

        return sql;
    }


    public RelNode buildRelTree(List<String> tableNames, List<String> joinPredicates, List<String> nonJoinPredicates,
                                Map<String, RelNode> tableRelNodes, RelBuilder relBuilder,Map<String,String> aliasToTableMap) {
        // Map to track subtrees for each table
        Map<String, RelNode> subtreeRoots = new HashMap<>(tableRelNodes);

        // Set to track already processed nodes
        Set<RelNode> processedNodes = new HashSet<>();

        for (String joinPredicate : joinPredicates) {
            // Parse the join predicate to extract left and right table names
            String[] tables = parseJoinPredicate(joinPredicate,aliasToTableMap); // Implement this function to extract table names
            String leftTable = tables[0];
            String rightTable = tables[1];

            // Get the RelNodes for the tables
            RelNode leftNode = subtreeRoots.get(leftTable);
            RelNode rightNode = subtreeRoots.get(rightTable);

            if (leftNode == null || rightNode == null) {
                throw new RuntimeException("Missing RelNode for tables in join predicate: " + joinPredicate);
            }

            if (leftNode == rightNode) {
                // Both tables are in the same subtree, add the join predicate
                relBuilder.push(leftNode)
                        .filter(buildPredicate(relBuilder.getCluster(), leftNode, joinPredicate));
                subtreeRoots.put(leftTable, relBuilder.build()); // Update subtree root
            } else {
                // Tables are in different subtrees, join them
                relBuilder.push(leftNode)
                        .push(rightNode)
                        .join(JoinRelType.INNER, buildPredicate(relBuilder.getCluster(), leftNode, joinPredicate));
                RelNode joinedNode = relBuilder.build();

                // Update subtree roots for both tables
                subtreeRoots.put(leftTable, joinedNode);
                subtreeRoots.put(rightTable, joinedNode);

                processedNodes.add(joinedNode); // Mark joined node as processed
            }
        }

        // After processing all join predicates, return the root of the tree
        return subtreeRoots.values().iterator().next(); // Assuming all roots eventually converge
    }

    // Helper method to parse join predicates and extract left and right table names
    public String[] parseJoinPredicate(String predicate,Map<String,String> aliasToTableMap) {
        // Example: "table1.id = table2.id" -> ["table1", "table2"]
        String[] parts = predicate.split("=");
        String leftTable = parts[0].split("\\.")[0].trim(); // Extract table name before "."
        if(aliasToTableMap.get(leftTable)!=null){
            leftTable=aliasToTableMap.get(leftTable);
        }
        String rightTable = parts[1].split("\\.")[0].trim();
        if(aliasToTableMap.get(rightTable)!=null){
            rightTable=aliasToTableMap.get(rightTable);
        }
        return new String[]{leftTable, rightTable};
    }


    // Method to estimate the cost of a RelNode
    public double computeCost(RelNode rootNode) {
        RelMetadataQuery metadataQuery = RelMetadataQuery.instance();
        return metadataQuery.getCumulativeCost(rootNode).getRows();
    }

    public void executeRelNode(RelNode rootNode) {
        if (rootNode == null) {
            System.out.println("The RelNode is null. Cannot execute.");
            return;
        }

        // 1. Print the execution plan
        System.out.println("Execution Plan:");
        printExecutionPlan(rootNode);

        // 2. Estimate and print the execution cost
        RelMetadataQuery metadataQuery = RelMetadataQuery.instance();
        double rowCount = metadataQuery.getRowCount(rootNode); // Estimate rows processed
        double cumulativeCost = metadataQuery.getCumulativeCost(rootNode).getRows(); // Get cumulative cost

        System.out.println("\nExecution Cost:");
        System.out.printf("Estimated Row Count: %.2f%n", rowCount);
        System.out.printf("Cumulative Cost (Rows): %.2f%n", cumulativeCost);

        // 3. Describe the output schema of the RelNode
        RelDataType rowType = rootNode.getRowType();
        System.out.println("\nOutput Schema:");
        rowType.getFieldList().forEach(field ->
                System.out.printf("Field: %s, Type: %s%n", field.getName(), field.getType()));
    }

    public void printExecutionPlan(RelNode rootNode) {
        if (rootNode == null) {
            System.out.println("Execution plan is null.");
            return;
        }

        // Option 1: JSON Format (easier for integration or debugging)
        RelJsonWriter jsonWriter = new RelJsonWriter();
        rootNode.explain(jsonWriter);
        System.out.println("Execution Plan (JSON Format):");
        System.out.println(jsonWriter.asString());

        // Option 2: Tree Format (more human-readable)
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelWriterImpl treeWriter = new RelWriterImpl(pw, SqlExplainLevel.ALL_ATTRIBUTES, false);
        rootNode.explain(treeWriter);
        System.out.println("Execution Plan (Tree Format):");
        System.out.println(sw.toString());
    }
}
