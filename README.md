# calcite-with-quickpick



## index



- `Java version：1.8`
- `Maven Dependency：`

```
<dependencies>
        <!-- https://mvnrepository.com/artifact/org.apache.calcite/calcite-core -->
        <dependency>
            <groupId>org.apache.calcite</groupId>
            <artifactId>calcite-core</artifactId>
            <version>1.31.0</version>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>42.2.24</version>
        </dependency>
        <dependency>
            <groupId>com.github.jsqlparser</groupId>
            <artifactId>jsqlparser</artifactId>
            <version>4.5</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13.1</version>
            <scope>compile</scope>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.assertj/assertj-core -->
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>3.26.3</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
```





## test



- **testExtractTables：**测试提取表名的功能
- **testExtractPredicates：**测试提取谓词的功能
- **testBuildPredicate：**测试构建谓词为 RelNode 的功能
- **testGetStringRelNodeMap：**测试谓词与 RelNode 的映射关系
- **testCombineTablesAndNonJoinPredicates：**测试表名（scan）与非连接谓词（filter）的结合功能