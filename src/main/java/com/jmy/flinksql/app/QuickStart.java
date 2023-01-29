package com.jmy.flinksql.app;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.CloseableIterator;

public class QuickStart {
    public static void main(String[] args) throws Exception{
        // 创建表环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 使用流处理模式
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment(),
                settings);

        // 创建表
        String userDDL = "CREATE TABLE sys_user (\n" +
                " id INT,\n" +
                " username STRING,\n" +
                " name STRING,\n" +
                " status INT,\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://192.168.1.95:3306/parkinsondb',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'X!wWoPhGbH+pFt-umBu2',\n" +
                " 'table-name' = 'sys_user'\n" +
                ")";

        String groupUserDDL = "CREATE TABLE pf_group_user (\n" +
                " id INT,\n" +
                " user_id INT,\n" +
                " group_id INT,\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://192.168.1.95:3306/parkinsondb',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'X!wWoPhGbH+pFt-umBu2',\n" +
                " 'table-name' = 'pf_group_user'\n" +
                ")";

        String groupDDL = "CREATE TABLE pf_group (\n" +
                " id INT,\n" +
                " group_name STRING,\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://192.168.1.95:3306/parkinsondb',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'X!wWoPhGbH+pFt-umBu2',\n" +
                " 'table-name' = 'pf_group'\n" +
                ")";
        tableEnv.executeSql(userDDL);
        tableEnv.executeSql(groupUserDDL);
        tableEnv.executeSql(groupDDL);


        // 查询表
        String sql = "SELECT\n" +
                "\tsu.id,\n" +
                "\tsu.username,\n" +
                "\tsu.`name`,\n" +
                "\tsu.`status`,\n" +
                "\tpg.group_name AS groupName\n" +
                "FROM\n" +
                "\tsys_user su\n" +
                "\tLEFT JOIN pf_group_user pgu ON su.id = pgu.user_id\n" +
                "\tLEFT JOIN pf_group pg ON pgu.group_id = pg.id\n" +
                "WHERE pg.group_name IS NOT NULL";

        Table sysUser = tableEnv.sqlQuery(sql);

        // 将表转换成流
        DataStream<Tuple2<Boolean, SysUser>> dataStream = tableEnv.toRetractStream(sysUser, SysUser.class);

        try (CloseableIterator<Tuple2<Boolean, SysUser>> rowCloseableIterator = dataStream.executeAndCollect()) {
            rowCloseableIterator.forEachRemaining(
                    System.out::println
            );
        }

        Thread.sleep(500000);
    }
}
