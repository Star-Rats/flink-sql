package com.jmy.flinksql.app;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

public class DataStream2Table {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        List<UserSchool> list = new ArrayList<UserSchool>(){
            {
                add(new UserSchool(20232,"五道口职业技术学院"));
                add(new UserSchool(20234,"珞珈山技校"));
            }
        };

        DataStreamSource<UserSchool> dataStream = env.fromCollection(list);


        // DataStream 转 table
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("user_school",dataStream);

        TableResult tableResult = tableEnv.executeSql("SELECT * FROM user_school");
        tableResult.print();
    }
}
