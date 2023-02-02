package com.jmy.flinksql.app;

import com.jmy.flinksql.domain.FlinkField;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;


public class MapToBean {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(streamEnv);

        DataStreamSource<FlinkField> userStream = streamEnv.fromCollection(getUsers());
        DataStreamSource<FlinkField> schoolStream = streamEnv.fromCollection(getSchool());


        stEnv.createTemporaryView("t_user",userStream);
        stEnv.createTemporaryView("t_school",schoolStream);
        TableResult result = stEnv.executeSql("SELECT u.f0,u.f1,s.f1 FROM t_user u LEFT JOIN t_school s ON u.f0 = s.f0");
        result.print();
    }

    private static List<FlinkField> getUsers(){
        return new ArrayList<FlinkField>(){
            {
                FlinkField zs = new FlinkField();
                zs.setF0("zhangsan");
                zs.setF1("张三");
                FlinkField ls = new FlinkField();
                ls.setF0("lisi");
                ls.setF1("李四");

                add(zs);
                add(ls);
            }
        };
    }

    private static List<FlinkField> getSchool(){
        return new ArrayList<FlinkField>(){
            {
                FlinkField zs = new FlinkField();
                zs.setF0("zhangsan");
                zs.setF1("五道口职业技术学院");
                FlinkField ls = new FlinkField();
                ls.setF0("lisi");
                ls.setF1("珞珈山技校");

                add(zs);
                add(ls);
            }
        };
    }

}
