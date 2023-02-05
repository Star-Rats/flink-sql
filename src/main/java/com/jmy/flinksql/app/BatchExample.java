package com.jmy.flinksql.app;

import org.apache.flink.table.api.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 模拟多个数据源的联邦查询
 */
public class BatchExample {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance()
                        .inBatchMode()
                        .build()
        );

        DataType userType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("username", DataTypes.STRING()),
                DataTypes.FIELD("name", DataTypes.STRING())
        );
        Row[] userRow = users().stream()
                .map(user -> Row.of(user.get("id"), user.get("username"), user.get("name")))
                .toArray(Row[]::new);
        Table user = tableEnv.fromValues(userType, (Object[]) userRow);

        DataType schoolType = DataTypes.ROW(
                DataTypes.FIELD("username", DataTypes.STRING()),
                DataTypes.FIELD("school", DataTypes.STRING())
        );
        Row[] schoolRow = schools().stream()
                .map(school -> Row.of(school.get("username"), school.get("school")))
                .toArray(Row[]::new);
        Table school = tableEnv.fromValues(schoolType, (Object[]) schoolRow);

        tableEnv.createTemporaryView("sys_user",user);
        tableEnv.createTemporaryView("sys_school",school);

        List<Map<String,Object>> res = new ArrayList<>();
        TableResult result = tableEnv.executeSql("SELECT u.id,u.username,u.name,s.school FROM sys_user u LEFT JOIN sys_school s ON u.username = s.username");
        result.collect()
                .forEachRemaining(
                        row -> {
                            Map<String, Object> map = new HashMap<>();
                            for (int i = 0; i < row.getArity(); i++) {
                                map.put(result.getTableSchema().getFieldNames()[i], row.getField(i));
                            }
                            res.add(map);
                        }
                );
        res.forEach(System.out::println);
    }

    private static List<Map<String,Object>> users(){
        return new ArrayList<Map<String,Object>>(){
            {
                Map<String,Object> zs = new HashMap<String,Object>(){
                    {
                        put("id",1);
                        put("username","zhangsan");
                        put("name","张三");
                    }
                };
                add(zs);
                Map<String,Object> ls = new HashMap<String,Object>(){
                    {
                        put("id",2);
                        put("username","lisi");
                        put("name","李四");
                    }
                };
                add(ls);
            }
        };
    }

    private static List<Map<String,Object>> schools(){
        return new ArrayList<Map<String,Object>>(){
            {
                Map<String,Object> zs = new HashMap<String,Object>(){
                    {
                        put("username","zhangsan");
                        put("school","五道口职业技术学院");
                    }
                };
                add(zs);
                Map<String,Object> ls = new HashMap<String,Object>(){
                    {
                        put("username","lisi");
                        put("school","珞珈山技校");
                    }
                };
                add(ls);
            }
        };
    }
}
