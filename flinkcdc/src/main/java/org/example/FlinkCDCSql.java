package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDCSql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("" +
                "create table t1(\n" +
                "id string primary key NOT ENFORCED,\n" +
                "name string"+
                ") WITH (\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = 'msyql57.test.com',\n" +
                "'port' = '3306',\n" +
                "'username' = 'root',\n" +
                "'password' = 'passwd',\n" +
                "'database-name' = 'test',\n" +
                "'table-name' = 'person'\n" +
                ")");

        Table table = tableEnv.sqlQuery("select * from t1");
        table.execute().print();

    }
}
