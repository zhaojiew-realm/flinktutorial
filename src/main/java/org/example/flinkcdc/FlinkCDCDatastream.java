package org.example.flinkcdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCDatastream {
    public static void main(String[] args) throws Exception {
        // 1. get flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. open checkpoint


        // 3. use flink cdc construct mysql source
        // 这里可能需要调整mysql的时区，否则会出现报错
        MySqlSource<String>  mySqlSource = MySqlSource.<String>builder()
                .hostname("msyql57.test.com")
                .port(3306)
                .username("root")
                .password("passwd")
                .databaseList("test")
                .tableList("test.person") // when write table, need database name. NULL means all tables.
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        // 4. read data
        DataStreamSource<String> mysqlds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");

        // 5. print
        mysqlds.print();
        //6. run
        env.execute();
    }
}