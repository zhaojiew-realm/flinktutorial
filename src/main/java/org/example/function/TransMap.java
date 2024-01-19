package org.example.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.WaterSensor;

public class TransMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );
        // 方式一：传入匿名类，实现 MapFunction
        stream.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor e) throws Exception {
                return e.id;
            }
        }).print();
        // 方式二：传入 MapFunction 的实现类
        // stream.map(new UserMap()).print();
        env.execute();
    }

    public static class UserMap implements MapFunction<WaterSensor,
            String> {
        @Override
        public String map(WaterSensor e) throws Exception {
            return e.id;
        }
    }
}
