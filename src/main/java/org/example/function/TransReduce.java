package org.example.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.WaterSensor;
import org.example.source.WaterSensorMapFunction;

public class TransReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("172.31.3.41", 18888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        int maxVc = Math.max(value1.getVc(), value2.getVc());
                        //实现 max(vc)的效果 取最大值，其他字段以当前组的第一个为主
                        //value1.setVc(maxVc);
                        //实现 maxBy(vc)的效果 取当前最大值的所有字段
                        if (value1.getVc() > value2.getVc()) {
                            value1.setVc(maxVc);
                            return value1;
                        } else {
                            value2.setVc(maxVc);
                            return value2;
                        }
                    }
                })
                .print();
        env.execute();
    }
}
