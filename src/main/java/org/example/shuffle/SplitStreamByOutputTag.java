package org.example.shuffle;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.WaterSensor;
import org.example.source.MyDatagenSource;

public class SplitStreamByOutputTag {
    public static void main(String[] args) throws Exception {
        RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        //        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("172.31.3.41", 18888)
//                .map(new WaterSensorMapFunction());

        // use the watersensor datagenerator for test
        DataGeneratorSource stringDataGeneratorSource = MyDatagenSource.creteaWaterSensorDataSource();
        DataStreamSource<WaterSensor> ds = env.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "WaterSensor Source");
        OutputTag<WaterSensor> s1 = new OutputTag<WaterSensor>("s1", Types.POJO(WaterSensor.class)) {};
        OutputTag<WaterSensor> s2 = new OutputTag<WaterSensor>("s2", Types.POJO(WaterSensor.class)) {};
        //返回的都是主流
        SingleOutputStreamOperator<WaterSensor> ds1 = ds.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context
                    ctx, Collector<WaterSensor> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    ctx.output(s1, value);
                } else if ("s2".equals(value.getId())) {
                    ctx.output(s2, value);
                } else {
                    //主流
                    out.collect(value);
                }
            }
        });
        // output:
        // s1:1> WaterSensor{id='s1', ts=1, vc=1}
        // s2:2> WaterSensor{id='s2', ts=2, vc=2}
        // 主流，非 s1,s2 的传感器:3> WaterSensor{id='s3', ts=3, vc=4}
        ds1.print("主流，非 s1,s2 的传感器");
        SideOutputDataStream<WaterSensor> s1DS = ds1.getSideOutput(s1);
        SideOutputDataStream<WaterSensor> s2DS = ds1.getSideOutput(s2);
        s1DS.printToErr("s1"); // red output
        s2DS.printToErr("s2");
        env.execute();
    }
}
