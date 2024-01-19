package org.example.source;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.WaterSensor;

public class WaterSensorGenSourceDemo {
    public static void main(String[] args) throws Exception {

        RandomDataGenerator randomDataGenerator = new RandomDataGenerator();

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        DataGeneratorSource<WaterSensor> stringDataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long,WaterSensor>() {
                    @Override
                    public WaterSensor map(Long number) throws Exception {
                        return new WaterSensor(
                                ""+number,
                                System.currentTimeMillis(),
                                randomDataGenerator.nextInt(1, 100)
                        );
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                new TypeHint<WaterSensor>(){}.getTypeInfo() // What is this?
        );

        senv.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "DataGen Source").print();
        senv.execute();
    }
}
