package org.example.source;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.WaterSensor;

public class MyDatagenSource {

    public static DataGeneratorSource createNumberDataSource(){
        return new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long number) throws Exception {
                        return "Number: " + number;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000),
                Types.STRING
        );
    }
    public static DataGeneratorSource creteaWaterSensorDataSource(){
        RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
        return new DataGeneratorSource<>(
                new GeneratorFunction<Long,WaterSensor>() {
                    @Override
                    public WaterSensor map(Long number) throws Exception {
                        Long sensorId = number % 4;
                        return new WaterSensor(
                                "s"+sensorId,
                                System.currentTimeMillis(),
                                randomDataGenerator.nextInt(1, 100)
                        );
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
//               new TypeHint<WaterSensor>(){}.getTypeInfo() // What is this?
                Types.POJO(WaterSensor.class)
        );
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        DataGeneratorSource numberDataSource = createNumberDataSource();
        senv.fromSource(numberDataSource, WatermarkStrategy.noWatermarks(), "DataGen Source").print();
        senv.execute();
    }
}
