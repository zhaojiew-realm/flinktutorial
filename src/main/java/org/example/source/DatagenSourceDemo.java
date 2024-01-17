package org.example.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DatagenSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long number) throws Exception {
                        return "Number: " + number;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        senv.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "DataGen Source").print();
        senv.execute();
    }
}
