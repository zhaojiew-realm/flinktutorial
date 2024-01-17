package org.example.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class S3SinkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        // Define datagen source
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

        // Define s3 sink
        String outputPath = "s3://zhaojiew-tmp/flinkfors3/";
        DataStreamSource<String> fd = senv.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        FileSink<String> filesink = FileSink.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofMinutes(3))
                        .withInactivityInterval(Duration.ofMinutes(1))
                        .withMaxPartSize(MemorySize.ofMebiBytes(5))
                        .build())
                .build();
        fd.sinkTo(filesink);
        try {
            senv.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
