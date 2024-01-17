package org.example.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class KFirehoseSinkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // kdf sink
        Properties sinkProperties = new Properties();
        sinkProperties.put(AWSConfigConstants.AWS_REGION, "cn-north-1");

        KinesisFirehoseSink<String> kdfSink = KinesisFirehoseSink.<String>builder()
                .setFirehoseClientProperties(sinkProperties)      // Required
                .setSerializationSchema(new SimpleStringSchema()) // Required
                .setDeliveryStreamName("PUT-S3-S9qP9")        // Required
//                        .setFailOnError(false)                            // Optional
//                        .setMaxBatchSize(500)                             // Optional
//                        .setMaxInFlightRequests(50)                       // Optional
//                        .setMaxBufferedRequests(10_000)                   // Optional
//                        .setMaxBatchSizeInBytes(4 * 1024 * 1024)          // Optional
//                        .setMaxTimeInBufferMS(5000)                       // Optional
//                        .setMaxRecordSizeInBytes(1000 * 1024)             // Optional
                .build();
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

        DataStreamSource<String> fd = senv.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        fd.sinkTo(kdfSink);
        try {
            senv.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
