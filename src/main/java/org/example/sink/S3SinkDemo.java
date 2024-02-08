package org.example.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.source.MyDatagenSource;

public class S3SinkDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define datagen source
        DataGeneratorSource dataGeneratorSource = MyDatagenSource.createNumberDataSource();

        // Define s3 sink
        String outputPath = "s3://zhaojiew-tmp/flinkfors3/";
        DataStreamSource<String> fd = senv.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "Datagen Source");
        fd.writeAsText(outputPath);
//        FileSink<String> filesink = FileSink.forRowFormat(
//                        new Path(outputPath),
//                        new SimpleStringEncoder<String>("UTF-8")).withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(Duration.ofMinutes(3))
//                                .withInactivityInterval(Duration.ofMinutes(1))
//                                .withMaxPartSize(MemorySize.ofMebiBytes(5))
//                                .build())
//                .build();
//        fd.sinkTo(filesink);
            senv.execute();

    }
}
