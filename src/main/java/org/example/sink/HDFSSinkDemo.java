package org.example.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.example.source.MyDatagenSource;

import java.time.Duration;

public class HDFSSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        // Define datagen source
        DataGeneratorSource dataGeneratorSource = MyDatagenSource.createNumberDataSource();

        // Define hdfs sink
        String outputPath = "hdfs:///test";
        DataStreamSource datagenSource = senv.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "Datagen Source");
//        DataStreamSource<Integer> integerDataStreamSource = senv.fromElements(1, 2, 3, 35, 6, 7, 8, 8);
//        fd.writeAsText(outputPath);
        FileSink<String> filesink = FileSink.forRowFormat(
                        new Path(outputPath),
                        new SimpleStringEncoder<String>("UTF-8")).withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(3))
                                .withInactivityInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(MemorySize.ofMebiBytes(5))
                                .build())
                .build();
//        datagenSource.print();
        datagenSource.map(e -> e.toString()).sinkTo(filesink);
        senv.execute();

    }
}
