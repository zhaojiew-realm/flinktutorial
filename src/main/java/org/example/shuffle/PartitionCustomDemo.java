package org.example.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataStreamSource<String> socketDS =
                env.socketTextStream("172.31.3.41", 18888);
        DataStream<String> myDS = socketDS
                .partitionCustom(
                        new MyPartitioner(),
                        value -> value
                );
        myDS.print();
        env.execute();
    }
}
