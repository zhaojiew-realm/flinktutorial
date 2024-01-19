package org.example.shuffle;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ShuffleExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> stream = env.socketTextStream("172.31.3.41", 18888);;
//        stream.shuffle().print();
        stream.rebalance().print();
//        stream.rescale().print();
//        stream.global().print();

        env.execute();
    }
}
