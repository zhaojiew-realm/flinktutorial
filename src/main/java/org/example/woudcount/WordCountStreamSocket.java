package org.example.woudcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamSocket {
    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        senv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> lineStream = senv.readTextFile("input");
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineStream.flatMap((String value, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.LONG)) // TypeHint
                .keyBy(data -> data.f0).sum(1);
        sum.print();

        senv.execute(); // start stream job
    }
}
