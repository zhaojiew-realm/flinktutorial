package org.example.shuffle;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> ds = env.socketTextStream("172.31.3.41", 18888)
                        .map(Integer::valueOf);
        //将 ds 分为两个流 ，一个是奇数流，一个是偶数流
        //使用 filter 过滤两次
        //代码显得有些冗余
//        SingleOutputStreamOperator<Integer> ds1 = ds.filter(x -> x % 2 == 0);
//        SingleOutputStreamOperator<Integer> ds2 = ds.filter(x -> x % 2 == 1);
//        ds1.print("偶数");
//        ds2.print("奇数");
        env.execute();
    }
}
