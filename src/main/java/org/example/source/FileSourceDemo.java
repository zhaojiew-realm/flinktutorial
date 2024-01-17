package org.example.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> filesource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("input/data.txt")).build();
        senv.fromSource(filesource, WatermarkStrategy.noWatermarks(),"filesource").print();
        senv.execute();
    }
}
