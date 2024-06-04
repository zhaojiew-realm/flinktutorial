package org.example.kinesis;

import com.amazonaws.services.kinesis.producer.KinesisProducer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class KPLProducer {
    public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException {

        KinesisProducer kinesis = new KinesisProducer();
        for (int i = 0; i < 100; ++i) {
            ByteBuffer data = ByteBuffer.wrap("myData".getBytes("UTF-8"));
            // doesn't block
            kinesis.addUserRecord("dsdefault", "myPartitionKey"+i, data);
            Thread.sleep(1000);
        }
    }
}
