package org.example.iimpl;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.bean.WaterSensor;

public class MapFunctionImpl implements MapFunction<WaterSensor, String> {
    @Override
    public String map(WaterSensor e) throws Exception {
        return e.id;
    }
}
