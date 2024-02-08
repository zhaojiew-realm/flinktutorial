package org.example.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    public String id;
    public Long ts;
    public Integer vc;

//    public WaterSensor(String id, Long ts, Integer vc) {
//        this.id = id;
//        this.ts = ts;
//        this.vc = vc;
//    }
}
