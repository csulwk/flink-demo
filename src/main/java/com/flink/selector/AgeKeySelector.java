package com.flink.selector;

import com.flink.bean.AgeCountOut;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author kai
 * @date 2021-03-07 15:38
 */
public class AgeKeySelector implements KeySelector<AgeCountOut, Integer> {
    @Override
    public Integer getKey(AgeCountOut out) throws Exception {
        return out.getAge();
    }
}
