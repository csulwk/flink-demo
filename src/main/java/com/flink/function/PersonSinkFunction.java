package com.flink.function;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kai
 * @date 2023-04-15 15:50
 */
public class PersonSinkFunction implements SinkFunction<String> {
    private static Logger LOG = LoggerFactory.getLogger(PersonSinkFunction.class);
    int cnt = 0;
    @Override
    public void invoke(String value, Context context) throws Exception {
        LOG.info("PersonSinkFunction: value={}", value);
        cnt = cnt + 1;
        LOG.info("cnt = {}", cnt);
    }
}
