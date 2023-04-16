package com.flink.main;

import com.flink.bean.Person;
import com.flink.function.PersonMapFunction;
import com.flink.function.PersonSinkFunction;
import com.flink.source.PersonSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kai
 * @date 2023-4-15 15:59
 */
public class PersonJob {
    private static Logger LOG = LoggerFactory.getLogger(PersonJob.class);
    private static final String JOB_NAME = "CustomJob";

    public static void main(String[] args) throws Exception {
        LOG.info("PersonJob...");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> stream = env
                .addSource(new PersonSource())
                .map(new PersonMapFunction());
        stream.addSink(new PersonSinkFunction());

        LOG.info("submit jobName = {}", JOB_NAME);
        env.execute(JOB_NAME);
    }
}
