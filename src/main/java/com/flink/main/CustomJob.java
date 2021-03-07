package com.flink.main;

import com.flink.bean.Person;
import com.flink.source.PersonSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kai
 * @date 2021-03-07 12:19
 */
public class CustomJob {
    private static Logger LOG = LoggerFactory.getLogger(CustomJob.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SourceFunction<Person> source = new PersonSource();
        DataStream<Person> stream = env.addSource(source);

        stream.print();

        env.execute("CustomJob");
    }
}
