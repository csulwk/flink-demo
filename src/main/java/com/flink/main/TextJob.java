package com.flink.main;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * @author kai
 * @date 2021-12-13 22:41
 */
@Slf4j
public class TextJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> stream = env.readTextFile("doc/test.txt")
                .map(new RichMapFunction<String, String>() {
                    @Override
                    public String map(String line) throws Exception {
                        String[] arr = StringUtils.split(line, ",");
                        String joinStr = StringUtils.joinWith("-", arr);
                        log.info("映射: {}", joinStr);
                        Thread.sleep(1000 * 5);
                        return joinStr;
                    }
                })
                .process(new ProcessFunction<String, String>() {
                    @Override
                        public void processElement(String str, Context ctx, Collector<String> out) throws Exception {
                        log.info("处理: {}", str);
                        out.collect(str);
                        Thread.sleep(1000 * 10);
                    }
                });

        stream.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String str, Context context) throws Exception {
                log.info("输出: {}", str);
                Thread.sleep(1000 * 15);
            }
        });

        env.execute("TextJob");
    }
}
