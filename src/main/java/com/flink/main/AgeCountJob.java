package com.flink.main;

import com.flink.bean.AgeCountOut;
import com.flink.bean.Person;
import com.flink.function.AgeCountFunction;
import com.flink.selector.AgeKeySelector;
import com.flink.source.PersonSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kai
 * @date 2021-03-07 15:22
 */
public class AgeCountJob {
    private static Logger LOG = LoggerFactory.getLogger(AgeCountJob.class);

    public static void main(String[] args) throws Exception {
        // 创建流式计算环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取自定义数据源
        DataStream<Person> text = env.addSource(new PersonSource()).name("source");

        // 将接收的数据进行拆分，分组，窗口计算并且进行聚合输出
        DataStream<AgeCountOut> windowCount = text
                .flatMap(new AgeCountFunction()).name("function")
                .keyBy(new AgeKeySelector())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                .sum("cnt").name("sum");
//                .reduce((ReduceFunction<AgeCountOut>) (o1, o2)-> new AgeCountOut(o1.getAge(), o1.getCnt() + o2.getCnt())).name("reduce");

        // 打印结果
        windowCount.print().setParallelism(1).name("sink");

        env.execute("Socket Window WordCount");
    }
}
