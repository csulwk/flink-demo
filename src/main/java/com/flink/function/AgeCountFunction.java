package com.flink.function;

import com.flink.bean.AgeCountOut;
import com.flink.bean.Person;
import com.flink.main.AgeCountJob;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 按年龄统计数量
 * @author kai
 * @date 2021-03-07 15:31
 */
public class AgeCountFunction implements FlatMapFunction<Person, AgeCountOut> {

    @Override
    public void flatMap(Person person, Collector<AgeCountOut> out) throws Exception {
        out.collect(new AgeCountOut(person.getAge(), 1L));
    }
}
