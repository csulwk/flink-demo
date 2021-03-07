package com.flink.source;

import cn.hutool.core.util.RandomUtil;
import com.flink.bean.Person;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 每隔数秒生成一条记录
 * @author kai
 * @date 2021-03-07 12:20
 */
public class PersonSource implements SourceFunction<Person> {
    private static Logger LOG = LoggerFactory.getLogger(PersonSource.class);

    private volatile boolean isRunning = true;
    private int counter = 1;

    @Override
    public void run(SourceContext<Person> ctx) throws Exception {
        final int ageMin = 11;
        final int ageMax = 16;
        final int nameLen = 3;
        final long interval = 1000 * 1;

        while (isRunning) {
            Person p = new Person(counter, RandomUtil.randomString(RandomUtil.BASE_CHAR, nameLen),
                    RandomUtil.randomInt(ageMin, ageMax));
            LOG.info("ID: {}, PERSON: {}", counter, p);
            ctx.collect(p);
            counter++;
            Thread.sleep(interval);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
