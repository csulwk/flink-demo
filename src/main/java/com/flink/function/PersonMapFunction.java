package com.flink.function;

import com.flink.bean.Person;
import com.flink.main.CustomJob;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kai
 * @date 2023-04-15 15:46
 */
public class PersonMapFunction implements MapFunction<Person, String> {
    private static Logger LOG = LoggerFactory.getLogger(PersonMapFunction.class);
    @Override
    public String map(Person person) throws Exception {
        LOG.info("PersonMapFunction: Person={}", person);
        return StringUtils.joinWith("+", person.getId(), person.getName());
    }
}
