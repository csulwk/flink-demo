package com.flink.main;

import com.flink.config.Constant;
import com.flink.config.KafkaConfig;
import com.flink.util.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 测试
 * @author kai
 * @date 2021-03-07 10:45
 */
public class Demo {
    private static Logger LOG = LoggerFactory.getLogger(Demo.class);

    public static void main(String[] args) {

        Properties props = PropertyUtils.readProps(Constant.PROP_FILE_PATH);
        PropertyUtils.print(props);
        LOG.info(props.getProperty("prop.test1"));

        Properties kp = KafkaConfig.buildProperties(props);
        PropertyUtils.print(kp);
    }
}
