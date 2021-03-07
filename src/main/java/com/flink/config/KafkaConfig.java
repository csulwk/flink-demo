package com.flink.config;

import com.flink.util.PropertyUtils;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author kai
 * @date 2021-03-07 10:07
 */
public class KafkaConfig {

    public static final String CONSUMER_BOOTSTRAP_SERVERS = "consumer.bootstrap.servers";
    public static final String CONSUMER_GROUP_ID = "consumer.group.id";
    public static final String CONSUMER_KEY_DESERIALIZER = "consumer.key.deserializer";
    public static final String CONSUMER_VALUE_DESERIALIZER = "consumer.value.deserializer";
    public static final String CONSUMER_AUTO_OFFSET_RESET = "consumer.auto.offset.reset";
    public static final String CONSUMER_SCHEMA_REGISTRY_URL = "consumer.schema.registry.url";
    public static final String CONSUMER_TOPIC_NAME = "consumer.topic.name";

    public static Properties buildProperties(Properties props) {
        Properties result = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(CONSUMER_BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, props.getProperty(CONSUMER_GROUP_ID));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, props.getProperty(CONSUMER_KEY_DESERIALIZER));
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, props.getProperty(CONSUMER_VALUE_DESERIALIZER));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.getProperty(CONSUMER_AUTO_OFFSET_RESET));
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(CONSUMER_SCHEMA_REGISTRY_URL));
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 50000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 3000);
        return result;
    }

    public static String getTopic(Properties props) {
        return props.getProperty(CONSUMER_TOPIC_NAME);
    }
}
