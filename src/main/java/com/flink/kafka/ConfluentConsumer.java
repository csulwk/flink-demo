package com.flink.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 参考：
 * http://zhongmingmao.me/2019/03/26/kafka-docker-schema-registry/
 * https://blog.csdn.net/qq_21383435/article/details/108522841
 * @author kai
 * @date 2021-03-04 0:42
 */
public class ConfluentConsumer {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9091,192.168.99.100:9092,192.168.99.100:9093");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        // 添加schema服务的地址，用于获取schema
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.99.100:9081");
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("mytopic"));
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    GenericRecord user = record.value();
                    System.out.println("value = [user.id = " + user.get("id") + ", " + "user.name = "
                            + user.get("name") + ", " + "user.age = " + user.get("age") + "], "
                            + "partition = " + record.partition() + ", " + "offset = " + record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
