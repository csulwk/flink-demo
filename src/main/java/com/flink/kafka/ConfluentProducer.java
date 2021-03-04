package com.flink.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * @author kai
 * @date 2021-03-04 0:40
 */
@Slf4j
public class ConfluentProducer {

    private static final String USER_SCHEMA = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}," +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"age\",\"type\":\"int\"}]}";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9091,192.168.99.100:9092,192.168.99.100:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://192.168.99.100:9081");

        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        Random rand = new Random();
        int id = 0;

        while(id < 100) {
            id++;
            String name = "name" + id;
            int age = rand.nextInt(40) + 1;
            GenericRecord user = new GenericData.Record(schema);
            user.put("id", id);
            user.put("name", name);
            user.put("age", age);

            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("mytopic", user);
            producer.send(record);

            log.info("User: {}, {}", id, user.toString());
            Thread.sleep(1000 * 10);
        }

        producer.close();

    }
}
