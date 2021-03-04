package com.flink.bean;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * @author kai
 * @date 2021-03-04 21:05
 */
public class GenericKafkaDeserializationSchema implements KafkaDeserializationSchema<GenericRecord> {

    private final String registryUrl;
    private transient KafkaAvroDeserializer deserializer;

    public GenericKafkaDeserializationSchema(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    @Override
    public boolean isEndOfStream(GenericRecord genericRecord) {
        return false;
    }

    @Override
    public GenericRecord deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        if (deserializer == null) {
            Map<String, Object> props = new HashMap<>();
            props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
            SchemaRegistryClient client = new CachedSchemaRegistryClient(registryUrl, AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
            deserializer = new KafkaAvroDeserializer(client, props);
        }
        return (GenericRecord) deserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
    }

    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return TypeExtractor.getForClass(GenericRecord.class);
    }
}
