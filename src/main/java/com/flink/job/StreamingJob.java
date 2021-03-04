/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.job;

import cn.hutool.core.map.CaseInsensitiveMap;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.flink.bean.GenericKafkaDeserializationSchema;
import com.flink.bean.GenericRichMapFunction;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * Flink Streaming Job.
 * @author kai
 */
@Slf4j
public class StreamingJob {

	public static void main(String[] args) throws Exception {
        final String topicName = "mytopic";
	    final String registryUrl = "http://192.168.99.100:9081";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
        env.getConfig().addDefaultKryoSerializer(GenericRecord.class, DefaultSerializers.StringSerializer.class);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9091,192.168.99.100:9092,192.168.99.100:9093");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "myflink");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 50000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 3000);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);

        FlinkKafkaConsumer<GenericRecord> consumer = new FlinkKafkaConsumer<GenericRecord>(
                topicName,
                new GenericKafkaDeserializationSchema(registryUrl),
                props);
        consumer.setCommitOffsetsOnCheckpoints(true);

//        SingleOutputStreamOperator<CaseInsensitiveMap> streamMap = env.addSource(consumer)
//                .name("kafkaSource").uid("kafkaSource")
//                .map(new GenericRichMapFunction()).name("genericMap").uid("genericMap");
//        streamMap.print();

        SingleOutputStreamOperator<String> streamStr = env.addSource(consumer)
                .name("kafkaSource").uid("kafkaSource")
                .map(record -> {
                    log.info("record: {}, {}",record.getSchema(), record.toString());
                    return record.get("name") + "; " + record.get("id");
                }).name("genericString").uid("genericString");
        streamStr.print();

        env.execute("MyJob");
	}
}
