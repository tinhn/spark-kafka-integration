package org.tinhn.common;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyKafkaConfig {
    private final String jaasCfg;

    public MyKafkaConfig(String username, String password) {
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        jaasCfg = String.format(jaasTemplate, username, password);
    }

    public Properties GetKafkaProperties(String brokers) {
        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();

        Properties props = new Properties();

        props.put("bootstrap.servers", brokers);
        props.put("group.id", "mylabs-consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);

        /*using SASL/SCRAM for authorize*/
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", jaasCfg);

        return props;
    }
}
