package org.tinhn.common;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaSender {
    private static Producer<String, String> producer;
    private static KafkaSender _instance = null;

    private final Boolean isAsync;

    public static KafkaSender getInstance(String brokers, String username, String password, Boolean isAsync) {
        if (_instance == null || producer == null)
            synchronized (KafkaSender.class) {
                if (_instance == null || producer == null)
                    _instance = new KafkaSender(brokers, username, password, isAsync);
            }
        return _instance;
    }

    public KafkaSender(String brokers, String username, String password, Boolean isAsync) {
        Properties props = new MyKafkaConfig(username, password).GetKafkaProperties(brokers);

        producer = new KafkaProducer<>(props);
        this.isAsync = isAsync;
    }

    public void SendMessage(String topic, String messageStr) {
        if (!messageStr.isEmpty()) {
            try {
                if (isAsync) {
                    long startTime = System.currentTimeMillis();
                    producer.send(new ProducerRecord<>(topic, messageStr), new SendResultCallBack(startTime, messageStr));
                } else {
                    ProducerRecord<String, String> data = new ProducerRecord<>(topic, System.currentTimeMillis() + "", messageStr);
                    producer.send(data);
                }
            } catch (Exception e) {
                System.err.println(String.format("Kafka.SendMessage(%s) ", topic) + e.getMessage());
            }
        }
    }

    public void Close() {
        producer.close();
        _instance = null;
    }

    private static class SendResultCallBack implements Callback {
        private final long startTime;
        private final String message;

        public SendResultCallBack(long startTime, String message) {
            this.startTime = startTime;
            this.message = message;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
         * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
         * non-null.
         *
         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *                  occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                System.out.println("message(" + message + ") sent to partition(" + metadata.partition() + "), offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            } else {
                System.err.println(exception.getMessage());
            }
        }
    }
}
