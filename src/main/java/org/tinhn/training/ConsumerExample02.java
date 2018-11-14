package org.tinhn.training;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tinhn.training.lambda.BatchLayer;

/**
 * spark-submit --class org.tinhn.training.ConsumerExample02 spark-kafka-consumer_2.3.2-1.0.jar <brokers> <readtopics> <username> <password>
 */
public class ConsumerExample02 {
    private static final Logger log = LoggerFactory.getLogger(ConsumerExample02.class);
    private static int KAFKA_RECURRENT_STREAMING = 30; // in second
    private static String SPARK_MASTER_DEFAULT = "local[3]";

    private ConsumerExample02() {
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: ConsumerExample <brokers> <topics> <username> <password>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <readtopics> is a list of one or more kafka topics to consume from\n\n" +
                    "  <username> is an account for authentication with kafka cluster\n\n" +
                    "  <password> is a password of this account for authentication\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String readtopics = args[1];

        String username = args[2];
        String password = args[3];

        try (BatchLayer<?, ?, ?> batchLayer = new BatchLayer<>(brokers, username, password, KAFKA_RECURRENT_STREAMING, SPARK_MASTER_DEFAULT)) {
            batchLayer.start(readtopics);
            batchLayer.await();
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }
}
