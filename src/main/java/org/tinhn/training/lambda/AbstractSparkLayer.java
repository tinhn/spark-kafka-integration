package org.tinhn.training.lambda;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tinhn.common.MyKafkaConfig;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSparkLayer<K, M> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractSparkLayer.class);
    private final int generationIntervalSec;
    private final String brokers;
    private final String username;
    private final String password;

    protected AbstractSparkLayer(String _brokers, String _username, String _password, int intervalSec) {
        Preconditions.checkArgument(intervalSec > 0);
        generationIntervalSec = intervalSec;
        brokers = _brokers;
        username = _username;
        password = _password;
    }

    protected abstract String getLayerName();

    protected final JavaStreamingContext buildStreamingContext(String sparkStreamingMaster) {
        log.info("Starting SparkContext with interval {} seconds", generationIntervalSec);

        SparkConf sparkConf = new SparkConf();

        // Only for tests, really
        if (sparkConf.getOption("spark.master").isEmpty()) {
            log.info("Overriding master to {} for tests", sparkStreamingMaster);
            sparkConf.setMaster(sparkStreamingMaster);
        }
        // Only for tests, really
        if (sparkConf.getOption("spark.app.name").isEmpty()) {
            String appName = "Demo Kafka Consumer - " + getLayerName();

            log.info("Overriding app name to {} for tests", appName);
            sparkConf.setAppName(appName);
        }

        // Turn this down to prevent long blocking at shutdown
        sparkConf.setIfMissing(
                "spark.streaming.gracefulStopTimeout",
                Long.toString(TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS)));
        sparkConf.setIfMissing("spark.cleaner.ttl", Integer.toString(20 * generationIntervalSec));

        sparkConf.setIfMissing("spark.network.timeout", "700")
                .setIfMissing("spark.rpc.askTimeout", "800")
                .setIfMissing("spark.sql.broadcastTimeout", "1200")
                .setIfMissing("spark.driver.maxResultSize", "0")
                .setIfMissing("spark.streaming.backpressure.enabled", "true")
                .setIfMissing("spark.streaming.backpressure.initialRate", "5000")
                .setIfMissing("spark.streaming.receiver.maxRate", "0")
                .setIfMissing("spark.streaming.kafka.maxRatePerPartition", "100");

        long generationIntervalMS = TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));

        return new JavaStreamingContext(jsc, new Duration(generationIntervalMS));
    }

    protected final JavaInputDStream<ConsumerRecord<K, M>> buildInputDStream(JavaStreamingContext streamingContext, String readTopic) {
        Properties props = new MyKafkaConfig(username, password).GetKafkaProperties(brokers);
        Map<String, Object> kafkaParams = new HashMap(props);

        LocationStrategy locationStrategy = LocationStrategies.PreferConsistent();
        ConsumerStrategy<K, M> consumerStrategy = ConsumerStrategies.Subscribe(Arrays.asList(readTopic), kafkaParams, Collections.emptyMap());

        return org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream(
                streamingContext,
                locationStrategy,
                consumerStrategy);
    }
}