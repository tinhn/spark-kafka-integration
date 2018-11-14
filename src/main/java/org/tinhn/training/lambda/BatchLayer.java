package org.tinhn.training.lambda;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public final class BatchLayer<K, M, U> extends AbstractSparkLayer<K, M> {

    private static final Logger log = LoggerFactory.getLogger(BatchLayer.class);

    private JavaStreamingContext streamingContext;
    private final String defaultMaster;

    public BatchLayer(String _brokers, String _username, String _password, int intervalSec, String _defaultMaster) {
        super(_brokers, _username, _password, intervalSec);
        defaultMaster = _defaultMaster;
    }

    @Override
    protected String getLayerName() {
        return "BatchLayer";
    }

    public synchronized void start(String readTopic) {
        streamingContext = buildStreamingContext(defaultMaster);

        /*
        JavaSparkContext javaSparkContext = streamingContext.sparkContext();

        SparkContext sparkContext = javaSparkContext.sc();
        SQLContext sqlContext = new SQLContext(sparkContext);*/

        log.info("Creating message stream from topic {}", readTopic);

        JavaInputDStream<ConsumerRecord<K, M>> kafkaDStream = buildInputDStream(streamingContext, readTopic);
        JavaPairDStream<K, M> pairDStream = kafkaDStream.mapToPair(mAndM -> new Tuple2<>(mAndM.key(), mAndM.value()));

        pairDStream.foreachRDD(rdd ->
                rdd.foreach(record -> {
                            String kafmsg = (String) record._2;
                            System.out.println("kafka: " + kafmsg);
                        }
                ));

        log.info("Starting Spark Streaming");
        streamingContext.start();
    }

    public void await() throws InterruptedException {
        JavaStreamingContext theStreamingContext;
        synchronized (this) {
            theStreamingContext = streamingContext;
            Preconditions.checkState(theStreamingContext != null);
        }
        log.info("Spark Streaming is running");
        theStreamingContext.awaitTermination(); // Can't do this with lock
    }

    @Override
    public synchronized void close() {
        if (streamingContext != null) {
            log.info("Shutting down Spark Streaming; this may take some time");
            streamingContext.stop(true, true);
            streamingContext = null;
        }
    }
}
