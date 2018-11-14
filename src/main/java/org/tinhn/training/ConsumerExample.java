package org.tinhn.training;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.tinhn.common.KafkaSender;
import org.tinhn.common.MyKafkaConfig;
import org.tinhn.training.model.CarModel;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * spark-submit --class org.tinhn.training.ConsumerExample spark-kafka-consumer_2.3.2-1.0.jar <brokers> <readtopics> <username> <password> <writetopics>
 */
public class ConsumerExample {
    private static int KAFKA_RECURRENT_STREAMING = 30; // in second
    private static String SPARK_MASTER_DEFAULT = "local[3]";
    private static final Pattern COMMA = Pattern.compile(",");
    private static String writetopics = "";
    private static Gson gson = new Gson();

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

        if (args.length >= 5)
            writetopics = args[4];

        SparkConf sparkConf = new SparkConf()
                .setAppName("Demo Kafka Consumer 2 Kafka")
                .setMaster(SPARK_MASTER_DEFAULT)
                .setIfMissing("spark.driver.maxResultSize", "0")
                .setIfMissing("spark.streaming.backpressure.enabled", "true")
                .setIfMissing("spark.streaming.backpressure.initialRate", "5000")
                .setIfMissing("spark.streaming.receiver.maxRate", "0")
                .setIfMissing("spark.streaming.kafka.maxRatePerPartition", "100")
                .setIfMissing("spark.streaming.gracefulStopTimeout", Long.toString(TimeUnit.MILLISECONDS.convert(KAFKA_RECURRENT_STREAMING, TimeUnit.SECONDS)))
                .setIfMissing("spark.cleaner.ttl", Integer.toString(20 * KAFKA_RECURRENT_STREAMING));

        SparkContext sparkContext = new SparkContext(sparkConf);

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .sparkContext(sparkContext)
                .getOrCreate();
        sparkContext.setLogLevel("ERROR");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
        JavaStreamingContext ssc = new JavaStreamingContext(javaSparkContext, Durations.seconds(KAFKA_RECURRENT_STREAMING));

        Properties props = new MyKafkaConfig(username, password).GetKafkaProperties(brokers);
        Map<String, Object> kafkaParams = new HashMap(props);

        Collection<String> topics = Arrays.asList(readtopics);
        ConsumerStrategy<String, String> consumerStrategies =
                ConsumerStrategies.Subscribe(
                        topics,
                        kafkaParams,
                        Collections.emptyMap()
                );

        System.out.println("Creating message stream from topic: " + readtopics + ", group.id: " + props.getProperty("group.id"));
        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        consumerStrategies
                );

        /*Way 1: Project data with JavaRDD */
        /*messages.foreachRDD(rdd -> {
            if (rdd != null) {
                JavaRDD<String> javaRDD = rdd.map(kafkaObj -> {
                            String lineStr = kafkaObj.value();
                            return lineStr;
                        }
                );

                if (javaRDD != null) {
                    //TO DO HERE
                }
            }
        });
        */

        /*Way 2: Process data direct*/
        /*messages.foreachRDD(rdd -> {
                    if (rdd != null) {
                        rdd.foreach(record -> {
                            String msgStr = record.value();
                            System.out.println("kafka value: " + msgStr);
                        });
                    }
                }
        );
        */

        /*Way 3: Read and count word*/
        // Data like "2018-11-09 11:04:45,Toyota", only needed count at Toyota
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(COMMA.split(x)[1]).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(car -> new Tuple2<>(car, 1)).reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();

        if (StringUtils.isNoneBlank(writetopics)) {
            wordCounts.foreachRDD(rdd -> {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String date_time = formatter.format(Calendar.getInstance().getTime());

                JavaRDD<CarModel> javaRDD = rdd.map(x -> {
                    return new CarModel(x._1, x._2, date_time);
                });

                javaRDD.foreach(carObj -> {
                    KafkaSender.getInstance(brokers, username, password, false).SendMessage(writetopics, gson.toJson(carObj));
                });
            });
        }

        try {
            System.out.println("Starting Spark Streaming");
            ssc.start();

            System.out.println("Spark Streaming is running");
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }
    }
}
