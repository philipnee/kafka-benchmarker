package net.benchmarker;

import net.benchmarker.producer.BenchmarkProducer;
import net.benchmarker.utils.ArgumentParser;
import net.benchmarker.utils.BenchmarkerConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

public class ThroughputRecorder {
    private static Logger log = LoggerFactory.getLogger(ThroughputRecorder.class);
    private static BenchmarkerConfig config = new BenchmarkerConfig();

    public static void main(String[] args) {
        // Parsing command-line arguments for configuration
        if (args.length == 0) {
            System.out.println("Usage: DataProducerConsumer <bootstrap_servers> <config_file_path>");
            return;
        }

        ArgumentParser parser = new ArgumentParser(args);

        final String groupId = "group";
        final String configFilePath = parser.getArgument("config");
        config.loadConfig(configFilePath);
        log.info("Using config {}", config.toJson());
        int totalSizeMB = config.totalMessageSizeMB();
        // currently we only use one
        int messageSizeKB = config.messageSizeKB()[0];
        String topicName = config.topic();
        // Kafka producer properties
        Properties producerConfig = BenchmarkerConfig.producerConfig(config);

        log.info("Starting data producer");
        BenchmarkProducer producer = new BenchmarkProducer(producerConfig);
        producer.produceData(totalSizeMB, messageSizeKB, topicName);

        // Kafka consumer instance
        Properties consumerProps = BenchmarkerConfig.consumerConfig(config);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        String consumerClass = config.implementations()[0];
        log.info("Loading Kafka Consumer class: {}", consumerClass);
        Consumer<String, String> consumer = createConfiguredKafkaConsumer(consumerClass, consumerProps);
        //consumer.subscribe(Collections.singletonList(topicName));
        consumer.assign(Collections.singletonList(new TopicPartition(topicName, 0)));
        Set<TopicPartition> partitions = consumer.assignment();
        consumer.seekToBeginning(partitions);

        // Start timing for consumer to finish all data
        long startTime = System.currentTimeMillis();
        int recordSize = 0;
        int numMessages = (totalSizeMB * 1024) / messageSizeKB;
        int tenthOfMessages = numMessages / 10;
        // KafkaOffsetChecker checker = new KafkaOffsetChecker();
        // checker.checkOffset(topicName, partitions.stream().findAny().get());
        log.info("Starting data consumer processing {}MB {} messages", totalSizeMB, numMessages);
        while (recordSize < numMessages) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                if (recordSize % tenthOfMessages == 0) {
                    log.info("current position: {}", consumer.position(partitions.stream().findAny().get()));
                    log.info("Received {} records", records.count());
                }
                if (records.count() == 0)
                    continue;
                recordSize += records.count();
            } catch (Exception e) {
                log.error("Error consuming records: ", e);
                e.printStackTrace();
            }
        }

        // End timing for consumer
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("Consumer finished in " + elapsedTime + " milliseconds");

        // Close the consumer
        consumer.close();
    }

    private static Consumer<String, String> createConfiguredKafkaConsumer(final String classPath,
                                                                               final Properties consumerProps) {
        return (Consumer<String, String>) ConsumerCreater.createCustomKafkaConsumer(classPath, consumerProps);
    }

    // Method to generate a random topic name


    public static class KafkaOffsetChecker {
        public void checkOffset(final String topic, final TopicPartition tp) {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Set your Kafka broker(s)

            try (AdminClient admin = AdminClient.create(props)) {
                // Check offset for a consumer group
                Map<TopicPartition, OffsetSpec> tpOffset = new HashMap<>();
                tpOffset.put(tp, OffsetSpec.latest());
                log.info("Checking offset for topic {}", tpOffset);
                ListOffsetsResult consumerOffsets = admin.listOffsets(tpOffset);
                consumerOffsets.all().get().forEach((t, offsetAndMetadata) ->
                        System.out.println("Topic: " + t.topic() + ", Partition: " + t.partition() + ", Offset: " + offsetAndMetadata.offset())
                );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ConsumerCreater {
        public static Consumer<?, ?> createCustomKafkaConsumer(final String classPath,
                                                               final Properties kafkaConsumerProps) {
            try {
                Class<? extends Consumer<?, ?>> kafkaConsumerClass = (Class<? extends Consumer<?, ?>>) Class.forName(classPath);
                Constructor<? extends Consumer<?, ?>> constructor = kafkaConsumerClass.getConstructor(Properties.class);
                return constructor.newInstance(kafkaConsumerProps);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
}