package net.benchmarker.consumer;

import net.benchmarker.recorder.MetricsRecorder;
import net.benchmarker.utils.BenchmarkerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.*;

public class BenchmarkConsumer<K, V> implements AutoCloseable {
    private static Logger log = LoggerFactory.getLogger(BenchmarkConsumer.class);
    private final String groupId;
    private final int sizeMb;
    private final int messageSizeKb;
    private Consumer<K, V> consumer;
    private final String clazz;
    private final Properties consumerConfig;
    private final String topic;
    private MetricsRecorder recorder;

    public BenchmarkConsumer(final String clazz, final String groupId, final BenchmarkerConfig config, final Optional<String> metricsFilePath) {
        this.consumerConfig = BenchmarkerConfig.consumerConfig(config);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        this.clazz = clazz;
        this.topic = config.topic();
        this.groupId = groupId;
        sizeMb = config.totalMessageSizeMB();
        messageSizeKb = config.messageSizeKB()[0];
        recorder = new MetricsRecorder(metricsFilePath);
    }

    public void initialize(final List<Integer> partitions) {
        consumer =  (Consumer<K, V>) createCustomKafkaConsumer(clazz, consumerConfig);
        List<TopicPartition> topicPartitions  = new ArrayList<>();
        for (Integer partition : partitions) {
            topicPartitions.add(new TopicPartition(topic, partition));
        }
        consumer.assign(topicPartitions);
    }

    public void simpleConsume() {
        // Kafka consumer instance
        log.info("Loading Kafka Consumer class: {}", clazz);
        initialize(Collections.singletonList(0));
        Set<TopicPartition> partitions = consumer.assignment();
        consumer.seekToBeginning(partitions);
        // Start timing for consumer to finish all data
        long startTime = System.currentTimeMillis();
        int recordSize = 0;
        int numMessages = (sizeMb * 1024) / messageSizeKb;
        int tenthOfMessages = numMessages / 10;
        // KafkaOffsetChecker checker = new KafkaOffsetChecker();
        // checker.checkOffset(topicName, partitions.stream().findAny().get());
        log.info("Starting data consumer processing {}MB {} messages", sizeMb, numMessages);
        while (recordSize < numMessages) {
            try {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(5000));
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

    @Override
    public void close() {
        consumer.close();
    }
}
