package net.benchmarker;

import net.benchmarker.producer.BenchmarkProducer;
import net.benchmarker.utils.ArgumentParser;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

public class ThroughputRecorder {
    private static Logger log = LoggerFactory.getLogger(ThroughputRecorder.class);

    public static void main(String[] args) {
        // Parsing command-line arguments for configuration
        if (args.length < 2) {
            System.out.println("Usage: DataProducerConsumer <bootstrap_servers> <total_size_MB> <message_size_KB> [<topic_name>]");
            return;
        }

        ArgumentParser parser = new ArgumentParser(args);

        final String groupId = "group";
        final String bootstrapServers = parser.getArgument("bootstrap");
        int totalSizeMB = Integer.parseInt(parser.getArgument("totalSizeMB"));
        int messageSizeKB = Integer.parseInt(parser.getArgument("messageSizeKB"));
        String topicName = parser.getArgument("topic");
        if (topicName == null) {
            topicName = generateRandomTopicName();
            log.info("Topic name: {}", topicName);
        }
        log.info("Using topic name {}", topicName);
        log.info("Using bootstrap servers {}", bootstrapServers);
        log.info("Using total size {} MB", totalSizeMB);
        log.info("Using message size {} KB", messageSizeKB);

        // Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        log.info("Starting data producer");
        BenchmarkProducer producer = new BenchmarkProducer(producerProps);
        producer.produceData(totalSizeMB, messageSizeKB, topicName);

        // Kafka consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Kafka consumer instance
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topicName));

        // Start timing for consumer to finish all data
        long startTime = System.currentTimeMillis();
        int recordSize = 0;
        int numMessages = (totalSizeMB * 1024) / messageSizeKB;
        KafkaOffsetChecker checker = new KafkaOffsetChecker();
        checker.checkOffset(topicName);
        log.info("Starting data consumer");
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                log.info("Received {} records", records.count());
                recordSize += records.count();
                if (recordSize >= numMessages)
                    break;
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

    // Method to generate a random topic name
    private static String generateRandomTopicName() {
        String today = LocalDate.now().toString();
        String uuid = UUID.randomUUID().toString().substring(0, 4); // Use a portion of UUID
        return "topic-" + today + "-" + uuid;
    }

    public static class KafkaOffsetChecker {
        public void checkOffset(final String topic) {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Set your Kafka broker(s)

            try (AdminClient admin = AdminClient.create(props)) {
                // Check offset for a consumer group
                Map<TopicPartition, OffsetSpec> tpOffset = new HashMap<>();
                tpOffset.put(new TopicPartition(topic, 0), OffsetSpec.latest());
                log.info("Checking offset for topic {}", tpOffset);
                ListOffsetsResult consumerOffsets = admin.listOffsets(tpOffset);
                consumerOffsets.all().get().forEach((tp, offsetAndMetadata) ->
                        System.out.println("Topic: " + tp.topic() + ", Partition: " + tp.partition() + ", Offset: " + offsetAndMetadata.offset())
                );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}