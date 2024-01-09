package net.benchmarker;

import net.benchmarker.consumer.BenchmarkConsumer;
import net.benchmarker.producer.BenchmarkProducer;
import net.benchmarker.utils.ArgumentParser;
import net.benchmarker.utils.BenchmarkerConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (config.produceTask())
            produce(config);

        if (config.consumeTask())
            consume(config, groupId);
    }

    private static void produce(final BenchmarkerConfig config) {
        // Kafka producer properties

        log.info("Starting data producer");
        final long startTime = System.currentTimeMillis();
        BenchmarkProducer producer = new BenchmarkProducer(config);
        producer.produceData();
        final long endTime = System.currentTimeMillis();
        log.info("Producer finished in {} milliseconds", endTime - startTime);
    }

    private static void consume(final BenchmarkerConfig config, final String groupId) {
        log.info("Starting data consumer");
        String clazz = config.implementations()[0];
        BenchmarkConsumer<String, String> consumer = new BenchmarkConsumer<>(clazz, groupId, config, Optional.empty());
        consumer.simpleConsume();
    }

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
}