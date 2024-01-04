package net.benchmarker.consumer;

import org.apache.kafka.clients.consumer.internals.PrototypeAsyncConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BenchmarkAsyncConsumer implements AutoCloseable {
    private final PrototypeAsyncConsumer<String, String> consumer;
    private final Logger log;

    public BenchmarkAsyncConsumer(final Properties props) {
        this.consumer = new PrototypeAsyncConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        this.log = new LogContext().logger(BenchmarkAsyncConsumer.class);
    }

    public void initialize(final String topic, List<Integer> partitions) {
        List<TopicPartition> topicPartitions  = new ArrayList<>();
        for (Integer partition : partitions) {
            topicPartitions.add(new TopicPartition(topic, partition));
        }
        consumer.assign(topicPartitions);
    }

    @Override
    public void close() {
        consumer.close();
    }
}
