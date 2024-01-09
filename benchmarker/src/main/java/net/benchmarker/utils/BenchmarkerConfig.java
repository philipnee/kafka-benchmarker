package net.benchmarker.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class BenchmarkerConfig {
    //Mandatory configs
    public static final String TOTAL_MESSAGE_SIZE_MB = "totalMessageSizeMB";
    public static final String MESSAGE_SIZE_KB = "messageSizeKB";
    public static final String TOPIC = "topic";
    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String NUM_PARTITIONS = "numPartitions";
    public static final String CONSUMER_IMPLEMENTATIONS = "implementations";

    private int totalMessageSizeMB = 100; // Default value if not specified in config
    private int[] messageSizeKB = new int[]{1, 10, 100}; // Default array if not specified
    private String topic;
    private String bootstrapServers;
    private String[] consumerImplementations;
    private int numPartitions = 1;
    private boolean isProduce = false;
    private boolean isConsume = false;

    public void loadConfig(String configFile) {
        File file = new File(configFile); //here you make a filehandler - not a filesystem file.
        if(!file.exists()) {
            throw new IllegalArgumentException("Config file does not exist: " + configFile);
        }
        try (InputStream input = new FileInputStream(configFile)) {
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(input);

            validateConfig(data);

            bootstrapServers = (String) data.get(BOOTSTRAP_SERVERS);
            totalMessageSizeMB = data.containsKey(TOTAL_MESSAGE_SIZE_MB) ?
                    (int) data.get(TOTAL_MESSAGE_SIZE_MB) : totalMessageSizeMB;
            messageSizeKB = data.containsKey(MESSAGE_SIZE_KB) ?
                    parseIntArray(data.get(MESSAGE_SIZE_KB)) : messageSizeKB;
            topic = data.containsKey(TOPIC) ?
                    (String) data.get(TOPIC) : generateRandomTopicName();
            numPartitions = data.containsKey(NUM_PARTITIONS) ?
                    (int) data.get(NUM_PARTITIONS) : numPartitions;
            consumerImplementations = parseStringArray(data.get(CONSUMER_IMPLEMENTATIONS));
            isProduce = data.containsKey("produce") ? (boolean) data.get("produce") : false;
            isConsume = data.containsKey("consume") ? (boolean) data.get("consume") : false;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void validateConfig(Map<String, Object> data) {
        if (!data.containsKey(BOOTSTRAP_SERVERS)) {
            throw new IllegalArgumentException("Missing mandatory field: BOOTSTRAP_SERVERS");
        }

        if (!data.containsKey(CONSUMER_IMPLEMENTATIONS)) {
            throw new IllegalArgumentException("Missing mandatory field: CONSUMER_IMPLEMENTATIONS");
        }
    }
    private <T> T[] parseArray(Object obj, Class<T> clazz) {
        if (obj instanceof Iterable) {
            int size = (int) ((Iterable<?>) obj).spliterator().getExactSizeIfKnown();
            T[] arr = (T[]) java.lang.reflect.Array.newInstance(clazz, size);
            int index = 0;
            for (Object item : (Iterable<?>) obj) {
                arr[index++] = clazz.cast(item);
            }
            return arr;
        }
        return (T[]) java.lang.reflect.Array.newInstance(clazz, 0);
    }

    public int[] parseIntArray(Object obj) {
        Integer[] res = parseArray(obj, Integer.class);
        return Arrays.stream(res).mapToInt(Integer::intValue).toArray();
    }

    public String[] parseStringArray(Object obj) {
        return parseArray(obj, String.class);
    }

    public int totalMessageSizeMB() {
        return totalMessageSizeMB;
    }

    public int[] messageSizeKB() {
        return messageSizeKB;
    }

    public String topic() {
        return topic;
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    public int numPartitions() {
        return numPartitions;
    }

    public String[] implementations() {
        return consumerImplementations;
    }

    public boolean produceTask() {
        return isProduce;
    }

    public boolean consumeTask() {
        return isConsume;
    }

    @Override
    public String toString() {
        return "BenchmarkerConfig{" +
                "totalMessageSizeMB=" + totalMessageSizeMB +
                ", messageSizeKB=" + Arrays.toString(messageSizeKB) +
                ", topic='" + topic + '\'' +
                ", bootstrapServers='" + bootstrapServers + '\'' +
                ", numPartitions=" + numPartitions +
                ", consumerImplementations=" + Arrays.toString(consumerImplementations) +
                ", isProduce=" + isProduce +
                ", isConsume=" + isConsume +
                '}';
    }

    public String toJson() {
        return "{" +
                "\"totalMessageSizeMB\":" + totalMessageSizeMB +
                ", \"messageSizeKB\":" + Arrays.toString(messageSizeKB) +
                ", \"topic\":\"" + topic + '\"' +
                ", \"bootstrapServers\":\"" + bootstrapServers + '\"' +
                ", \"numPartitions\":" + numPartitions +
                ", \"implementations\":" + Arrays.stream(consumerImplementations).map(s -> "\"" + s + "\"").collect(Collectors.joining(",")) +
                ", \"produce\":" + isProduce +
                ", \"consume\":" + isConsume +
                '}';
    }

    private static String generateRandomTopicName() {
        String today = LocalDate.now().toString();
        String uuid = UUID.randomUUID().toString().substring(0, 4); // Use a portion of UUID
        return "topic-" + today + "-" + uuid;
    }

    public static Properties producerConfig(final BenchmarkerConfig config) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerProps;
    }

    public static Properties consumerConfig(final BenchmarkerConfig config) {
        // Kafka consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return consumerProps;
    }
}

