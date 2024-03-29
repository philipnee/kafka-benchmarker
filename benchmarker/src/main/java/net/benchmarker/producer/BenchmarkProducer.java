package net.benchmarker.producer;

import net.benchmarker.recorder.MetricsRecorder;
import net.benchmarker.recorder.Recorder;
import net.benchmarker.utils.BenchmarkerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class BenchmarkProducer implements AutoCloseable {
    private final Logger log;
    final private Producer<String, String> producer;
    final private Recorder metricsRecorder;
    final int sizeMb;
    final int messageSizeKb;
    final String topic;
    final Properties producerConfig;

    public BenchmarkProducer(final BenchmarkerConfig props) {
        this(props, null);
    }

    public BenchmarkProducer(final BenchmarkerConfig props, final String metricsFilePath) {
        super();
        producerConfig = BenchmarkerConfig.producerConfig(props);
        sizeMb = props.totalMessageSizeMB();
        messageSizeKb = props.messageSizeKB()[0];
        topic = props.topic();
        producer = new KafkaProducer<>(producerConfig);
        log = new LogContext().logger(BenchmarkProducer.class);
        metricsRecorder = new MetricsRecorder(Optional.ofNullable(metricsFilePath));
    }

    private RecordMetadata send(final String topic, final String key, final String value) throws ExecutionException, InterruptedException {
        try {
            // send the record and then call get, which blocks waiting for the ack from the broker
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
            return metadata;
        } catch (AuthorizationException | UnsupportedVersionException | ProducerFencedException
                 | FencedInstanceIdException | OutOfOrderSequenceException | SerializationException e) {
            // we can't recover from these exceptions
            producer.close();
        } catch (KafkaException e) {
            log.error("Unexpected Kafka exception: ", e);

        }
        return null;
    }

    public void produceData() {
        final int numRecords = (sizeMb * 1024) / messageSizeKb;
        log.info("Producing {} records of size {} KB to topic {}", numRecords, messageSizeKb, topic);
        for (int i = 0; i < numRecords; i++) {
            final String key = DataGenerator.generateString(messageSizeKb);
            final String value = DataGenerator.generateString(messageSizeKb * 1024);
            try {
                send(topic, key, value);
            } catch (ExecutionException | InterruptedException e) {
                log.error("Error sending record: ", e);
                e.printStackTrace();
            }
        }
        log.info("Finished producing {} records of size {} KB to topic {}", numRecords, messageSizeKb, topic);
    }

    @Override
    public void close() {
        producer.close();
    }
}
