package fgm.messageproducer.implementation;

import fgm.messageproducer.IMessageProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class KafkaMessageProducer<K, V> implements IMessageProducer<K, V> {

    private final Producer<K, V> kafkaProducer;

    public KafkaMessageProducer(Producer<K, V> kafkaProducer) {
        if (kafkaProducer == null) {
            throw new IllegalArgumentException("Kafka producer cannot be null.");
        }
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> streamRecord) {
        // Delegate the call to the real Kafka producer
        return this.kafkaProducer.send(streamRecord);
    }

    @Override
    public void flush() {
        this.kafkaProducer.flush();
    }

    @Override
    public void close() {
        this.kafkaProducer.close();
    }
}
