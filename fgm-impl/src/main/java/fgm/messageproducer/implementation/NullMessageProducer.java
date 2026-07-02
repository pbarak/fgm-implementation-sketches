package fgm.messageproducer.implementation;

import fgm.messageproducer.IMessageProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;

public class NullMessageProducer<K, V> implements IMessageProducer<K, V> {

    private static final NullMessageProducer<?, ?> INSTANCE = new NullMessageProducer<>();

    @SuppressWarnings("unchecked")
    public static <K, V> NullMessageProducer<K, V> getInstance() {
        return (NullMessageProducer<K, V>) INSTANCE;
    }

    private NullMessageProducer() {
        // private constructor for singleton
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> streamRecord) {
        // Do nothing. ✨
        // Return a completed Future with a null value to satisfy the interface.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void flush() {
        // Do nothing.
    }

    @Override
    public void close() {
        // Do nothing.
    }
}
