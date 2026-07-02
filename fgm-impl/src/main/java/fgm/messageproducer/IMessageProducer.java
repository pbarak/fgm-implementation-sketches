package fgm.messageproducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Closeable;
import java.util.concurrent.Future;

// A generic producer interface that can be used for any message type.
// It extends Closeable so we don't forget to manage resources.
public interface IMessageProducer<K, V> extends Closeable {

    Future<RecordMetadata> send(ProducerRecord<K, V> streamRecord);

    void flush();

    @Override
    void close();
}
