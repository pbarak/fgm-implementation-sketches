package fgm.worker.messagesender;

import org.apache.kafka.streams.processor.ProcessorContext;

public interface IMessageSender {
     void sendMessageToSink(ProcessorContext context, String key, Object value, String sinkName);
     void sendMessage(ProcessorContext context, String key, Object value);

}
