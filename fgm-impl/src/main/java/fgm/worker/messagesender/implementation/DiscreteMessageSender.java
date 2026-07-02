package fgm.worker.messagesender.implementation;

import fgm.worker.messagesender.IMessageSender;
import org.apache.kafka.streams.processor.ProcessorContext;

public class DiscreteMessageSender implements IMessageSender {

    public void sendMessageToSink(ProcessorContext context, String key, Object value, String sinkName) {
        context.forward(key, value, org.apache.kafka.streams.processor.To.child(sinkName));
    }


    public void sendMessage(ProcessorContext context, String key, Object value){
        context.forward(key, value);
    }
}