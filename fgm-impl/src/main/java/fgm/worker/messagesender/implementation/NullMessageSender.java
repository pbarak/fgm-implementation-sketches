package fgm.worker.messagesender.implementation;

import fgm.worker.messagesender.IMessageSender;
import org.apache.kafka.streams.processor.ProcessorContext;

public class NullMessageSender implements IMessageSender {

    //Null Message Sender Pattern
    public void sendMessageToSink(ProcessorContext context, String key, Object value, String sinkName){
        System.out.println("No Message Sent");
    }

    public  void sendMessage(ProcessorContext context, String key, Object value){
        System.out.println("No Message Sent");
    }
}