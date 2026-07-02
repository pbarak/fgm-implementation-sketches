package fgm.core.coordinator;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;

public class CoordinatorApplication extends CoordinatorApplicationBase {
    static final CountDownLatch latch = new CountDownLatch(1);
    private static final String respTimeoutProp = "sun.rmi.transport.tcp.responseTimeout";
    private static final String rmiConnectionTimeoutProp = "sun.rmi.transport.connectionTimeout";

    public static void main(String[] args) {

        String applicationId = args[0];

	    // apply time out properties before starting KafkaStreams
	    System.setProperty(respTimeoutProp, "180000"); // 180 seconds
	    System.setProperty(rmiConnectionTimeoutProp, "120000"); // 120 seconds


        BootContext ctx = boot();

        CoordinatorSerdes serdes = CoordinatorSerdes.create();


        ctx.topology
                .addSource(INCREMENTS_SOURCE_NAME,
                        serdes.stringSerde.deserializer(),
                        serdes.incrementDeserializer,
                        ctx.baseConfiguration.getIncrementTopic())
                .addSource(DRIFT_MESSAGES_SOURCE_NAME,
                        serdes.stringSerde.deserializer(),
                        serdes.driftVectorDeserializer,
                        ctx.baseConfiguration.getDriftMessagesTopic())
                .addSource(PHI_MESSAGES_SOURCE_NAME,
                        serdes.stringSerde.deserializer(),
                        serdes.phiDeserializer,
                        ctx.baseConfiguration.getPhiMessagesTopic())
                .addProcessor(INCREMENT_PROCESSOR_NAME,
                        () -> getIncrementProcessor(ctx),
                        INCREMENTS_SOURCE_NAME)
                .addProcessor(DRIFT_MESSAGES_PROCESSOR_NAME,
                        () -> getDriftCollectionProcessor(ctx),
                        DRIFT_MESSAGES_SOURCE_NAME)
                .addProcessor(PHI_MESSAGES_PROCESSOR_NAME,
                        () -> getPhisCollectionProcessor(ctx),
                        PHI_MESSAGES_SOURCE_NAME)
                .addSink(SINK_NAME, ctx.baseConfiguration.getCoordinatorLogTopic(),
                        serdes.stringSerde.serializer(), serdes.stringSerde.serializer(),
                        INCREMENT_PROCESSOR_NAME, DRIFT_MESSAGES_PROCESSOR_NAME, PHI_MESSAGES_PROCESSOR_NAME)
                .addStateStore(ctx.customStoreBuilder,
                        INCREMENT_PROCESSOR_NAME, DRIFT_MESSAGES_PROCESSOR_NAME, PHI_MESSAGES_PROCESSOR_NAME);

        Properties props = CoordinatorStreamProperties.build(ctx.baseConfiguration, applicationId);

        KafkaStreams kafkaStreams = new KafkaStreams(ctx.topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(SHUT_DOWN_HOOK) {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
