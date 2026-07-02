package fgm.core.site;

import fgm.common.cfg.BaseConfiguration;
import fgm.factory.FactoryInjection;
import fgm.factory.specs.IFgmFactory;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.UUID;

public class SiteApplication extends SiteApplicationBase {

    public static void main(String[] args) throws Exception {

        String topicName = args[0];
        String nodeId = args[1];

	    // apply system properties before starting KafkaStreams
	    System.setProperty("sun.rmi.transport.tcp.responseTimeout", "180000"); // 180 seconds
	    System.setProperty("sun.rmi.transport.connectionTimeout", "120000"); // 120 seconds
	    System.setProperty("nodeId", nodeId);

        IFgmFactory<?> factory = FactoryInjection.createFactory();
        BaseConfiguration baseConfiguration = factory.getBaseCfg();

        SiteSerdes serdes = SiteSerdes.create(factory, baseConfiguration);

        Topology topology = SiteTopology.build(
                topicName, SOURCE_NODE_NAME, SITE_PROCESSOR_NAME, nodeId, serdes, factory, baseConfiguration, SiteStreamProperties.partitionsOf(topicName, baseConfiguration)
        );

        Properties props = SiteStreamProperties.build(factory, nodeId, baseConfiguration);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(SHUT_DOWN_HOOK) {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        System.out.println("Starting SiteApplication now...");
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
