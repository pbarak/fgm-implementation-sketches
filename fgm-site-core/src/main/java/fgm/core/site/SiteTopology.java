package fgm.core.site;

import fgm.common.cfg.BaseConfiguration;
import fgm.factory.specs.IFgmFactory;
import org.apache.kafka.streams.Topology;

public final class SiteTopology {

    private SiteTopology() {
    }

    public static Topology build(String topicName,
                                 String vectorSourceNodeName,
                                 String siteProcessorName,
                                 String nodeId,
                                 SiteSerdes serdes,
                                 IFgmFactory<?> fgmFactory,
                                 BaseConfiguration baseConfiguration,
                                 int srcTopicPartitionsNum) {

        Topology topology = new Topology();

        topology.addSource(vectorSourceNodeName,
                        serdes.recordDeserializer,
                        serdes.doubleDeserializer,
                        topicName)
                .addProcessor(siteProcessorName, () -> fgmFactory.getStreamRecordsProcessor(nodeId, srcTopicPartitionsNum), vectorSourceNodeName)
                .addSink(baseConfiguration.getWorkerIncrementSinkName(),
                        baseConfiguration.getIncrementTopic(),
                        serdes.stringSerde.serializer(),
                        serdes.incrementMessageSerializer,
                        siteProcessorName)
                .addSink(baseConfiguration.getWorkerStatisticEventsSinkName(),
                        baseConfiguration.getStatisticEventsTopic(),
                        serdes.stringSerde.serializer(),
                        serdes.statisticsMessageSerializer,
                        siteProcessorName);

        return topology;
    }
}

