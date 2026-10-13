package fgm.factory.specs;

import com.fasterxml.jackson.databind.JsonDeserializer;
import fgm.common.GlobalStatistic;
import fgm.common.Vector;
import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.DriftVectorMessage;
import fgm.common.messages.IncrementMessage;
import fgm.common.messages.PhiMessage;
import fgm.common.messages.StatisticsMessage;
import fgm.coordinator.collectorsinterfaces.implementation.processors.statestore.CoordinatoreStateStoreBuilder;
import fgm.messageproducer.IMessageProducer;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.queryfunction.QueryFunction;
import fgm.safefunction.SafeFunction;
import fgm.worker.windowoperator.Window;
import fgm.worker.statistics.LocalStatistic;
import fgm.streamdata.StreamRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.processor.Processor;


public interface IFgmFactory<IMPL extends Vector> {

    FgmNode<IMPL> getFgmNode();

    LocalStatistic<IMPL> getLocalStatistic();

    Window<IMPL> getWindowOperator();

    GlobalStatistic<IMPL> getGlobalStatistic();

    SafeFunction<IMPL> getSafeFunction();

    QueryFunction<IMPL> getQueryFunction();

    IMessageProducer<String, DriftVectorMessage> getDriftMessageProducer();

    IMessageProducer<String, PhiMessage> getPhiMessageProducer();

    IMessageProducer<String, StatisticsMessage> getStatisticsMessageProducer();

    BaseConfiguration getBaseCfg();

    JsonDeserializer<StreamRecord> getRecordDeserializer();

    JsonDeserializer<StatisticsMessage> getStatisticsMessageDeserializer();

    IMessageSender getMessageSender();

    Processor<StreamRecord, Double> getStreamRecordsProcessor(String id, int srcTopicPartitionsNum);

    Processor<String, IncrementMessage> getIncrementProcessor(CoordinatoreStateStoreBuilder<?> customStoreBuilder,
                                                String remoteStoreUrl, IMessageSender messageSender);

    Processor<String, PhiMessage> getPhisCollectionProcessor(String stateStoreName, String rmiServerUrl, IMessageSender messageSender,
                                                     QueryFunction<?> queryFunction);

    Processor<String, DriftVectorMessage> getDriftCollectionProcessor(String stateStoreName, String rmiServerUrl, IMessageSender messageSender,
                                                   QueryFunction<?> queryFunction);

    CoordinatoreStateStoreBuilder<?> createGenericCoordinatorClient();

    Class<?> getTimestampExtractor();

    Processor<String, StatisticsMessage> getStatisticsProcessor(IFgmFactory<?> factory);
}
