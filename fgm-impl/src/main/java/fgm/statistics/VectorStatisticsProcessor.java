package fgm.statistics;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.StatisticsMessage;
import fgm.common.statevector.HashMapState;
import fgm.factory.specs.IFgmFactory;
import fgm.queryfunction.QueryFunction;
import fgm.worker.statistics.LocalStatistic;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorStatisticsProcessor implements Processor<String, StatisticsMessage> {

    private static final String SEPARATOR = ",";
    private static final double UNIT_FREQUENCY = 1.0d;
    private static final Logger logger = LoggerFactory.getLogger(VectorStatisticsProcessor.class);
    private LocalStatistic<HashMapState> localStatistic;
    private BaseConfiguration baseConfiguration;
    private QueryFunction<HashMapState> queryFunction;
    private ProcessorContext processorContext;

    //create a file to collect statistics and create graphical representations

    public VectorStatisticsProcessor(IFgmFactory<HashMapState> factory) {
        try {
            this.baseConfiguration = factory.getBaseCfg();
            this.localStatistic = factory.getLocalStatistic();
            this.queryFunction = factory.getQueryFunction();
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    @Override
    public void init(ProcessorContext context) {
        this.processorContext = context;
    }

    public void process(String key, StatisticsMessage value) {
        if (value != null) {
            if (key.equals(baseConfiguration.getStateUpdateStatisticsKey())) { //here, except for the update of central local state, throughput should be calculated
                this.localStatistic.update(value.getRecord(), UNIT_FREQUENCY);

                String updateMessage = value.getTimeStamp() +
                        SEPARATOR +
                        System.currentTimeMillis() +
                        SEPARATOR +
                        value.getSiteID() +
                        SEPARATOR +
                        value.getMessageType();
                logger.info(updateMessage);
            } else if (key.equals(baseConfiguration.getDriftMessagesTopicKey())) {

                HashMapState statistic;
                statistic =  (this.localStatistic.getVector()).deepCopy();
                statistic.scale(((double) this.baseConfiguration.getNumberOfNodes()));

                String driftLogMessage = value.getTimeStamp() +
                        SEPARATOR +
                        System.currentTimeMillis() +
                        SEPARATOR +
                        value.getSiteID() +
                        SEPARATOR +
                        value.getMessageType();

                logger.info(driftLogMessage);

                logger.info("at the end of the round: {} the query function value is: {}", value.getRound(),
                        this.queryFunction.computeQuery(statistic));
            } else if (key.equals(baseConfiguration.getIncrementTopicKey())) {
                String incrementMessage = value.getTimeStamp() +
                        SEPARATOR +
                        System.currentTimeMillis() +
                        SEPARATOR +
                        value.getSiteID() +
                        SEPARATOR +
                        value.getMessageType();
                logger.info(incrementMessage);
            } else if (key.equals(baseConfiguration.getPhiMessagesTopicKey())) {
                String phiMessage = value.getTimeStamp() +
                        SEPARATOR +
                        System.currentTimeMillis() +
                        SEPARATOR +
                        value.getSiteID() +
                        SEPARATOR +
                        value.getMessageType();
                logger.info(phiMessage);
            }
        }
    }

    @Override
    public void close() {
        logger.info("VectorStatisticsProcessor is shutting down.");
    }
}



