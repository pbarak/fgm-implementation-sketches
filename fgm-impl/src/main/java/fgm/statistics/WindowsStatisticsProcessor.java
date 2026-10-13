package fgm.statistics;

import java.time.Duration;
import java.util.*;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.StatisticsMessage;
import fgm.common.statevector.FastAGMS;
import fgm.factory.specs.IFgmFactory;
import fgm.worker.metrictypes.StatisticsMessageType;
import fgm.worker.windowoperator.Window;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowsStatisticsProcessor implements Processor<String, StatisticsMessage> {

    private static final String SEPARATOR = ",";
    private static final double STATISTICS_FREQUENCY = 1.0d;
    private static final Logger logger = LoggerFactory.getLogger(WindowsStatisticsProcessor.class);
    private Window<?> customWindow;
    private BaseConfiguration baseConfiguration;
    private ProcessorContext processorContext;

    //create a file to collect statistics and create graphical representations

    public WindowsStatisticsProcessor(IFgmFactory<?> factory) {
        try {
            this.baseConfiguration = factory.getBaseCfg();
            this.customWindow = factory.getWindowOperator();
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    @Override
    public void init(ProcessorContext context) {
        this.processorContext = context;
            processorContext.schedule(
                    baseConfiguration.getSlideSizeMillis(),
                    PunctuationType.STREAM_TIME,        // time domain
                                          // lambda = Punctuator
                    timestamp -> {
                        customWindow.slidePunctuate(this.processorContext.timestamp());
                    }
            );

    }

    public void process(String key, StatisticsMessage value) {
        if (value != null) {
            if (value.getMessageType().equals(StatisticsMessageType.StreamRecordForStatistics.toString())) { //here, except for the update of central local state, throughput should be calculated
                this.customWindow.update(value.getRecord(), STATISTICS_FREQUENCY, (value.getRecord()).getTimeStamp());

                String updateMessage = value.getTimeStamp() +
                        SEPARATOR +
                        System.currentTimeMillis() +
                        SEPARATOR +
                        value.getSiteID() +
                        SEPARATOR +
                        value.getMessageType();
                logger.info(updateMessage);
            } else if (value.getMessageType().equals(StatisticsMessageType.DriftMessage.toString())/*streamRecord.key().equals(baseConfiguration.getDriftMessagesTopicKey())*/) { //some record keys are common to statistics topics
                //and coordinator topics
                FastAGMS statistic;
                statistic = (FastAGMS) (this.customWindow.getLocalStatistic().getVector()).deepCopy();
                statistic.scale(((double) this.baseConfiguration.getNumberOfNodes()));

                int depth = statistic.depth();
                double[] innerProductsArray = new double[depth];

                double update = 0.0;
                int j = 0;
                for (int i = 0; i < depth; i++) {
                    double[] sketchColumn = statistic.getSketchColumn(i);

                    while (j < sketchColumn.length) {
                        update = update + sketchColumn[j] * sketchColumn[j];
                        j++;
                    }
                    innerProductsArray[i] = update;
                    update = 0.0;
                    j = 0;
                }
                Arrays.sort(innerProductsArray);
                Median median = new Median();

                String driftLogMessage = value.getTimeStamp() +
                        SEPARATOR +
                        System.currentTimeMillis() +
                        SEPARATOR +
                        value.getSiteID() +
                        SEPARATOR +
                        value.getMessageType();

                logger.info(driftLogMessage);
                logger.info("at the end of the round: {} the median value is: {}", value.getRound(),
                        median.evaluate(innerProductsArray));
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
        logger.info("WindowsStatisticsProcessor is shutting down.");
    }
}



