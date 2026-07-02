package fgm.worker.commands.implementation;

import fgm.common.cfg.implementation.FilePropertiesLocalStore;
import fgm.worker.commands.FgmCommand;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.common.GlobalStatistic;
import fgm.util.genericsutil.ComputePhiInvocationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartNewSubroundCommand implements FgmCommand {
    private static final int INITIAL_LOCAL_COUNTER_VALUE = 0;
    private double theta;
    private FgmNode<?> fgmNode;
    private static final Logger logger = LoggerFactory.getLogger(StartNewSubroundCommand.class);

    FilePropertiesLocalStore filePropertiesLocalStore;

    public StartNewSubroundCommand() {
    }

    public StartNewSubroundCommand(double theta, FgmNode<?> fgmNode) {
        this.fgmNode = fgmNode;
        this.theta = theta;
        filePropertiesLocalStore = FilePropertiesLocalStore.getInstance();
    }

    @Override
    public void execute() {
        try {
            this.fgmNode.getNodeInternalFgmState().setTheta(this.theta);

            this.fgmNode.getNodeInternalFgmState().setZi(
                    ComputePhiInvocationHelper.invokeComputePhi(this.fgmNode));
            this.fgmNode.setCi(INITIAL_LOCAL_COUNTER_VALUE);
            this.fgmNode.getNodeInternalFgmState().setCurrentSubRound(this.fgmNode.getCurrentSubRound() + 1);
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }

}
