package fgm.worker.commands.implementation;

import fgm.worker.commands.FgmCommand;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.common.GlobalStatistic;
import fgm.common.Vector;
import fgm.util.genericsutil.ComputePhiInvocationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartNewRoundCommand implements FgmCommand {
    private static final Logger logger = LoggerFactory.getLogger(StartNewRoundCommand.class);
    private static final int INITIAL_LOCAL_COUNTER_VALUE = 0;
    private double theta;
    private GlobalStatistic<? extends Vector> localE;
    private FgmNode<? extends Vector> fgmNode;

    public StartNewRoundCommand() {
    }

    public StartNewRoundCommand(double theta, GlobalStatistic<? extends Vector> globalEstimation,
                                FgmNode<? extends Vector> fgmNode) {
        this.theta = theta;
        this.localE = globalEstimation;
        this.fgmNode = fgmNode;
    }

    @Override
    public void execute() {
        try {
            this.fgmNode.setE(this.localE);
            this.fgmNode.getNodeInternalFgmState().getEi().setVector(this.fgmNode.getNodeInternalFgmState().getSi().getVector().deepCopy());
            this.fgmNode.getNodeInternalFgmState().setTheta(this.theta);
            this.fgmNode.getNodeInternalFgmState().setZi(
                    ComputePhiInvocationHelper.invokeComputePhi(this.fgmNode));

            this.fgmNode.setCi(INITIAL_LOCAL_COUNTER_VALUE);
            this.fgmNode.getNodeInternalFgmState().setCurrentSubRound(0);
            this.fgmNode.getNodeInternalFgmState().setCurrentRound(this.fgmNode.getCurrentRound() + 1);

            logger.info("LEAVED START_NEW_ROUND");
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }
}
