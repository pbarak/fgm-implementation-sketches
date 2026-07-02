package fgm.worker.commands.implementation;

import fgm.streamdata.StreamRecord;
import fgm.worker.commands.FgmCommand;
import fgm.worker.remoteobject.implementation.FgmNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class UpdateStateCommand implements FgmCommand {

    private static final Logger logger = LoggerFactory.getLogger(UpdateStateCommand.class);

    StreamRecord streamRecord;
    double frequency;
    FgmNode<?> fgmNode;
    Long timestamp;

    public UpdateStateCommand(StreamRecord streamRecord, double frequency, Long timestamp, FgmNode<?> fgmNode) {
        this.streamRecord = streamRecord;
        this.fgmNode = fgmNode;
        this.timestamp = timestamp;
        this.frequency = frequency;
    }

    @Override
    public void execute() {
        try {
            if(streamRecord != null) {
                if (this.fgmNode.getNodeInternalFgmState().getWindow() != null) {
                    this.fgmNode.getNodeInternalFgmState().getWindow().update(this.streamRecord, this.frequency, this.timestamp);
                    this.fgmNode.getNodeInternalFgmState().setMaxTimeStamp(this.timestamp);
                } else {
                    this.fgmNode.getNodeInternalFgmState().getSi().update(this.streamRecord, this.frequency);
                }
            }

        } catch (Exception ex) {
            logger.info(ex.toString());
        }
    }
}
