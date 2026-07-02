package fgm.common.cfg.implementation;

import fgm.common.cfg.FastAGMSConfiguration;


public class FastAGMSCfgImpl extends BaseCfgImpl implements FastAGMSConfiguration {
    private static final String SKETCH_DEPTH = "sketch.depth";
    private static final String SKETCH_BUCKETS = "sketch.buckets";

    public FastAGMSCfgImpl() {
        super();
    }

    public Integer getSketchDepth() {
        return super.filePropertiesLocalStore.retrieveInt(SKETCH_DEPTH);
    }

    public Integer getSketchBuckets() {
        return super.filePropertiesLocalStore.retrieveInt(SKETCH_BUCKETS);
    }


}
