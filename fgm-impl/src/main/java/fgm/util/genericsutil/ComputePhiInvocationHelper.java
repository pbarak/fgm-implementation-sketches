package fgm.util.genericsutil;

import fgm.common.Vector;
import fgm.worker.remoteobject.implementation.FgmNode;

public class ComputePhiInvocationHelper {

    public static <T extends Vector> double invokeComputePhi(FgmNode<T> node) {
        return node.getSafeFunction().computePhi(
                node.getDrift().getVector(),
                node.getE().getVector(),
                node.getT());
    }

}
