package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.net.Node;

import java.util.List;
import java.util.Set;

public class ReplicationWork {
    private final Block block;
    private final String srcPath;
    private final long blockSize;
    private final byte storagePolicyID;
    private final DatanodeDescriptor srcNode;
    private final int additionalReplRequired;
    private final int priority;
    private final List<DatanodeDescriptor> containingNodes;
    private final List<DatanodeStorageInfo> liveReplicaStorages;
    private DatanodeStorageInfo targets[];

    public Block getBlock() {
        return block;
    }

    public DatanodeDescriptor getSrcNode() {
        return srcNode;
    }

    public int getPriority() {
        return priority;
    }

    public List<DatanodeDescriptor> getContainingNodes() {
        return containingNodes;
    }

    public DatanodeStorageInfo[] getTargets() {
        return targets;
    }

    public void setTargets(DatanodeStorageInfo[] targets) {
        this.targets = targets;
    }

    public ReplicationWork(Block block,
                           BlockSet bc,
                           DatanodeDescriptor srcNode,
                           List<DatanodeDescriptor> containingNodes,
                           List<DatanodeStorageInfo> liveReplicaStorages,
                           int additionalReplRequired,
                           int priority) {
        this.block = block;
        this.srcPath = bc.getName();
        this.blockSize = block.getNumBytes();
        this.storagePolicyID = bc.getStoragePolicyID();
        this.srcNode = srcNode;
        this.srcNode.incrementPendingReplicationWithoutTargets();
        this.containingNodes = containingNodes;
        this.liveReplicaStorages = liveReplicaStorages;
        this.additionalReplRequired = additionalReplRequired;
        this.priority = priority;
        this.targets = null;
    }

    public void chooseTargets(BlockPlacementPolicy blockplacement,
                               BlockStoragePolicySuite storagePolicySuite,
                               Set<Node> excludedNodes) {
        try {
            targets = blockplacement.chooseTarget(getSrcPath(),
                    additionalReplRequired, srcNode, liveReplicaStorages, false,
                    excludedNodes, blockSize,
                    storagePolicySuite.getPolicy(getStoragePolicyID()));
        } finally {
            srcNode.decrementPendingReplicationWithoutTargets();
        }
    }

    private String getSrcPath() {
        return srcPath;
    }

    private byte getStoragePolicyID() {
        return storagePolicyID;
    }
}

