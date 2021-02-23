package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.base.Preconditions;

/**
 * CorruptedBlockInfo is used to build the "toCorrupt" list, which is a
 * list of blocks that should be considered corrupt due to a block report.
 */
public class CorruptedBlockInfo {
    /** The corrupted block in a datanode. */
    final BlockNeighborInfo corrupted;
    /** The corresponding block stored in the BlockManager. */
    final BlockNeighborInfo stored;
    /** The reason to mark corrupt. */
    final String reason;
    /** The reason code to be stored */
    final CorruptReplicasMap.Reason reasonCode;

    CorruptedBlockInfo(BlockNeighborInfo corrupted,
                       BlockNeighborInfo stored, String reason,
                       CorruptReplicasMap.Reason reasonCode) {
        Preconditions.checkNotNull(corrupted, "corrupted is null");
        Preconditions.checkNotNull(stored, "stored is null");

        this.corrupted = corrupted;
        this.stored = stored;
        this.reason = reason;
        this.reasonCode = reasonCode;
    }

    CorruptedBlockInfo(BlockNeighborInfo stored, String reason,
                       CorruptReplicasMap.Reason reasonCode) {
        this(stored, stored, reason, reasonCode);
    }

    CorruptedBlockInfo(BlockNeighborInfo stored, long gs, String reason,
                       CorruptReplicasMap.Reason reasonCode) {
        this(new BlockNeighborInfo(stored), stored, reason, reasonCode);
        //the corrupted block in datanode has a different generation stamp
        corrupted.setGenerationStamp(gs);
    }

    public BlockNeighborInfo getCorrupted() {
        return corrupted;
    }

    @Override
    public String toString() {
        return corrupted + "("
                + (corrupted == stored? "same as stored": "stored=" + stored) + ")";
    }
}
