package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 * A simple result enum for the result of
 * {@link BlockManager#processMisReplicatedBlock(BlockNeighborInfo)}.
 */
enum MisReplicationResult {
    /** The block should be invalidated since it belongs to a deleted file. */
    INVALID,
    /** The block is currently under-replicated. */
    UNDER_REPLICATED,
    /** The block is currently over-replicated. */
    OVER_REPLICATED,
    /** A decision can't currently be made about this block. */
    POSTPONE,
    /** The block is under construction, so should be ignored */
    UNDER_CONSTRUCTION,
    /** The block is properly replicated */
    OK
}
