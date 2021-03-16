package org.apache.hadoop.hdfs.server.namenode.namesystem;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

public class GetBlockLocationsResult {
  final boolean updateAccessTime;
  final LocatedBlocks blocks;


  public GetBlockLocationsResult(
    boolean updateAccessTime, LocatedBlocks blocks) {
    this.updateAccessTime = updateAccessTime;
    this.blocks = blocks;
  }

  public boolean updateAccessTime() {
    return updateAccessTime;
  }

  public LocatedBlocks getLocatedBlocks() {
    return blocks;
  }
}
