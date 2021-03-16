package org.apache.hadoop.hdfs.server.namenode.namesystem;

import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;

public class FileState {
  public final INodeFile inode;
  public final String path;
  public final INodesInPath iip;

  public FileState(INodeFile inode, String fullPath, INodesInPath iip) {
    this.inode = inode;
    this.path = fullPath;
    this.iip = iip;
  }
}
