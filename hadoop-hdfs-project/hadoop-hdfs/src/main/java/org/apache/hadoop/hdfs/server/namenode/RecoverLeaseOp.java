package org.apache.hadoop.hdfs.server.namenode;

public enum RecoverLeaseOp {
  CREATE_FILE,
  APPEND_FILE,
  TRUNCATE_FILE,
  RECOVER_LEASE;

  public String getExceptionMessage(String src, String holder,
    String clientMachine, String reason) {
    return "Failed to " + this + " " + src + " for " + holder +
      " on " + clientMachine + " because " + reason;
  }
}
