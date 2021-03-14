package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.ipc.Client;

public class ClientTrace {
  public static final String DN_CLIENTTRACE_FORMAT = "src: %s" +      // src IP
    ", dest: %s" +   // dst IP
    ", bytes: %s" +  // byte count
    ", op: %s" +     // operation
    ", cliID: %s" +  // DFSClient id
    ", offset: %s" + // offset
    ", srvID: %s" +  // DatanodeRegistration
    ", blockid: %s" + // block id
    ", duration: %s";  // duration time

  private String src;
  private String dest;
  private long bytes;
  private String op;
  private String cliID;
  private long offset;
  private String srvID;
  private String blockid;
  private long duration;

  private ClientTrace(Builder builder) {
    src = builder.src;
    dest = builder.dest;
    bytes = builder.bytes;
    op = builder.op;
    cliID = builder.cliID;
    offset = builder.offset;
    srvID = builder.srvID;
    blockid = builder.blockid;
    duration = builder.duration;
  }

  public static class Builder {
    private String src;
    private String dest;
    private long bytes;
    private String op;
    private String cliID;
    private long offset;
    private String srvID;
    private String blockid;
    private long duration;

    public Builder setSrc(String src) {
      this.src = src;
      return this;
    }

    public Builder setDest(String dest) {
      this.dest = dest;
      return this;
    }

    public Builder setBytes(long bytes) {
      this.bytes = bytes;
      return this;
    }

    public Builder setOp(String op) {
      this.op = op;
      return this;
    }

    public Builder setCliID(String cliID) {
      this.cliID = cliID;
      return this;
    }

    public Builder setOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public Builder setSrvID(String srvID) {
      this.srvID = srvID;
      return this;
    }

    public Builder setBlockid(String blockid) {
      this.blockid = blockid;
      return this;
    }

    public Builder setDuration(long duration) {
      this.duration = duration;
      return this;
    }

    public ClientTrace build() {
      return new ClientTrace(this);
    }
  }

}

