/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class represents the primary identifier for a Datanode.
 * Datanodes are identified by how they can be contacted (hostname
 * and ports) and their storage ID, a unique number that associates
 * the Datanodes blocks with a particular Datanode.
 *
 * {@link DatanodeInfo#getName()} should be used to get the network
 * location (for topology) of a datanode, instead of using
 * {@link DatanodeID#getDataTransferIpAndPort()} here. Helpers are defined below
 * for each context in which a DatanodeID is used.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeID implements Comparable<DatanodeID> {
  public static final DatanodeID[] EMPTY_ARRAY = {};

  private String ip;     // IP address
  private String hostName;   // hostname claimed by datanode
  private String peerHostName; // hostname from the actual connection
  private int dataStreamingPort;      // data streaming port
  private int infoPort;      // info server port
  private int infoSecurePort; // info server port
  private int ipcPort;       // IPC server port
  private String dataTransferIpAndPort;

  /**
   * UUID identifying a given datanode. For upgraded Datanodes this is the
   * same as the StorageID that was previously used by this Datanode. 
   * For newly formatted Datanodes it is a UUID.
   */
  private final String datanodeUuid;

  public DatanodeID(DatanodeID from) {
    this(from.getDatanodeUuid(), from);
  }

  @VisibleForTesting
  public DatanodeID(String datanodeUuid, DatanodeID from) {
    this(from.getIp(),
        from.getHostName(),
        datanodeUuid,
        from.getDataStreamingPort(),
        from.getInfoPort(),
        from.getInfoSecurePort(),
        from.getIpcPort());
    this.peerHostName = from.getPeerHostName();
  }

  /**
   * Create a DatanodeID
   * @param ip IP
   * @param hostName hostname
   * @param datanodeUuid data node ID, UUID for new Datanodes, may be the
   *                     storage ID for pre-UUID datanodes. NULL if unknown
   *                     e.g. if this is a new datanode. A new UUID will
   *                     be assigned by the namenode.
   * @param dataStreamingPort data transfer port
   * @param infoPort info server port 
   * @param ipcPort ipc server port
   */
  public DatanodeID(String ip, String hostName, String datanodeUuid,
                    int dataStreamingPort, int infoPort, int infoSecurePort, int ipcPort) {
    setIpAndDataStreamingPort(ip, dataStreamingPort);
    this.hostName = hostName;
    this.datanodeUuid = checkDatanodeUuid(datanodeUuid);
    this.infoPort = infoPort;
    this.infoSecurePort = infoSecurePort;
    this.ipcPort = ipcPort;
  }
  
  public void setIp(String ip) {
    //updated during registration, preserve former xferPort
    setIpAndDataStreamingPort(ip, dataStreamingPort);
  }

  private void setIpAndDataStreamingPort(String ip, int dataStreamingPort) {
    // build xferAddr string to reduce cost of frequent use
    this.ip = ip;
    this.dataStreamingPort = dataStreamingPort;
    this.dataTransferIpAndPort = ip + ":" + dataStreamingPort;
  }

  public void setPeerHostName(String peerHostName) {
    this.peerHostName = peerHostName;
  }
  
  /**
   * @return data node ID.
   */
  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  private String checkDatanodeUuid(String uuid) {
    if (uuid == null || uuid.isEmpty()) {
      return null;
    } else {
      return uuid;
    }
  }

  /**
   * @return ipAddr;
   */
  public String getIp() {
    return ip;
  }

  /**
   * @return hostname
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * @return hostname from the actual connection 
   */
  public String getPeerHostName() {
    return peerHostName;
  }
  
  /**
   * @return IP:xferPort string
   */
  public String getDataTransferIpAndPort() {
    return dataTransferIpAndPort;
  }

  /**
   * @return IP:ipcPort string
   */
  private String getIpcAddress() {
    return ip + ":" + ipcPort;
  }

  /**
   * @return IP:infoPort string
   */
  public String getInfoAddr() {
    return ip + ":" + infoPort;
  }

  /**
   * @return IP:infoPort string
   */
  public String getInfoSecureAddr() {
    return ip + ":" + infoSecurePort;
  }

  /**
   * @return hostname:xferPort
   */
  public String getXferAddrWithHostname() {
    return hostName + ":" + dataStreamingPort;
  }

  /**
   * @return hostname:ipcPort
   */
  private String getIpcAddrWithHostname() {
    return hostName + ":" + ipcPort;
  }

  /**
   * @param useHostname true to use the DN hostname, use the IP otherwise
   * @return name:xferPort
   */
  public String getDataTransferIpAndPort(boolean useHostname) {
    return useHostname ? getXferAddrWithHostname() : getDataTransferIpAndPort();
  }

  /**
   * @param useHostname true to use the DN hostname, use the IP otherwise
   * @return name:ipcPort
   */
  public String getIpcAddress(boolean useHostname) {
    return useHostname ? getIpcAddrWithHostname() : getIpcAddress();
  }

  /**
   * @return xferPort (the port for data streaming)
   */
  public int getDataStreamingPort() {
    return dataStreamingPort;
  }

  /**
   * @return infoPort (the port at which the HTTP server bound to)
   */
  public int getInfoPort() {
    return infoPort;
  }

  /**
   * @return infoSecurePort (the port at which the HTTPS server bound to)
   */
  public int getInfoSecurePort() {
    return infoSecurePort;
  }

  /**
   * @return ipcPort (the port at which the IPC server bound to)
   */
  public int getIpcPort() {
    return ipcPort;
  }

  @Override
  public boolean equals(Object to) {
    if (this == to) {
      return true;
    }
    if (!(to instanceof DatanodeID)) {
      return false;
    }
    return (getDataTransferIpAndPort().equals(((DatanodeID)to).getDataTransferIpAndPort()) &&
        datanodeUuid.equals(((DatanodeID)to).getDatanodeUuid()));
  }
  
  @Override
  public int hashCode() {
    return datanodeUuid.hashCode();
  }
  
  @Override
  public String toString() {
    return getDataTransferIpAndPort();
  }
  
  /**
   * Update fields when a new registration request comes in.
   * Note that this does not update storageID.
   */
  public void updateRegInfo(DatanodeID nodeReg) {
    setIpAndDataStreamingPort(nodeReg.getIp(), nodeReg.getDataStreamingPort());
    hostName = nodeReg.getHostName();
    peerHostName = nodeReg.getPeerHostName();
    infoPort = nodeReg.getInfoPort();
    infoSecurePort = nodeReg.getInfoSecurePort();
    ipcPort = nodeReg.getIpcPort();
  }
    
  /**
   * Compare based on data transfer address.
   *
   * @param that datanode to compare with
   * @return as specified by Comparable
   */
  @Override
  public int compareTo(DatanodeID that) {
    return getDataTransferIpAndPort().compareTo(that.getDataTransferIpAndPort());
  }
}
