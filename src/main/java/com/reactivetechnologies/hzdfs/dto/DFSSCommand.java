/* ============================================================================
*
* FILE: DFSSCommand.java
*
The MIT License (MIT)

Copyright (c) 2016 Sutanu Dalui

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*
* ============================================================================
*/
package com.reactivetechnologies.hzdfs.dto;

import java.io.IOException;
import java.util.UUID;

import org.springframework.util.Assert;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class DFSSCommand implements DataSerializable 
{
  public static final String CMD_INIT_ASCII_RCVRS = "CMD_INIT_ASCII_RCVRS";
  public static final String CMD_INIT_ASCII_RCVRS_ACK = "CMD_INIT_ASCII_RCVRS_ACK";
  public static final String CMD_ABORT_JOB = "CMD_ABORT_JOB";
  
  private String command;
  private String recordMap;
  private String chunkMap;
  private String sessionId;
  private DFSSTaskConfig config;

  public String getRecordMap() {
    return recordMap;
  }

  @Override
  public String toString() {
    return "DFSSCommand [command=" + command + ", recordMap=" + recordMap
        + ", chunkMap=" + chunkMap + ", sessionId=" + sessionId + "]";
  }

  public DFSSCommand()
  {
    sessionId = UUID.randomUUID().toString();
  }
  public void setRecordMap(String recordMap) {
    this.recordMap = recordMap;
  }

  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  @Override
  public void writeData(ObjectDataOutput out) throws IOException {
    out.writeUTF(command);
    out.writeUTF(recordMap);
    out.writeUTF(chunkMap);
    out.writeUTF(sessionId);
    Assert.notNull(config, "DFSSTaskConfig is null");
    config.getEofSyncAckAwaitTime().writeData(out);
    config.getFinalChunkAwaitTime().writeData(out);
  }

  @Override
  public void readData(ObjectDataInput in) throws IOException {
    command = in.readUTF();
    recordMap = in.readUTF();
    chunkMap = in.readUTF();
    sessionId = in.readUTF();
    config = new DFSSTaskConfig();
    Time time = new Time();
    time.readData(in);
    config.setEofSyncAckAwaitTime(time);
    time = new Time();
    time.readData(in);
    config.setFinalChunkAwaitTime(time);
  }

  public String getChunkMap() {
    return chunkMap;
  }

  public void setChunkMap(String chunkMap) {
    this.chunkMap = chunkMap;
  }

  public String getSessionId() {
    return sessionId;
  }

  public DFSSTaskConfig getConfig() {
    return config;
  }

  public void setConfig(DFSSTaskConfig config) {
    this.config = config;
  }

}
