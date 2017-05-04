/* ============================================================================
*
* FILE: DFSSTaskConfig.java
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

import java.util.concurrent.TimeUnit;
/**
 * Bean class for configuring different wait times during a file distribution execution.
 */
public class DFSSTaskConfig {

  public Time getClusterPreparationTime() {
    return clusterPreparationTime;
  }
  public void setClusterPreparationTime(Time clusterPreparationTime) {
    this.clusterPreparationTime = clusterPreparationTime;
  }
  public Time getEofSyncAckAwaitTime() {
    return eofSyncAckAwaitTime;
  }
  public void setEofSyncAckAwaitTime(Time eofSyncAckAwaitTime) {
    this.eofSyncAckAwaitTime = eofSyncAckAwaitTime;
  }
  public Time getFinalChunkAwaitTime() {
    return finalChunkAwaitTime;
  }
  public void setFinalChunkAwaitTime(Time finalChunkAwaitTime) {
    this.finalChunkAwaitTime = finalChunkAwaitTime;
  }
  /**
   * Time to await acknowledgement from cluster members prior starting
   * a distribution task on the coordinator node.
   */
  Time clusterPreparationTime = new Time(60, TimeUnit.SECONDS);
  /**
   * Time to await acknowledgment from other members, once a EOF chunk
   * has been received by a node. Then it will be able to signal end of distribution
   * to the coordinator.
   */
  Time eofSyncAckAwaitTime = new Time(60, TimeUnit.SECONDS);
  /**
   * Time to await for any unfinished record to complete, after a EOF
   * chunk acknowledgment is requested from the EOF receiver node.
   */
  Time finalChunkAwaitTime = new Time(30, TimeUnit.SECONDS);
  /**
   * 
   */
  public DFSSTaskConfig() {
    
  }

}
