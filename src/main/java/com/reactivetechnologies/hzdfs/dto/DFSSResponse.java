/* ============================================================================
*
* FILE: DFSSResponse.java
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

import java.util.ArrayList;
import java.util.List;

public enum DFSSResponse {

  COMPLETE,ERROR, TIMEOUT;
  private String sessionId;
  private String recordMap;
  private int noOfRecords;
  private long sourceByteSize, sinkByteSize;
  private List<String> errorNodes = new ArrayList<>();
  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getRecordMap() {
    return recordMap;
  }

  public void setRecordMap(String recordMap) {
    this.recordMap = recordMap;
  }

  public int getNoOfRecords() {
    return noOfRecords;
  }

  public void setNoOfRecords(int noOfRecords) {
    this.noOfRecords = noOfRecords;
  }

  public long getSinkByteSize() {
    return sinkByteSize;
  }

  public void setSinkByteSize(long sinkByteSize) {
    this.sinkByteSize = sinkByteSize;
  }

  public long getSourceByteSize() {
    return sourceByteSize;
  }

  public void setSourceByteSize(long sourceByteSize) {
    this.sourceByteSize = sourceByteSize;
  }

  public List<String> getErrorNodes() {
    return errorNodes;
  }

  public void setErrorNodes(List<String> errorNodes) {
    this.errorNodes = errorNodes;
  }
  
 

}
