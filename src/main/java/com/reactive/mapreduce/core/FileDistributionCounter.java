/* ============================================================================
*
* FILE: FileDistributionCounter.java
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
package com.reactive.mapreduce.core;

class FileDistributionCounter
{
  long recordIdHash = -1;
  int recordIdx = -1;
  String fileName = null;
  long creatTime = 0;
  public long getRecordIdHash() {
    return recordIdHash;
  }
  public void setRecordIdHash(long recordIdHash) {
    this.recordIdHash = recordIdHash;
  }
  public int getRecordIdx() {
    return recordIdx;
  }
  public void setRecordIdx(int recordIdx) {
    this.recordIdx = recordIdx;
  }
  public String getFileName() {
    return fileName;
  }
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }
  public long getCreatTime() {
    return creatTime;
  }
  public void setCreatTime(long creatTime) {
    this.creatTime = creatTime;
  }
}