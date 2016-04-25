/* ============================================================================
*
* FILE: DFSSWorker.java
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
package com.reactive.hzdfs.core;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactive.hzdfs.DFSSException;

class DFSSWorker implements Callable<DFSSResponse>
{
  private static final Logger log = LoggerFactory.getLogger(DFSSWorker.class);
  /**
   * 
   */
  private final DistributedFileSupportService dfss;
  private String chunkMap;
  String sessionId;
  AsciiFileDistributor fileDist;
  String recordMap;
  private long consumableBytesLen = 0;
  /**
   * 
   * @param chunkMap
   * @param distributedFileSupportService TODO
   */
  DFSSWorker(DistributedFileSupportService distributedFileSupportService, String chunkMap, File sourceFile) {
    super();
    this.dfss = distributedFileSupportService;
    this.chunkMap = chunkMap;
    this.sourceFile = sourceFile;
  }
  private Counter putChunk(AsciiFileChunk chunk)
  {
    Counter ctr = new Counter();
    ctr.setRecordIdHash(chunk.generateHashCode());
    ctr.setRecordIdx(chunk.getRecordIndex());
    ctr.setFileName(chunk.getFileName());
    ctr.setCreatTime(chunk.getCreationTime());
    
    putEntry(ctr.getRecordIdHash(), chunk);
    
    return ctr;
  }
  private void putEntry(long recordIdHash, AsciiFileChunk chunk) {
    this.dfss.hzService.set(recordIdHash, chunk, chunkMap);
    consumableBytesLen += chunk.getChunk().length;
  }
  private final File sourceFile;
  
  private void submitChunks() throws IOException
  {
    try(AsciiChunkReader reader = new AsciiChunkReader(sourceFile, 8192))
    {
      log.info("[DFSS#"+sessionId+"] Start file chunk distribution..");
      AsciiFileChunk chunk = null;
      
      Counter counter = new Counter();
          
      chunk = reader.readNext();
      
      if(chunk != null)
      {
        try 
        {
          counter = putChunk(chunk);
          
          while((chunk = reader.readNext()) != null)
          {
            counter = putChunk(chunk);
          }
          
          chunk = putEOFChunk(counter);
        } 
        finally {
          log.info("[DFSS#"+sessionId+"] End file chunk distribution..");
        }
                
      }
     }
  }
  
  private AsciiFileChunk putEOFChunk(Counter counter)
  {
    AsciiFileChunk chunk = new AsciiFileChunk();
    chunk.recordIndex = counter.getRecordIdx();
    chunk.setChunk(new byte[]{-1});
    chunk.setFileName(counter.getFileName());
    chunk.setCreationTime(counter.getCreatTime());
    
    putEntry(counter.getRecordIdHash(), chunk);
    
    return chunk;
  }
  private DFSSResponse prepareResponse() throws DFSSException
  {
    try 
    {
      long size = this.dfss.hzService.getAtomicLong(chunkMap).get();
      if(size != consumableBytesLen)
        throw new DFSSException("["+sessionId+"] Distributed file size mismatch! Bytes expected=> "+consumableBytesLen+" Bytes distributed=> "+size);
      DFSSResponse success = fileDist.isClosed() ? DFSSResponse.SUCCESS : DFSSResponse.ERROR;
      success.setSessionId(sessionId);
      success.setRecordMap(recordMap);
      success.setNoOfRecords(this.dfss.hzService.getMap(recordMap).size());
      success.setSourceByteSize(sourceFile.length());
      success.setSinkByteSize(size);
      return success;
    } finally {
      this.dfss.hzService.getAtomicLong(chunkMap).destroy();
    }
  }
  @Override
  public DFSSResponse call() throws IOException {
    submitChunks();
    try {
      fileDist.awaitOnClose(10, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    boolean b = this.dfss.hzService.isDistributedObjectDestroyed(fileDist.keyspace());
    if(!b)
      log.warn(">>> Temporary map ["+fileDist.keyspace()+"] not destroyed <<<");
    
    return prepareResponse();
  }
}