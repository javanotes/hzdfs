/* ============================================================================
*
* FILE: DFSSTaskExecutor.java
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

import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.reactive.hzdfs.DFSSException;
/**
 * The executor for a single file distribution process.
 */
class DFSSTaskExecutor implements Callable<DFSSResponse>
{
  private static final Logger log = LoggerFactory.getLogger(DFSSTaskExecutor.class);
  /**
   * 
   */
  private final DistributedFileSupportService dfss;
  private String chunkMap;
  String sessionId;
  private final AsciiFileDistributor fileDist;
  String recordMap;
  private long consumableBytesLen = 0;
  /**
   * 
   * @param distributedFileSupportService
   * @param chunkMap
   * @param sourceFile
   * @param fileDist
   */
  DFSSTaskExecutor(DistributedFileSupportService distributedFileSupportService, String chunkMap, File sourceFile, AsciiFileDistributor fileDist) {
    super();
    this.dfss = distributedFileSupportService;
    this.chunkMap = chunkMap;
    this.sourceFile = sourceFile;
    this.fileDist = fileDist;
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
          
      try {
        chunk = reader.readNext();
      } catch (IOException e) {
        DFSSException dfse = new DFSSException("["+sessionId+"] "+e.getMessage(), e);
        dfse.setErrorCode(DFSSException.ERR_IO_RW_EXCEPTION);
        throw dfse;
      }
      
      if(chunk != null)
      {
        try 
        {
          counter = putChunk(chunk);
          
          try {
            while((chunk = reader.readNext()) != null)
            {
              counter = putChunk(chunk);
            }
          } catch (IOException e) {
            DFSSException dfse = new DFSSException("["+sessionId+"] "+e.getMessage(), e);
            dfse.setErrorCode(DFSSException.ERR_IO_RW_EXCEPTION);
            throw dfse;
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
      
      ISet<String> errNodes = fileDist.errOnSynch();
      DFSSResponse success = errNodes.isEmpty() ? DFSSResponse.SUCCESS : DFSSResponse.ERROR;
      success.setSessionId(sessionId);
      success.setRecordMap(recordMap);
      success.setNoOfRecords(this.dfss.hzService.getMap(recordMap).size());
      success.setSourceByteSize(sourceFile.length());
      success.setSinkByteSize(size);
      success.getErrorNodes().addAll(errNodes);
      errNodes.destroy();
      return success;
    } finally {
      this.dfss.hzService.getAtomicLong(chunkMap).destroy();
    }
  }
  @Override
  public DFSSResponse call() throws DFSSException
  {
    try 
    {
      submitChunks();
    } catch (IOException e) {
      DFSSException dfse = new DFSSException("["+sessionId+"] "+e.getMessage(), e);
      dfse.setErrorCode(DFSSException.ERR_IO_EXCEPTION);
      throw dfse;
    }
    try 
    {
      fileDist.awaitCompletion(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    boolean b = this.dfss.hzService.isDistributedObjectDestroyed(fileDist.keyspace(), IMap.class);
    if(!b)
      log.warn("[DFSS#"+sessionId+"] >>> Temporary map ["+fileDist.keyspace()+"] not destroyed <<<");
    
    return prepareResponse();
  }
}