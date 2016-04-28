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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.reactive.hzdfs.DFSSException;
import com.reactive.hzdfs.dto.Counter;
import com.reactive.hzdfs.dto.DFSSResponse;
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
  private final FileChunkCollector chunkCollector;
  String recordMap;
  private long consumableBytesLen = 0;
  /**
   * 
   * @param distributedFileSupportService
   * @param chunkMap
   * @param sourceFile
   * @param fileDist
   */
  DFSSTaskExecutor(DistributedFileSupportService distributedFileSupportService, String chunkMap, File sourceFile, FileChunkCollector fileDist) {
    super();
    this.dfss = distributedFileSupportService;
    this.chunkMap = chunkMap;
    this.sourceFile = sourceFile;
    this.chunkCollector = fileDist;
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
  
  private void submitChunks() throws IOException, DFSSException 
  {
    try(FileChunkReader reader = new FileChunkReader(sourceFile, 8192))
    {
      log.info("[DFSS#"+sessionId+"] Start file chunk distribution..");
      AsciiFileChunk chunk = null;
      
      Counter counter = new Counter();
          
      try {
        chunk = reader.readNext();
      } catch (IOException e) {
        DFSSException dfse = new DFSSException("["+sessionId+"] "+e.getMessage(), e);
        dfse.setErrorCode(DFSSException.ERR_IO_OPERATION);
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
            dfse.setErrorCode(DFSSException.ERR_IO_OPERATION);
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
  private static boolean hasTimeOut(Set<String> errNodes)
  {
    for(String s : errNodes)
    {
      if(s.contains(DFSSException.ERR_IO_TIMEOUT))
        return true;
    }
    return false;
  }
  private DFSSResponse prepareResponse() throws DFSSException
  {
    try 
    {
      long size = this.dfss.hzService.getAtomicLong(chunkMap).get();
      if(size != consumableBytesLen)
        throw new DFSSException("["+sessionId+"] Distributed file size mismatch! Bytes expected=> "+consumableBytesLen+" Bytes distributed=> "+size);
      
      ISet<String> errNodes = chunkCollector.errOnSynch();
      errNodes.toArray();
      
      DFSSResponse response = errNodes.isEmpty() ? DFSSResponse.COMPLETE : hasTimeOut(errNodes) ? DFSSResponse.TIMEOUT : DFSSResponse.ERROR;
      response.setSessionId(sessionId);
      response.setRecordMap(recordMap);
      response.setNoOfRecords(this.dfss.hzService.getMap(recordMap).size());
      response.setSourceByteSize(sourceFile.length());
      response.setSinkByteSize(size);
      response.getErrorNodes().addAll(errNodes);
      
      errNodes.destroy();
      
      return response;
    } 
    finally 
    {
      this.dfss.hzService.getAtomicLong(chunkMap).destroy();
    }
  }
  @Override
  public DFSSResponse call() throws DFSSException
  {
    try 
    {
      submitChunks();
    } 
    catch (IOException e) {
      DFSSException dfse = new DFSSException("["+sessionId+"] "+e.getMessage(), e);
      dfse.setErrorCode(DFSSException.ERR_IO_FILE);
      throw dfse;
    }
    try 
    {
      chunkCollector.awaitCompletion(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    boolean b = this.dfss.hzService.isDistributedObjectDestroyed(chunkCollector.keyspace(), IMap.class);
    if(!b)
      log.warn("[DFSS#"+sessionId+"] >>> Temporary map ["+chunkCollector.keyspace()+"] not destroyed <<<");
    
    return prepareResponse();
  }
}