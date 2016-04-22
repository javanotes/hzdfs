/* ============================================================================
*
* FILE: DistributedFileSystemService.java
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
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.hazelcast.core.Message;
import com.reactive.hzdfs.IDistributedFileSystem;
import com.reactive.hzdfs.datagrid.HazelcastClusterServiceBean;
import com.reactive.hzdfs.datagrid.intf.MessageChannel;
/**
 * Service class for distributing a text file over Hazelcast cluster. This is the facade for 
 * performing distributed file operations.
 */
@Service
public class DistributedFileSystemService implements MessageChannel<DFSSCommand>, IDistributedFileSystem {

  private static final Logger log = LoggerFactory.getLogger(DistributedFileSystemService.class);
  @Autowired
  private HazelcastClusterServiceBean hzService;
  
  /**
   * 
   */
  public DistributedFileSystemService() {
    
  }

  private class Worker implements Callable<DFSSResponse>
  {
    private String chunkMap;
    private String sessionId;
    private AsciiFileDistributor fileDist;
    private String recordMap;
    /**
     * 
     * @param chunkMap
     */
    private Worker(String chunkMap, File sourceFile) {
      super();
      this.chunkMap = chunkMap;
      this.sourceFile = sourceFile;
    }
    private DistributionCounter putChunk(AsciiFileChunk chunk)
    {
      DistributionCounter ctr = new DistributionCounter();
      ctr.setRecordIdHash(chunk.generateHashCode());
      ctr.setRecordIdx(chunk.getRecordIndex());
      ctr.setFileName(chunk.getFileName());
      ctr.setCreatTime(chunk.getCreationTime());
      
      putEntry(ctr.getRecordIdHash(), chunk);
      
      return ctr;
    }
    private void putEntry(long recordIdHash, AsciiFileChunk chunk) {
      hzService.set(recordIdHash, chunk, chunkMap);
      
    }
    private final File sourceFile;
    private void submitChunks() throws IOException
    {
      try(AsciiChunkReader reader = new AsciiChunkReader(sourceFile, 8192))
      {
        log.info("[DFSS] Start file distribution job..");
        AsciiFileChunk chunk = null;
        
        DistributionCounter counter = new DistributionCounter();
            
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
            log.info("[DFSS] End file distribution job..");
          }
                  
        }
       }
    }
    
    private AsciiFileChunk putEOFChunk(DistributionCounter counter)
    {
      AsciiFileChunk chunk = new AsciiFileChunk();
      chunk.recordIndex = counter.getRecordIdx();
      chunk.setChunk(new byte[]{-1});
      chunk.setFileName(counter.getFileName());
      chunk.setCreationTime(counter.getCreatTime());
      
      putEntry(counter.getRecordIdHash(), chunk);
      
      return chunk;
    }
    
    @Override
    public DFSSResponse call() throws IOException {
      submitChunks();
      try {
        fileDist.awaitOnClose(10, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      boolean b = hzService.isDistributedObjectDestroyed(fileDist.keyspace());
      if(!b)
        log.warn(">>> Temporary map ["+fileDist.keyspace()+"] not destroyed <<<");
      DFSSResponse success = fileDist.isClosed() ? DFSSResponse.SUCCESS : DFSSResponse.ERROR;
      success.setSessionId(sessionId);
      success.setRecordMap(recordMap);
      return success;
    }
  }
  private String commandTopicId;
  @Value("${dfss.threadCount:4}")
  private int nThreads;
  @PostConstruct
  private void init()
  {
    commandTopicId = hzService.addMessageChannel(this);
    threads = Executors.newFixedThreadPool(nThreads, new ThreadFactory() {
      int n=0;
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "DFSS.Worker-"+(n++));
        return t;
      }
    });
    log.info("-------------------------------------");
    log.info("File distribution service initialized");
    log.info("-------------------------------------");
  }
  @PreDestroy
  private void destroy()
  {
    threads.shutdown();
    try {
      threads.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      
    }
    hzService.removeMessageChannel(topic(), commandTopicId);
    removeAllDistributor();
  }
  
  
 /* (non-Javadoc)
 * @see com.reactive.hzdfs.core.IDistributedFileSystem#distribute(java.io.File)
 */
  @Override
  public Future<DFSSResponse> distribute(File sourceFile) throws IOException 
  {
    checkFile(sourceFile);
    Worker w = prepareTaskExecutor(sourceFile);
    return threads.submit(w);
  }
  private ExecutorService threads;
  /**
   * 
   * @param sourceFile
   * @return
   * @throws IOException
   */
  private Worker prepareTaskExecutor(File sourceFile) throws IOException
  {
    DFSSCommand cmd = prepareCluster(sourceFile);
    Worker w = new Worker(sourceFile.getName().toUpperCase(), sourceFile);
    w.sessionId = cmd.getSessionId();
    w.fileDist = cmd.getDistInstance();
    w.recordMap = cmd.getRecordMap();
    return w;
  }
  private CountDownLatch latch;
  /**
   * 
   * @param sourceFile
   * @throws IOException
   */
  private void checkFile(File sourceFile) throws IOException
  {
    if(sourceFile == null)
      throw new IOException("Source file is null");
    if(!sourceFile.exists())
      throw new IOException("Source file does not exist");
    if(!sourceFile.isFile())
      throw new IOException("Not a valid file");
    if(!sourceFile.canRead())
      throw new IOException("Cannot read source file");
  }
  private static String chunkMapName(File sourceFile)
  {
    //TODO: Using replaceAll breaks the code!!
    return sourceFile.getName().toUpperCase()/*.replaceAll("\\.", "")*/;
  }
  
  /**
   * 
   * @param sourceFile
   * @return
   * @throws IOException
   */
  private DFSSCommand prepareCluster(File sourceFile) throws IOException {
        
    DFSSCommand cmd = new DFSSCommand();
    cmd.setCommand(DFSSCommand.CMD_INIT_ASCII_RCVRS);
    cmd.setChunkMap(chunkMapName(sourceFile));
    cmd.setRecordMap(cmd.getChunkMap()+"-REC");
    sendMessage(cmd);
    latch = new CountDownLatch(hzService.size()-1);
    try 
    {
      log.info("[DFSS] Preparing cluster for file distribution with sessionId => "+cmd.getSessionId());
      boolean b = latch.await(30, TimeUnit.SECONDS);
      if(!b){
        cmd.setCommand(DFSSCommand.CMD_ABORT_JOB);
        sendMessage(cmd);
        throw new IOException("Unable to prepare cluster for distribution in 30 secs. Job aborted!");
      }
    } 
    catch (InterruptedException e) {
      log.debug("", e);
    }
    AsciiFileDistributor dist = createDistributorInstance(cmd);
    cmd.setDistInstance(dist);
    log.info("[DFSS] Cluster preparation complete..");
    return cmd;
  }

  private final Map<String, AsciiFileDistributor> distributors = new WeakHashMap<>();
  private void removeDistributor(String sessionId)
  {
    AsciiFileDistributor dist = distributors.remove(sessionId);
    if(dist != null)
      dist.close();
  }
  private void removeAllDistributor()
  {
    for(Iterator<AsciiFileDistributor> iter = distributors.values().iterator(); iter.hasNext();)
    {
      AsciiFileDistributor afd = iter.next();
      afd.close();
      iter.remove();
    }
  }
  private AsciiFileDistributor createDistributorInstance(DFSSCommand cmd)
  {
    AsciiFileDistributor dist = new AsciiFileDistributor(hzService, cmd.getRecordMap(), cmd.getChunkMap(), cmd.getSessionId());
    distributors.put(cmd.getSessionId(), dist);
    log.info("[DFSS] New distribution task created for session => "+cmd.getSessionId());
    return dist;
  }
  @Override
  public void onMessage(Message<DFSSCommand> message) 
  {
    DFSSCommand cmd = message.getMessageObject();
    if(DFSSCommand.CMD_INIT_ASCII_RCVRS.equals(cmd.getCommand()))
    {
      
      if(!message.getPublishingMember().localMember())
      {
        //will be closed by self
        createDistributorInstance(cmd);
        cmd.setCommand(DFSSCommand.CMD_INIT_ASCII_RCVRS_ACK);
        sendMessage(cmd);
        
      }
    }
    else if(DFSSCommand.CMD_INIT_ASCII_RCVRS_ACK.equals(cmd.getCommand()))
    {
      if(latch != null)
      {
        latch.countDown();
      }
      
    }
    else if(DFSSCommand.CMD_ABORT_JOB.equals(cmd.getCommand()))
    {
      removeDistributor(cmd.getSessionId());
      
    }
  }



  @Override
  public String topic() {
    return DFSSCommand.class.getSimpleName().toUpperCase();
  }



  @Override
  public void sendMessage(DFSSCommand message) {
    hzService.publish(message, topic());
    
  }
  
  
}
