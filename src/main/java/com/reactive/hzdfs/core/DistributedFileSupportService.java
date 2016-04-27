/* ============================================================================
*
* FILE: DistributedFileSupportService.java
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

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.Message;
import com.reactive.hzdfs.IDistributedFileSupport;
import com.reactive.hzdfs.cluster.HazelcastClusterServiceBean;
import com.reactive.hzdfs.cluster.intf.MessageChannel;
/**
 * Implementation class for {@linkplain IDistributedFileSupport}.
 */
@Service
public class DistributedFileSupportService implements MessageChannel<DFSSCommand>, IDistributedFileSupport {

  private static final Logger log = LoggerFactory.getLogger(DistributedFileSupportService.class);
  @Autowired HazelcastClusterServiceBean hzService;
  
  /**
   * 
   */
  public DistributedFileSupportService() {
    
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
    log.info("-- File distribution service initialized --");
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
    log.info("[DFSS] Initiating distribution for local file "+sourceFile+"; Checking attributes..");
    checkFile(sourceFile);
    DFSSTaskExecutor w = prepareTaskExecutor(sourceFile);
    log.info("[DFSS#"+w.sessionId+"] Coordinator prepared. Submitting task for execution..");
    return threads.submit(w);
  }
  private ExecutorService threads;
  /**
   * 
   * @param sourceFile
   * @return
   * @throws IOException
   */
  private DFSSTaskExecutor prepareTaskExecutor(File sourceFile) throws IOException
  {
    DFSSCommand cmd = prepareCluster(sourceFile);
    DFSSTaskExecutor w = new DFSSTaskExecutor(this, sourceFile.getName().toUpperCase(), sourceFile, createDistributorInstance(cmd));
    w.sessionId = cmd.getSessionId();
    w.recordMap = cmd.getRecordMap();
    return w;
  }
  //private CountDownLatch latch;
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
    return sourceFile.getName()/*.replaceAll("\\.", "_")*/.toUpperCase();
  }
  private ICountDownLatch sessionLatch(DFSSCommand cmd)
  {
    return hzService.getClusterLatch("DFSS_"+cmd.getSessionId());
  }
  /**
   * 
   * @param sourceFile
   * @return
   * @throws IOException
   */
  private DFSSCommand prepareCluster(File sourceFile) throws IOException {
        
    DFSSCommand cmd = new DFSSCommand();
    log.info("[DFSS] New task created with sessionId => "+cmd.getSessionId()+" for file => "+sourceFile);
    cmd.setCommand(DFSSCommand.CMD_INIT_ASCII_RCVRS);
    cmd.setChunkMap(chunkMapName(sourceFile));
    cmd.setRecordMap(cmd.getChunkMap()+"-REC");
    sendMessage(cmd);
    
    ICountDownLatch latch = sessionLatch(cmd);
    try 
    {
      log.info("[DFSS#"+cmd.getSessionId()+"] Preparing cluster for file distribution.. ");
      boolean b = latch.await(60, TimeUnit.SECONDS);
      if(!b)
      {
        cmd.setCommand(DFSSCommand.CMD_ABORT_JOB);
        sendMessage(cmd);
        
        throw new IOException("["+cmd.getSessionId()+"] Unable to prepare cluster for distribution in 60 secs. Job aborted!");
      }
    } 
    catch (InterruptedException e) {
      log.debug("", e);
    }
    finally
    {
      latch.destroy();
    }
    
    log.info("[DFSS#"+cmd.getSessionId()+"] Cluster preparation complete..");
    return cmd;
  }

  private final Map<String, AsciiFileDistributor> distributors = new WeakHashMap<>();
  /**
   * 
   * @param sessionId
   */
  private void removeDistributor(String sessionId)
  {
    AsciiFileDistributor dist = distributors.remove(sessionId);
    if(dist != null)
    {
      //destroy will be invoked multiple times on the same DO, but we are ignoring it
      clean(dist);
    }
  }
  private static void clean(AsciiFileDistributor dist)
  {
    dist.close();
    dist.destroyTempDO();
  }
  /**
   * 
   */
  private void removeAllDistributor()
  {
    for(Iterator<AsciiFileDistributor> iter = distributors.values().iterator(); iter.hasNext();)
    {
      AsciiFileDistributor afd = iter.next();
      clean(afd);
      iter.remove();
    }
  }
  private AsciiFileDistributor createDistributorInstance(DFSSCommand cmd)
  {
    AsciiFileDistributor dist = new AsciiFileDistributor(hzService, cmd.getRecordMap(), cmd.getChunkMap(), cmd.getSessionId());
    distributors.put(cmd.getSessionId(), dist);
    log.info("[DFSS#"+cmd.getSessionId()+"] New distribution task created for session..");
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
        sessionLatch(cmd).countDown();
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
