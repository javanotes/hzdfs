/* ============================================================================
*
* FILE: FileChunkCollector.java
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

import java.io.Closeable;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.reactive.hzdfs.DFSSException;
import com.reactive.hzdfs.cluster.HazelcastClusterServiceBean;
import com.reactive.hzdfs.cluster.IMapConfig;
import com.reactive.hzdfs.cluster.intf.AbstractLocalMapEntryPutListener;
import com.reactive.hzdfs.cluster.intf.MessageChannel;
import com.reactive.hzdfs.dto.DFSSTaskConfig;
/**
 * Infrastructure class for a simple distributed file system over Hazelcast. This class is responsible for
 * collating file chunks received on each node and aligning them as text records.
 */
class FileChunkCollector extends AbstractLocalMapEntryPutListener<AsciiFileChunk> implements Closeable, MessageChannel<String>
{

  private static final Logger log = LoggerFactory.getLogger(FileChunkCollector.class);
  private static final IMapConfig dfsConfig = DFSMapConfig.class.getAnnotation(IMapConfig.class);
  
  private String targetMap, distributionMap;
  private String topicRegId;
  private final String sessionId;
  @Override
  public String toString() {
    return "[targetMap=" + targetMap + ", distributionMap="
        + distributionMap + ", topicRegId=" + topicRegId + ", sessionId="
        + sessionId + ", closed=" + closed + "]";
  }

  private final DFSSTaskConfig config;

  /**
   * New distributor instance that would be instantiated on each node.
   * @param hzService Hazelcast service
   * @param recordMap IMap where the distributed records would be stored as text
   * @param distributionMap IMap where the file chunks are being distributed
   */
  public FileChunkCollector(HazelcastClusterServiceBean hzService, String recordMap, String distributionMap, String sessionId, DFSSTaskConfig cfg) {
    super(hzService, false);
    
    this.targetMap = recordMap;
    this.distributionMap = distributionMap;
    this.hzService.setMapConfiguration(dfsConfig, distributionMap);
    listenerId = this.hzService.addLocalEntryListener(this);
    topicRegId = this.hzService.addMessageChannel(this);
    this.sessionId = sessionId;
    config = cfg;
    log.debug("[DFSS#"+sessionId+"] "+toString());
    
  }
  
  
  @Override
  public String keyspace() {
    return distributionMap;
  }
  
  private final ConcurrentMap<String, RecordsBuilder> builders = new ConcurrentHashMap<>();
  /**
   * If any unfinished record exists
   * @return
   */
  public boolean isAppendableRecordExist()
  {
    for(RecordsBuilder rb : builders.values())
    {
      if(!rb.isBuildersEmpty())
        return true;
    }
    return false;
  }
  static String makeFileKey(AsciiFileChunk chunk)
  {
    return chunk.getFileName() + "." + chunk.getCreationTime();
  }
  private AtomicBoolean recordEmitted = new AtomicBoolean();
  
  private void incrementByteCounter(int len)
  {
    hzService.getAtomicLong(keyspace()).addAndGet(len);
  }
  private void handleNextChunk(Serializable key, AsciiFileChunk chunk) 
  {
    String fileKey = makeFileKey(chunk);
    incrementByteCounter(chunk.getChunk().length);
    if(!builders.containsKey(fileKey))
    {
      RecordsBuilder rBuilder = new RecordsBuilder(fileKey, hzService);
      rBuilder.setMap(targetMap);
      builders.putIfAbsent(fileKey, rBuilder);
    }
    boolean b = builders.get(fileKey).handleNextChunk(chunk, key);
    if(recordEmitted.compareAndSet(!b, b) && iswaitForLastEmittedRecord)
    {
      synchronized (this) {
        notifyAll();
      }
    }
    
    if(chunk.isEOF()){
      builders.remove(fileKey);
      awaitAckFromMembers();
    }
  }
  @Override
  public void entryAdded(EntryEvent<Serializable, AsciiFileChunk> event) {
    if (log.isDebugEnabled()) {
      log.debug("[onEntryAdded] " + event.getValue());
    }
    handleNextChunk(event.getKey(), event.getValue());
  }
  private volatile boolean isawaitAckFromMembers;
  /**
   * 
   * @return
   */
  ILock getClusterLock()
  {
    return (ILock) hzService.getClusterLock(getSessionId());
  }
  
  /**
   * 
   */
  private void awaitAckFromMembers() 
  {
    log.info("[DFSS#"+sessionId+"] EOF chunk received. Start sync with cluster members to signal end of distribution.");
    isawaitAckFromMembers = true;
    ICountDownLatch latch = hzService.getClusterLatch(topic());
    sendMessage(END_OF_DATA);
    boolean awaitSuccess = false;
    try 
    {
      awaitSuccess = latch.await(config.getEofSyncAckAwaitTime().getDuration(), config.getEofSyncAckAwaitTime().getUnit());
      if(!awaitSuccess)
      {
        DFSSException dfe = new DFSSException("["+sessionId+"] Unable to acquire EOF ack from members in "+config.getEofSyncAckAwaitTime());
        dfe.setErrorCode(DFSSException.ERR_IO_TIMEOUT);
        log.error("", dfe);
        addErrNode(hzService.thisMember(), DFSSException.ERR_IO_TIMEOUT);
      }
    } 
    catch (InterruptedException e1) 
    {
      log.debug("", e1);
    }
    finally
    {
      isawaitAckFromMembers = false;
      latch.destroy();
      cleanSlate(awaitSuccess);
    }
    log.info("[DFSS#"+sessionId+"] Synch completed successfully. Marking as end of job.");
  }
  
  private void cleanSlate(boolean awaitSuccess)
  {
    close();
    destroyTempDO();
    unlock(awaitSuccess);
    log.info("[DFSS#"+sessionId+"] Released intermediary resources..");
  }
  /**
   * Signal to the coordinator that all chunk readers have synched.
   */
  private void unlock(boolean awaitSuccess) {
    ILock lock = getClusterLock();
    lock.lock();
    try
    {
      lock.newCondition(getSessionId()).signalAll();
    }
    finally
    {
      lock.unlock();
    }
    
  }


  @Override
  public void entryUpdated(EntryEvent<Serializable, AsciiFileChunk> event) {
    entryAdded(event);

  }
  
  /**
   * To await when the job has been ended. That is to say, until all chunks have been distributed and acknowledgement (success/error)
   * received from all chunk receivers.
   * @param time
   * @param unit
   * @throws InterruptedException
   */
  void awaitCompletion(long time, TimeUnit unit) throws InterruptedException
  {
    ILock lock = getClusterLock();
    lock.lock();
    try
    {
      lock.newCondition(getSessionId()).await(time, unit);
    }
    finally
    {
      lock.unlock();
      lock.destroy();
    }
  }
  private volatile boolean closed;
  public boolean isClosed() {
    return closed;
  }
  
  @Override
  public void close()  {
    if (!closed) 
    {
      removeMapListener();
      hzService.removeMessageChannel(topic(), topicRegId);
      builders.clear();
      closed = true;
      
    }
  }
  /**
   * To destroy the common intermediate distributed data structures used.
   */
  void destroyTempDO()
  {
    if(!hzService.isDistributedObjectDestroyed(keyspace(), IMap.class))
    {
      IMap<?, ?> map = (IMap<?, ?>) hzService.getMap(keyspace());
      map.destroy();
      log.debug("[DFSS#"+sessionId+"] Temporary IMap destroyed.."+keyspace());
    }
    if(!hzService.isDistributedObjectDestroyed(topic(), ITopic.class)){
      hzService.getTopic(topic()).destroy();
      log.debug("[DFSS#"+sessionId+"] Temporary ITopic destroyed.."+topic());
    }
    
  }
  private void onEOFSignalled()
  {
    log.info("[DFSS#"+sessionId+"] EOF synch signal was received. Should expect no more chunks. Waiting for 30 secs.");
    try 
    {
      if (recordEmitted.compareAndSet(false, false)) 
      {
        waitForLastEmittedRecord();
      }
      if(!recordEmitted.get() || isAppendableRecordExist())
      {
        signalEOFErrAck();
      }
      else
        signalEOFAck();
    } 
    finally {
      close();
    }
  }
  @Override
  public void onMessage(Message<String> message) {
    if(!message.getPublishingMember().localMember())
    {
      if(END_OF_DATA.equals(message.getMessageObject()))
      {
        onEOFSignalled();
      }
      //these will be executed on the node that received the EOF chunk.
      else if(isawaitAckFromMembers && DFSSException.ERR_INCOMPLETE_REC.equals(message.getMessageObject()))
      {
        log.error("[DFSS#"+sessionId+"] ERR_INCOMPLETE_REC synch ack was received from "+message.getPublishingMember());
        addErrNode(message.getPublishingMember(), DFSSException.ERR_INCOMPLETE_REC);
        countdownEOFLatch();
      }
      else if(isawaitAckFromMembers && END_OF_DATA_ACK.equals(message.getMessageObject()))
      {
        log.info("[DFSS#"+sessionId+"] END_OF_DATA_ACK synch ack was received from "+message.getPublishingMember());
        countdownEOFLatch();
      }
    }
    
  }
  private void addErrNode(Member m, String msg)
  {
    errOnSynch().add(m.toString()+";"+msg);
  }
  @SuppressWarnings("unchecked")
  public ISet<String> errOnSynch()
  {
    return (ISet<String>) hzService.getSet(getSessionId());
  }
  private void countdownEOFLatch()
  {
    ICountDownLatch latch = hzService.getClusterLatch(topic());
    latch.countDown();
  }
  private void signalEOFAck()
  {
    sendMessage(END_OF_DATA_ACK);
    log.info("[DFSS#"+sessionId+"] ACK signalled for end of job.");
  }
  private void signalEOFErrAck()
  {
    log.error("["+sessionId+"] Final record did not emit on this node after EOF was received!");
    sendMessage(DFSSException.ERR_INCOMPLETE_REC);
  }
  private volatile boolean iswaitForLastEmittedRecord;
  
  /**
   * 
   */
  private synchronized void waitForLastEmittedRecord() 
  {
    iswaitForLastEmittedRecord = true;
    try 
    {
      wait(config.getFinalChunkAwaitTime().getUnit().toMillis(config.getFinalChunkAwaitTime().getDuration()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug("", e);
    }
    finally
    {
      iswaitForLastEmittedRecord = false;
    }
        
  }

  @Override
  public String topic() {
    return distributionMap+"HZDFSCOMM";
  }

  @Override
  public void sendMessage(String message) {
    hzService.publish(message, topic());
    
  }

  public String getSessionId() {
    return sessionId;
  }

  static final String END_OF_DATA = "END_OF_DATA";
  static final String END_OF_DATA_ACK = "END_OF_DATA_ACK";
  
    
}
