/* ============================================================================
*
* FILE: AsciiFileDistributor.java
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
import com.hazelcast.core.IMap;
import com.hazelcast.core.Message;
import com.reactive.hzdfs.datagrid.HazelcastClusterServiceBean;
import com.reactive.hzdfs.datagrid.intf.AbstractLocalMapEntryPutListener;
import com.reactive.hzdfs.datagrid.intf.MessageChannel;
/**
 * Infrastructure class for a simple distributed file system over Hazelcast. This class is responsible for
 * collating file chunks received on each node and aligning them as text records.
 */
class AsciiFileDistributor extends AbstractLocalMapEntryPutListener<AsciiFileChunk> implements Closeable, MessageChannel<String>
{

  private static final Logger log = LoggerFactory.getLogger(AsciiFileDistributor.class);
  private String targetMap, distributionMap;
  private String topicRegId;
  private final String sessionId;
  @Override
  public String toString() {
    return "[targetMap=" + targetMap + ", distributionMap="
        + distributionMap + ", topicRegId=" + topicRegId + ", sessionId="
        + sessionId + ", closed=" + closed + "]";
  }


  /**
   * New distributor instance that would be instantiated on each node.
   * @param hzService Hazelcast service
   * @param recordMap IMap where the distributed records would be stored as text
   * @param distributionMap IMap where the file chunks are being distributed
   */
  public AsciiFileDistributor(HazelcastClusterServiceBean hzService, String recordMap, String distributionMap, String sessionId) {
    super(hzService, false);
    this.hzService.setMapConfiguration(DFSMapConfig.class);
    this.targetMap = recordMap;
    this.distributionMap = distributionMap;
    listenerId = this.hzService.addLocalEntryListener(this);
    topicRegId = this.hzService.addMessageChannel(this);
    this.sessionId = sessionId;
    log.info("[DFSS] "+toString());
  }
  
  
  @Override
  public String keyspace() {
    return distributionMap;
  }
  
  private ConcurrentMap<String, RecordsBuilder> builders = new ConcurrentHashMap<>();
  
  static String makeFileKey(AsciiFileChunk chunk)
  {
    return chunk.getFileName() + "." + chunk.getCreationTime();
  }
  private AtomicBoolean recordEmitted = new AtomicBoolean();
  
  private void handleNextChunk(Serializable key, AsciiFileChunk chunk)
  {
    String fileKey = makeFileKey(chunk);
    if(!builders.containsKey(fileKey))
    {
      RecordsBuilder rBuilder = new RecordsBuilder(fileKey, hzService);
      rBuilder.setMap(targetMap);
      builders.putIfAbsent(fileKey, rBuilder);
    }
    boolean b = builders.get(fileKey).handleNextChunk(chunk, key);
    recordEmitted.compareAndSet(!b, b);
    
    if(chunk.isEOF()){
      builders.remove(fileKey);
      awaitAckFromMembers();
    }
  }
  @Override
  public void entryAdded(EntryEvent<Serializable, AsciiFileChunk> event) {
    log.debug("[onEntryAdded] "+event.getValue());
    handleNextChunk(event.getKey(), event.getValue());
  }
  private String sessId()
  {
    return "Session#["+getSessionId()+"] => ";
  }
  private void awaitAckFromMembers() {
    log.info("[DFSS] EOF received. Start sync with cluster members to signal end of distribution");
    ICountDownLatch latch = hzService.getClusterLatch(topic());
    sendMessage(END_OF_DATA);
    try {
      boolean b = latch.await(60, TimeUnit.SECONDS);
      if(!b)
        throw new DFSSException(sessId()+"Unable to acquire EOF ack from members in 60 secs. Check node which could not emit final record.");
    } catch (InterruptedException e1) {
      log.debug("", e1);
    }
    finally
    {
      latch.destroy();
      close();
    }
    log.info("[DFSS] Synch completed successfully..");
  }

  @Override
  public void entryUpdated(EntryEvent<Serializable, AsciiFileChunk> event) {
    entryAdded(event);

  }
  private final Object closeLock = new Object();
  /**
   * To await when this instance is closed. That would signal an end of execution for this task.
   * @param time
   * @param unit
   * @throws InterruptedException
   */
  void awaitOnClose(long time, TimeUnit unit) throws InterruptedException
  {
    if(!closed)
    {
      synchronized (closeLock) {
        if(!closed)
        {
          closeLock.wait(unit.toMillis(time));
        }
        
      }
    }
  }
  private volatile boolean closed;
  public boolean isClosed() {
    return closed;
  }


  @Override
  public void close()  {
    synchronized (closeLock) {
      if (!closed) {
        removeMapListener();
        hzService.removeMessageChannel(topic(), topicRegId);
        builders.clear();
        IMap<?, ?> map = (IMap<?, ?>) hzService.getMap(keyspace());
        map.destroy();
        hzService.getTopic(topic()).destroy();
        closed = true;
        
      }
      closeLock.notifyAll();
    }
  }

  @Override
  public void onMessage(Message<String> message) {
    if(!message.getPublishingMember().localMember())
    {
      if(END_OF_DATA.equals(message.getMessageObject()))
      {
        log.info("[DFSS] EOF synch signal was received. Should expect no more chunks. Waiting for 10 secs");
        try {
          waitForLastEmittedRecord();
          if(!recordEmitted.get())
          {
            throw new DFSSException(sessId()+"Final record did not emit on this node after EOF was received!");
          }
          ICountDownLatch latch = hzService.getClusterLatch(topic());
          latch.countDown();
        } finally {
          close();
        }
      }
      
    }
    
  }

  private void waitForLastEmittedRecord() {
    if(!recordEmitted.get()){
      try {
        wait(10000);
      } catch (InterruptedException e) {
        
      }
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
