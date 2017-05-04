package com.reactivetechnologies.hzdfs.cluster;
import java.io.IOException;
/* ============================================================================
*
* FILE: HazelcastClusterServiceBean.java
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
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.util.StringUtils;

import com.hazelcast.config.ConfigurationException;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.Member;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.mapreduce.JobTracker;
import com.reactivetechnologies.hzdfs.utils.ResourceLoaderHelper;

/**
 * Hazelcast instance wrapper. This class would expose interactions with the underlying datagrid.
 * A singleton instance across the VM.
 * @author esutdal
 *
 */
public final class HazelcastClusterServiceBean implements HazelcastOperations {
	
	HazelcastInstanceProxy hzInstance = null;
			
	final static String REST_CONTEXT_URI = "http://@IP:@PORT/hazelcast/rest";
	static final Logger log = LoggerFactory.getLogger(HazelcastClusterServiceBean.class);
	
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#isDistributedObjectDestroyed(java.lang.String, java.lang.Class)
	 */
  @Override
public boolean isDistributedObjectDestroyed(String objectName, Class<?> type)
  {
    boolean match = false;
    for(DistributedObject dObj : hzInstance.getHazelcast().getDistributedObjects())
    {
      log.debug("[isDistributedObjectDestroyed] Checking => "+dObj.getName());
      if(dObj.getName().equals(objectName) && type.isAssignableFrom(dObj.getClass()))
      {
        match = true;
        break;
      }
        
    }
    return !match;
  }
	
	//we will be needing these for short time tasks.  Since a member addition / removal operation should not occur very frequently
	private final ExecutorService worker = Executors.newCachedThreadPool(new ThreadFactory() {
		private int n = 0;
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "hz.member.thread" + "-" +(n++));
			t.setDaemon(true);
			return t;
		}
	});
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#setMapStoreImplementation(java.lang.String, com.hazelcast.core.MapStore)
	 */
	@Override
	public void setMapStoreImplementation(String map, MapStore<? extends Serializable, ? extends Serializable> backingStore)
	{
	  hzInstance.setMapStoreImplementation(map, backingStore, true);
	  log.info("Set write through backing store for "+map);
	}
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#setMapConfiguration(java.lang.Class)
	 */
	@Override
	public void setMapConfiguration(Class<?> annotatedClass)
	{
	  hzInstance.addMapConfig(annotatedClass);
	}
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#setMapConfiguration(com.reactivetechnologies.hzdfs.cluster.IMapConfig, java.lang.String)
	 */
	@Override
	public void setMapConfiguration(IMapConfig imapConfig, String imap)
  {
    hzInstance.addMapConfig(imapConfig, imap);
  }
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#remove(java.lang.Object, java.lang.String)
	 */
	@Override
	public void remove(Object key, String map)
  {
	  hzInstance.remove(key, map);
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#getMap(java.lang.String)
 */
  @Override
public <K, V> Map<K, V> getMap(String map)
  {
    return hzInstance.getMap(map);
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#put(java.lang.Object, java.lang.Object, java.lang.String)
 */
  @Override
public Object put(Object key, Object value, String map) {
    return put(key, value, map, false);
    
  }
  /**
   * Synchronized put operation across cluster.
   * @param key
   * @param value
   * @param map
   * @param synchronize
   * @deprecated use {@linkplain IMap#tryPut(Object, Object, long, TimeUnit)} instead.
   * @return
   */
  public Object put(Object key, Object value, String map, boolean synchronize) {
    return synchronize ? hzInstance.synchronizePut(key, value, map) : hzInstance.put(key, value, map);
    
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#set(java.lang.Object, java.lang.Object, java.lang.String)
 */
  @Override
public void set(Object key, Object value, String map) {
    set(key, value, map, false);
    
  }
  /**
   * Synchronized set operation across cluster.
   * @param key
   * @param value
   * @param map
   * @param synchronize
   */
  public void set(Object key, Object value, String map, boolean synchronize) {
    if(synchronize)
      hzInstance.synchronizeSet(key, value, map);
    else
      hzInstance.set(key, value, map);
    
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#get(java.lang.Object, java.lang.String)
 */
  @Override
public Object get(Object key, String map) {
    return hzInstance.get(key, map);
    
  }
  				
	final AtomicBoolean migrationRunning = new AtomicBoolean();	
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#isMigrationRunning()
	 */
	@Override
	public boolean isMigrationRunning() {
		return migrationRunning.compareAndSet(true, true);
	}

	final Map<String, MigratedEntryProcessor<?>> migrCallbacks = new HashMap<>();
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#addPartitionMigrationCallback(com.reactivetechnologies.hzdfs.cluster.MigratedEntryProcessor)
	 */
	@Override
	public boolean addPartitionMigrationCallback(MigratedEntryProcessor<?> callback) 
	{
	  if (!isMigrationRunning()) {
      migrCallbacks.put(callback.keyspace(), callback);
      return true;
    }
	  else
      return false;
	}
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#removeMigrationCallback(com.reactivetechnologies.hzdfs.cluster.MigratedEntryProcessor)
	 */
	@Override
	public boolean removeMigrationCallback(MigratedEntryProcessor<?> callback) 
  {
    if(isMigrationRunning()) {
      synchronized (migrationRunning) {
        if(isMigrationRunning())
        {
          try {
            migrationRunning.wait(TimeUnit.SECONDS.toMillis(30));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        
      }
      
    }
    migrCallbacks.remove(callback.keyspace());
    return true;
  }
	
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#addLocalEntryListener(java.io.Serializable, com.hazelcast.map.listener.MapListener)
	 */
	@Override
	public String addLocalEntryListener(Serializable keyspace, MapListener listener)
  {
	  return hzInstance.addLocalEntryListener(keyspace.toString(), listener);
  }
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#removeEntryListener(java.lang.String, com.reactivetechnologies.hzdfs.cluster.LocalMapEntryPutListener)
	 */
	@Override
	public <T> boolean removeEntryListener(String id, LocalMapEntryPutListener<T> listener)
  {
    return hzInstance.removeEntryListener(id, listener.keyspace());
  }
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#addLocalEntryListener(com.reactivetechnologies.hzdfs.cluster.LocalMapEntryPutListener)
	 */
	@Override
	public <V> String addLocalEntryListener(LocalMapEntryPutListener<V> addUpdateListener)
  {
	  return addLocalEntryListener(addUpdateListener.keyspace(), addUpdateListener);
  }
	private final InstanceListener instanceListener = new InstanceListener();
	
	final Set<String> migratingPartitions = Collections.synchronizedSet(new HashSet<String>());
	/**
	 * Register lifecycle listeners.
	 */
	private void registerListeners()
	{
	  hzInstance.init(instanceListener);
	  
    hzInstance.addMigrationListener(new DefaultMigrationListener(this));
	}
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#setProperty(java.lang.String, java.lang.String)
	 */
	@Override
	public void setProperty(String prop, String val)
	{
	  hzInstance.setProperty(prop, val);
	}
	/**
	 * Public constructor
	 * @param props
	 * @throws ConfigurationException
	 */
	HazelcastClusterServiceBean(String cfgXml, String entityScanPath) {
		if(hzInstance == null)
 {
      try {
        hzInstance = StringUtils.hasText(cfgXml) ? new HazelcastInstanceProxy(
            ResourceLoaderHelper.loadFromFileOrClassPath(cfgXml),
            entityScanPath) : new HazelcastInstanceProxy(entityScanPath);
      } catch (IOException e) {
        throw new BeanCreationException("Unable to start Hazelcast", e);
      }
    }
	}
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#join(java.lang.String)
	 */
	@Override
	public void join(String instanceId)
	{
	  hzInstance.requestJoin(instanceId);
	}
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#join(java.lang.String, java.lang.String)
	 */
	@Override
	public void join(String instanceId, String group)
  {
    hzInstance.requestJoin(instanceId, group);
  }
	private volatile boolean startedListeners;
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#isStarted()
	 */
	@Override
	public boolean isStarted() {
    return startedListeners;
  }

  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#startInstanceListeners()
 */
	@Override
	public void startInstanceListeners()
	{
	  if (!startedListeners) {
      registerListeners();
      startedListeners = true;//silently ignore
      log.info("Started lifecycle and partition listeners..");
    }
	  else
	    log.warn("[startInstanceListeners] invoked more than once. Ignored silently.");
	}
	
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#size()
	 */
	@Override
	public int size()
	{
		return hzInstance.noOfMembers();
	}
	
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#stopService()
	 */
	@Override
	@PreDestroy
	public void stopService() {
		if(hzInstance != null)
		{
			hzInstance.stop();
		}
		worker.shutdown();
		try {
			worker.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			
		}
				
	}
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#removeNow(java.io.Serializable, java.lang.String)
	 */
  @Override
public Object removeNow(Serializable key, String map) {
    return hzInstance.removeNow(key, map);
    
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#getClusterLock(java.lang.String)
 */
  @Override
public Lock getClusterLock(String name)
  {
    return hzInstance.getLock(name);
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#addInstanceListenerObserver(com.reactivetechnologies.hzdfs.cluster.MembershipEventObserver)
 */
  @Override
public void addInstanceListenerObserver(MembershipEventObserver observer) {
    instanceListener.addObserver(observer);      
    
  }
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#addMessageChannel(com.reactivetechnologies.hzdfs.cluster.MessageChannel)
	 */
  @Override
public <E> String addMessageChannel(MessageChannel<E> channel)
  {
    return addMessageChannel(channel, false);
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#addMessageChannel(com.reactivetechnologies.hzdfs.cluster.MessageChannel, boolean)
 */
  @Override
public <E> String addMessageChannel(MessageChannel<E> channel, boolean orderingEnabled)
  {
    return hzInstance.addMessageChannelHandler(channel, orderingEnabled);
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#removeMessageChannel(com.reactivetechnologies.hzdfs.cluster.AbstractMessageChannel)
 */
  @Override
public <E> void removeMessageChannel(AbstractMessageChannel<E> channel)
  {
    removeMessageChannel(channel.topic(), channel.getRegistrationId());
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#removeMessageChannel(java.lang.String, java.lang.String)
 */
  @Override
public <E> void removeMessageChannel(String topic, String regID)
  {
    hzInstance.removeTopicListener(topic, regID);
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#publish(java.lang.Object, java.lang.String)
 */
  @Override
public void publish(Object message, String topic) {
    hzInstance.publish(message, topic);
    
  }
	/* (non-Javadoc)
	 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#acquireLock(java.util.concurrent.TimeUnit, long)
	 */
  @Override
public boolean acquireLock(TimeUnit unit, long time) throws InterruptedException
  {
    return hzInstance.getClusterSyncLock().tryLock(time, unit);
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#releaseLock(boolean)
 */
  @Override
public void releaseLock(boolean forced)
  {
    if (forced) {
      hzInstance.getClusterSyncLock().forceUnlock();
    }
    else
      hzInstance.getClusterSyncLock().unlock();
  }
  
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#contains(java.io.Serializable, java.lang.String)
 */
  @Override
public boolean contains(Serializable id, String imap) {
    return hzInstance.getMap(imap).containsKey(id);
    
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#getTopic(java.lang.String)
 */
  @Override
public ITopic<?> getTopic(String topic) {
    return hzInstance.getTopic(topic);
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#getClusterLatch(java.lang.String)
 */
  @Override
public ICountDownLatch getClusterLatch(String latch) {
    return hzInstance.getLatch(latch);
    
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#getAtomicLong(java.lang.String)
 */
@Override
public IAtomicLong getAtomicLong(String key) {
   return hzInstance.getHazelcast().getAtomicLong(key);
    
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#getSet(java.lang.String)
 */
@Override
public ISet<?> getSet(String set) {
    return hzInstance.getSet(set);
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#thisMember()
 */
@Override
public Member thisMember() {
    return hzInstance.me();
  }
  /* (non-Javadoc)
 * @see com.reactivetechnologies.hzdfs.cluster.HazelcastOperations#newJobTracker(java.lang.String)
 */
@Override
public JobTracker newJobTracker(String string) {
    return hzInstance.getHazelcast().getJobTracker(string);
    
  }
  
}
