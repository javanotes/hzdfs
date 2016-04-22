package com.reactive.hzdfs.datagrid;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
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
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.map.listener.MapListener;
import com.reactive.hzdfs.datagrid.intf.AbstractMessageChannel;
import com.reactive.hzdfs.datagrid.intf.LocalMapEntryPutListener;
import com.reactive.hzdfs.datagrid.intf.MembershipEventObserver;
import com.reactive.hzdfs.datagrid.intf.MessageChannel;
import com.reactive.hzdfs.datagrid.intf.MigratedEntryProcessor;
import com.reactive.hzdfs.utils.ResourceLoaderHelper;

/**
 * Hazelcast instance wrapper. This class would expose interactions with the underlying datagrid.
 * A singleton instance across the VM.
 * @author esutdal
 *
 */
public final class HazelcastClusterServiceBean {
	
	private HazelcastInstanceProxy hzInstance = null;
			
	final static String REST_CONTEXT_URI = "http://@IP:@PORT/hazelcast/rest";
	private static final Logger log = LoggerFactory.getLogger(HazelcastClusterServiceBean.class);
	
	/**
   * Check if the distributed object is destroyed.
   * @param objectName
   * @return
   */
  public boolean isDistributedObjectDestroyed(String objectName)
  {
    boolean match = false;
    for(DistributedObject dObj : hzInstance.getHazelcast().getDistributedObjects())
    {
      log.debug("[isDistributedObjectDestroyed] Checking => "+dObj.getName());
      if(dObj.getName().equals(objectName))
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
	/**
	 * Set IMap store using a spring bean. This is for programmatic configuration of {@linkplain MapStore}.
	 * @param backingStore
	 */
	public void setMapStoreImplementation(String map, MapStore<? extends Serializable, ? extends Serializable> backingStore)
	{
	  hzInstance.setMapStoreImplementation(map, backingStore, true);
	  log.info("Set write through backing store for "+map);
	}
	/**
	 * Set IMap configuration programmatically. The provided class must be annotated with {@linkplain IMapConfig}.
	 * @param annotatedClass
	 */
	public void setMapConfiguration(Class<?> annotatedClass)
	{
	  hzInstance.addMapConfig(annotatedClass);
	}
	/**
	 * Asynchronous removal of a key from IMap.
	 * @param key
	 * @param map
	 */
	public void remove(Object key, String map)
  {
	  hzInstance.remove(key, map);
  }
  /**
   * Gets the Hazelcast IMap instance.
   * @param map
   * @return
   */
  public <K, V> Map<K, V> getMap(String map)
  {
    return hzInstance.getMap(map);
  }
  /**
   * Map put operation.
   * @param key
   * @param value
   * @param map
   * @return
   */
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
  /**
   * 
   * @param key
   * @param value
   * @param map
   */
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
  /**
   * Get the value corresponding to the key from an {@linkplain IMap}.
   * @param key
   * @param map
   * @return
   */
  public Object get(Object key, String map) {
    return hzInstance.get(key, map);
    
  }
  				
	private final AtomicBoolean migrationRunning = new AtomicBoolean();	
	/**
	 * Is migration ongoing
	 * @return
	 */
	public boolean isMigrationRunning() {
		return migrationRunning.compareAndSet(true, true);
	}

	private final Map<String, MigratedEntryProcessor<?>> migrCallbacks = new HashMap<>();
	/**
	 * Register a new partition migration listener. The listener callback will be invoked on each entry being migrated.
	 * @param callback
	 * @throws IllegalAccessException if service is already started
	 */
	public void addPartitionMigrationCallback(MigratedEntryProcessor<?> callback) throws IllegalAccessException
	{
	  if (!startedListeners) {
      migrCallbacks.put(callback.keyspace(), callback);
    }
	  else
      throw new IllegalAccessException("PartitionMigrationListener cannot be added after Hazelcast service has been started");
	}
	
	/**
	 * Register a local add/update entry listener on a given {@linkplain IMap} by name. Only a single listener for a given {@linkplain MapListener} instance would be 
	 * registered. So subsequent invocation with the same instance would first remove any existing registration for that instance.
	 * @param keyspace map name
	 * @param listener callback 
	 * @return 
	 * @throws IllegalAccessException 
	 */
	public String addLocalEntryListener(Serializable keyspace, MapListener listener)
  {
	  return hzInstance.addLocalEntryListener(keyspace.toString(), listener);
  }
	/**
	 * Removes the specified map entry listener. Returns silently if there is no such listener added before.
	 * @param id
	 * @param listener
	 * @return
	 */
	public <T> boolean removeEntryListener(String id, LocalMapEntryPutListener<T> listener)
  {
    return hzInstance.removeEntryListener(id, listener.keyspace());
  }
	/**
	 * Register a local add/update entry listener on a given {@linkplain IMap} by name. Only a single listener for a given {@linkplain MapListener} instance would be 
   * registered. So subsequent invocation with the same instance would first remove any existing registration for that instance.
	 * @param addUpdateListener listener with map name
	 * @return 
	 */
	public <V> String addLocalEntryListener(LocalMapEntryPutListener<V> addUpdateListener)
  {
	  return addLocalEntryListener(addUpdateListener.keyspace(), addUpdateListener);
  }
	private final InstanceListener instanceListener = new InstanceListener();
	/**
	 * Register lifecycle listeners.
	 */
	private void registerListeners()
	{
	  hzInstance.init(instanceListener);
	  
    hzInstance.addMigrationListener(new MigrationListener() {
      
      @Override
      public void migrationStarted(MigrationEvent migrationevent) {
        migrationRunning.getAndSet(true);
      }
      
      @Override
      public void migrationFailed(MigrationEvent migrationevent) {
        migrationRunning.getAndSet(false);
        synchronized (migrationRunning) {
          migrationRunning.notifyAll();
        }
      }
      
      @Override
      public void migrationCompleted(MigrationEvent migrationevent) 
      {
        migrationRunning.getAndSet(false);
        synchronized (migrationRunning) {
          migrationRunning.notifyAll();
        }
        if(migrationevent.getNewOwner().localMember())
        {
          log.debug(">>>>>>>>>>>>>>>>> Migration detected of partition ..."+migrationevent.getPartitionId());
          for(Entry<String, MigratedEntryProcessor<?>> e : migrCallbacks.entrySet())
          {
            IMap<Serializable, Object> map = hzInstance.getMap(e.getKey());
            Set<Serializable> keys = new HashSet<>();
            for(Serializable key : map.localKeySet())
            {
              if(hzInstance.getPartitionIDForKey(key) == migrationevent.getPartitionId())
              {
                keys.add(key);
              }
            }
            if(!keys.isEmpty())
            {
              map.executeOnKeys(keys, e.getValue());
            }
          }
          
        }
        
      }
    });
	}
	/**
	 * Set a Hazelcast configuration property.
	 * @param prop
	 * @param val
	 */
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
	/**
	 * Try and join to default cluster, and start Hazelcast service.
	 * @param instanceId
	 */
	public void join(String instanceId)
	{
	  hzInstance.requestJoin(instanceId);
	}
	/**
	 * Try and join cluster with given name, and start Hazelcast service.
	 * @param instanceId
	 * @param group
	 */
	public void join(String instanceId, String group)
  {
    hzInstance.requestJoin(instanceId, group);
  }
	private volatile boolean startedListeners;
	/**
	 * True if the listeners have been started.
	 * @return
	 */
	public boolean isStarted() {
    return startedListeners;
  }

  /**
	 * Start lifecycle and partition listeners
	 */
	public void startInstanceListeners()
	{
	  if (!startedListeners) {
      registerListeners();
      startedListeners = true;//silently ignore
    }
	  else
	    log.warn("[startInstanceListeners] invoked more than once. Ignored silently.");
	}
	
	/**
	 * No of members in the cluster at this point.
	 * @return
	 */
	public int size()
	{
		return hzInstance.noOfMembers();
	}
	
	/**
	 * 
	 * @param retry
	 */
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
	/**
	 * Synchronous removal of a key from IMap.
	 * @param key
	 * @param map
	 * @return
	 */
  public Object removeNow(Serializable key, String map) {
    return hzInstance.removeNow(key, map);
    
  }
  /**
   * Get a distributed cluster wide lock.
   * @param name
   * @return
   */
  public Lock getClusterLock(String name)
  {
    return hzInstance.getLock(name);
  }
  /**
   * Register a group membership event callback.
   * @param observer
   */
  public void addInstanceListenerObserver(MembershipEventObserver observer) {
    instanceListener.addObserver(observer);      
    
  }
	/**
	 * Adds a message channel with no message ordering.
	 * @param channel
	 * @return regID
	 */
  public <E> String addMessageChannel(MessageChannel<E> channel)
  {
    return addMessageChannel(channel, false);
  }
  /**
   * Adds a message channel.
   * @param channel
   * @param orderingEnabled whether to order messages
   * @return regID
   */
  public <E> String addMessageChannel(MessageChannel<E> channel, boolean orderingEnabled)
  {
    return hzInstance.addMessageChannelHandler(channel, orderingEnabled);
  }
  /**
   * Removes the channel topic listener.
   * @param channel
   */
  public <E> void removeMessageChannel(AbstractMessageChannel<E> channel)
  {
    removeMessageChannel(channel.topic(), channel.getRegistrationId());
  }
  /**
   * Removes a topic listener.
   * @param topic
   * @param regID
   */
  public <E> void removeMessageChannel(String topic, String regID)
  {
    hzInstance.removeTopicListener(topic, regID);
  }
  /**
   * Publish a message to a {@linkplain ITopic}
   * @param message
   * @param topic
   */
  public void publish(Object message, String topic) {
    hzInstance.publish(message, topic);
    
  }
	/**
	 * Acquire a cluster wide lock.
	 * @param unit
	 * @param time
	 * @return
	 * @throws InterruptedException
	 */
  public boolean acquireLock(TimeUnit unit, long time) throws InterruptedException
  {
    return hzInstance.getClusterSyncLock().tryLock(time, unit);
  }
  /**
   * Release a cluster wide lock.
   * @param forced
   */
  public void releaseLock(boolean forced)
  {
    if (forced) {
      hzInstance.getClusterSyncLock().forceUnlock();
    }
    else
      hzInstance.getClusterSyncLock().unlock();
  }
  
  /**
   * Checks if an {@linkplain IMap} contains the given key.
   * @param id
   * @param imap
   * @return
   */
  public boolean contains(Serializable id, String imap) {
    return hzInstance.getMap(imap).containsKey(id);
    
  }
  /**
   * Get topic instance with the given name.
   * @param topic
   * @return
   */
  public ITopic<?> getTopic(String topic) {
    return hzInstance.getTopic(topic);
  }
  /**
   * Get a cluster wide count down latch with the given name.
   * @param latch
   * @return
   */
  public ICountDownLatch getClusterLatch(String latch) {
    return hzInstance.getLatch(latch);
    
  }
  
}
