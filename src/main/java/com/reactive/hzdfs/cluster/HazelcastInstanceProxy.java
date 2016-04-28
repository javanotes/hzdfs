package com.reactive.hzdfs.cluster;
import java.io.File;
/* ============================================================================
*
* FILE: HzInstanceProxy.java
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
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.util.StringUtils;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.map.listener.MapListener;
import com.reactive.hzdfs.Configurator;
import com.reactive.hzdfs.cluster.intf.AbstractMessageChannel;
import com.reactive.hzdfs.cluster.intf.MessageChannel;
import com.reactive.hzdfs.utils.EntityFinder;

/**
 * Class to proxy a Hazelcast cluster member. <p><b>Note:</b>
 * If multicast communication doesn't work in your env, try to add this rule to iptables:<p>
 * <i>iptables -A INPUT -m pkttype --pkt-type multicast -j ACCEPT</i>
 * @author esutdal
 *
 */
class HazelcastInstanceProxy {
	
	/*
	 * The broadcast address for an IPv4 host can be obtained by performing a bitwise OR operation 
	 * between the bit complement of the subnet mask and the host's IP address.

	   Example: For broadcasting a packet to an entire IPv4 subnet using the private IP address space 172.16.0.0/12, 
	   which has the subnet mask 255.240.0.0, the broadcast address is 172.16.0.0 | 0.15.255.255 = 172.31.255.255.
	 */
		
  
  private static final Logger log = LoggerFactory.getLogger(HazelcastInstanceProxy.class);
	/**
	 * Gets a lock
	 * @param name
	 * @return
	 */
  ILock getLock(String name)
	{
		if(isRunning())
		{
			return hazelcast.getLock(name);
		}
		throw new IllegalStateException("Hazelcast not running");
	}
	
	private HazelcastInstance hazelcast = null;
	/**
	 * 
	 * @return
	 */
	HazelcastInstance getHazelcast() {
	  if(isRunning())
	    return hazelcast;
	  else
	    throw new IllegalStateException("Hazelcast not running!");
  }


  <T> IQueue<T> getQueue(String name)
	{
		return hazelcast.getQueue(name);
	}
		
		
	public int noOfMembers(){
		if(isRunning()){
			return hazelcast.getCluster().getMembers().size();
		}
		return 0;
	}
	
	/**
	 * If the particular instance is running an active hazelcast service
	 * @return
	 */
	private boolean isRunning(){
		try {
			return hazelcast != null && hazelcast.getLifecycleService() != null && hazelcast.getLifecycleService().isRunning();
		} catch (Exception e) {
			//log.warn(e);
		}
		return false;
	}
	private void addMapConfigs(Collection<Class<?>> entityClasses)
	{
	  for(Class<?> c : entityClasses)
	  {
	    addMapConfig(c);
	  }
	}
	private Config getHazelcastConfig()
	{
	  return started ? hazelcast.getConfig() : hzConfig;
	}
	/**
	 * Add an IMap configuration programmatically.
	 * @param hc
	 * @param mapName
	 */
	void addMapConfig(IMapConfig hc, String mapName)
	{
	  MapConfig mapC = new MapConfig(mapName);
    final Config configuration = getHazelcastConfig();
    
    if(configuration.getMapConfigs().containsKey(mapName))
    {
      mapC = configuration.getMapConfig(mapName);
    }
    
    mapC.setAsyncBackupCount(hc.asyncBackupCount());
    mapC.setBackupCount(hc.backupCount());
    mapC.setEvictionPercentage(hc.evictPercentage());
    mapC.setEvictionPolicy(EvictionPolicy.valueOf(hc.evictPolicy()));
    mapC.setInMemoryFormat(InMemoryFormat.valueOf(hc.inMemoryFormat()));
    mapC.setMaxIdleSeconds(hc.idleSeconds());
    mapC.setMergePolicy(hc.evictPolicy());
    mapC.setMinEvictionCheckMillis(hc.evictCheckMillis());
    mapC.setTimeToLiveSeconds(hc.ttlSeconds());
    mapC.setMaxSizeConfig(new MaxSizeConfig(hc.maxSize(), MaxSizePolicy.valueOf(hc.maxSizePolicy())));
    mapC.setStatisticsEnabled(hc.statisticsOn());
    
    configuration.getMapConfigs().put(mapC.getName(), mapC);
    
    log.debug("Added IMap configuration.."+mapC);
	}
	/**
	 * Add an IMap configuration programmatically from an annotated class.
	 * @param c
	 * @see IMapConfig
	 */
	void addMapConfig(Class<?> c)
	{
	  if(!c.isAnnotationPresent(IMapConfig.class))
	    throw new IllegalArgumentException(c+" not annotated with @"+IMapConfig.class.getSimpleName());
	  
	  IMapConfig hc = c.getAnnotation(IMapConfig.class);
	  
    addMapConfig(hc, hc.name());
	}
	/**
	 * 
	 * @param map
	 * @param backingStore
	 * @param writeThrough
	 */
	public void setMapStoreImplementation(String map, Object backingStore, boolean writeThrough)
	{
	  MapStoreConfig mStoreCfg = getHazelcastConfig().getMapConfig(map).getMapStoreConfig();
	  if(mStoreCfg == null)
	  {
	    mStoreCfg = new MapStoreConfig();
	  }
	  mStoreCfg.setImplementation(backingStore);
	  mStoreCfg.setEnabled(true);
	  mStoreCfg.setWriteDelaySeconds(writeThrough ? 0 : 5);
	  mStoreCfg.setInitialLoadMode(InitialLoadMode.LAZY);
	  
	  getHazelcastConfig().getMapConfig(map).setMapStoreConfig(mStoreCfg);
	}
	/**
	 * Local member IP address as registered with Hazelcast.
	 * @return
	 */
	public InetSocketAddress getLocalMemberAddress()
	{
	  return hazelcast.getCluster().getLocalMember().getSocketAddress();
	}
	private volatile boolean started;
	/**
	 * Whether Hazelcast service is started.
	 * @return
	 */
	public boolean isStarted() {
    return started;
  }

	private void start()
	{
	  hazelcast = Hazelcast.getOrCreateHazelcastInstance(hzConfig);
    Set<Member> members = hazelcast.getCluster().getMembers();
    
    int memberIdCnt = 0;
    for(Member m : members)
    {
      
      if(m.getStringAttribute(Configurator.NODE_INSTANCE_ID).equals(getInstanceId()))
      {
        memberIdCnt++;
      }
      if(memberIdCnt >= 2){
        stop();
        throw new IllegalStateException("Instance not allowed to join cluster as ["+getInstanceId()+"]. Duplicate name!");
      }
      
    }
    log.info("** Instance ["+getInstanceId()+"] joined cluster ["+hzConfig.getGroupConfig().getName()+"] successfully **");
    started = true;
	}
  /**
	 * 
	 */
	private void startInstance()
	{
	  if(!started)
	  {
	    synchronized (this) {
        if(!started)
        {
          start();
        }
      }
	  }
	  
	}
	/**
	 * Joins cluster.
	 * @param instanceId instance name
	 * @param name group/cluster name
	 * @param password 
	 */
	public void requestJoin(String instanceId, String name, String password)
	{
	  setInstanceId(instanceId);
    setGroupIds(name, password);
    startInstance();
	}
	private void setGroupIds(String name, String password)
	{
	  hzConfig.getMemberAttributeConfig().setStringAttribute(Configurator.NODE_INSTANCE_ID, getInstanceId());
    hzConfig.setInstanceName(getInstanceId());
    if (StringUtils.hasText(name)) {
      hzConfig.getGroupConfig().setName(name);
    }
    if (StringUtils.hasLength(password)) {
      hzConfig.getGroupConfig().setPassword(password);
    }
	}
	/**
   * Joins cluster
   * @param instanceId instance name
   * @param name group/cluster name
   */
  public void requestJoin(String instanceId, String name)
  {
    requestJoin(instanceId, name, null);
  }
	/**
   * Joins cluster
   * @param instanceId instance name
   */
  public void requestJoin(String instanceId)
  {
    requestJoin(instanceId, null, null);
  }
	private  final Config hzConfig;
	/**
	 * 
	 * @throws FileNotFoundException
	 * @throws ConfigurationException 
	 */
	private HazelcastInstanceProxy(Config hzConfig, String entityScanPath) {
		this.hzConfig = hzConfig;
		setProperty("hazelcast.event.thread.count", "6");
    setProperty("hazelcast.operation.thread.count", "4");
    setProperty("hazelcast.io.thread.count", "4"); //4+4+1
    setProperty("hazelcast.shutdownhook.enabled", "false");
    
    try {
      addMapConfigs(EntityFinder.findMapEntityClasses(entityScanPath));
    } catch (Exception e) {
      throw new BeanCreationException("Unable to load entity classes", e);
    }
    
    
	}
	/**
	 * 
	 * @param entityBasePkg
	 */
  public HazelcastInstanceProxy(String entityBasePkg)
	{
	  this(new Config(), entityBasePkg);
	}
  /**
   * 
   * @param configFile
   * @param entityBasePkg
   * @throws FileNotFoundException
   */
	public HazelcastInstanceProxy(File configFile, String entityBasePkg) throws FileNotFoundException
	{
	  this(new FileSystemXmlConfig(configFile), entityBasePkg);
	}
	
	/**
	 * Adds a migration listener
	 * @param listener
	 */
	public void addMigrationListener(MigrationListener listener)
	{
		hazelcast.getPartitionService().addMigrationListener(listener);
	}
	/**
	 * Adds a local map entry listener. Only a single listener for a given {@linkplain MapListener} instance would be registered.
	 * So subsequent invocation with the same instance would first remove any existing registration for that instance.
	 * @param map
	 * @param el
	 * @return 
	 */
	public String addLocalEntryListener(String map, MapListener el)
	{
		if(localEntryListeners.containsKey(el.toString()))
		{
			hazelcast.getMap(map).removeEntryListener(localEntryListeners.get(el.toString()));
		}
		String _id = hazelcast.getMap(map).addLocalEntryListener(el);
		
		localEntryListeners.put(el.toString(), _id);
		return _id;
	}
	/**
	 * Removes the specified entry listener. Returns silently if there is no such listener added before.
	 * @param id
	 * @param map
	 * @return
	 */
	public boolean removeEntryListener(String id, String map)
	{
	  return hazelcast.getMap(map).removeEntryListener(id);
	}
	/**
	 * Register a topic listener on the message channel topic. Note: The ordering is set the first time topic 
	 * is registered configuration is registered. Subsequent invocation of this method would simply keep on adding 
	 * listeners only, without modifying its configuration.
	 * @param channel
	 * @param orderingEnabled
	 * @return registration id
	 */
	public <E> String addMessageChannelHandler(MessageChannel<E> channel, boolean orderingEnabled)
	{
	  if(!hazelcast.getConfig().getTopicConfigs().containsKey(channel.topic()))
	  {
	    TopicConfig tc = new TopicConfig(channel.topic());
	    tc.setStatisticsEnabled(true);
	    tc.setGlobalOrderingEnabled(orderingEnabled);
	    hazelcast.getConfig().addTopicConfig(tc);
	  }
	  	  
	  ITopic<E> topic = hazelcast.getTopic(channel.topic());
	  String id = topic.addMessageListener(channel);
	  if(channel instanceof AbstractMessageChannel)
	  {
	    ((AbstractMessageChannel<E>) channel).setRegistrationId(id);
	  }
	  return id;
	}
	/**
	 * Removes topic listener with given registration id.
	 * @param topic
	 * @param regID
	 * @return 
	 */
	public <E> boolean removeTopicListener(String topic, String regID)
  {
    if(hazelcast.getConfig().getTopicConfigs().containsKey(topic))
    {
      ITopic<E> t = hazelcast.getTopic(topic);
      return t.removeMessageListener(regID);
    }
    return false;
    
  }
	
	private final Map<String, String> localEntryListeners = new HashMap<String, String>();
	/**
	 * Adds a life cycle listener
	 * @param listener
	 */
	public void addLifeCycleListener(LifecycleListener listener)
	{
		hazelcast.getLifecycleService().addLifecycleListener(listener);
	}
	//private PartitionService partitionService = null;
	/**
	 * Initialize with a member event listener, and broadcast message listener			
	 * @param clusterListener
	 * @param msgListener
	 */
	public void init(final MembershipListener clusterListener){
			
		hazelcast.getCluster().addMembershipListener(clusterListener);
								
	}
	
			
	public void stop(){
		if(isRunning()){
			hazelcast.getLifecycleService().shutdown();
			log.info("** Stopped instance ["+getInstanceId()+"] **");
		}
		
		
	}
	
	public void remove(Object key, String map)
	{
		if(isRunning()){
			hazelcast.getMap(map).removeAsync(key);
		}
	}
	public Object removeNow(Object key, String map)
  {
    if(isRunning()){
      return hazelcast.getMap(map).remove(key);
    }
    return map;
  }
	
	public <K, V> IMap<K, V> getMap(String map)
	{
		if(isRunning()){
			return hazelcast.getMap(map);
		}
		return null;
	}
	public Object put(Object key, Object value, String map) {
		if(isRunning()){
			return hazelcast.getMap(map).put(key, value);
		}
    return null;
		
	}
	public Object synchronizePut(Object key, Object value, String map) {
	  if(isRunning()){
      ILock lock = hazelcast.getLock(map);
      
      try
      {
        if(lock.tryLock(10, TimeUnit.SECONDS))
        {
          return hazelcast.getMap(map).put(key, value);
        }
        else
        {
          log.warn("[synchronizePut] Operation did not synchroznize in 10 secs");
          return hazelcast.getMap(map).put(key, value);
          
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug("", e);
      }
      finally
      {
        lock.unlock();
      }
      
    }
    return null;
    
  }
	public void set(Object key, Object value, String map) {
    if(isRunning()){
      hazelcast.getMap(map).set(key, value);
    }
    
  }
	public void synchronizeSet(Object key, Object value, String map) {
    if(isRunning()){
      ILock lock = hazelcast.getLock(map);
      
      try
      {
        if(lock.tryLock(10, TimeUnit.SECONDS))
        {
          hazelcast.getMap(map).set(key, value);
        }
        else
        {
          hazelcast.getMap(map).set(key, value);
          log.warn("[synchronizeSet] Operation did not synchroznize in 10 secs");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug("", e);
      }
      finally
      {
        lock.unlock();
      }
      
    }
    
  }
	public Object get(Object key, String map) {
		if(isRunning())
		{
			return hazelcast.getMap(map).get(key);
		}
		return null;
		
	}
	Set<Entry<Object, Object>> getAll(String map)
	{
		if(isRunning())
		{
			return hazelcast.getMap(map).entrySet();
		}
		return null;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public int getPartitionIDForKey(Object key)
	{
		return hazelcast.getPartitionService().getPartition(key).getPartitionId();
	}
	Set<Object> getLocalKeys(String map)
	{
		if(isRunning())
		{
			IMap<Object, Object> imap = hazelcast.getMap(map);
			return imap.localKeySet();
			
		}
		return null;
	}

	private ICondition syncLockCondition;
	/**
	 * 
	 * @return
	 */
	public ILock getClusterSyncLock() {
		return getLock("HazelcastInstanceProxy");
	}

	public ICondition getSyncLockCondition() {
		return syncLockCondition;
	}


  void publish(Object message, String topic) {
    hazelcast.getTopic(topic).publish(message);
    
  }


  public <T> boolean addToSet(String string, T model) {
    return hazelcast.getSet(string).add(model);
    
  }


  public ISet<?> getSet(String string) {
    return hazelcast.getSet(string);
  }


  public String getInstanceId() {
    return instanceId;
  }
  private String instanceId;
  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }
  /**
   * Cluster wide id generator
   * @param context
   * @return
   */
  public long getNextLong(String context)
  {
    return hazelcast.getIdGenerator(context).newId();
  }
  public Long getAndIncrementLong(String key) {
    return hazelcast.getAtomicLong(key).getAndIncrement();
  }
  public Long getLong(String key) {
    return hazelcast.getAtomicLong(key).get();
  }

  /**
   * 
   * @param prop
   * @param val
   */
  void setProperty(String prop, String val) {
    //hazelcast.performance.monitoring.enabled
    //hazelcast.operation.thread.count
    //hazelcast.io.thread.count
    //hazelcast.event.thread.count
    //hazelcast.client.event.thread.count
    
    getHazelcastConfig().setProperty(prop, val);
    
  }


  public ITopic<?> getTopic(String topic) {
    return hazelcast.getTopic(topic);
  }


  public ICountDownLatch getLatch(String latch) {
    return hazelcast.getCountDownLatch(latch);
    
  }


  public Member me() {
    return hazelcast.getCluster().getLocalMember();
  }
}
