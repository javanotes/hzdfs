/**
 * Copyright 2017 esutdal

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.reactivetechnologies.hzdfs.cluster;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.Member;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.mapreduce.JobTracker;

public interface HazelcastOperations {

	/**
	   * Check if the distributed object is destroyed.
	   * @param objectName
	   * @return
	   */
	boolean isDistributedObjectDestroyed(String objectName, Class<?> type);

	/**
	 * Set IMap store using a spring bean. This is for programmatic configuration of {@linkplain MapStore}.
	 * @param backingStore
	 */
	void setMapStoreImplementation(String map, MapStore<? extends Serializable, ? extends Serializable> backingStore);

	/**
	 * Set IMap configuration programmatically. The provided class must be annotated with {@linkplain IMapConfig}.
	 * @param annotatedClass
	 */
	void setMapConfiguration(Class<?> annotatedClass);

	/**
	 * Set IMap configuration programmatically. This method is to reuse the same configuration for different IMaps.
	 * @param imapConfig
	 * @param imap
	 */
	void setMapConfiguration(IMapConfig imapConfig, String imap);

	/**
	 * Asynchronous removal of a key from IMap.
	 * @param key
	 * @param map
	 */
	void remove(Object key, String map);

	/**
	   * Gets the Hazelcast IMap instance.
	   * @param map
	   * @return
	   */
	<K, V> Map<K, V> getMap(String map);

	/**
	   * Map put operation.
	   * @param key
	   * @param value
	   * @param map
	   * @return
	   */
	Object put(Object key, Object value, String map);

	/**
	   * 
	   * @param key
	   * @param value
	   * @param map
	   */
	void set(Object key, Object value, String map);

	/**
	   * Get the value corresponding to the key from an {@linkplain IMap}.
	   * @param key
	   * @param map
	   * @return
	   */
	Object get(Object key, String map);

	/**
	 * Is migration ongoing
	 * @return
	 */
	boolean isMigrationRunning();

	/**
	 * Register a new partition migration listener on IMap entry migration event. The listener callback will be invoked on each entry being migrated.
	 * @param callback
	 * @return if no migration is running and the listener was registered
	 */
	boolean addPartitionMigrationCallback(MigratedEntryProcessor<?> callback);

	/**
	 * De-register a partition migration listener if no migration is executing.
	 * @param callback
	 * @return
	 */
	boolean removeMigrationCallback(MigratedEntryProcessor<?> callback);

	/**
	 * Register a local add/update entry listener on a given {@linkplain IMap} by name. Only a single listener for a given {@linkplain MapListener} instance would be 
	 * registered. So subsequent invocation with the same instance would first remove any existing registration for that instance.
	 * @param keyspace map name
	 * @param listener callback 
	 * @return 
	 * @throws IllegalAccessException 
	 */
	String addLocalEntryListener(Serializable keyspace, MapListener listener);

	/**
	 * Removes the specified map entry listener. Returns silently if there is no such listener added before.
	 * @param id
	 * @param listener
	 * @return
	 */
	<T> boolean removeEntryListener(String id, LocalMapEntryPutListener<T> listener);

	/**
		 * Register a local add/update entry listener on a given {@linkplain IMap} by name. Only a single listener for a given {@linkplain MapListener} instance would be 
	   * registered. So subsequent invocation with the same instance would first remove any existing registration for that instance.
		 * @param addUpdateListener listener with map name
		 * @return 
		 */
	<V> String addLocalEntryListener(LocalMapEntryPutListener<V> addUpdateListener);

	/**
	 * Set a Hazelcast configuration property.
	 * @param prop
	 * @param val
	 */
	void setProperty(String prop, String val);

	/**
	 * Try and join to default cluster, and start Hazelcast service.
	 * @param instanceId
	 */
	void join(String instanceId);

	/**
	 * Try and join cluster with given name, and start Hazelcast service.
	 * @param instanceId
	 * @param group
	 */
	void join(String instanceId, String group);

	/**
	 * True if the listeners have been started.
	 * @return
	 */
	boolean isStarted();

	/**
	 * Start lifecycle and partition listeners.
	 */
	void startInstanceListeners();

	/**
	 * No of members in the cluster at this point.
	 * @return
	 */
	int size();

	/**
	 * 
	 * @param retry
	 */
	void stopService();

	/**
		 * Synchronous removal of a key from IMap.
		 * @param key
		 * @param map
		 * @return
		 */
	Object removeNow(Serializable key, String map);

	/**
	   * Get a distributed cluster wide lock.
	   * @param name
	   * @return
	   */
	Lock getClusterLock(String name);

	/**
	   * Register a group membership event callback.
	   * @param observer
	   */
	void addInstanceListenerObserver(MembershipEventObserver observer);

	/**
		 * Adds a message channel with no message ordering.
		 * @param channel
		 * @return regID
		 */
	<E> String addMessageChannel(MessageChannel<E> channel);

	/**
	   * Adds a message channel.
	   * @param channel
	   * @param orderingEnabled whether to order messages
	   * @return regID
	   */
	<E> String addMessageChannel(MessageChannel<E> channel, boolean orderingEnabled);

	/**
	   * Removes the channel topic listener.
	   * @param channel
	   */
	<E> void removeMessageChannel(AbstractMessageChannel<E> channel);

	/**
	   * Removes a topic listener.
	   * @param topic
	   * @param regID
	   */
	<E> void removeMessageChannel(String topic, String regID);

	/**
	   * Publish a message to a {@linkplain ITopic}
	   * @param message
	   * @param topic
	   */
	void publish(Object message, String topic);

	/**
		 * Acquire a cluster wide lock.
		 * @param unit
		 * @param time
		 * @return
		 * @throws InterruptedException
		 */
	boolean acquireLock(TimeUnit unit, long time) throws InterruptedException;

	/**
	   * Release a cluster wide lock.
	   * @param forced
	   */
	void releaseLock(boolean forced);

	/**
	   * Checks if an {@linkplain IMap} contains the given key.
	   * @param id
	   * @param imap
	   * @return
	   */
	boolean contains(Serializable id, String imap);

	/**
	   * Get topic instance with the given name.
	   * @param topic
	   * @return
	   */
	ITopic<?> getTopic(String topic);

	/**
	   * Get a cluster wide count down latch with the given name.
	   * @param latch
	   * @return
	   */
	ICountDownLatch getClusterLatch(String latch);

	/**
	 * 
	 * @param key
	 * @return
	 */
	IAtomicLong getAtomicLong(String key);

	/**
	 * 
	 * @param set
	 * @return
	 */
	ISet<?> getSet(String set);

	/**
	 * The running instance as {@linkplain Member}
	 * @return
	 */
	Member thisMember();

	/**
	 * Hazelcast mapreduce job tracker.
	 * @param string
	 * @return
	 */
	JobTracker newJobTracker(String string);

}