/* ============================================================================
*
* FILE: DefaultMigrationListener.java
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
package com.reactivetechnologies.hzdfs.cluster;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;

class DefaultMigrationListener implements MigrationListener {
  private static final Logger log = LoggerFactory.getLogger(DefaultMigrationListener.class);
  /**
   * 
   */
  private final HazelcastClusterServiceBean hazelcastClusterServiceBean;

  /**
   * @param hazelcastClusterServiceBean
   */
  public DefaultMigrationListener(HazelcastClusterServiceBean hazelcastClusterServiceBean) {
    this.hazelcastClusterServiceBean = hazelcastClusterServiceBean;
  }

  @Override
  public void migrationStarted(MigrationEvent migrationevent) {
    this.hazelcastClusterServiceBean.migrationRunning.getAndSet(true);
    
  }

  @Override
  public void migrationFailed(MigrationEvent migrationevent) {
    this.hazelcastClusterServiceBean.migrationRunning.getAndSet(false);
    synchronized (this.hazelcastClusterServiceBean.migrationRunning) {
      this.hazelcastClusterServiceBean.migrationRunning.notifyAll();
    }
  }

  /**
   * 
   * @param migrationevent
   */
  private void invokeIMapCallbacks(MigrationEvent migrationevent)
  {
    for(Entry<String, MigratedEntryProcessor<?>> e : this.hazelcastClusterServiceBean.migrCallbacks.entrySet())
    {
      IMap<Serializable, Object> map = this.hazelcastClusterServiceBean.hzInstance.getMap(e.getKey());
      Set<Serializable> keys = new HashSet<>();
      for(Serializable key : map.localKeySet())
      {
        if(this.hazelcastClusterServiceBean.hzInstance.getPartitionIDForKey(key) == migrationevent.getPartitionId())
        {
          keys.add(key);
        }
      }
      if(!keys.isEmpty())
      {
        this.hazelcastClusterServiceBean.migratingPartitions.add(map.getName());
        try {
          map.executeOnKeys(keys, e.getValue());
        } finally {
          this.hazelcastClusterServiceBean.migratingPartitions.remove(map.getName());
          synchronized (this.hazelcastClusterServiceBean.migratingPartitions) {
            
          }
        }
      }
    }
  }

  @Override
  public void migrationCompleted(MigrationEvent migrationevent) 
  {
    this.hazelcastClusterServiceBean.migrationRunning.getAndSet(false);
    synchronized (this.hazelcastClusterServiceBean.migrationRunning) {
      this.hazelcastClusterServiceBean.migrationRunning.notifyAll();
    }
    if(migrationevent.getNewOwner().localMember())
    {
      log.info("Invoking MigratedEntryProcessors. Migration detected of partition => "+migrationevent.getPartitionId());
      invokeIMapCallbacks(migrationevent);
      invokeNonPartitionedObjectCallbacks(migrationevent);
    }
    
  }

  private void invokeNonPartitionedObjectCallbacks(MigrationEvent migrationevent) {
    // TODO Auto-generated method stub
    
  }
}