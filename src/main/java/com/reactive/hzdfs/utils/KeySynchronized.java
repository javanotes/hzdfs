/* ============================================================================
*
* FILE: KeySynchronized.java
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
package com.reactive.hzdfs.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
/**
 * A synchronization utility using atomic operations. Locks on a named identity.
 */
public class KeySynchronized {

  private final ConcurrentMap<String, Object> map = new ConcurrentHashMap<String, Object>();
  /**
   * Try synchronize on a given key. Return true if success.
   * @param key
   * @return true if lock acquired
   * @throws InterruptedException
   */
  public boolean tryLock(String key)
  {
    if(map.putIfAbsent(key, new Object()) == null)
      return true;
    return map.replace(key, nullValue, new Object());
    
  }
  /**
   * Try synchronize on a given key. Return true if success.
   * @param key
   * @param duration
   * @param unit
   * @return
   */
  public boolean tryLock(String key, long duration, TimeUnit unit)
  {
    boolean locked = tryLock(key);
    if(!locked)
    {
      try {
        awaitUnlock(key, duration, unit);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      
    }
    locked = tryLock(key);
    return locked;
    
  }
  /**
   * Synchronize on a given key using wait spin.
   * @param key
   */
  public void lock(String key)
  {
    boolean locked = tryLock(key);
    while(!locked)
    {
      try {
        awaitUnlock(key, 1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      locked = tryLock(key);
    }
  }
   
  /**
   * Awaits the key to be unlocked. Returns true on success.
   * @param key
   * @param duration
   * @param unit
   * @return if unlocked
   * @throws InterruptedException
   */
  private void awaitUnlock(String key, long duration, TimeUnit unit) throws InterruptedException
  {
    Object o = map.get(key);
    if(o != null && o != nullValue)
    {
      synchronized (o) {
        Object oo = map.get(key);
        if(o == oo)
        {
          o.wait(unit.toMillis(duration));
        }
        else
        {
          //illegal monitor state?
          //some other thread has locked the same key meanwhile, or it has been unlocked
          //so no reason to wait on this instance anymore
        }
        
      }
    }
    
  }
  /**
   * Check if the key is unlocked. This check is not atomic however.
   * @param key
   * @return
   */
  public boolean isUnlocked(String key)
  {
    //either the map does not contain the key, or key maps to nullValue
    return map.putIfAbsent(key, nullValue) == null || map.replace(key, nullValue, nullValue);
  }
  private final Object nullValue = new Object();
  /**
   * 
   * @param key
   */
  public void unlock(String key)
  {
    Object o = map.replace(key, nullValue);
    if(o != null)
    {
      synchronized (o) {
        o.notifyAll();
      }
    }
  }
}
