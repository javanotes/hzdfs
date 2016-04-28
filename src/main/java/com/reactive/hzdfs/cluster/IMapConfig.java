/* ============================================================================
*
* FILE: HzMapConfig.java
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
package com.reactive.hzdfs.cluster;

import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.Persistent;
import org.springframework.data.keyvalue.annotation.KeySpace;
@Persistent
@Retention(RetentionPolicy.RUNTIME)
@Target(value = { TYPE })

/**
 * Quick settings for a Map config. NOTE: This will override any setting made in the hazelcast config xml.
 */
public @interface IMapConfig {

  /**
   * 
   * @return IMap name
   */
  @KeySpace
  String name();
  /**
   * BINARY, OBJECT, NATIVE
   * @return
   */
  String inMemoryFormat() default "BINARY";
  /**
   * 
   * @return
   */
  int backupCount() default 1;
  /**
   * 
   * @return
   */
  int asyncBackupCount() default 0;
  /**
   * 
   * @return
   */
  int ttlSeconds()default 0;
  /**
   * 
   * @return
   */
  int idleSeconds()default 0;
  /**
   * LRU, LFU, NONE
   * @return
   */
  String evictPolicy() default "NONE";
  /**
   * 
   * @return
   */
  int evictPercentage() default 25;
  /**
   * 
   * @return
   */
  int maxSize() default 0;
  /**
   * 
    Decide maximum entry count according to
    PER_NODE,PER_PARTITION,USED_HEAP_PERCENTAGE,USED_HEAP_SIZE,FREE_HEAP_PERCENTAGE,FREE_HEAP_SIZE
        
   */
  String maxSizePolicy() default "PER_NODE";
  /**
   * 
   * @return
   */
  long evictCheckMillis() default 100;
  /**
   * 
   * @return
   */
  boolean statisticsOn() default true;
}
