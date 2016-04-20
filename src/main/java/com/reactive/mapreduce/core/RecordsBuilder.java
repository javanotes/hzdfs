/* ============================================================================
*
* FILE: RecordsBuilder.java
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
package com.reactive.mapreduce.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactive.mapreduce.datagrid.HazelcastClusterServiceBean;

class RecordsBuilder{

  private static final Logger log = LoggerFactory.getLogger(RecordsBuilder.class);
  private final String name;
  private final HazelcastClusterServiceBean hzService;
  /**
   * 
   * @param name
   */
  public RecordsBuilder(String name, HazelcastClusterServiceBean hzService)
  {
    this.name = name;
    this.hzService = hzService;
  }
  /**
   * We could have simply used the Hazelcast key. But to be double sure, simply appending 
   * the record index to it.
   * @param event
   * @return
   */
  private static String makeEntryKey(Serializable key, AsciiFileChunk event)
  {
    return key + "$" + event.getRecordIndex();
  }
  /**
   * Handle next chunk.
   * @param chunk
   * @return 
   */
  boolean handleNextChunk(AsciiFileChunk chunk, Serializable key)
  {
    String rId = makeEntryKey(key, chunk);
    synchronized (builders) 
    {
      if(!builders.containsKey(rId))
      {
        builders.put(rId, new RecordBuilder(this, chunk.getRecordIndex(), rId));
      }
      
      return builders.get(rId).handleNextChunk(chunk);
    }    
  }

  private final Map<String, RecordBuilder> builders = new HashMap<>();
  private String map;

  public String getMap() {
    return map;
  }
  public void setMap(String map) {
    this.map = map;
  }
  public void remove(String key) {
    builders.remove(key);
    
  }
  public void readAsUTF(String record, int index) {
    log.info("{"+name+" ["+index+"]} RECEIVED=> "+record);
    hzService.set(index, record, map);
  }
  

}
