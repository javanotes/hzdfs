/* ============================================================================
*
* FILE: RecordBuilder.java
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

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactive.mapreduce.utils.ByteArrayBuilder;

/**
 * 
 */
class RecordBuilder
{
  private static final Logger log = LoggerFactory.getLogger(RecordBuilder.class);
  private final RecordsBuilder parent;
  /**
   * 
   * @param builders
   * @param index
   * @param key
   */
  public RecordBuilder(RecordsBuilder builders, int index, String key) {
    super();
    this.parent = builders;
    this.index = index;
    this.key = key;
  }
  
  private final int index;
  private final String key;
  
  private void readAsUTF()
  {
    String record = new String(builder.toArray(), StandardCharsets.UTF_8);
    try {
      parent.remove(key);
      finalize();
    } catch (Exception e) {
      log.debug("", e);
    }
    parent.readAsUTF(record, index);
    
  }
  
  private ByteArrayBuilder builder = new ByteArrayBuilder();
  private Set<AsciiFileChunk> orderdChunks = new TreeSet<>(new Comparator<AsciiFileChunk>() {

    @Override
    public int compare(AsciiFileChunk o1, AsciiFileChunk o2) {
      return Integer.compare(o1.getOffset(), o2.getOffset());
    }
  });
  @Override
  public void finalize()
  {
    orderdChunks.clear();
    orderdChunks = null;
    builder.free(false);
    builder = null;
    
  }
  private void build()
  {
    for(AsciiFileChunk c : orderdChunks)
    {
      builder.append(c.getChunk());
    }
          
  }
  private void append(AsciiFileChunk chunk)
  {
    orderdChunks.add(chunk);
  }
  private void emitRecord()
  {
    build();
    readAsUTF();
  }
  /**
   * Handle next chunk.
   * @param chunk
   * @return true if a record was emitted.
   */
  boolean handleNextChunk(AsciiFileChunk chunk)
  {
    if(chunk.isEOF())
    {
      emitRecord();
      return true;
    }
    
    append(chunk);
    
    if(chunk.getSplitType() == AsciiFileChunk.SPLIT_TYPE_FULL || chunk.getSplitType() == AsciiFileChunk.SPLIT_TYPE_POST)
    {
      //is a complete record
      emitRecord();
      return true;
    }
    return false;
    
    
  }
}