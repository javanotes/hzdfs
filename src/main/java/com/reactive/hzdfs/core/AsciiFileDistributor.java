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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.reactive.hzdfs.datagrid.HazelcastClusterServiceBean;
import com.reactive.hzdfs.datagrid.handlers.AbstractLocalMapEntryPutListener;
import com.reactive.hzdfs.files.FileShareResponse;
import com.reactive.hzdfs.files.FileSharingAgent;
import com.reactive.hzdfs.utils.ByteArrayBuilder;
/**
 * Infrastructure class for a simple distributed file system over Hazelcast.
 */
public class AsciiFileDistributor
    extends AbstractLocalMapEntryPutListener<AsciiFileChunk>
    implements FileSharingAgent {

  private static final Logger log = LoggerFactory.getLogger(AsciiFileDistributor.class);
  /**
   * New distributor instance.
   * @param hzService
   */
  public AsciiFileDistributor(HazelcastClusterServiceBean hzService) {
    super(hzService);
    
  }

  @PostConstruct
  private void init()
  {
    log.info("-------------------------------- AsciiFileDistributor.init() -----------------------------------");
    try {
      distribute(new File("C:\\Users\\esutdal\\Documents\\vp-client.log"));
    } catch (IOException e) {
      log.error("File distribution failed.", e);
    }
    log.info("-------------------------------- AsciiFileDistributor.end() -----------------------------------");
  }
  @Override
  public String keyspace() {
    return "HAZELCAST-DFS";
  }

  
  /**
   * 
   */
  private class RecordBuilder
  {
    private void readAsUTF(byte[] bytes, String recId)
    {
      String record = new String(bytes, StandardCharsets.UTF_8);
      log.info("["+recId+"] RECEIVED=> "+record);
      builders.remove(recId);
      finalize();
    }
    private boolean isEOF(AsciiFileChunk chunk)
    {
      return chunk.getChunk().length == 1 && chunk.getChunk()[0] == -1;
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
    private void build(String recId)
    {
      for(AsciiFileChunk c : orderdChunks)
      {
        builder.append(c.getChunk());
      }
      readAsUTF(builder.toArray(), recId);
      
    }
    private void handleNext(AsciiFileChunk chunk, String recId)
    {
      if(isEOF(chunk))
      {
        build(recId);
        return;
      }
      if(chunk.getSplitType() == AsciiFileChunk.SPLIT_TYPE_FULL)
      {
        //is a complete record
        readAsUTF(chunk.getChunk(), recId);
      }
      else if(chunk.getSplitType() == AsciiFileChunk.SPLIT_TYPE_POST)
      {
        //append and complete record
        orderdChunks.add(chunk);
        build(recId);
        
      }
      else
      {
        //append
        orderdChunks.add(chunk);
      }
    }
  }
  private final Map<String, RecordBuilder> builders = new HashMap<>();
  /**
   * We could have simply used the Hazelcast key. But to be double sure, simply appending 
   * the record index to it.
   * @param event
   * @return
   */
  private static String makeEntryKey(EntryEvent<Serializable, AsciiFileChunk> event)
  {
    return event.getKey() + "$" + event.getValue().getRecordIndex();
  }
  @Override
  public void entryAdded(EntryEvent<Serializable, AsciiFileChunk> event) {
    log.debug("[onEntryAdded] "+event.getValue());
    synchronized (builders) {
      String rId = makeEntryKey(event);
      if(!builders.containsKey(rId))
      {
        builders.put(rId, new RecordBuilder());
      }
      
      builders.get(rId).handleNext(event.getValue(), rId);
    }
  }

  @Override
  public void entryUpdated(EntryEvent<Serializable, AsciiFileChunk> event) {
    entryAdded(event);

  }

  @Override
  public Future<FileShareResponse> distribute(File f)
      throws IOException {
    try(AsciiChunkReader reader = new AsciiChunkReader(f, 8192))
    {
      log.info("Submitting file chunks..");
      AsciiFileChunk chunk = null;
      long recordIdHash = -1;
      int recordIdx = -1;
      while((chunk = reader.readNext()) != null)
      {
        recordIdHash = chunk.generateHashCode();
        recordIdx = chunk.getRecordIndex();
        putEntry(recordIdHash, chunk);
      }
      chunk = new AsciiFileChunk();
      chunk.recordIndex = recordIdx;
      chunk.setChunk(new byte[]{-1});
      putEntry(recordIdHash, chunk);
    }
    return null;
  }

}
