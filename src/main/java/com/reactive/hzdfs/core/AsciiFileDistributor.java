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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.reactive.hzdfs.datagrid.HazelcastClusterServiceBean;
import com.reactive.hzdfs.datagrid.intf.AbstractLocalMapEntryPutListener;
import com.reactive.hzdfs.files.FileShareResponse;
import com.reactive.hzdfs.files.FileSharingAgent;
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
  
  private ConcurrentMap<String, RecordsBuilder> builders = new ConcurrentHashMap<>();
  
  static String makeFileKey(AsciiFileChunk chunk)
  {
    return chunk.getFileName() + "." + chunk.getCreationTime();
  }
  @Override
  public void entryAdded(EntryEvent<Serializable, AsciiFileChunk> event) {
    log.debug("[onEntryAdded] "+event.getValue());
    Serializable key = event.getKey();
    AsciiFileChunk chunk = event.getValue();
    
    String fileKey = makeFileKey(chunk);
    if(!builders.containsKey(fileKey))
    {
      builders.putIfAbsent(fileKey, new RecordsBuilder(fileKey));
    }
    builders.get(fileKey).handleNext(chunk, key);
    
    if(chunk.isEOF())
      builders.remove(fileKey);
  }

  @Override
  public void entryUpdated(EntryEvent<Serializable, AsciiFileChunk> event) {
    entryAdded(event);

  }

  private FileDistributionCounter putChunk(AsciiFileChunk chunk)
  {
    FileDistributionCounter ctr = new FileDistributionCounter();
    ctr.setRecordIdHash(chunk.generateHashCode());
    ctr.setRecordIdx(chunk.getRecordIndex());
    ctr.setFileName(chunk.getFileName());
    ctr.setCreatTime(chunk.getCreationTime());
    
    putEntry(ctr.getRecordIdHash(), chunk);
    
    return ctr;
  }
  private AsciiFileChunk putEOFChunk(FileDistributionCounter counter)
  {
    AsciiFileChunk chunk = new AsciiFileChunk();
    chunk.recordIndex = counter.getRecordIdx();
    chunk.setChunk(new byte[]{-1});
    chunk.setFileName(counter.getFileName());
    chunk.setCreationTime(counter.getCreatTime());
    
    putEntry(counter.getRecordIdHash(), chunk);
    
    return chunk;
  }
  @Override
  public Future<FileShareResponse> distribute(File f)
      throws IOException {
    try(AsciiChunkReader reader = new AsciiChunkReader(f, 8192))
    {
      log.info("Submitting file chunks..");
      AsciiFileChunk chunk = null;
      
      FileDistributionCounter counter = new FileDistributionCounter();
          
      chunk = reader.readNext();
      
      if(chunk != null)
      {
        try 
        {
          counter = putChunk(chunk);
          
          while((chunk = reader.readNext()) != null)
          {
            counter = putChunk(chunk);
          }
          
          chunk = putEOFChunk(counter);
        } 
        finally {
          //
        }
                
      }
      
      
    }
    return null;
  }

}
