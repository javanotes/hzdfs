/* ============================================================================
*
* FILE: FileChunkSender.java
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
package com.reactive.hzdfs.io;

import com.hazelcast.core.Message;
import com.reactive.hzdfs.Configurator;
import com.reactive.hzdfs.cluster.HazelcastClusterServiceBean;
import com.reactive.hzdfs.cluster.intf.AbstractMessageChannel;
/**
 * A class to publish {@linkplain FileChunk}.
 */
public class FileChunkSender extends AbstractMessageChannel<FileChunk> {
  /**
   * 
   * @param hzService
   */
  public FileChunkSender(HazelcastClusterServiceBean hzService) {
    super(hzService, true);
  }
  @Override
  public String topic() {
    return Configurator.PIPED_TOPIC_FILE;
  }

  @Override
  public void onMessage(Message<FileChunk> message) {
    // ignored
    
  }
  
}
