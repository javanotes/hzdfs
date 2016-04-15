/* ============================================================================
*
* FILE: FileKeyValueSource.java
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

import java.io.IOException;
import java.util.Map.Entry;

import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.spi.NodeEngine;

public class FileKeyValueSource<K,V> extends KeyValueSource<K, V> {

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

  private NodeEngine nodeEngine;
  @Override
  public boolean open(NodeEngine nodeEngine) {
    this.nodeEngine = nodeEngine;
    return false;
  }

  @Override
  public boolean hasNext() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public K key() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Entry<K, V> element() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean reset() {
    // TODO Auto-generated method stub
    return false;
  }

}
