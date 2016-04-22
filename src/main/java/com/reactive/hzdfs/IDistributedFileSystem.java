/* ============================================================================
*
* FILE: IDistributedFileSystem.java
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
package com.reactive.hzdfs;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import com.hazelcast.core.IMap;
import com.reactive.hzdfs.core.DFSSResponse;
/**
 * A facade for performing distributed (text) file operations over a Hazelcast
 * cluster. By a distributed file system, we model a {@linkplain IMap distributed map} of file records.
 * Each record is a simply a UTF8 text of each line of the file. The data distribution is, however, optimized 
 * by memory mapped reading of byte chunks instead of a plain {@linkplain BufferedReader#readLine()} iteration.
 */
public interface IDistributedFileSystem {

  /**
  * Distribute a local file on to Hazelcast cluster.
  * @param request
  * @return
  * @throws IOException if the cluster could not be prepared, or file is invalid
  */
  Future<DFSSResponse> distribute(File sourceFile) throws IOException;

}