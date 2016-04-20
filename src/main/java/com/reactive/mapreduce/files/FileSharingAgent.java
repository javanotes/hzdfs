/* ============================================================================
*
* FILE: FileSharingAgent.java
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
package com.reactive.mapreduce.files;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import com.reactive.mapreduce.OperationsException;
import com.reactive.mapreduce.files.dist.AbstractFileSharingAgent;
/**
 * A worker for file sharing. Ideally a single worker would be needed per node.
 * @see AbstractFileSharingAgent
 */
public interface FileSharingAgent {

  /**
   * Shares a file across the cluster. File sharing is an exclusive process.
   * So at a time only 1 sharing can be processed.
   * @param f
   * @return a Future for the file share response.
   * @throws IOException
   * @throws OperationsException recoverable exception, can be tried later probably
   */
  Future<FileShareResponse> distribute(File f) throws IOException, OperationsException;

}