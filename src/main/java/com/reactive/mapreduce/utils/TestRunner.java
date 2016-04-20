/* ============================================================================
*
* FILE: TestRunner.java
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
package com.reactive.mapreduce.utils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.reactive.mapreduce.core.DFSSResponse;
import com.reactive.mapreduce.core.DistributedFileSystemService;
@Component
public class TestRunner implements CommandLineRunner{

  private static final Logger log = LoggerFactory.getLogger(TestRunner.class);
  public TestRunner() {
    // TODO Auto-generated constructor stub
  }

  @Autowired
  private DistributedFileSystemService dfss;
  @Override
  public void run(String... args)  {
    
    log.info("-- Starting test run --" );
    File f = new File("C:\\Users\\esutdal\\Documents\\test\\vp-client.log");
    try {
      Future<DFSSResponse> fut = dfss.distribute(f);
      DFSSResponse dfs = fut.get();
      log.info("Response:- "+dfs.getSessionId()+"\t"+dfs+" map=> "+dfs.getRecordMap());
    } catch (IOException e) {
      log.error("Job did not start", e);
    } catch (InterruptedException e) {
      log.info("", e);
    } catch (ExecutionException e) {
      log.error("File distribution error", e.getCause());
    }
  }

}
