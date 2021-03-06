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
package com.reactive.hzdfs;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.mapreduce.JobTracker;
import com.reactivetechnologies.hzdfs.DFSSException;
import com.reactivetechnologies.hzdfs.IDistributedFileSupport;
import com.reactivetechnologies.hzdfs.Server;
import com.reactivetechnologies.hzdfs.cluster.HazelcastOperations;
import com.reactivetechnologies.hzdfs.dto.DFSSResponse;
import com.reactivetechnologies.hzdfs.dto.DFSSTaskConfig;
import com.reactivetechnologies.hzdfs.utils.ResourceLoaderHelper;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = Server.class)
public class TestRunner {

  @Autowired
  private IDistributedFileSupport dfss;
  @Autowired
  private HazelcastOperations hzService;
  public TestRunner() {
    // TODO Auto-generated constructor stub
  }

  @Test
  public void testFailOnNonExistentFile()
  {
    Object ex = null;
    File f = new File("vp-client-notexists.log");
    try 
    {
      dfss.distribute(f, new DFSSTaskConfig());
      Assert.fail();
    } catch (IOException e) {
      ex = e;
    }
    Assert.assertNotNull(ex);
  }
  @Test
  public void testFailOnZIPFile()
  {
    Throwable c = null;
    
    try 
    {
      File f = ResourceLoaderHelper.loadFromFileOrClassPath("text_example.zip");
      Future<DFSSResponse> fut = dfss.distribute(f, new DFSSTaskConfig());
      fut.get();
      Assert.fail();
    } catch (IOException e) {
      Assert.fail("IOException - "+e);
    } catch (InterruptedException e) {
      Assert.fail("InterruptedException - "+e);
    } catch (ExecutionException e) {
      c = e.getCause();
      
    }
    Assert.assertNotNull(c);
    Assert.assertTrue(c instanceof DFSSException);
    Assert.assertEquals(DFSSException.ERR_IO_FILE, ((DFSSException) c).getErrorCode());
  }
  @Test
  public void testFailOnPPTFile()
  {
    Throwable c = null;
    
    try 
    {
      File f = ResourceLoaderHelper.loadFromFileOrClassPath("Presentation1.pptx");
      Future<DFSSResponse> fut = dfss.distribute(f, new DFSSTaskConfig());
      fut.get();
      Assert.fail();
    } catch (IOException e) {
      Assert.fail("IOException - "+e);
    } catch (InterruptedException e) {
      Assert.fail("InterruptedException - "+e);
    } catch (ExecutionException e) {
      c = e.getCause();
      
    }
    Assert.assertNotNull(c);
    Assert.assertTrue(c instanceof DFSSException);
    Assert.assertEquals(DFSSException.ERR_IO_FILE, ((DFSSException) c).getErrorCode());
  }
  
  @Test
  public void testDistributeSimpleFile()
  {
    
    
    try 
    {
      File f = ResourceLoaderHelper.loadFromFileOrClassPath("vp-client.log");
      Future<DFSSResponse> fut = dfss.distribute(f, new DFSSTaskConfig());
      resp = fut.get();
      Assert.assertEquals("Records do not match", 48, resp.getNoOfRecords());
      Assert.assertTrue("error list not empty", resp.getErrorNodes().isEmpty());
      return;
    } catch (IOException e) {
      Assert.fail("Job did not start - "+e);
    } catch (InterruptedException e) {
      Assert.fail("InterruptedException - "+e);
    } catch (ExecutionException e) {
      Assert.fail("File distribution error - "+e.getCause());
    }
    return;
  }
  
  //@Test
  public void testDistributeLargeFile()
  {
    
    File f = new File("C:\\Users\\esutdal\\Documents\\test\\cbp14co.txt");
    try 
    {
      Future<DFSSResponse> fut = dfss.distribute(f, new DFSSTaskConfig());
      DFSSResponse dfs = fut.get();
      Assert.assertEquals("Record size do not match", 2125362, dfs.getNoOfRecords());
      Assert.assertTrue("error list not empty", dfs.getErrorNodes().isEmpty());
      return;
    } catch (IOException e) {
      Assert.fail("Job did not start - "+e);
    } catch (InterruptedException e) {
      Assert.fail("InterruptedException - "+e);
    } catch (ExecutionException e) {
      Assert.fail("File distribution error - "+e.getCause());
    }
    return;
  }
  DFSSResponse resp = null;
  @After
  public void clean()
  {
    if(resp != null && resp.getRecordMap() != null)
    {
      ((DistributedObject) hzService.getMap(resp.getRecordMap())).destroy();
    }
  }
  @Test
  public void testDistributedSimpleFileRecords()
  {
    try 
    {
      File f = ResourceLoaderHelper.loadFromFileOrClassPath("AirPassengers.csv");
      Future<DFSSResponse> fut = dfss.distribute(f, new DFSSTaskConfig());
      resp = fut.get();
      Assert.assertNotNull(resp);
      Assert.assertEquals("Records do not match", 145, resp.getNoOfRecords());
      Assert.assertTrue("error list not empty", resp.getErrorNodes().isEmpty());
      
    } catch (IOException e) {
      Assert.fail("Job did not start - "+e);
    } catch (InterruptedException e) {
      Assert.fail("InterruptedException - "+e);
    } catch (ExecutionException e) {
      Assert.fail("File distribution error - "+e.getCause());
    }
    
    
    Assert.assertNotNull(resp.getRecordMap());
    Object record = hzService.get(48, resp.getRecordMap());
    Assert.assertEquals("\"47\",1952.83333333333,172", record);
    record = hzService.get(1, resp.getRecordMap());
    Assert.assertEquals("\"\",\"time\",\"AirPassengers\"", record);
    record = hzService.get(145, resp.getRecordMap());
    Assert.assertEquals("\"144\",1960.91666666667,432", record);
  }
  @Test
  public void testMapReduceSimpleFileRecords()
  {
    
    try 
    {
      File f = ResourceLoaderHelper.loadFromFileOrClassPath("AirPassengers.csv");
      Future<DFSSResponse> fut = dfss.distribute(f, new DFSSTaskConfig());
      resp = fut.get();
      Assert.assertNotNull(resp);
      Assert.assertEquals("Records do not match", 145, resp.getNoOfRecords());
      Assert.assertTrue("error list not empty", resp.getErrorNodes().isEmpty());
      
    } catch (IOException e) {
      Assert.fail("Job did not start - "+e);
    } catch (InterruptedException e) {
      Assert.fail("InterruptedException - "+e);
    } catch (ExecutionException e) {
      Assert.fail("File distribution error - "+e.getCause());
    }
    
    
    Assert.assertNotNull(resp.getRecordMap());
    Object record = hzService.get(48, resp.getRecordMap());
    Assert.assertEquals("\"47\",1952.83333333333,172", record);
    record = hzService.get(1, resp.getRecordMap());
    Assert.assertEquals("\"\",\"time\",\"AirPassengers\"", record);
    record = hzService.get(145, resp.getRecordMap());
    Assert.assertEquals("\"144\",1960.91666666667,432", record);
    
    JobTracker tracker = hzService.newJobTracker("default");
    JobRunner runner = new JobRunner(resp, tracker);
    runner.run();
    
    try 
    {
      Map<String, Integer> result = runner.future.get(60, TimeUnit.SECONDS);
      Assert.assertTrue(!result.isEmpty());
      Assert.assertTrue(result.containsKey("461"));
      Assert.assertEquals(2, (int)result.get("461"));
    } catch (InterruptedException e) {
      Assert.fail("InterruptedException");
    } catch (ExecutionException e) {
      Assert.fail("ExecutionException - "+e.getCause());
    } catch (TimeoutException e) {
      Assert.fail("TimeoutException");
    }
    
  }
}
