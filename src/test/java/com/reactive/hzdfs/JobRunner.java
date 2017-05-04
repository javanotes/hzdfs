/* ============================================================================
*
* FILE: JobRunner.java
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

import java.io.Serializable;
import java.util.Map;

import org.springframework.util.StringUtils;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.impl.MapKeyValueSource;
import com.reactivetechnologies.hzdfs.dto.DFSSResponse;

class JobRunner implements Serializable,Runnable
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  
  //map of recordIdx to recordString
  Job<Integer, String> job;
  JobCompletableFuture<Map<String, Integer>> future;
  
  public JobRunner(DFSSResponse resp, JobTracker tracker)
  {
    job = tracker.newJob(new MapKeyValueSource<Integer, String>(resp.getRecordMap()));
  }

  @Override
  public void run() {
    
    future = job
    .mapper(new Mapper<Integer, String, String, Integer>() {

      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public void map(Integer key, String value, Context<String, Integer> context) {
        String[] splits = StringUtils.commaDelimitedListToStringArray(value);
        context.emit(splits[splits.length-1], 1);
      }
    })
    .reducer(new ReducerFactory<String, Integer, Integer>() {

      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Reducer<Integer, Integer> newReducer(String key) {
        
        return new Reducer<Integer, Integer>() {
          int n = 0;
          @Override
          public void reduce(Integer value) {
            n += value;
            
          }

          @Override
          public Integer finalizeReduce() {
            return n;
          }
        };
      }
    })
    .submit();
    
  }
}