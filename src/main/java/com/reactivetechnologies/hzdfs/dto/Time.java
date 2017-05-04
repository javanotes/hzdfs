/* ============================================================================
*
* FILE: Time.java
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
package com.reactivetechnologies.hzdfs.dto;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class Time implements DataSerializable{

  @Override
  public String toString() {
    return duration + " " + unit;
  }
  public long getDuration() {
    return duration;
  }
  public TimeUnit getUnit() {
    return unit;
  }
  private long duration;
  private TimeUnit unit = TimeUnit.MILLISECONDS;
  /**
   * 
   * @param duration
   * @param unit
   */
  public Time(long duration, TimeUnit unit) {
    super();
    this.duration = duration;
    this.unit = unit;
  }
  Time() {
    
  }
  @Override
  public void writeData(ObjectDataOutput out) throws IOException {
    out.writeLong(duration);
    out.writeUTF(unit.name());
  }
  @Override
  public void readData(ObjectDataInput in) throws IOException {
    duration = in.readLong();
    unit = TimeUnit.valueOf(in.readUTF());
  }
  
}
