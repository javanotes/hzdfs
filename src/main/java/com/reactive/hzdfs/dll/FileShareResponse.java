/* ============================================================================
*
* FILE: FileShareResponse.java
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
package com.reactive.hzdfs.dll;

import java.util.HashSet;
import java.util.Set;

public enum FileShareResponse {

  FINISH,TIMEOUT,ERROR,UNKNOWN;
  private int errorCount;
  private Set<String> errorNodes = new HashSet<>();
  public int getErrorCount() {
    return errorCount;
  }

  public void setErrorCount(int errorCount) {
    this.errorCount = errorCount;
  }

  public Set<String> getErrorNodes() {
    return errorNodes;
  }

  public void setErrorNodes(Set<String> errorNodes) {
    this.errorNodes = errorNodes;
  }
}
