/* ============================================================================
*
* FILE: DFSSException.java
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
package com.reactivetechnologies.hzdfs;
/**
 * Any runtime exception during distribution execution.
 */
public class DFSSException extends Exception {

  public static final String ERR_IO_FILE = "001";
  public static final String ERR_IO_OPERATION = "002";
  public static final String ERR_IO_TIMEOUT = "003";
  public static final String ERR_INCOMPLETE_REC = "020";
  public static final String ERR_RUNTIME = "000";
  
  private String errorCode = ERR_RUNTIME;
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public DFSSException() {
    super();
  }

  public DFSSException(String s) {
    super(s);
  }

  public DFSSException(Throwable cause) {
    super(cause);
  }

  public DFSSException(String message, Throwable cause) {
    super(message, cause);
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

}
