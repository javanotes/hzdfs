/**
 * Copyright 2017 esutdal

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.reactivetechnologies.hzdfs.utils;

import java.io.Closeable;

import org.springframework.util.Assert;

public class ObjectPointer implements Closeable{

	private final long address;
	private final int length;
	public int getLength() {
		return length;
	}

	private boolean isClosed = false;
	public ObjectPointer(byte[] b) {
		address = BytesCopy.putMemory(b);
		length = b.length;
	}
	public byte[] get()
	{
		Assert.isTrue(!isClosed, "Memory deallocated!");
		byte[] b = new byte[length];
		BytesCopy.getMemory(address, b);
		return b;
	}
	
	@Override
	public void close() {
		BytesCopy.freeMemory(address);
		isClosed = true;
	}

}
