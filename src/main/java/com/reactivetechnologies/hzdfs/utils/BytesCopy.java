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

import java.lang.reflect.Field;
import java.util.Arrays;

import sun.misc.Unsafe;
@SuppressWarnings("restriction")
public class BytesCopy
{
	
	private static Unsafe UNSAFE;
	private static boolean isUnsafeAvailable;
	static
	{
		try {

			Field f = Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			UNSAFE = (Unsafe) f.get(null);
			isUnsafeAvailable = true;

		} catch (Exception e) {

			e.printStackTrace();
			isUnsafeAvailable = false;

		}
		System.out.println("Unsafe found? "+isUnsafeAvailable);
	}
	public static long putMemory(byte[] src, int off, int len)
	{
		long addr = UNSAFE.allocateMemory(len);
		UNSAFE.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET+off, null, addr, len);
		return addr;
	}
	public static long putMemory(byte[] src)
	{
		return putMemory(src, 0, src.length);
	}
	public static void getMemory(long addr, byte[] dest, int off, int len)
	{
		UNSAFE.copyMemory(null, addr, dest, Unsafe.ARRAY_BYTE_BASE_OFFSET+off, len);
	}
	public static void getMemory(long addr, byte[] dest)
	{
		getMemory(addr, dest, 0, dest.length);
	}
	public static void freeMemory(long addr)
	{
		UNSAFE.freeMemory(addr);
	}
	private static void arraycopy(byte[] src, int srcPos, byte[] dest, int destPos, int len) 
	{
		long addr = UNSAFE.allocateMemory(len);
		
		UNSAFE.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET+srcPos, null, addr, len);
		UNSAFE.copyMemory(null, addr, dest, Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
		UNSAFE.freeMemory(addr);
	}
	/**
	 * 
	 * @param src
	 * @param srcPos
	 * @param dest
	 * @param destPos
	 * @param len
	 */
	public static void copy(byte[] src, int srcPos, byte[] dest, int destPos, int len) 
	{
		if(isUnsafeAvailable)
			arraycopy(src, srcPos, dest, destPos, len);
		else
			System.arraycopy(src, srcPos, dest, destPos, len);
	}
	/**
	 * 
	 * @param original
	 * @param from
	 * @param to
	 * @return
	 */
	public static byte[] copyOfRange(byte[] original, int from, int to)
	{
		if(original == null)
			throw new IllegalArgumentException("null array");
		if(from < 0 || from > original.length)
			throw new ArrayIndexOutOfBoundsException();
		if(from > to)
			throw new IllegalArgumentException("from > to");
		
		if(isUnsafeAvailable)
		{
			int len = to-from;
			byte[] b = new byte[len];
			arraycopy(original, from, b, 0, len);
			return b;
		}
		else
			return Arrays.copyOfRange(original, from, to);
		
	}
}