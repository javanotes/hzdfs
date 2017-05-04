/* ============================================================================
*
* FILE: FileChunkReader.java
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
package com.reactivetechnologies.hzdfs.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactivetechnologies.hzdfs.io.EOFChunk;
import com.reactivetechnologies.hzdfs.io.FileChunk;
import com.reactivetechnologies.hzdfs.io.PartitionedMemoryMappedChunkHandler;

/**
 * 
 */
public class ConcurrentFileChunkReader extends PartitionedMemoryMappedChunkHandler {

	private static final Logger log = LoggerFactory.getLogger(ConcurrentFileChunkReader.class);
	private ExecutorService threadPool;
	@Override
	public void close() throws IOException {
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		super.close();
	}
	
	/**
	 * 
	 * @author esutdal
	 *
	 */
	private class FileChunkReaderTask implements Callable<FileChunk>
	{
		@Override
		public FileChunk call() throws Exception {
			return ConcurrentFileChunkReader.super.readNext();
		}
		
	}
	
	private class FileChunkReaderTask2 implements Runnable
	{
		private final AtomicInteger ai;
		private final BlockingQueue<FileChunk> buffer;
		/**
		 * 
		 * @param ai
		 * @param chunks2
		 */
		public FileChunkReaderTask2(AtomicInteger ai, BlockingQueue<FileChunk> chunks2) {
			this.ai = ai;
			this.buffer = chunks2;
		}

		@Override
		public void run() {
			FileChunk chunk = null;
			boolean intr = false;
			try 
			{
				while ((chunk = ConcurrentFileChunkReader.super.readNext()) != null) {
					try {
						buffer.put(chunk);
					} catch (InterruptedException e) {
						intr = true;
					}
				}
				
			} catch (IOException e) {
				log.error("Error while reading file chunks", e);
			}
			
			if(ai.decrementAndGet() == 0)
				buffer.add(new EOFChunk());
			
			if(intr)
				Thread.currentThread().interrupt();
		}
		
	}
	
	private ConcurrentLinkedQueue<Future<FileChunk>> chunks = new ConcurrentLinkedQueue<>();
	private FileChunk pollUninterruptibly() throws Throwable
	{
		boolean intrr = false;
		FileChunk c = null;
		try 
		{
			do {
				Future<FileChunk> f = chunks.poll();
				if (f == null)
					return null;
				try {
					c = f.get();
				} catch (InterruptedException e) {
					intrr = true;
				} catch (ExecutionException e) {
					throw e.getCause();
				}
			} while (c == null);
		} 
		finally {
			if(intrr)
				Thread.currentThread().interrupt();
		}
		return c;
	}
	/**
	 * @deprecated Use {@link #readAsync()} instead.
	 */
	public FileChunk readNext() throws IOException {

		Future<FileChunk> f = threadPool.submit(new FileChunkReaderTask());
		chunks.add(f);
		
		try {
			return pollUninterruptibly();
		} catch (Throwable e) {
			throw new IOException(e);
		}
	}
	public static class FileChunkIterator implements Iterator<FileChunk>
	{

		private FileChunkIterator(BlockingQueue<FileChunk> chunks) {
			super();
			this.chunks = chunks;
		}

		private final BlockingQueue<FileChunk> chunks;
		private FileChunk chunk;
		@Override
		public boolean hasNext() {
			try {
				chunk = chunks.poll(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return chunk == null || chunk instanceof EOFChunk;
		}

		@Override
		public FileChunk next() {
			return chunk;
		}
		
	}
	/**
	 * Read chunks concurrently via a thread pool. The collected items are returned in an {@linkplain FileChunkIterator}. 
	 * Internally, the chunks are buffered in a {@linkplain BlockingQueue}, as a consequence {@link FileChunkIterator#hasNext()} is a
	 * finite blocking call.
	 * @return FileChunkIterator
	 * @throws IOException
	 */
	public FileChunkIterator readAsync() throws IOException {

		final BlockingQueue<FileChunk> buffer = new ArrayBlockingQueue<>(asyncBuffSize);
		final AtomicInteger ai = new AtomicInteger(concurrency);
		for (int i = 0; i < concurrency; i++) {
			threadPool.submit(new FileChunkReaderTask2(ai, buffer));
		}
		
		return new FileChunkIterator(buffer);
	}
	
	private int asyncBuffSize = 8192;
	
	/**
	 * New concurrent reader. Max heap memory used at any time would be 
	 * (chunkSize * {@link #getAsyncBuffSize()}) bytes.
	 * @param f the input file
	 * @param chunkSize read block size
	 * @param threads no of concurrent reader threads
	 * @throws IOException
	 */
	public ConcurrentFileChunkReader(File f, int chunkSize, int threads) throws IOException {
		super(f, chunkSize, threads);
		checkFileType();
		
		threadPool = Executors.newFixedThreadPool(threads, new ThreadFactory() {
			int n = 1;
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, getFileName()+".Reader-"+(n++));
				return t;
			}
		});
	}

	private void checkUtf8Encoding(byte[] read) throws CharacterCodingException {
		CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
		decoder.onMalformedInput(CodingErrorAction.REPORT);
		decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
		try {
			decoder.decode(ByteBuffer.wrap(read));
		} catch (CharacterCodingException e) {
			throw e;
		}
	}

	private void checkFileType() throws IOException {
		byte[] read = sliceOfHeadBytes(8192);
		if (read != null) {
			try {
				checkUtf8Encoding(read);
			} catch (CharacterCodingException e) {
				log.error("Unrecognized character set. "+e.getMessage());
				log.debug("Unrecognized character set. --Stacktrace--", e);
				throw new IOException("Unrecognized character set. Expected UTF8 for ASCII reader");
			}

		} else
			throw new IOException("Empty file!");

	}
	public int getAsyncBuffSize() {
		return asyncBuffSize;
	}
	/**
	 * The buffer size for taking the mapped file into java memory. 
	 * @param asyncBuffSize
	 */
	public void setAsyncBuffSize(int asyncBuffSize) {
		this.asyncBuffSize = asyncBuffSize;
	}
	
}
