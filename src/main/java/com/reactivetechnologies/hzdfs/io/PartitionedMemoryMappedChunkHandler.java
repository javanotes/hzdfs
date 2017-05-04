/* ============================================================================
*
* FILE: ConcurrentMemoryMappedChunkHandler.java
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
package com.reactivetechnologies.hzdfs.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * Reads concurrently with multiple reader threads (from different file region)
 * using a {@linkplain MappedByteBuffer}. The {@link #readNext()} method should
 * be invoked concurrently from multiple threads. The number of concurrent
 * threads should be equal to {@link #getConcurrency()}, else some chunks will
 * remain unread! This is because internally a thread local variable is used to
 * keep track of multiple chunks to be read.
 *
 */
public class PartitionedMemoryMappedChunkHandler extends MemoryMappedChunkHandler {

	
	private static final Logger log = LoggerFactory.getLogger(PartitionedMemoryMappedChunkHandler.class);
	protected final int concurrency;

	public int getConcurrency() {
		return concurrency;
	}

	protected byte[] sliceOfHeadBytes(int size) {
		Assert.isTrue(!mapBuffs.isEmpty(), "Buffer cache is empty! Not initialized?");
		MappedByteBuffer mapBuff = mapBuffs.firstEntry().getValue().buffer;
		if (mapBuff.hasRemaining()) {
			byte[] read = new byte[mapBuff.remaining() > size ? size : mapBuff.remaining()];
			mapBuff.get(read);
			mapBuff.rewind();
			return read;
		}
		return null;
	}

	private TreeMap<Long, MappedByteBufferCache> mapBuffs;

	/**
	 * 
	 * @author esutdal
	 *
	 */
	public class MappedByteBufferCache implements Closeable, Comparable<MappedByteBufferCache> {
		private final MappedByteBuffer buffer;
		private final int chunks;
		private final long partition;
		private final AtomicInteger offset = new AtomicInteger();
		private final int size;
		public int getChunks() {
			return chunks;
		}

		public int getOffset() {
			return offset.get();
		}

		public int getSize() {
			return size;
		}

		/**
		 * 
		 * @param buffer the byte buffer
		 * @param partition the position at which this buffer starts
		 */
		public MappedByteBufferCache(MappedByteBuffer buffer, long partition) {
			super();
			this.buffer = buffer;
			this.partition = partition;
			size = buffer.limit();
			chunks = buffer.limit() % readSize == 0 ? (int) ((buffer.limit() / readSize))
					: (int) ((buffer.limit() / readSize) + 1);
		}

		@Override
		public void close() {
			if (buffer != null)
				unmap(buffer);
		}

		@Override
		public int compareTo(MappedByteBufferCache o) {
			return Long.compare(partition, o.partition);
		}
	}

	private ThreadLocal<MappedByteBufferCache> mapBuffCache;

	/**
	 * 
	 * @param position
	 * @param size
	 * @param index
	 * @throws IOException
	 */
	private void mapBuffer(long position, long size, int index) throws IOException {
		MappedByteBuffer mapBuff = iStream.map(MapMode.READ_ONLY, position, size);
		//mapBuffs[index] = new MappedByteBufferCache(mapBuff, position);
		mapBuffs.put(position, new MappedByteBufferCache(mapBuff, position));
	}

	private void createBuffers(long chunkLenPerThread, int rem) throws IOException {
		mapBuffs = new TreeMap<>();
		int i = 0;
		for (; i < concurrency - 1; i++) {
			mapBuffer(chunkLenPerThread * i, chunkLenPerThread, i);
		}

		mapBuffer(chunkLenPerThread * i, chunkLenPerThread + rem, i);
	}

	private void initializeCache() {
		mapBuffCache = ThreadLocal.withInitial(new Supplier<MappedByteBufferCache>() {

			private AtomicInteger count = new AtomicInteger();
			private long lastKey = -1;
			@Override
			public MappedByteBufferCache get() {
				Assert.isTrue(count.getAndIncrement() < mapBuffs.size(), "No. of reader threads greater than configured concurrency");
				
				if(lastKey == -1)
					lastKey = mapBuffs.firstKey();
				else
					lastKey = mapBuffs.higherKey(lastKey);
				
				return mapBuffs.get(lastKey);
			}
		});
	}

	private void prepareBufferCache() throws IOException {
		Assert.isTrue(concurrency > 0, "'concurrency' should be > 0");
		long chunkLenPerThread = fileSize / concurrency;
		int rem;
		try {
			rem = (int) (fileSize % concurrency);
		} catch (Exception e) {
			throw new IOException(
					new IllegalArgumentException("No of concurrency to be increased, or file size to be decreased"));
		}

		createBuffers(chunkLenPerThread, rem);
		initializeCache();
	}

	/**
	 * Create a new instance for reading.
	 * 
	 * @param _file
	 *            the file to be read
	 * @param pageSize
	 *            page size to be fetched each time. This will cause page faults
	 *            and load file in native memory.
	 * @param concurrency
	 *            The number of concurrent threads that will be reading.
	 * @throws IOException
	 */
	public PartitionedMemoryMappedChunkHandler(File _file, int pageSize, int concurrency) throws IOException {
		super(_file);
		this.concurrency = concurrency;
		iStream = FileChannel.open(file.toPath(), StandardOpenOption.READ);
		readSize = pageSize;

		prepareBufferCache();

		log.info("Chunk per thread allocated");
	}

	@Override
	public void close() throws IOException {
		super.close();
		for (Entry<Long, MappedByteBufferCache> e: mapBuffs.entrySet()) {
			MappedByteBufferCache mapBuff = e.getValue();
			mapBuff.close();
		}

	}

	protected int readSize;

	/**
	 * Reads the next chunk available or returns null if EOF encountered. This
	 * method is to be invoked from multiple threads. The thread pool size
	 * should be equal to the {@link #getConcurrency()}, else some chunks would
	 * stay unused! Internally we are caching the chunks in a thread local.
	 */
	@Override
	public FileChunk readNext() throws IOException {

		MappedByteBufferCache mapBuff = mapBuffCache.get();
		return readNextInThread(mapBuff);
	}

	private FileChunk readNextInThread(MappedByteBufferCache mapBuff) {
		byte[] b = readChunkBytes(mapBuff.buffer);
		if(b != null)
		{
			FileChunk chunk = newFileChunk(b, chunks, 0, idx++);
			log.debug("[readNext] " + chunk);
			return chunk;
		}
		return null;
	}

	@Override
	public void writeNext(FileChunk chunk) throws IOException {
		throw new UnsupportedOperationException();
	}

}
