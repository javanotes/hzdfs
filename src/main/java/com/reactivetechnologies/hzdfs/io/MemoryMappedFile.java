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
package com.reactivetechnologies.hzdfs.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import sun.misc.Unsafe;
@SuppressWarnings("restriction")
public class MemoryMappedFile extends Reader implements Iterable<String>, WritableByteChannel {
	
	public static interface LineTransformer
	{
		/**
		 * 
		 * @param input
		 * @return
		 */
		String transform(String input);
	}
	private static class Line
	{
		public Line(String text) {
			super();
			this.text = text;
		}

		final String text;
	}
	static class LineProcessor implements Closeable, Runnable
	{
		private boolean allowReorder = true;
		public LineProcessor() {
		}
		private Charset charset;
		public void open(String srcPath, String destPath, Charset charset) throws IOException
		{
			MemoryMappedFile mmFile = new MemoryMappedFile(Paths.get(destPath));
			mmFile.setCharset(charset);
			writer = new BufferedWriter(Channels.newWriter(mmFile, charset.name()));
			
			mmFile = new MemoryMappedFile(Paths.get(srcPath).toFile());
			mmFile.setCharset(charset);
			reader = new BufferedReader(mmFile);
			
			setCharset(charset);
		}
		private final AtomicBoolean mutex = new AtomicBoolean();
		private BufferedWriter writer;
		private BufferedReader reader;
		private LineTransformer transformer;
		private ExecutorService threadPool;
		@Override
		public void close() throws IOException {
			if(reader != null)
				reader.close();
			if(writer != null){
				writer.flush();
				writer.close();
			}
			
		}
		
		private void write(String line) throws IOException
		{
			while(!mutex.compareAndSet(false, true));//busy spin
			try
			{
				writer.write(line);
				writer.newLine();
			}
			finally
			{
				mutex.getAndSet(false);
			}
		}
		private int numThreads = 1;
		private ConcurrentLinkedQueue<Line> queue = new ConcurrentLinkedQueue<>();
		
		private void runWriter()
		{
			for (int i = 0; i < numThreads; i++) {
				threadPool.submit(new Runnable() {
					
					@Override
					public void run() {
						Line l;
						boolean intr = false;
						while (true) 
						{
							l = queue.poll();
							if (l != null) 
							{
								if(l.text == null)
									break;
								else
								{
									try {
										write(transformer.transform(l.text));
										writeCount.incrementAndGet();
									} 
									catch (Exception e) {
										LOG.log(Level.SEVERE, "While writing transformed line", e);
									}
								}
							} 
							else
							{
								try {
									Thread.sleep(100);
								} catch (InterruptedException e) {
									intr = true;
								}
							}
						}
						
						if(intr)
							Thread.currentThread().interrupt();
					}
				});
			}
		}
		private final AtomicInteger readCount = new AtomicInteger(), writeCount = new AtomicInteger();
		private void runReader()
		{
			String line = null;
			try 
			{
				while((line = reader.readLine()) != null)
				{
					readCount.incrementAndGet();
					queue.offer(new Line(line));
					
				}
				
			} 
			catch (Exception e) {
				LOG.log(Level.SEVERE, "While reading source file ", e);
			}
			finally
			{
				for (int i = 0; i < numThreads; i++) {
					queue.offer(new Line(null));
				}
			}
		}
		@Override
		public void run() 
		{
			
			if(allowReorder)
				numThreads = Runtime.getRuntime().availableProcessors();
			
			threadPool = Executors.newFixedThreadPool(numThreads);
			
			LOG.info("Starting transformation ..");
			long time = System.currentTimeMillis();
			runWriter();
			runReader();
			
			threadPool.shutdown();
			long end = System.currentTimeMillis();
			try {
				boolean b = threadPool.awaitTermination(10, TimeUnit.MINUTES);
				if(!b)
					LOG.warning("Process did not terminate in clean manner!");
				
			} catch (InterruptedException e) {
				
			}
			LOG.info("End transformation. Time taken: "+toTimeElapsedString(end-time));
			LOG.info("Lines read: "+readCount);
			LOG.info("Lines written: "+writeCount);
		}
		public void setTransformer(LineTransformer transformer) {
			this.transformer = transformer;
		}
		public void setAllowReorder(boolean allowReorder) {
			this.allowReorder = allowReorder;
		}

		public Charset getCharset() {
			return charset;
		}

		public void setCharset(Charset charset) {
			this.charset = charset;
		}
		
		
	}
	private static String toTimeElapsedString(long duration)
	{
		StringBuilder s = new StringBuilder();
		long t = TimeUnit.MILLISECONDS.toMinutes(duration);
		s.append(t).append(" min ");
		duration -= TimeUnit.MINUTES.toMillis(t);
		t = TimeUnit.MILLISECONDS.toSeconds(duration);
		s.append(t).append(" sec ");
		duration -= TimeUnit.SECONDS.toMillis(t);
		s.append(duration).append(" ms");
		return s.toString();
		
	}
	/**
	 * 
	 * @param srcFile
	 * @param destPath
	 * @param transformer
	 * @param allowReorder
	 * @param charset
	 * @throws IOException
	 */
	public static void transform(String srcFile, String destPath, LineTransformer transformer, boolean allowReorder, Charset charset) throws IOException
	{
		LineProcessor processor = new LineProcessor();
		try 
		{
			processor.setTransformer(transformer);
			processor.setAllowReorder(allowReorder);
			processor.open(srcFile, destPath, charset);
			processor.run();
		} 
		finally {
			processor.close();
		}
	}
	/**
	 * Transform with allowReorder and utf8 encoding.
	 * @param srcFile
	 * @param destPath
	 * @param transformer
	 * @throws IOException
	 */
	public static void transform(String srcFile, String destPath, LineTransformer transformer) throws IOException
	{
		transform(srcFile, destPath, transformer, true, StandardCharsets.UTF_8);
	}
	
	public static class BytesCopy
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
	static{
		//System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$-6s %2$s %5$s%6$s%n");
		System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s [%3$s] %5$s%6$s%n");
	}
	static final Logger LOG = Logger.getLogger(MemoryMappedFile.class.getSimpleName());

	//private static final Logger log = Logger.getLogger("TEST");

	public static final int DEFAULT_MAP_REGION_SIZE = 1024 * 1024 * 2000;// 2 gb
	public static final int DEFAULT_READ_CHUNK_SIZE = 8192 * 100;
	private volatile long position = 0;

	protected FileChannel channel;
	protected MappedByteBuffer mapBuff;
	private long mapRegionSize;
	private int readChunkSize;
	protected long fileSize;

	String fileName;
	private final boolean isWritable;
	/**
	 * New reader with the given region and read size and UTF8 character
	 * encoding.
	 * 
	 * @param f
	 * @throws IOException
	 */
	public MemoryMappedFile(File f, int mapRegionSize, int readChunkSize) throws IOException {
		super();
		fileName = f.getName();
		channel = FileChannel.open(f.toPath(), StandardOpenOption.READ);
		this.mapRegionSize = mapRegionSize;
		this.readChunkSize = readChunkSize;
		isWritable = false;
		fileSize = f.length();
		mapToMemory(false);
	}
	/**
	 * Open in write mode.
	 * @param size
	 * @param path
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public MemoryMappedFile(long size, Path path) throws IOException
	{
		this.mapRegionSize = size;
		this.fileSize = mapRegionSize;
		//channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
		File f = path.toFile();
		fileName = f.getName();
		if(f.exists() && !f.delete())
			LOG.warning("Existing file not deleted!");
		channel = new RandomAccessFile(f, "rw").getChannel();
		mapToMemory(true);
		isWritable = true;
	}
	/**
	 * Open in write mode.
	 * @param target
	 * @throws IOException
	 */
	public MemoryMappedFile(Path target) throws IOException
	{
		this(DEFAULT_MAP_REGION_SIZE, target);
	}

	/**
	 * Open in read mode.
	 * @param f
	 * @throws IOException
	 */
	public MemoryMappedFile(File f) throws IOException {
		this(f, DEFAULT_MAP_REGION_SIZE, DEFAULT_READ_CHUNK_SIZE);
	}

	private byte[] readBytes(final int readChunkSize) {
		byte[] read = new byte[mapBuff.remaining() > readChunkSize ? readChunkSize : mapBuff.remaining()];
		mapBuff.get(read);
		position += read.length;
		return read;
	}

	private byte[] read0() throws IOException {
		return read0(readChunkSize);
	}

	private byte[] read0(int readChunkSize) throws IOException {
		if (mapBuff.hasRemaining()) {
			return readBytes(readChunkSize);
		} else if (position < fileSize) {
			mapToMemory(false);
			return readBytes(readChunkSize);
		} else
			return null;
	}

	private Charset charset = StandardCharsets.UTF_8;

	private char[] readChars(int len) throws IOException {
		byte[] b = read0(len);
		if (b == null)
			return null;

		return new String(b, charset).toCharArray();
	}

	protected long remaining() {
		return fileSize - position;
	}

	private void mapToMemory(boolean write) throws IOException {
		mapToMemory(write, mapRegionSize);
	}
	
	private void mapToMemory(final boolean write, final long mapRegionSize) throws IOException {
		if(mapBuff != null)
			unmap(mapBuff);
		mapBuff = channel.map(write ? MapMode.READ_WRITE : MapMode.READ_ONLY, position, remaining() > 0 ? Math.min(mapRegionSize, remaining()) : mapRegionSize);
		if(write)
			fileSize += mapBuff.limit();
		
		LOG.info(fileName+"> Mapped " + mapBuff.limit() + " bytes to direct memory, using "+(write ? "write mode " : "read mode"));
	}

	@Override
	public void close() throws IOException {
		try 
		{
			if(mapBuff != null)
			{
				//mapBuff.force();
				unmap(mapBuff);
			}
			if (channel != null) 
			{
				if(isWritable){
					channel.truncate(position);
					LOG.fine("Truncated written file to "+position);
				}
				
				channel.close();
			}
		} finally {
			
		}
		LOG.info(fileName+"> close() "+(isWritable ? "write" : "read"));
	}

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		char[] read = readChars(len);
		if (read == null)
			return -1;
		System.arraycopy(read, 0, cbuf, off, read.length);
		return read.length;
	}

	private static boolean unmap(MappedByteBuffer bb) {
		try 
		{
			Method cleaner_method = bb.getClass().getMethod("cleaner");
			if (cleaner_method != null) {
				cleaner_method.setAccessible(true);
				Object cleaner = cleaner_method.invoke(bb);
				if (cleaner != null) {
					Method clean_method = cleaner.getClass().getMethod("clean");
					if (clean_method != null) {
						clean_method.setAccessible(true);
						clean_method.invoke(cleaner);
					}
					return true;
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return false;
	}

	public static void main(String[] args) throws IOException {
		
		transform("C:\\Users\\esutdal\\WORK\\DM-BSS\\big_test_1_million_t1.csv", "C:\\Users\\esutdal\\WORK\\DM-BSS\\big_test_1_million_t2.csv", new LineTransformer() {
			
			@Override
			public String transform(String input) {
				//return "{" + input + "}";
				return input;
			}
		});
		
		/*
		
		//using buffered reader
		log.info("------------- Buffered Reader -----------");
		long c1 = 0;
		long start = System.currentTimeMillis();
		
		//C:\Users\esutdal\WORK\DM-BSS\big_test_1_million.csv
		try (BufferedReader rdr = new BufferedReader(
				new FileReader(new File("C:\\Users\\esutdal\\WORK\\DM-BSS\\big_test_1_million.csv")))) {
			String line = null;
			while ((line = rdr.readLine()) != null){
				if(line.startsWith("\"") )
				{
					line.substring(1, line.length());
				}
				c1++;
			}
				//System.out.println(line);
		}
		log.info("Time taken: "+(System.currentTimeMillis()-start)+", lines read "+c1);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			
		}
		log.info("------------- Iterator Reader -----------");
		
		//using iterator
		long c2 = 0;
		start = System.currentTimeMillis();
		try(MemoryMappedReader mr = new MemoryMappedReader(new File("C:\\Users\\esutdal\\WORK\\DM-BSS\\big_test_1_million.csv")))
		{
			for(Iterator<String> iter = mr.iterator(); iter.hasNext();)
			{
				String line = iter.next();c2++;
				//System.out.println(line);
				if(line.startsWith("\"") )
				{
					line.substring(1, line.length());
				}
				c2++;
			}
		}
		log.info("Time taken: "+(System.currentTimeMillis()-start)+", lines read "+c2);
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			
		}
		log.info("------------- Buffered Mem Reader -----------");
		
		//using iterator
		c2 = 0;
		start = System.currentTimeMillis();
		try(BufferedReader rdr = new BufferedReader(new MemoryMappedFile(new File("C:\\Users\\esutdal\\WORK\\DM-BSS\\big_test_1_million.csv"))))
		{
			String line = null;
			while ((line = rdr.readLine()) != null)
			{
				if(line.startsWith("\"") )
				{
					line.substring(1, line.length());
				}
				c2++;
			}
				
		}
		log.info("Time taken: "+(System.currentTimeMillis()-start)+", lines read "+c2);
		
		
	*/}

	public Charset getCharset() {
		return charset;
	}

	public void setCharset(Charset charset) {
		this.charset = charset;
	}

	/*
	 * A line is considered to be terminated by any one of a line feed ('\n'), a
	 * carriage return ('\r'), or a carriage return followed immediately by a
	 * line feed.
	 */
	final static byte CARRIAGE_RETURN = 0xD;
	final static byte LINE_FEED = 0xA;
	
	private LinkedList<byte[]> pendingSplits = new LinkedList<>();
	private LinkedList<String> collectedLines = new LinkedList<>();
	
	/**
	 * 
	 * @param unicodeBytes
	 * @param splits
	 * @return
	 */
	private static byte[] splitBytesAtLineBreak(final byte[] unicodeBytes, final List<byte[]> splits) {
		byte[] oneSplit;
		int len = unicodeBytes.length;
		int posOffset = 0;

		for (int pos = 0; pos < len; pos++) 
		{
			if (pos < len - 1 && unicodeBytes[pos] == CARRIAGE_RETURN && unicodeBytes[pos + 1] == LINE_FEED) {
				oneSplit = BytesCopy.copyOfRange(unicodeBytes, posOffset, pos);
				pos++;

				posOffset = pos + 1;
				if (oneSplit != null && oneSplit.length > 0) {

					splits.add(oneSplit);
				}

			} else if (unicodeBytes[pos] == LINE_FEED || unicodeBytes[pos] == CARRIAGE_RETURN) {
				oneSplit = BytesCopy.copyOfRange(unicodeBytes, posOffset, pos);

				posOffset = pos + 1;
				if (oneSplit != null && oneSplit.length > 0) {

					splits.add(oneSplit);
				}

			} 

		}
		byte[] rem = BytesCopy.copyOfRange(unicodeBytes, posOffset, len);
		return rem;
		
	}
	private boolean emitIfHasNext(byte[] b, List<byte[]> splits) throws IOException
	{
		byte[] rem = splitBytesAtLineBreak(b, splits);
		if (splits.isEmpty()) 
		{
			pendingSplits.offer(b);
		} 
		else
		{
			emit(splits);
			pendingSplits.offer(rem);
			return true;
		}
		return false;
	}
	private boolean isHasNext() throws IOException
	{
		boolean emitted = false;
		byte[] b = read0();
		if(b != null)
		{
			List<byte[]> splits;
			
			while (!emitted) 
			{
				splits = new LinkedList<>();
				emitted = emitIfHasNext(b, splits);
			}
		}
		return emitted;
	}
	
	private boolean hasNext0() {
		if(!collectedLines.isEmpty())
			return true;
		try 
		{
			return isHasNext();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private void emit(List<byte[]> splits) throws IOException
	{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		//pop all pending splits
		byte[] p = pendingSplits.poll();
		while(p != null)
		{
			out.write(p);
			p = pendingSplits.poll();
		}
		//take the first split from this iteration splits and buffer
		out.write(splits.get(0));
		emitLine(out.toByteArray());
		
		//take the remaining splits from this iteration and buffer
		for (int i = 1; i < splits.size(); i++) {
			emitLine(splits.get(i));
		}
	}
	private void emitLine(byte[] b)
	{
		collectedLines.offer(new String(b, charset));
	}
	
	private String next0() {
		return collectedLines.poll();
	}

	@Override
	public Iterator<String> iterator() {
		return new Iterator<String>() {

			@Override
			public boolean hasNext() {
				return hasNext0();
			}

			@Override
			public String next() {
				return next0();
			}
		};
	}

	@Override
	public boolean isOpen() {
		return isWritable;
	}

	private void writeBytes(ByteBuffer src) throws IOException {
		int lastPos = src.position();
		while (src.hasRemaining()) {
			try 
			{
				mapBuff.put(src);
				position += (src.position() - lastPos);
				lastPos = src.position();
			} 
			catch (Exception e) {
				throw new IOException(e);
			} 
		}
	}
	private void ensureSize(int n) throws IOException {
		if (mapBuff.remaining() > n) {
			return;
		} 
		else
		{
			mapBuff.force();
			mapToMemory(true);
		}
	}
	@Override
	public int write(final ByteBuffer src) throws IOException {
		int n = src.remaining();
		ensureSize(n);
		try 
		{
			writeBytes(src);
		} 
		catch (Exception e) {
			throw new IOException(e);
		}
		return n;
	}
}
