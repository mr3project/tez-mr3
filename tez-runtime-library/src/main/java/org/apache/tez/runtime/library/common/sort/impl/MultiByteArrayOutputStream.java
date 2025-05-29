package org.apache.tez.runtime.library.common.sort.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * An OutputStream that grows in fixed-size chunks and can exceed 2GB without integer overflow.
 */
public class MultiByteArrayOutputStream extends OutputStream {
    private final int cacheSize;
    private final List<byte[]> buffers = new ArrayList<>();
    private byte[] currentBuffer;
    private int posInBuf = 0;
    private long totalBytes = 0;

    /**
     * @param cacheSize size of each internal byte[] buffer; must be > 0
     */
    public MultiByteArrayOutputStream(int cacheSize) {
        if (cacheSize <= 0) {
            throw new IllegalArgumentException("cacheSize must be positive");
        }
        this.cacheSize = cacheSize;
        this.currentBuffer = new byte[cacheSize];
        buffers.add(currentBuffer);
    }

    @Override
    public void write(int b) throws IOException {
        ensureCapacity(1);
        currentBuffer[posInBuf++] = (byte) b;
        totalBytes++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // avoid off+len overflow: check len against remaining length
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        int remaining = len;
        int inputPos = off;
        while (remaining > 0) {
            ensureCapacity(1);
            int space = cacheSize - posInBuf;
            int toCopy = Math.min(space, remaining);
            System.arraycopy(b, inputPos, currentBuffer, posInBuf, toCopy);
            posInBuf += toCopy;
            totalBytes += toCopy;
            inputPos += toCopy;
            remaining -= toCopy;
        }
    }

    private void ensureCapacity(int needed) {
        if (cacheSize - posInBuf < needed) {
            currentBuffer = new byte[cacheSize];
            buffers.add(currentBuffer);
            posInBuf = 0;
        }
    }

    @Override
    public void flush() throws IOException {
        // no-op
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    /**
     * @return total number of bytes written (may exceed Integer.MAX_VALUE)
     */
    public long size() {
        return totalBytes;
    }

    /**
     * @return a List of ByteBuffers whose concatenated content equals all bytes written.
     *         All buffers except possibly the last will be full length.
     * @throws IllegalStateException if the number of full buffers exceeds Integer.MAX_VALUE
     */
    public List<ByteBuffer> getData() {
        long fullCountLong = totalBytes / cacheSize;
        if (fullCountLong > Integer.MAX_VALUE) {
            throw new IllegalStateException("Too many buffers to represent: " + fullCountLong);
        }
        int fullBuffers = (int) fullCountLong;
        int remainder = (int) (totalBytes % cacheSize);

        List<ByteBuffer> out = new ArrayList<>(buffers.size());
        for (int i = 0; i < fullBuffers; i++) {
            out.add(ByteBuffer.wrap(buffers.get(i), 0, cacheSize));
        }
        if (remainder > 0) {
            out.add(ByteBuffer.wrap(buffers.get(fullBuffers), 0, remainder));
        }
        return out;
    }
}
