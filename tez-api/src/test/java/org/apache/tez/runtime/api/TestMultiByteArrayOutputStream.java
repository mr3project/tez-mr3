package org.apache.tez.runtime.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.UUID;

public class TestMultiByteArrayOutputStream {

  @Test
  public void testCreateInputStreamFromRangeWithinMemory() throws Exception {
    byte[] data = new byte[3 * 1024 * 1024 + 123];
    new Random(12345L).nextBytes(data);

    MultiByteArrayOutputStream stream = newStream();
    stream.write(data);
    stream.close();

    long offset = 1024 * 17L;
    long length = 2 * 1024 * 1024L + 77;
    byte[] actual = readFully(stream.createInputStreamFrom(offset, length));

    byte[] expected = new byte[(int) length];
    System.arraycopy(data, (int) offset, expected, 0, (int) length);
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testCreateInputStreamFromZeroLengthAtEnd() throws Exception {
    byte[] data = new byte[64 * 1024];
    new Random(7L).nextBytes(data);

    MultiByteArrayOutputStream stream = newStream();
    stream.write(data);
    stream.close();

    InputStream in = stream.createInputStreamFrom(data.length, 0);
    Assert.assertEquals(-1, in.read());
    in.close();
  }

  @Test
  public void testCreateInputStreamFromRejectsOutOfBoundsRanges() throws Exception {
    MultiByteArrayOutputStream stream = newStream();
    stream.write(new byte[128]);
    stream.close();

    assertThrowsIndexOutOfBounds(stream, -1, 1);
    assertThrowsIndexOutOfBounds(stream, 0, -1);
    assertThrowsIndexOutOfBounds(stream, 129, 0);
    assertThrowsIndexOutOfBounds(stream, 64, 65);
  }

  @Test
  public void testCreateInputStreamFromRangeCrossingSpillBoundary() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path path = new Path(System.getProperty("java.io.tmpdir"),
        "tez-mb-aos-spill-test-" + UUID.randomUUID());

    long totalSize = 260L * 1024 * 1024; // exceeds in-memory capacity, forcing spill
    MultiByteArrayOutputStream stream = new MultiByteArrayOutputStream(fs, path);
    byte[] chunk = new byte[1024 * 1024];
    long written = 0;
    while (written < totalSize) {
      int toWrite = (int) Math.min(chunk.length, totalSize - written);
      fillPattern(chunk, toWrite, written);
      stream.write(chunk, 0, toWrite);
      written += toWrite;
    }
    stream.close();

    long spillLen = fs.getFileStatus(path).getLen();
    Assert.assertTrue("Expected spilled bytes", spillLen > 0);
    long bufferBytes = totalSize - spillLen;

    long offset = bufferBytes - 32;
    long length = 128;
    byte[] actual = readFully(stream.createInputStreamFrom(offset, length));

    byte[] expected = expectedPattern(offset, (int) length);
    Assert.assertArrayEquals(expected, actual);

    fs.delete(path, false);
  }

  private static MultiByteArrayOutputStream newStream() throws IOException {
    Path path = new Path("tez-mb-aos-test-" + UUID.randomUUID());
    return new MultiByteArrayOutputStream(null, path);
  }

  private static void fillPattern(byte[] target, int length, long baseOffset) {
    for (int i = 0; i < length; i++) {
      target[i] = patternAt(baseOffset + i);
    }
  }

  private static byte[] expectedPattern(long offset, int length) {
    byte[] expected = new byte[length];
    fillPattern(expected, length, offset);
    return expected;
  }

  private static byte patternAt(long position) {
    return (byte) ((position * 31 + 7) & 0xFF);
  }

  private static void assertThrowsIndexOutOfBounds(
      MultiByteArrayOutputStream stream, long offset, long length) {
    try {
      stream.createInputStreamFrom(offset, length);
      Assert.fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
      // expected
    } catch (IOException e) {
      Assert.fail("Unexpected IOException: " + e);
    }
  }

  private static byte[] readFully(InputStream in) throws IOException {
    try (InputStream input = in; ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[8192];
      int read;
      while ((read = input.read(buffer)) >= 0) {
        out.write(buffer, 0, read);
      }
      return out.toByteArray();
    }
  }
}
