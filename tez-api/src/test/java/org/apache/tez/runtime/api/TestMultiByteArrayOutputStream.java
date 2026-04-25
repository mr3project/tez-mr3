package org.apache.tez.runtime.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public class TestMultiByteArrayOutputStream {

  private static final long TOTAL_SIZE = 260L * 1024 * 1024;

  private static FileSystem fs;
  private static Path path;
  private static MultiByteArrayOutputStream stream;
  private static long spillLen;
  private static long bufferBytes;

  @BeforeClass
  public static void setUpClass() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    path = new Path(System.getProperty("java.io.tmpdir"),
        "tez-mb-aos-spill-test-" + UUID.randomUUID());

    stream = new MultiByteArrayOutputStream(fs, path);
    byte[] chunk = new byte[1024 * 1024];
    long written = 0;
    while (written < TOTAL_SIZE) {
      int toWrite = (int) Math.min(chunk.length, TOTAL_SIZE - written);
      fillPattern(chunk, toWrite, written);
      stream.write(chunk, 0, toWrite);
      written += toWrite;
    }
    stream.close();

    spillLen = fs.getFileStatus(path).getLen();
    Assert.assertTrue("Expected spilled bytes", spillLen > 0);
    bufferBytes = TOTAL_SIZE - spillLen;
    Assert.assertTrue("Expected in-memory bytes as well", bufferBytes > 0);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (fs != null && path != null) {
      fs.delete(path, false);
    }
  }

  @Test
  public void testCreateInputStreamFromMultipleRanges() throws Exception {
    long[][] ranges = new long[][] {
        {0, 1},
        {1024 * 17L, 2 * 1024 * 1024L + 77}, // memory-only range
        {bufferBytes - 32, 128}, // crossing memory/spill boundary
        {bufferBytes + 37, 4096}, // spill-only range
        {TOTAL_SIZE - 1024, 1024} // tail range
    };

    for (long[] range : ranges) {
      long offset = range[0];
      int length = (int) range[1];
      byte[] actual = readFully(stream.createInputStreamFrom(offset, length));
      Assert.assertArrayEquals(expectedPattern(offset, length), actual);
    }
  }

  @Test
  public void testCreateInputStreamFromZeroLengthRanges() throws Exception {
    assertZeroLengthRange(0);
    assertZeroLengthRange(bufferBytes);
    assertZeroLengthRange(TOTAL_SIZE);
  }

  @Test
  public void testCreateInputStreamFromRejectsOutOfBoundsRanges() throws Exception {
    assertThrowsIndexOutOfBounds(stream, -1, 1);
    assertThrowsIndexOutOfBounds(stream, 0, -1);
    assertThrowsIndexOutOfBounds(stream, TOTAL_SIZE + 1, 0);
    assertThrowsIndexOutOfBounds(stream, TOTAL_SIZE - 1, 2);
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

  private static void assertZeroLengthRange(long offset) throws IOException {
    InputStream in = stream.createInputStreamFrom(offset, 0);
    Assert.assertEquals(-1, in.read());
    in.close();
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
