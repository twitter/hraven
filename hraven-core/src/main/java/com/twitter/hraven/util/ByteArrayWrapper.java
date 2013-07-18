/*
Copyright 2013 Twitter, Inc.

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
package com.twitter.hraven.util;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * An input stream class backed by a byte array that also implements
 * <code>PositionedReadable</code>, <code>Seekable</code>, and <code>Closeable</code>. It can be
 * used as an input to the <code>FSDataInputStream</code>.
 *
 * @see org.apache.hadoop.fs.FSDataInputStream
 */
public class ByteArrayWrapper extends ByteArrayInputStream
    implements PositionedReadable, Seekable, Closeable {
  /**
   * Constructor that creates an instance of <code>ByteArrayWrapper</code>.
   */
  public ByteArrayWrapper(byte[] buf) {
    super(buf);
  }

  /**
   * Seeks and sets position to the specified value.
   *
   * @throws IOException if position is negative or exceeds the buffer size
   *
   * {@inheritDoc}
   */
  public synchronized void seek(long position) throws IOException {
    if (position < 0 || position >= count) {
      throw new IOException("cannot seek position " + position + " as it is out of bounds");
    }
    pos = (int) position;
  }

  /**
   * {@inheritDoc}
   */
  public synchronized long getPos() throws IOException {
    return pos;
  }

  /**
   * This is not applicable to ByteArrayWrapper, and always returns false.
   *
   * {@inheritDoc}
   */
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  public synchronized int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    long oldPos = getPos();
    int nread = -1;
    try {
      seek(position);
      nread = read(buffer, offset, length);
    } finally {
      seek(oldPos);
    }
    return nread;
  }

  /**
   * {@inheritDoc}
   */
  public synchronized void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    int nread = 0;
    while (nread < length) {
      int nbytes = read(position + nread, buffer, offset + nread, length - nread);
      if (nbytes < 0) {
        throw new EOFException("End of file reached before reading fully.");
      }
      nread += nbytes;
    }
  }

  /**
   * {@inheritDoc}
   */
  public synchronized void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
}
