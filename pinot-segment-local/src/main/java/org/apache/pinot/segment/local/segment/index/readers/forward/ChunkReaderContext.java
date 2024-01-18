/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Context for the chunk-based forward index readers.
 * <p>Information saved in the context can be used by subsequent reads as cache:
 * <ul>
 *   <li>
 *     Chunk Buffer from the previous read. Useful if the subsequent read is from the same buffer, as it avoids extra
 *     chunk decompression.
 *   </li>
 *   <li>Id for the chunk</li>
 * </ul>
 */
public class ChunkReaderContext implements ForwardIndexReaderContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkReaderContext.class);

  private static final boolean USE_HEAP_FOR_LARGE_CHUNKS;
  private static final long MAX_DIRECT_BUFFER_CHUNK_SIZE;
  // default max direct buffer threshold size: 2MB
  private static final long DEFAULT_MAX_DIRECT_BUFFER_CHUNK_SIZE = 2 * 1024 * 1024L;

  private final ByteBuffer _chunkBuffer;

  private int _chunkId;

  private List<ForwardIndexReader.ByteRange> _ranges;

  static {
    USE_HEAP_FOR_LARGE_CHUNKS = Boolean.parseBoolean(System.getProperty("useHeapForLargeChunks", "false"));
    MAX_DIRECT_BUFFER_CHUNK_SIZE = Long.parseLong(System.getProperty("maxDirectBufferChunkSize",
            Long.toString(DEFAULT_MAX_DIRECT_BUFFER_CHUNK_SIZE)));
    LOGGER.info("useHeapForLargeChunks: {}, maxDirectBufferChunkSize: {}",
            USE_HEAP_FOR_LARGE_CHUNKS, MAX_DIRECT_BUFFER_CHUNK_SIZE);
  }

  public ChunkReaderContext(int maxChunkSize) {
    if (!USE_HEAP_FOR_LARGE_CHUNKS || maxChunkSize < MAX_DIRECT_BUFFER_CHUNK_SIZE) {
      _chunkBuffer = ByteBuffer.allocateDirect(maxChunkSize);
    } else {
      _chunkBuffer = ByteBuffer.allocate(maxChunkSize);
    }
    _chunkId = -1;
    _ranges = new ArrayList<>();
  }

  @Override
  public void close()
      throws IOException {
    if (CleanerUtil.UNMAP_SUPPORTED && _chunkBuffer.isDirect()) {
      CleanerUtil.getCleaner().freeBuffer(_chunkBuffer);
    }
  }

  public ByteBuffer getChunkBuffer() {
    return _chunkBuffer;
  }

  public int getChunkId() {
    return _chunkId;
  }

  public void setChunkId(int chunkId) {
    _chunkId = chunkId;
  }

  public List<ForwardIndexReader.ByteRange> getRanges() {
    return _ranges;
  }

  public void setRanges(List<ForwardIndexReader.ByteRange> ranges) {
    _ranges = ranges;
  }
}
