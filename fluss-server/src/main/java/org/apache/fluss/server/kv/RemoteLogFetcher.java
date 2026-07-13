/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.log.remote.RemoteLogStorage;
import org.apache.fluss.server.log.remote.RemoteLogTablet.RemoteLogSegmentPage;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

import static org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly;

/**
 * A utility class that fetches remote log segments and makes them available as {@link
 * FileLogRecords} for KV recovery. It downloads remote log data into a local temporary directory
 * using a UUID to avoid conflicts with other concurrent recovery operations.
 *
 * <p>The fetcher is {@link Closeable} and the caller must close it after use to clean up the
 * temporary directory. It is recommended to use try-with-resources to ensure proper resource
 * cleanup:
 *
 * <pre>{@code
 * try (RemoteLogFetcher fetcher = new RemoteLogFetcher(...)) {
 *     for (LogRecordBatch batch : fetcher.fetch(startOffset, localLogStartOffset)) {
 *         // process batch
 *     }
 * }
 * }</pre>
 *
 * <p>Iteration remains single-threaded, but {@link #close()} may run concurrently and takes
 * ownership of resources acquired by an in-progress iteration.
 */
public class RemoteLogFetcher implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogFetcher.class);

    private static final String REMOTE_LOG_RECOVERY_DIR_PREFIX = "remote-log-recovery-";
    private static final Object TEMP_DIR_OWNERSHIP_LOCK = new Object();
    private static final Set<Path> ACTIVE_TEMP_DIRS = new HashSet<>();
    private static final int MAX_SEGMENT_METADATA_PER_PAGE = 16;

    /** Defines whether batch views outlive a segment transition. */
    public enum ConsumerMode {
        KV_STREAMING,
        INDEX_RETAINED
    }

    /** Explains why a bounded fetch stopped after it has been fully consumed. */
    public enum StopReason {
        NONE,
        END,
        BYTE_LIMIT
    }

    private final RemoteLogManager remoteLogManager;
    private final TableBucket tableBucket;
    private final Path tempDir;
    private final ConsumerMode consumerMode;
    private final Object lifecycleLock = new Object();

    /** Tracks the currently active iterator to ensure proper cleanup on close. */
    private RemoteLogBatchIterator activeIterator;

    @Nullable private CachedSegment cachedSegment;
    private boolean closed;

    public RemoteLogFetcher(
            RemoteLogManager remoteLogManager, TableBucket tableBucket, File logTabletDir) {
        this(remoteLogManager, tableBucket, logTabletDir, ConsumerMode.KV_STREAMING);
    }

    public RemoteLogFetcher(
            RemoteLogManager remoteLogManager,
            TableBucket tableBucket,
            File logTabletDir,
            ConsumerMode consumerMode) {
        this(
                remoteLogManager,
                tableBucket,
                logTabletDir
                        .toPath()
                        .resolve("tmp")
                        .resolve(REMOTE_LOG_RECOVERY_DIR_PREFIX + UUID.randomUUID()),
                consumerMode,
                true);
    }

    @VisibleForTesting
    RemoteLogFetcher(RemoteLogManager remoteLogManager, TableBucket tableBucket, Path tempDir) {
        this(remoteLogManager, tableBucket, tempDir, ConsumerMode.KV_STREAMING, false);
    }

    private RemoteLogFetcher(
            RemoteLogManager remoteLogManager,
            TableBucket tableBucket,
            Path tempDir,
            ConsumerMode consumerMode,
            boolean scavengeAbandonedDirectories) {
        this.remoteLogManager = remoteLogManager;
        this.tableBucket = tableBucket;
        this.tempDir = tempDir.toAbsolutePath().normalize();
        this.consumerMode = consumerMode;
        synchronized (TEMP_DIR_OWNERSHIP_LOCK) {
            if (scavengeAbandonedDirectories) {
                scavengeAbandonedDirectories(this.tempDir.getParent());
            }
            ACTIVE_TEMP_DIRS.add(this.tempDir);
        }
    }

    /**
     * Fetches all relevant remote log segments that cover the range from {@code startOffset} up to
     * {@code localLogStartOffset}, and iterates over the log record batches in order.
     *
     * <p>The returned {@link Iterable} is lazily loaded - remote log segments are downloaded and
     * processed only when iterating through the batches. This means that file downloads and I/O
     * operations occur during iteration, not when this method is called.
     *
     * @param startOffset the offset to start fetching from (inclusive)
     * @param localLogStartOffset the local log start offset (exclusive, stop before this)
     * @return an iterable over all {@link LogRecordBatch} from the fetched remote segments. The
     *     iterator lazily downloads segments as needed.
     * @throws Exception if any error occurs during fetching or reading
     */
    public FetchResult fetch(long startOffset, long localLogStartOffset) throws Exception {
        return fetchInternal(startOffset, localLogStartOffset, Long.MAX_VALUE);
    }

    /**
     * Fetches a byte-bounded range while retaining at most one small page of segment metadata. The
     * first batch may exceed {@code maxBytes}, matching local log min-one-message behavior.
     */
    public FetchResult fetch(long startOffset, long localLogStartOffset, int maxBytes)
            throws Exception {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive");
        }
        return fetchInternal(startOffset, localLogStartOffset, maxBytes);
    }

    private FetchResult fetchInternal(long startOffset, long localLogStartOffset, long maxBytes)
            throws Exception {
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("RemoteLogFetcher is closed");
            }
            // Lazily create the temp directory on first fetch.
            if (!Files.exists(tempDir)) {
                Files.createDirectories(tempDir);
            }

            // Close any previously active iterator before creating a new one to avoid leaking file
            // descriptors.
            RemoteLogBatchIterator prev = this.activeIterator;
            if (prev != null) {
                prev.close();
            }

            RemoteLogSegmentPage firstPage =
                    remoteLogManager.relevantRemoteLogSegmentPage(
                            tableBucket, startOffset, null, MAX_SEGMENT_METADATA_PER_PAGE);
            if (firstPage.segments().isEmpty() && !firstPage.hasMore()) {
                throw new RemoteStorageException(
                        String.format(
                                "No remote log segments found for table bucket %s at offset %d",
                                tableBucket, startOffset));
            }

            LOG.info(
                    "Found first remote log metadata page with {} segments for table bucket {} from offset {} to localLogStartOffset {}",
                    firstPage.segments().size(),
                    tableBucket,
                    startOffset,
                    localLogStartOffset);

            RemoteLogBatchIterator iterator =
                    new RemoteLogBatchIterator(
                            firstPage, startOffset, localLogStartOffset, maxBytes);
            this.activeIterator = iterator;
            return new FetchResult(iterator);
        }
    }

    @Override
    public void close() {
        RemoteLogBatchIterator iterator;
        CachedSegment cached;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            iterator = activeIterator;
            activeIterator = null;
            cached = cachedSegment;
            cachedSegment = null;
        }
        try {
            if (iterator != null) {
                iterator.close();
            }
        } finally {
            try {
                if (cached != null) {
                    IOUtils.closeQuietly(cached.records, "FileLogRecords");
                }
                cleanupTempDirectory();
            } finally {
                synchronized (TEMP_DIR_OWNERSHIP_LOCK) {
                    ACTIVE_TEMP_DIRS.remove(tempDir);
                }
            }
        }
    }

    private static void scavengeAbandonedDirectories(@Nullable Path parent) {
        if (parent == null || !Files.isDirectory(parent)) {
            return;
        }
        try (DirectoryStream<Path> directories =
                Files.newDirectoryStream(parent, REMOTE_LOG_RECOVERY_DIR_PREFIX + "*")) {
            for (Path directory : directories) {
                Path normalized = directory.toAbsolutePath().normalize();
                if (!ACTIVE_TEMP_DIRS.contains(normalized)
                        && Files.isDirectory(normalized, LinkOption.NOFOLLOW_LINKS)) {
                    LOG.info("Cleaning up abandoned remote log recovery dir: {}", normalized);
                    deleteDirectoryQuietly(normalized.toFile());
                }
            }
        } catch (IOException e) {
            LOG.debug("Unable to scan remote log recovery parent {}", parent, e);
        }
    }

    private void cleanupTempDirectory() {
        if (Files.exists(tempDir)) {
            LOG.info("Cleaning up remote log recovery dir: {}", tempDir);
            deleteDirectoryQuietly(tempDir.toFile());
        }
        Path tmpDir = tempDir.getParent();
        if (tmpDir != null && Files.exists(tmpDir)) {
            try {
                Files.deleteIfExists(tmpDir);
            } catch (DirectoryNotEmptyException ignored) {
                // Another recovery owns a sibling directory.
            } catch (IOException e) {
                LOG.debug("Unable to remove empty remote log recovery parent {}", tmpDir, e);
            }
        }
    }

    @VisibleForTesting
    Path getTempDir() {
        return tempDir;
    }

    /**
     * Downloads the log data of a remote log segment to a local temporary file.
     *
     * @return the local file containing the downloaded log data
     */
    protected File downloadSegment(RemoteLogSegment segment) throws IOException {
        File localFile =
                tempDir.resolve(
                                FlussPaths.filenamePrefixFromOffset(segment.remoteLogStartOffset())
                                        + ".log")
                        .toFile();

        RemoteLogStorage remoteLogStorage = remoteLogManager.getRemoteLogStorage();
        LOG.info(
                "Downloading remote log segment {} (offsets {}-{}) to {}",
                segment.remoteLogSegmentId(),
                segment.remoteLogStartOffset(),
                segment.remoteLogEndOffset(),
                localFile);

        try (InputStream inputStream = remoteLogStorage.fetchLogData(segment);
                OutputStream outputStream = Files.newOutputStream(localFile.toPath())) {
            IOUtils.copyBytes(inputStream, outputStream, false);
        } catch (RemoteStorageException e) {
            throw new IOException(
                    "Failed to download remote log segment: " + segment.remoteLogSegmentId(), e);
        }
        return localFile;
    }

    /** Opens downloaded records before the iterator atomically registers their ownership. */
    protected FileLogRecords openDownloadedSegment(File localFile) throws IOException {
        return FileLogRecords.open(localFile, false);
    }

    /** A closeable per-fetch lease over batch views and their backing files. */
    public final class FetchResult implements Iterable<LogRecordBatch>, AutoCloseable {
        private final RemoteLogBatchIterator iterator;

        private FetchResult(RemoteLogBatchIterator iterator) {
            this.iterator = iterator;
        }

        public StopReason stopReason() {
            return iterator.stopReason;
        }

        @Override
        public Iterator<LogRecordBatch> iterator() {
            return iterator;
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

    /**
     * An iterator that lazily downloads remote log segments and iterates over their batches in
     * order. It respects the startOffset and localLogStartOffset boundaries, yielding only batches
     * within [startOffset, localLogStartOffset).
     */
    private class RemoteLogBatchIterator implements Iterator<LogRecordBatch> {
        private final long localLogStartOffset;
        private final long maxBytes;
        private List<RemoteLogSegment> segments;
        @Nullable private Long metadataCursor;
        private boolean hasMoreMetadata;

        /** Tracks the current read offset, advancing as batches are consumed. */
        private long currentOffset;

        private long returnedBytes;

        private int currentSegmentIndex = 0;
        private FileLogRecords currentFileLogRecords;
        private Iterator<LogRecordBatch> currentBatchIterator;
        private final List<FileLogRecords> openedFileLogRecords = new ArrayList<>();
        private LogRecordBatch nextBatch;
        private boolean finished = false;
        private volatile boolean closed = false;
        private volatile StopReason stopReason = StopReason.NONE;
        @Nullable private RemoteLogSegment currentSegment;

        RemoteLogBatchIterator(
                RemoteLogSegmentPage firstPage,
                long startOffset,
                long localLogStartOffset,
                long maxBytes) {
            this.segments = firstPage.segments();
            this.metadataCursor = firstPage.nextStartOffsetExclusive();
            this.hasMoreMetadata = firstPage.hasMore();
            this.currentOffset = startOffset;
            this.localLogStartOffset = localLogStartOffset;
            this.maxBytes = maxBytes;
        }

        /** Closes this iterator and releases all held resources. */
        public void close() {
            List<FileLogRecords> recordsToClose;
            FileLogRecords currentRecords;
            RemoteLogSegment segment;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                recordsToClose = new ArrayList<>(openedFileLogRecords);
                openedFileLogRecords.clear();
                currentRecords = currentFileLogRecords;
                segment = currentSegment;
                currentFileLogRecords = null;
                currentSegment = null;
                currentBatchIterator = null;
            }
            releaseIterator(this, recordsToClose, currentRecords, segment);
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (nextBatch == null && returnedBytes >= maxBytes) {
                finished = true;
                stopReason = StopReason.BYTE_LIMIT;
                return false;
            }
            // Lazily advance while retaining opened files until close. Index replay may collect
            // lightweight batch views whose bytes must stay owned by its ReadResult.
            if (nextBatch == null && !finished) {
                advance();
            }
            return nextBatch != null;
        }

        @Override
        public LogRecordBatch next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            LogRecordBatch result = nextBatch;
            nextBatch = null;
            return result;
        }

        private void advance() {
            nextBatch = null;
            while (!finished) {
                if (closed) {
                    finished = true;
                    return;
                }
                // try to get next batch from current iterator
                if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
                    LogRecordBatch batch = currentBatchIterator.next();
                    // skip batches entirely before currentOffset
                    if (batch.nextLogOffset() <= currentOffset) {
                        continue;
                    }
                    // stop if we've reached localLogStartOffset
                    if (batch.baseLogOffset() >= localLogStartOffset) {
                        finished = true;
                        stopReason = StopReason.END;
                        return;
                    }
                    if (returnedBytes > 0 && returnedBytes + batch.sizeInBytes() > maxBytes) {
                        finished = true;
                        stopReason = StopReason.BYTE_LIMIT;
                        return;
                    }
                    nextBatch = batch;
                    returnedBytes += batch.sizeInBytes();
                    // advance currentOffset so subsequent segments use updated position
                    currentOffset = batch.nextLogOffset();
                    return;
                }

                boolean completedSegment = currentFileLogRecords != null;
                if (completedSegment && consumerMode == ConsumerMode.KV_STREAMING) {
                    closeStreamingSegment(currentFileLogRecords);
                }
                currentFileLogRecords = null;
                currentSegment = null;
                currentBatchIterator = null;

                // A bounded retaining result stops at the segment boundary rather than opening a
                // segment merely to reject its first batch. The next read resumes at currentOffset.
                if (completedSegment
                        && consumerMode == ConsumerMode.INDEX_RETAINED
                        && maxBytes != Long.MAX_VALUE
                        && returnedBytes > 0
                        && currentOffset < localLogStartOffset) {
                    finished = true;
                    stopReason = StopReason.BYTE_LIMIT;
                    return;
                }

                // move to next segment
                if (currentSegmentIndex >= segments.size() && !loadNextMetadataPage()) {
                    finished = true;
                    stopReason = StopReason.END;
                    return;
                }

                RemoteLogSegment segment = segments.get(currentSegmentIndex++);
                // Remote segment ends are exclusive.
                if (segment.remoteLogEndOffset() <= currentOffset) {
                    continue;
                }
                // skip segments that start at or after localLogStartOffset
                if (segment.remoteLogStartOffset() >= localLogStartOffset) {
                    finished = true;
                    stopReason = StopReason.END;
                    return;
                }

                try {
                    // TODO optimize to async downloading the next segment while processing the
                    // current one. Otherwise, the recovery process may be significantly slowed down
                    // by the download time of remote segments, especially when there are many
                    // segments to fetch. trace by: https://github.com/apache/fluss/issues/3091
                    FileLogRecords openedRecords = acquireCachedSegment(this, segment);
                    if (openedRecords == null) {
                        File localFile = downloadSegment(segment);
                        openedRecords = openDownloadedSegment(localFile);
                        if (!registerOpenedFileLogRecords(openedRecords)) {
                            IOUtils.closeQuietly(openedRecords, "FileLogRecords");
                            cleanupTempDirectory();
                            finished = true;
                            return;
                        }
                    }
                    currentFileLogRecords = openedRecords;
                    currentSegment = segment;
                    int startPosition = 0;
                    // if this segment contains data before currentOffset, find the right position
                    if (segment.remoteLogStartOffset() < currentOffset) {
                        startPosition =
                                remoteLogManager.lookupPositionForOffset(segment, currentOffset);
                    }
                    if (startPosition > 0) {
                        // Calculate actual length to avoid potential issues with Integer.MAX_VALUE
                        int remainingLength =
                                (int)
                                        Math.min(
                                                Integer.MAX_VALUE,
                                                currentFileLogRecords.sizeInBytes()
                                                        - startPosition);
                        FileLogRecords sliced =
                                currentFileLogRecords.slice(startPosition, remainingLength);
                        currentBatchIterator = sliced.batches().iterator();
                    } else {
                        currentBatchIterator = currentFileLogRecords.batches().iterator();
                    }
                } catch (Exception e) {
                    // Ensure resources are cleaned up if an exception occurs during segment loading
                    closeOpenedFileLogRecords();
                    if (closed) {
                        cleanupTempDirectory();
                        finished = true;
                        return;
                    }
                    throw new RuntimeException(
                            "Failed to fetch remote log segment: " + segment.remoteLogSegmentId(),
                            e);
                }
            }
        }

        private boolean loadNextMetadataPage() {
            while (currentSegmentIndex >= segments.size() && hasMoreMetadata) {
                RemoteLogSegmentPage page =
                        remoteLogManager.relevantRemoteLogSegmentPage(
                                tableBucket,
                                currentOffset,
                                metadataCursor,
                                MAX_SEGMENT_METADATA_PER_PAGE);
                segments = page.segments();
                currentSegmentIndex = 0;
                metadataCursor = page.nextStartOffsetExclusive();
                hasMoreMetadata = page.hasMore();
            }
            return currentSegmentIndex < segments.size();
        }

        private synchronized boolean registerOpenedFileLogRecords(FileLogRecords records) {
            if (closed) {
                return false;
            }
            openedFileLogRecords.add(records);
            return true;
        }

        private void closeStreamingSegment(FileLogRecords records) {
            boolean owned;
            synchronized (this) {
                owned = openedFileLogRecords.remove(records);
            }
            if (owned) {
                IOUtils.closeQuietly(records, "FileLogRecords");
            }
        }

        private void closeOpenedFileLogRecords() {
            List<FileLogRecords> recordsToClose;
            synchronized (this) {
                recordsToClose = new ArrayList<>(openedFileLogRecords);
                openedFileLogRecords.clear();
                currentFileLogRecords = null;
                currentBatchIterator = null;
            }
            closeFileLogRecords(recordsToClose);
        }

        private void closeFileLogRecords(List<FileLogRecords> recordsToClose) {
            for (FileLogRecords records : recordsToClose) {
                IOUtils.closeQuietly(records, "FileLogRecords");
            }
        }
    }

    @Nullable
    private FileLogRecords acquireCachedSegment(
            RemoteLogBatchIterator owner, RemoteLogSegment segment) {
        synchronized (lifecycleLock) {
            CachedSegment cached = cachedSegment;
            if (cached == null) {
                return null;
            }
            if (!cached.segment.remoteLogSegmentId().equals(segment.remoteLogSegmentId())
                    || !cached.records.channel().isOpen()) {
                cachedSegment = null;
                IOUtils.closeQuietly(cached.records, "FileLogRecords");
                return null;
            }
            if (!owner.registerOpenedFileLogRecords(cached.records)) {
                return null;
            }
            cachedSegment = null;
            return cached.records;
        }
    }

    private void releaseIterator(
            RemoteLogBatchIterator iterator,
            List<FileLogRecords> ownedRecords,
            @Nullable FileLogRecords currentRecords,
            @Nullable RemoteLogSegment currentSegment) {
        CachedSegment priorCached = null;
        synchronized (lifecycleLock) {
            if (activeIterator == iterator) {
                activeIterator = null;
            }
            if (!closed
                    && consumerMode == ConsumerMode.INDEX_RETAINED
                    && currentRecords != null
                    && currentSegment != null
                    && currentRecords.channel().isOpen()) {
                ownedRecords.remove(currentRecords);
                priorCached = cachedSegment;
                cachedSegment = new CachedSegment(currentSegment, currentRecords);
            }
        }
        if (priorCached != null) {
            IOUtils.closeQuietly(priorCached.records, "FileLogRecords");
        }
        closeFileLogRecords(ownedRecords);
    }

    private static void closeFileLogRecords(List<FileLogRecords> recordsToClose) {
        for (FileLogRecords records : recordsToClose) {
            IOUtils.closeQuietly(records, "FileLogRecords");
        }
    }

    private static final class CachedSegment {
        private final RemoteLogSegment segment;
        private final FileLogRecords records;

        private CachedSegment(RemoteLogSegment segment, FileLogRecords records) {
            this.segment = segment;
            this.records = records;
        }
    }
}
