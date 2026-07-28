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
import org.apache.fluss.utils.ExponentialBackoff;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly;

/**
 * A utility class that fetches remote log segments and makes them available as {@link
 * FileLogRecords} for KV recovery and index replay. It downloads remote log data into a local
 * temporary directory using a UUID to avoid conflicts with other concurrent recovery operations.
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
 * <p>In {@link ConsumerMode#KV_STREAMING} mode segments are prefetched in a bounded sliding window
 * ({@code prefetchNum} slots, {@code downloadThreads} concurrent downloads). As the consumer
 * advances, consumed slots are freed and back-filled, overlapping network I/O with local
 * iteration. In {@link ConsumerMode#INDEX_RETAINED} mode segments are downloaded synchronously and
 * the most recently used segment may be cached across bounded fetches.
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
    private static final long DOWNLOAD_RETRY_BACKOFF_INITIAL_MS = 100L;
    private static final int DOWNLOAD_RETRY_BACKOFF_MULTIPLIER = 2;
    private static final long DOWNLOAD_RETRY_BACKOFF_MAX_MS = 5_000L;
    private static final double DOWNLOAD_RETRY_BACKOFF_JITTER = 0.25D;
    @VisibleForTesting static final int DOWNLOAD_MAX_RETRIES = 5;

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
    private final int prefetchNum;
    /** Non-null iff {@link #consumerMode} is {@link ConsumerMode#KV_STREAMING}. */
    @Nullable private final ExecutorService downloadExecutor;

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
                defaultTempDir(logTabletDir),
                consumerMode,
                1,
                1,
                true);
    }

    public RemoteLogFetcher(
            RemoteLogManager remoteLogManager,
            TableBucket tableBucket,
            File logTabletDir,
            int prefetchNum,
            int downloadThreads) {
        this(
                remoteLogManager,
                tableBucket,
                defaultTempDir(logTabletDir),
                ConsumerMode.KV_STREAMING,
                prefetchNum,
                downloadThreads,
                true);
    }

    @VisibleForTesting
    RemoteLogFetcher(RemoteLogManager remoteLogManager, TableBucket tableBucket, Path tempDir) {
        this(remoteLogManager, tableBucket, tempDir, ConsumerMode.KV_STREAMING, 1, 1, false);
    }

    @VisibleForTesting
    RemoteLogFetcher(
            RemoteLogManager remoteLogManager,
            TableBucket tableBucket,
            Path tempDir,
            int prefetchNum,
            int downloadThreads) {
        this(
                remoteLogManager,
                tableBucket,
                tempDir,
                ConsumerMode.KV_STREAMING,
                prefetchNum,
                downloadThreads,
                false);
    }

    private RemoteLogFetcher(
            RemoteLogManager remoteLogManager,
            TableBucket tableBucket,
            Path tempDir,
            ConsumerMode consumerMode,
            int prefetchNum,
            int downloadThreads,
            boolean scavengeAbandonedDirectories) {
        this.remoteLogManager = remoteLogManager;
        this.tableBucket = tableBucket;
        this.tempDir = tempDir.toAbsolutePath().normalize();
        this.consumerMode = consumerMode;
        this.prefetchNum = Math.max(1, prefetchNum);
        if (consumerMode == ConsumerMode.KV_STREAMING) {
            int threads = Math.max(1, Math.min(downloadThreads, this.prefetchNum));
            AtomicInteger threadIndex = new AtomicInteger();
            this.downloadExecutor =
                    Executors.newFixedThreadPool(
                            threads,
                            runnable -> {
                                Thread thread =
                                        new Thread(
                                                runnable,
                                                "remote-log-fetcher-download-"
                                                        + tableBucket.getTableId()
                                                        + "-"
                                                        + tableBucket.getBucket()
                                                        + "-"
                                                        + threadIndex.getAndIncrement());
                                thread.setDaemon(true);
                                return thread;
                            });
        } else {
            // Index replay performs bounded reads and caches the current segment across
            // fetches; async prefetching would fight with retained batch ownership.
            this.downloadExecutor = null;
        }
        synchronized (TEMP_DIR_OWNERSHIP_LOCK) {
            if (scavengeAbandonedDirectories) {
                scavengeAbandonedDirectories(this.tempDir.getParent());
            }
            ACTIVE_TEMP_DIRS.add(this.tempDir);
        }
    }

    private static Path defaultTempDir(File logTabletDir) {
        return logTabletDir
                .toPath()
                .resolve("tmp")
                .resolve(REMOTE_LOG_RECOVERY_DIR_PREFIX + UUID.randomUUID());
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
        RemoteLogBatchIterator iterator;
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
                    "Found first remote log metadata page with {} segments for table bucket {} "
                            + "from offset {} to localLogStartOffset {} (mode={}, prefetchNum={})",
                    firstPage.segments().size(),
                    tableBucket,
                    startOffset,
                    localLogStartOffset,
                    consumerMode,
                    prefetchNum);

            iterator =
                    new RemoteLogBatchIterator(
                            firstPage, startOffset, localLogStartOffset, maxBytes);
            this.activeIterator = iterator;
        }
        // Kick off the initial prefetch window so downloads start before the first advance().
        iterator.fillPrefetchWindow();
        return new FetchResult(iterator);
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
                shutdownDownloadExecutor();
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

    private void shutdownDownloadExecutor() {
        if (downloadExecutor == null) {
            return;
        }
        downloadExecutor.shutdownNow();
        boolean terminated = false;
        try {
            terminated = downloadExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn(
                    "Interrupted while waiting for remote log fetcher download executor termination for table bucket {}.",
                    tableBucket,
                    e);
        }
        if (!terminated) {
            LOG.warn(
                    "Download executor did not terminate within 1 second for table bucket {}. "
                            + "Proceeding with best-effort cleanup.",
                    tableBucket);
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

    @VisibleForTesting
    int getPrefetchNum() {
        return prefetchNum;
    }

    /**
     * Returns an immutable snapshot of in-flight prefetch futures in submission order. Must be
     * called on the consumer thread while it is not advancing the iterator.
     */
    @VisibleForTesting
    List<Future<File>> snapshotPrefetchFuturesForTest() {
        RemoteLogBatchIterator iterator = this.activeIterator;
        if (iterator == null) {
            return Collections.emptyList();
        }
        return iterator.snapshotPrefetchFuturesForTest();
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

        boolean success = false;
        try (InputStream inputStream = remoteLogStorage.fetchLogData(segment);
                OutputStream outputStream = Files.newOutputStream(localFile.toPath())) {
            IOUtils.copyBytes(inputStream, outputStream, false);
            success = true;
        } catch (RemoteStorageException e) {
            throw new IOException(
                    "Failed to download remote log segment: " + segment.remoteLogSegmentId(), e);
        } finally {
            // Most InputStreams don't honor Thread.interrupt() mid-read, so the copy may
            // succeed despite interruption. Treat interrupt-after-success as failure to
            // avoid leaving stale files in tempDir.
            if (!success || Thread.currentThread().isInterrupted()) {
                try {
                    Files.deleteIfExists(localFile.toPath());
                } catch (IOException cleanupException) {
                    LOG.warn(
                            "Failed to cleanup partial/interrupted local segment file {} for segment {}.",
                            localFile,
                            segment.remoteLogSegmentId(),
                            cleanupException);
                }
                // File was deleted above; throw instead of returning a dangling reference
                // so the retry layer or consumer sees a clear failure.
                if (success && Thread.currentThread().isInterrupted()) {
                    throw new IOException(
                            "Download completed but was interrupted for segment "
                                    + segment.remoteLogSegmentId());
                }
            }
        }
        return localFile;
    }

    private File downloadSegmentWithRetry(RemoteLogSegment segment) throws IOException {
        ExponentialBackoff backoff =
                new ExponentialBackoff(
                        DOWNLOAD_RETRY_BACKOFF_INITIAL_MS,
                        DOWNLOAD_RETRY_BACKOFF_MULTIPLIER,
                        DOWNLOAD_RETRY_BACKOFF_MAX_MS,
                        DOWNLOAD_RETRY_BACKOFF_JITTER);

        IOException lastException = null;
        for (int attempt = 0; attempt <= DOWNLOAD_MAX_RETRIES; attempt++) {
            try {
                return downloadSegment(segment);
            } catch (IOException e) {
                lastException = e;
                if (attempt == DOWNLOAD_MAX_RETRIES) {
                    break;
                }

                long retryDelayMs = backoff.backoff(attempt);
                LOG.warn(
                        "Failed to download remote log segment {} on attempt {}/{}. Retry after {} ms.",
                        segment.remoteLogSegmentId(),
                        attempt + 1,
                        DOWNLOAD_MAX_RETRIES + 1,
                        retryDelayMs,
                        e);

                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while retrying remote log segment download: "
                                    + segment.remoteLogSegmentId(),
                            interruptedException);
                }
            }
        }

        throw new IOException(
                "Failed to download remote log segment after retries: "
                        + segment.remoteLogSegmentId(),
                lastException);
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

    /** A remote segment whose download has been submitted to the prefetch pipeline. */
    private static final class PrefetchedSegment {
        private final RemoteLogSegment segment;
        private final Future<File> future;

        private PrefetchedSegment(RemoteLogSegment segment, Future<File> future) {
            this.segment = segment;
            this.future = future;
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
        /** The local .log file currently opened as {@link #currentFileLogRecords}. */
        @Nullable private File currentLocalFile;

        private final List<FileLogRecords> openedFileLogRecords = new ArrayList<>();
        private LogRecordBatch nextBatch;
        private boolean finished = false;
        private volatile boolean closed = false;
        private volatile StopReason stopReason = StopReason.NONE;
        @Nullable private RemoteLogSegment currentSegment;

        /**
         * FIFO window of in-flight prefetch downloads (KV_STREAMING only). The single metadata
         * walker feeds this queue, so the head is always the next segment to consume.
         */
        private final ArrayDeque<PrefetchedSegment> prefetchQueue = new ArrayDeque<>();

        /** True once the metadata walker has passed {@code localLogStartOffset} or ran dry. */
        private boolean noMoreSegments = false;

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

        private boolean prefetchEnabled() {
            return downloadExecutor != null;
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
            drainPrefetchQueue();
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
                    closeStreamingSegment(currentFileLogRecords, currentLocalFile);
                }
                currentFileLogRecords = null;
                currentSegment = null;
                currentBatchIterator = null;
                currentLocalFile = null;

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

                // move to next segment: from the prefetch pipeline in streaming mode, or from
                // the metadata walker directly in retained mode.
                RemoteLogSegment segment;
                if (prefetchEnabled()) {
                    fillPrefetchWindow();
                    PrefetchedSegment head = prefetchQueue.peekFirst();
                    if (head == null) {
                        finished = true;
                        stopReason = StopReason.END;
                        return;
                    }
                    segment = head.segment;
                    // Defensive: the walker enqueued it before consumption advanced this far.
                    if (segment.remoteLogEndOffset() <= currentOffset) {
                        prefetchQueue.removeFirst();
                        if (!head.future.cancel(true)) {
                            cleanupPrefetchedEntry(head);
                        }
                        continue;
                    }
                    if (segment.remoteLogStartOffset() >= localLogStartOffset) {
                        drainPrefetchQueue();
                        finished = true;
                        stopReason = StopReason.END;
                        return;
                    }
                } else {
                    segment = nextAcceptedSegment();
                    if (segment == null) {
                        finished = true;
                        stopReason = StopReason.END;
                        return;
                    }
                }

                try {
                    FileLogRecords openedRecords = null;
                    File localFile = null;
                    if (consumerMode == ConsumerMode.INDEX_RETAINED) {
                        openedRecords = acquireCachedSegment(this, segment);
                    }
                    if (openedRecords == null) {
                        localFile =
                                prefetchEnabled()
                                        ? takePrefetchedFile(segment)
                                        : downloadSegment(segment);
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
                    currentLocalFile = localFile;
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
                    // Ensure resources are cleaned up if an exception occurs during segment
                    // loading. After a failed segment the fetch cannot continue, so drop the
                    // entire prefetch window as well.
                    drainPrefetchQueue();
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

        /**
         * The single metadata walker: returns the next segment passing the skip rules, paging in
         * more metadata as needed, or {@code null} when the range is exhausted.
         */
        @Nullable
        private RemoteLogSegment nextAcceptedSegment() {
            if (noMoreSegments) {
                return null;
            }
            while (true) {
                if (currentSegmentIndex >= segments.size() && !loadNextMetadataPage()) {
                    noMoreSegments = true;
                    return null;
                }
                RemoteLogSegment segment = segments.get(currentSegmentIndex++);
                // Remote segment ends are exclusive.
                if (segment.remoteLogEndOffset() <= currentOffset) {
                    continue;
                }
                // segments that start at or after localLogStartOffset end the range
                if (segment.remoteLogStartOffset() >= localLogStartOffset) {
                    noMoreSegments = true;
                    return null;
                }
                return segment;
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

        /**
         * Submit as many download tasks as fit in the window. Non-blocking: if the window is full
         * we stop and wait for the consumer to advance.
         */
        void fillPrefetchWindow() {
            if (!prefetchEnabled() || closed) {
                return;
            }
            while (prefetchQueue.size() < prefetchNum) {
                RemoteLogSegment segment = nextAcceptedSegment();
                if (segment == null) {
                    return;
                }
                final RemoteLogSegment target = segment;
                Future<File> future;
                try {
                    // submit(Callable) returns a FutureTask whose cancel(true) interrupts the
                    // worker thread, unlike CompletableFuture which only flips state.
                    future =
                            downloadExecutor.submit(
                                    (Callable<File>) () -> downloadSegmentWithRetry(target));
                } catch (Throwable submitError) {
                    // The executor rejected the task (e.g. we're shutting down). Rewind the
                    // walker so the segment is not lost, then stop refilling.
                    currentSegmentIndex--;
                    LOG.debug(
                            "Failed to submit prefetch for segment {} (executor likely shutting down).",
                            target.remoteLogSegmentId(),
                            submitError);
                    return;
                }
                prefetchQueue.addLast(new PrefetchedSegment(target, future));
                LOG.debug(
                        "Prefetching remote log segment {} for bucket {} (window size={}, free slots={}).",
                        target.remoteLogSegmentId(),
                        tableBucket,
                        prefetchQueue.size(),
                        prefetchNum - prefetchQueue.size());
            }
        }

        /**
         * Obtain the local file for {@code segment} from the prefetch window head. On success the
         * head is consumed and a refill is triggered, so the window stays full as long as more
         * segments remain.
         */
        private File takePrefetchedFile(RemoteLogSegment segment) throws IOException {
            PrefetchedSegment head = prefetchQueue.pollFirst();
            if (head == null || !isSameSegmentId(segment, head.segment)) {
                // Window head mismatch — drain the window and fail fast rather than consume
                // the wrong file.
                if (head != null) {
                    cleanupPrefetchedEntry(head);
                }
                drainPrefetchQueue();
                throw new IOException(
                        "Prefetch window head mismatch: requested "
                                + segment.remoteLogSegmentId()
                                + " but head is "
                                + (head == null ? "null" : head.segment.remoteLogSegmentId()));
            }
            try {
                return head.future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "Interrupted while waiting for remote log segment download: "
                                + segment.remoteLogSegmentId(),
                        e);
            } catch (CancellationException e) {
                if (closed) {
                    throw new IOException(
                            "Remote log fetcher closed while downloading segment "
                                    + segment.remoteLogSegmentId(),
                            e);
                }
                LOG.warn(
                        "Prefetched segment {} was cancelled, fallback to sync download.",
                        segment.remoteLogSegmentId(),
                        e);
                return downloadSegmentWithRetry(segment);
            } catch (ExecutionException e) {
                LOG.warn(
                        "Prefetched segment {} failed even after async retries.",
                        segment.remoteLogSegmentId(),
                        e.getCause());
                throw new IOException(
                        "Failed to download remote log segment after async retries: "
                                + segment.remoteLogSegmentId(),
                        e.getCause());
            } finally {
                // Window slot released — try to submit the next prefetch right away.
                fillPrefetchWindow();
            }
        }

        private boolean isSameSegmentId(RemoteLogSegment left, RemoteLogSegment right) {
            return left != null
                    && right != null
                    && left.remoteLogSegmentId().equals(right.remoteLogSegmentId());
        }

        /** Cancel or clean up all entries in the window, leaving it empty. */
        private void drainPrefetchQueue() {
            PrefetchedSegment entry;
            while ((entry = prefetchQueue.pollFirst()) != null) {
                if (entry.future.isDone()) {
                    if (!cleanupPrefetchedEntry(entry)) {
                        return;
                    }
                } else {
                    if (!entry.future.cancel(true) && !cleanupPrefetchedEntry(entry)) {
                        return;
                    }
                }
            }
        }

        /**
         * Retrieve and delete the file from a completed future. Returns {@code false} if
         * interrupted (caller should bail out).
         */
        private boolean cleanupPrefetchedEntry(PrefetchedSegment entry) {
            try {
                // isDone() is true so get() will not block; it returns either the
                // successfully downloaded file, or throws Cancellation/Execution.
                cleanupUnusedPrefetchedFile(entry.future.get());
            } catch (CancellationException | ExecutionException ignored) {
                // no local file to clean up
            } catch (InterruptedException e) {
                // Interrupt came from elsewhere; restore flag and bail out. close()'s
                // shutdownNow path will reclaim remaining slots.
                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        }

        private void cleanupUnusedPrefetchedFile(@Nullable File prefetchedFile) {
            if (prefetchedFile == null) {
                return;
            }
            try {
                Files.deleteIfExists(prefetchedFile.toPath());
            } catch (IOException cleanupException) {
                LOG.warn(
                        "Failed to cleanup unused prefetched segment file {} for table bucket {}.",
                        prefetchedFile,
                        tableBucket,
                        cleanupException);
            }
        }

        private synchronized boolean registerOpenedFileLogRecords(FileLogRecords records) {
            if (closed) {
                return false;
            }
            openedFileLogRecords.add(records);
            return true;
        }

        private void closeStreamingSegment(FileLogRecords records, @Nullable File localFile) {
            boolean owned;
            synchronized (this) {
                owned = openedFileLogRecords.remove(records);
            }
            if (owned) {
                IOUtils.closeQuietly(records, "FileLogRecords");
            }
            // Delete the consumed segment file eagerly to bound disk usage during recovery.
            if (localFile != null) {
                try {
                    Files.deleteIfExists(localFile.toPath());
                } catch (IOException e) {
                    LOG.warn("Failed to delete consumed segment file {}", localFile, e);
                }
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

        List<Future<File>> snapshotPrefetchFuturesForTest() {
            List<Future<File>> snapshot = new ArrayList<>(prefetchQueue.size());
            for (PrefetchedSegment entry : prefetchQueue) {
                snapshot.add(entry.future);
            }
            return Collections.unmodifiableList(snapshot);
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
