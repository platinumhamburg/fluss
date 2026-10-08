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
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Manages the lazy lifecycle state machine for a {@link KvTablet}. Encapsulates all state
 * transitions (LAZY → OPENING → OPEN → RELEASING → LAZY), pin management, open/release/drop logic,
 * and callback orchestration.
 *
 * <p>This class is only instantiated for lazy-mode tablets. EAGER-mode tablets do not use this
 * class.
 */
public final class KvTabletLazyLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(KvTabletLazyLifecycle.class);

    /** Coarsen access timestamp updates to reduce volatile writes on hot path. */
    private static final long ACCESS_TIMESTAMP_GRANULARITY_MS = 1000;

    // ---- Lazy lifecycle states ----

    /** Internal RocksDB lazy lifecycle states. */
    enum LazyState {
        /** Lazy mode: RocksDB not yet opened. */
        LAZY,
        /** Lazy mode: RocksDB is being opened by another thread. */
        OPENING,
        /** Lazy mode: RocksDB is open and serving requests. */
        OPEN,
        /** Lazy mode: RocksDB is being released (closing without deleting data). */
        RELEASING,
        /** Lazy mode: last open attempt failed, in backoff cooldown. */
        FAILED,
        /** Closing: new requests are fenced while outstanding work drains. */
        CLOSING,
        /**
         * Terminal state: tablet is closed and will not be reopened. Local data may or may not have
         * been deleted depending on whether close or drop was called.
         */
        CLOSED
    }

    // ---- Callback interfaces ----

    /**
     * Callback for the actual RocksDB open work. Provided by Replica since it requires access to
     * snapshot context, log recovery, etc.
     */
    @FunctionalInterface
    public interface OpenCallback {
        /** Performs the open and returns the opened RocksDB KvTablet instance. */
        KvTablet doOpen(boolean hasLocalData) throws Exception;
    }

    /**
     * Callback invoked after a successful open commit. Used by Replica to install the tablet
     * reference and start periodic snapshot.
     */
    @FunctionalInterface
    public interface OpenCommitCallback {
        /** Called after RocksDB state has been installed into the sentinel. */
        void onOpenCommitted(KvTablet kvTablet);
    }

    /** Callback for releasing (closing) RocksDB without destroying local data. */
    @FunctionalInterface
    public interface ReleaseCallback {
        /** Performs release-time cleanup (e.g. stop snapshots, unregister metrics). */
        void doRelease(KvTablet kvTablet);
    }

    /** Callback for dropping (destroying) a KvTablet and cleaning up associated resources. */
    @FunctionalInterface
    public interface DropCallback {
        /** Performs drop-time cleanup (e.g. unregister metrics, stop snapshots). */
        void doDrop(KvTablet kvTablet);
    }

    // ---- The owning tablet ----

    private final KvTablet tablet;

    // ---- State machine fields ----

    private volatile LazyState lazyState = LazyState.LAZY;

    /** Lock guarding lazy state transitions. */
    private final ReentrantLock lazyStateLock = new ReentrantLock();

    private final Condition lazyStateChanged = lazyStateLock.newCondition();

    /** Active pin count — prevents release while operations are in-flight. */
    private final AtomicInteger activePins = new AtomicInteger(0);

    private volatile boolean rejectNewPins;

    /** Generation counter for fencing stale open results. */
    private long openGeneration;

    /** Semaphore for throttling concurrent open operations across all tablets. */
    private @Nullable Semaphore openSemaphore;

    private long openTimeoutMs;
    private long releaseDrainTimeoutMs;
    private @Nullable ExponentialBackoff failedBackoff;
    private Clock clock = SystemClock.getInstance();

    /** FAILED state tracking. */
    private long failedTimestamp;

    private int failureCount;
    private @Nullable Throwable lastFailureCause;

    /** Cached values served when RocksDB is not open. */
    private volatile long cachedRowCount;

    private volatile long cachedFlushedLogOffset;

    /** Whether local RocksDB data directory exists from a previous open. */
    @GuardedBy("lazyStateLock")
    private boolean hasLocalData;

    /** Access timestamp for idle release decisions (coarsened to reduce volatile writes). */
    private volatile long lastAccessTimestamp;

    /** Epoch suppliers for fencing stale open results. */
    private @Nullable IntSupplier leaderEpochSupplier;

    private @Nullable IntSupplier bucketEpochSupplier;

    /** Supplier for tablet directory path — used by sentinel in LAZY/FAILED state. */
    private @Nullable Supplier<File> tabletDirSupplier;

    /** Callbacks (set by Replica after construction). */
    private @Nullable OpenCallback openCallback;

    private @Nullable OpenCommitCallback commitCallback;
    private @Nullable DropCallback dropCallback;
    private @Nullable ReleaseCallback releaseCallback;

    /**
     * Includes open callbacks, candidate cleanup and idle release cleanup. Guarded by the state
     * lock.
     */
    private boolean operationInFlight;

    /** Retains ownership if cleaning up an uncommitted open fails. */
    private @Nullable KvTablet failedOpenTablet;

    /** Guarded by the termination monitor; prevents repeated drops from deleting a reused path. */
    private boolean localDirectoryDeleted;

    KvTabletLazyLifecycle(KvTablet tablet) {
        this.tablet = tablet;
    }

    // ---- Configuration (called by Replica after sentinel creation) ----

    /**
     * Configure lazy open parameters. Must be called before any {@link #acquireGuard()} calls.
     *
     * @param clock the clock for time-based operations
     * @param openSemaphore semaphore to limit concurrent open operations (can be null)
     * @param openTimeoutMs timeout for open operations in milliseconds
     * @param failedBackoffBaseMs base backoff time for failed opens in milliseconds
     * @param failedBackoffMaxMs max backoff time for failed opens in milliseconds
     * @param releaseDrainTimeoutMs timeout for draining pins during release in milliseconds
     */
    public void configureLazyOpen(
            Clock clock,
            Semaphore openSemaphore,
            long openTimeoutMs,
            long failedBackoffBaseMs,
            long failedBackoffMaxMs,
            long releaseDrainTimeoutMs) {
        this.clock = clock;
        this.openSemaphore = openSemaphore;
        this.openTimeoutMs = openTimeoutMs;
        this.failedBackoff = new ExponentialBackoff(failedBackoffBaseMs, 2, failedBackoffMaxMs, 0);
        this.releaseDrainTimeoutMs = releaseDrainTimeoutMs;
        // Register initial LAZY state in metrics
        updateStateGauges(null, LazyState.LAZY);
    }

    /**
     * Sets the callback responsible for performing the actual RocksDB open operation. This is
     * provided by Replica since it requires access to snapshot context, log recovery, etc.
     *
     * @param callback the open callback to invoke when transitioning from LAZY to OPEN
     */
    public void setOpenCallback(OpenCallback callback) {
        this.openCallback = callback;
    }

    /**
     * Sets the callback invoked after a successful open commit. Used by Replica to install the
     * tablet reference and start periodic snapshot.
     *
     * @param callback the commit callback to invoke after RocksDB state is installed
     */
    public void setCommitCallback(OpenCommitCallback callback) {
        this.commitCallback = callback;
    }

    /**
     * Sets the callback for full-state cleanup when the tablet is dropped. Handles all states,
     * waits for in-progress operations, then transitions to CLOSED.
     *
     * @param callback the drop callback to invoke during tablet deletion
     */
    public void setDropCallback(DropCallback callback) {
        this.dropCallback = callback;
    }

    /**
     * Sets the callback for releasing (closing) RocksDB without destroying local data. Called by
     * the idle release controller when transitioning from OPEN to LAZY.
     *
     * @param callback the release callback to invoke during idle release
     */
    public void setReleaseCallback(ReleaseCallback callback) {
        this.releaseCallback = callback;
    }

    /**
     * Sets the supplier for the tablet directory path. Used by sentinel in LAZY/FAILED state for
     * cleanup operations.
     *
     * @param supplier the directory supplier
     */
    public void setTabletDirSupplier(Supplier<File> supplier) {
        this.tabletDirSupplier = supplier;
    }

    /** Returns the tablet directory from the supplier, or null if not configured. */
    @Nullable
    File getTabletDir() {
        return tabletDirSupplier != null ? tabletDirSupplier.get() : null;
    }

    /**
     * Sets the supplier for the leader epoch used in fencing stale open operations.
     *
     * @param supplier the leader epoch supplier
     */
    public void setLeaderEpochSupplier(IntSupplier supplier) {
        this.leaderEpochSupplier = supplier;
    }

    /**
     * Sets the supplier for the bucket epoch used in fencing stale open operations.
     *
     * @param supplier the bucket epoch supplier
     */
    public void setBucketEpochSupplier(IntSupplier supplier) {
        this.bucketEpochSupplier = supplier;
    }

    /**
     * Initializes the cached row count from snapshot metadata. Called during sentinel creation to
     * serve read queries while in LAZY state before the first open. Also sets the sentinel's
     * rowCount field so that OPENING state reads return the correct value.
     *
     * @param rowCount the initial row count (-1 if unknown/disabled)
     */
    public void initCachedRowCount(long rowCount) {
        this.cachedRowCount = rowCount;
        tablet.setRowCount(rowCount);
    }

    // ---- State queries ----

    /** Returns whether the tablet is currently in OPEN state with RocksDB loaded and serving. */
    boolean isOpen() {
        return lazyState == LazyState.OPEN;
    }

    /** Returns the current lifecycle state of this lazy tablet. */
    LazyState getLazyState() {
        return lazyState;
    }

    /**
     * Returns the cached row count from the last flush or release operation. This value may be
     * stale but is sufficient for serving read queries while in LAZY state.
     */
    long getCachedRowCount() {
        return cachedRowCount;
    }

    /**
     * Returns the flushed log offset cached from the last flush or release operation. Used to
     * determine how much incremental log needs to be replayed when reopening from local data.
     */
    long getCachedFlushedLogOffset() {
        return cachedFlushedLogOffset;
    }

    /**
     * Returns the timestamp (in milliseconds) of the last access to this tablet. Used by the idle
     * release controller to determine eligibility for release.
     */
    long getLastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    /**
     * Returns the number of active pins that are currently preventing RocksDB from being released.
     * Each pin corresponds to a {@link KvTablet.Guard} held by an in-flight operation.
     */
    int getActivePins() {
        return activePins.get();
    }

    /**
     * Returns true if this tablet is in LAZY or FAILED state (RocksDB not open), meaning
     * KvTablet.getRowCount() should return the cached value instead of querying RocksDB.
     */
    boolean needsCachedRowCount() {
        LazyState s = lazyState;
        return s == LazyState.LAZY || s == LazyState.FAILED;
    }

    // ---- Guard / Pin management ----

    /**
     * Acquire a guard that prevents RocksDB from being released while held. Ensures RocksDB is open
     * (blocking if necessary) and pins it.
     *
     * <p>Must be called OUTSIDE any Replica-level locks (e.g. leaderIsrUpdateLock) to avoid
     * blocking leader transitions during slow opens.
     *
     * @throws IllegalStateException if the lifecycle has not been fully configured via {@link
     *     #configureLazyOpen} and callback setters
     */
    KvTablet.Guard acquireGuard() {
        // Defensive check: ensure all required callbacks are set before allowing access
        if (openCallback == null) {
            throw new IllegalStateException(
                    "KvTabletLazyLifecycle not fully configured: openCallback is null. "
                            + "Ensure configureLazyOpen() and setOpenCallback() are called before use.");
        }

        // Fast path: try to pin without blocking
        KvTablet.Guard fast = tryAcquirePinAsGuard();
        if (fast != null) {
            touchAccessTimestamp();
            return fast;
        }

        // Slow path: ensure open, then pin
        ensureOpen();
        // Between ensureOpen() returning and pin, a concurrent release could happen.
        // Retry up to 3 times.
        for (int attempt = 0; ; attempt++) {
            lazyStateLock.lock();
            try {
                if (lazyState == LazyState.OPEN && !rejectNewPins) {
                    activePins.incrementAndGet();
                    touchAccessTimestamp();
                    return new KvTablet.Guard(tablet);
                }
                if (lazyState == LazyState.CLOSED || lazyState == LazyState.CLOSING) {
                    throw new KvStorageException(
                            "KvTablet is closed for " + tablet.getTableBucket());
                }
            } finally {
                lazyStateLock.unlock();
            }
            if (attempt >= 2) {
                throw new KvStorageException(
                        "Failed to pin KvTablet after open for " + tablet.getTableBucket());
            }
            // Re-open and retry
            ensureOpen();
        }
    }

    /**
     * Try to acquire a guard only if RocksDB is already OPEN.
     *
     * <p>Unlike {@link #acquireGuard()}, this method never triggers a lazy open. It is intended for
     * background maintenance paths (flush, snapshot init) that must coordinate with idle release
     * but should not reopen a lazy tablet on their own.
     */
    @Nullable
    KvTablet.Guard tryAcquireExistingGuard() {
        return tryAcquirePinAsGuard();
    }

    /**
     * Fast-path pin attempt. Returns Guard if OPEN and accepting pins; null otherwise.
     *
     * <p>Safety relies on the double-check pattern: we optimistically increment the pin count, then
     * re-verify the state. If the state changed concurrently (e.g., release started), the re-check
     * fails and we undo the increment. This is safe on all architectures (x86, ARM) regardless of
     * volatile read ordering between independent variables.
     */
    @Nullable
    private KvTablet.Guard tryAcquirePinAsGuard() {
        if (lazyState == LazyState.OPEN && !rejectNewPins) {
            activePins.incrementAndGet();
            if (lazyState == LazyState.OPEN && !rejectNewPins) {
                return new KvTablet.Guard(tablet);
            }
            decrementAndMaybeSignal();
        }
        return null;
    }

    /** Release a pin. Called by Guard.close(). */
    void releasePin() {
        decrementAndMaybeSignal();
    }

    private void decrementAndMaybeSignal() {
        if (activePins.decrementAndGet() == 0) {
            if (rejectNewPins) {
                lazyStateLock.lock();
                try {
                    lazyStateChanged.signalAll();
                } finally {
                    lazyStateLock.unlock();
                }
            }
        }
    }

    private void touchAccessTimestamp() {
        long now = clock.milliseconds();
        if (now - lastAccessTimestamp > ACCESS_TIMESTAMP_GRANULARITY_MS) {
            lastAccessTimestamp = now;
        }
    }

    /**
     * Update state gauge metrics on state transitions. Only LAZY, OPEN, FAILED are tracked as
     * "stable" states; OPENING, RELEASING, CLOSED fall into default (no-op).
     */
    private void updateStateGauges(@Nullable LazyState from, LazyState to) {
        TabletServerMetricGroup metricGroup = tablet.serverMetricGroup;
        if (metricGroup == null) {
            return;
        }
        if (from != null) {
            switch (from) {
                case LAZY:
                    metricGroup.kvTabletLazyCount().decrementAndGet();
                    break;
                case OPEN:
                    metricGroup.kvTabletOpenCount().decrementAndGet();
                    break;
                case FAILED:
                    metricGroup.kvTabletFailedCount().decrementAndGet();
                    break;
                default:
                    break;
            }
        }
        switch (to) {
            case LAZY:
                metricGroup.kvTabletLazyCount().incrementAndGet();
                break;
            case OPEN:
                metricGroup.kvTabletOpenCount().incrementAndGet();
                break;
            case FAILED:
                metricGroup.kvTabletFailedCount().incrementAndGet();
                break;
            default:
                break;
        }
    }

    // ---- Open management ----

    /**
     * Ensures RocksDB is open. If already OPEN, returns immediately. If LAZY or FAILED (past
     * cooldown), triggers a slow-path open. If OPENING or RELEASING, blocks until state changes.
     */
    private void ensureOpen() {
        lazyStateLock.lock();
        try {
            while (true) {
                switch (lazyState) {
                    case OPEN:
                        return;

                    case OPENING:
                        if (!lazyStateChanged.await(openTimeoutMs, TimeUnit.MILLISECONDS)) {
                            throw new KvStorageException(
                                    "KvTablet open timed out for " + tablet.getTableBucket());
                        }
                        continue;

                    case RELEASING:
                        if (!lazyStateChanged.await(openTimeoutMs, TimeUnit.MILLISECONDS)) {
                            throw new KvStorageException(
                                    "KvTablet open timed out waiting for release: "
                                            + tablet.getTableBucket());
                        }
                        continue;

                    case FAILED:
                        long elapsed = clock.milliseconds() - failedTimestamp;
                        long cooldown = failedBackoff.backoff(failureCount);
                        if (elapsed < cooldown) {
                            throw new KvStorageException(
                                    "KvTablet open failed for "
                                            + tablet.getTableBucket()
                                            + ", remaining cooldown: "
                                            + (cooldown - elapsed)
                                            + " ms",
                                    lastFailureCause);
                        }
                        // Fall through to LAZY — cooldown expired, retry open.

                    case LAZY:
                        transitionTo(LazyState.OPENING);
                        operationInFlight = true;
                        openGeneration++;
                        long myGeneration = openGeneration;
                        int myLeaderEpoch =
                                leaderEpochSupplier != null ? leaderEpochSupplier.getAsInt() : -1;
                        int myBucketEpoch =
                                bucketEpochSupplier != null ? bucketEpochSupplier.getAsInt() : -1;
                        boolean localDataExists = hasLocalData;
                        lazyStateLock.unlock();
                        try {
                            doSlowOpen(myGeneration, myLeaderEpoch, myBucketEpoch, localDataExists);
                        } catch (Throwable t) {
                            // doSlowOpen completes candidate cleanup before publishing failure.
                            if (t instanceof RuntimeException) {
                                throw (RuntimeException) t;
                            }
                            throw new KvStorageException(
                                    "KvTablet open failed for " + tablet.getTableBucket(), t);
                        }
                        return;

                    case CLOSING:
                    case CLOSED:
                        throw new KvStorageException(
                                "KvTablet is closed for " + tablet.getTableBucket());

                    default:
                        throw new IllegalStateException("Unexpected state: " + lazyState);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KvStorageException(
                    "Interrupted waiting for KvTablet open: " + tablet.getTableBucket(), e);
        } finally {
            if (lazyStateLock.isHeldByCurrentThread()) {
                lazyStateLock.unlock();
            }
        }
    }

    private void doSlowOpen(
            long myGeneration, int myLeaderEpoch, int myBucketEpoch, boolean localDataExists)
            throws Exception {
        KvTablet candidate = null;
        boolean committed = false;
        boolean semaphoreAcquired = false;
        Throwable failure = null;
        boolean cleanupFailed = false;
        try {
            if (openSemaphore != null) {
                semaphoreAcquired = openSemaphore.tryAcquire(openTimeoutMs, TimeUnit.MILLISECONDS);
                if (!semaphoreAcquired) {
                    throw new KvStorageException(
                            "KvTablet open semaphore timeout for " + tablet.getTableBucket());
                }
            }
            candidate = openCallback.doOpen(localDataExists);
            lazyStateLock.lock();
            try {
                int leaderEpoch = leaderEpochSupplier != null ? leaderEpochSupplier.getAsInt() : -1;
                int bucketEpoch = bucketEpochSupplier != null ? bucketEpochSupplier.getAsInt() : -1;
                if (lazyState != LazyState.OPENING
                        || openGeneration != myGeneration
                        || leaderEpoch != myLeaderEpoch
                        || bucketEpoch != myBucketEpoch) {
                    throw new CancellationException(
                            "open result fenced by concurrent epoch/generation change");
                }
                tablet.installRocksDB(candidate);
                committed = true;
                transitionTo(LazyState.OPEN);
                failureCount = 0;
                lastAccessTimestamp = clock.milliseconds();
                rejectNewPins = false;
                lazyStateChanged.signalAll();
            } finally {
                lazyStateLock.unlock();
            }
            try {
                if (commitCallback != null) {
                    commitCallback.onOpenCommitted(tablet);
                }
            } catch (Exception e) {
                LOG.warn("Post-open commit callback failed for {}", tablet.getTableBucket(), e);
            }
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            try {
                if (!committed && candidate != null) {
                    candidate.close();
                    File dir = candidate.getKvTabletDir();
                    if (dir != null) {
                        FileUtils.deleteDirectory(dir);
                    }
                }
            } catch (Exception | Error e) {
                cleanupFailed = true;
                if (failure != null) {
                    failure.addSuppressed(e);
                } else {
                    failure = e;
                    throw e;
                }
            } finally {
                if (semaphoreAcquired) {
                    openSemaphore.release();
                }
                lazyStateLock.lock();
                try {
                    if (cleanupFailed) {
                        failedOpenTablet = candidate;
                        rejectNewPins = true;
                        transitionTo(LazyState.CLOSING);
                    } else if (!committed && lazyState != LazyState.CLOSING) {
                        transitionToFailed(failure, failure instanceof CancellationException);
                    }
                    operationInFlight = false;
                    lazyStateChanged.signalAll();
                } finally {
                    lazyStateLock.unlock();
                }
            }
        }
    }

    /** Must be called while holding {@code lazyStateLock}. */
    private void transitionTo(LazyState state) {
        updateStateGauges(lazyState, state);
        lazyState = state;
    }

    /** Must be called while holding {@code lazyStateLock}. */
    private void transitionToFailed(Throwable cause, boolean resetCount) {
        transitionTo(LazyState.FAILED);
        failedTimestamp = clock.milliseconds();
        if (resetCount) {
            failureCount = 0;
        } else {
            failureCount++;
        }
        lastFailureCause = cause;
        lazyStateChanged.signalAll();
    }

    // ---- Release logic (idle release by KvManager) ----

    /** Pre-check for idle release eligibility. */
    boolean canRelease(long closeIdleIntervalMs, long nowMs) {
        if (lazyState != LazyState.OPEN) {
            return false;
        }
        if (activePins.get() > 0) {
            return false;
        }
        return nowMs - lastAccessTimestamp >= closeIdleIntervalMs;
    }

    /**
     * Release RocksDB resources: OPEN -> RELEASING -> LAZY. Caches metadata before close for
     * serving queries while in LAZY state.
     *
     * @return true if release succeeded, false if aborted
     */
    boolean releaseKv() {
        lazyStateLock.lock();
        try {
            if (lazyState != LazyState.OPEN || operationInFlight) {
                return false;
            }
            rejectNewPins = true;
            transitionTo(LazyState.RELEASING);
            operationInFlight = true;
            if (!drainPins()
                    || lazyState == LazyState.CLOSING
                    || tablet.getFlushedLogOffset() < tablet.logTablet.localLogEndOffset()
                    || tablet.hasActiveResourceLeases()) {
                if (lazyState == LazyState.RELEASING) {
                    transitionBackToOpen();
                }
                operationInFlight = false;
                lazyStateChanged.signalAll();
                return false;
            }
            cachedRowCount = tablet.currentRowCount();
            cachedFlushedLogOffset = tablet.getFlushedLogOffset();
        } finally {
            lazyStateLock.unlock();
        }

        boolean releaseSucceeded = false;
        boolean nativeCloseStarted = false;
        try {
            if (releaseCallback != null) {
                releaseCallback.doRelease(tablet);
            }
            nativeCloseStarted = true;
            tablet.detachRocksDB();
            releaseSucceeded = true;
        } catch (Exception e) {
            LOG.warn("Failed to release KV tablet {}", tablet.getTableBucket(), e);
        } finally {
            lazyStateLock.lock();
            try {
                if (lazyState == LazyState.RELEASING) {
                    if (releaseSucceeded) {
                        transitionTo(LazyState.LAZY);
                        hasLocalData = true;
                        rejectNewPins = false;
                    } else if (!nativeCloseStarted) {
                        transitionBackToOpen();
                    } else {
                        transitionTo(LazyState.CLOSING);
                    }
                }
                operationInFlight = false;
                lazyStateChanged.signalAll();
            } finally {
                lazyStateLock.unlock();
            }
        }
        return releaseSucceeded;
    }

    /** Close resources and remove local data only after outstanding operations finish. */
    void dropKvLazy() {
        terminate(KvCloseMode.DISCARD_UNPERSISTED_STATE, true);
    }

    void closeKvLazy(KvCloseMode closeMode) {
        terminate(closeMode, false);
    }

    /** Serializes close callers without holding the state lock during callbacks or native I/O. */
    private synchronized void terminate(KvCloseMode closeMode, boolean deleteDirectory) {
        if (lazyState == LazyState.CLOSED) {
            if (deleteDirectory && !localDirectoryDeleted) {
                if (dropCallback != null) {
                    dropCallback.doDrop(tablet);
                }
                tablet.deleteLocalDirectory();
                localDirectoryDeleted = true;
            }
            return;
        }
        lazyStateLock.lock();
        try {
            rejectNewPins = true;
            openGeneration++;
            transitionTo(LazyState.CLOSING);
            lazyStateChanged.signalAll();
            // A request timeout cannot transfer ownership of native resources or the local path.
            while (operationInFlight || activePins.get() > 0) {
                lazyStateChanged.awaitUninterruptibly();
            }
        } finally {
            lazyStateLock.unlock();
        }

        if (deleteDirectory) {
            if (dropCallback != null) {
                dropCallback.doDrop(tablet);
            }
        } else if (releaseCallback != null) {
            releaseCallback.doRelease(tablet);
        }
        if (failedOpenTablet != null) {
            try {
                failedOpenTablet.close();
                File dir = failedOpenTablet.getKvTabletDir();
                if (dir != null) {
                    FileUtils.deleteDirectory(dir);
                }
                failedOpenTablet = null;
            } catch (Exception e) {
                throw new KvStorageException(
                        "Failed to clean up fenced open for " + tablet.getTableBucket(), e);
            }
        }
        tablet.detachRocksDB(closeMode);
        if (deleteDirectory) {
            tablet.deleteLocalDirectory();
            localDirectoryDeleted = true;
        }
        lazyStateLock.lock();
        try {
            transitionTo(LazyState.CLOSED);
            hasLocalData = false;
            failureCount = 0;
            failedTimestamp = 0;
            lastFailureCause = null;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }
    }

    private boolean drainPins() {
        long deadline = clock.milliseconds() + releaseDrainTimeoutMs;
        while (activePins.get() > 0) {
            long remaining = deadline - clock.milliseconds();
            if (remaining <= 0) {
                return false;
            }
            try {
                lazyStateChanged.await(remaining, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    /** Must be called while holding {@code lazyStateLock}. */
    private void transitionBackToOpen() {
        transitionTo(LazyState.OPEN);
        rejectNewPins = false;
        lazyStateChanged.signalAll();
    }

    // ---- Test helpers ----

    @VisibleForTesting
    void setLazyStateForTesting(LazyState state) {
        lazyStateLock.lock();
        try {
            this.lazyState = state;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }
    }

    @VisibleForTesting
    void setLastAccessTimestampForTesting(long timestamp) {
        this.lastAccessTimestamp = timestamp;
    }

    @VisibleForTesting
    void setClockForTesting(Clock clock) {
        this.clock = clock;
    }
}
