package com.alibaba.fluss.server.log;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.predicate.Predicate;

import java.util.Map;

import static com.alibaba.fluss.server.log.FetchParams.DEFAULT_MAX_WAIT_MS;
import static com.alibaba.fluss.server.log.FetchParams.DEFAULT_MIN_FETCH_BYTES;

/** Builder of FetchParams. */
public final class FetchParamsBuilder {
    private int replicaId;
    private boolean fetchOnlyLeader;
    private int maxFetchBytes;
    private Map<Long, Predicate> tableLooseFilterMap;
    private int minFetchBytes;
    private long maxWaitMs;

    public FetchParamsBuilder(int replicaId, int maxFetchBytes) {
        this.replicaId = replicaId;
        this.maxFetchBytes = maxFetchBytes;
        this.minFetchBytes = DEFAULT_MIN_FETCH_BYTES;
        this.maxWaitMs = DEFAULT_MAX_WAIT_MS;
    }

    @VisibleForTesting
    public FetchParamsBuilder withFetchOnlyLeader(boolean fetchOnlyLeader) {
        this.fetchOnlyLeader = fetchOnlyLeader;
        return this;
    }

    public FetchParamsBuilder withTableLooseFilterMap(Map<Long, Predicate> tableLooseFilterMap) {
        this.tableLooseFilterMap = tableLooseFilterMap;
        return this;
    }

    public FetchParamsBuilder withMinFetchBytes(int minFetchBytes) {
        this.minFetchBytes = minFetchBytes;
        return this;
    }

    public FetchParamsBuilder withMaxWaitMs(long maxWaitMs) {
        this.maxWaitMs = maxWaitMs;
        return this;
    }

    public FetchParams build() {
        return new FetchParams(
                replicaId,
                fetchOnlyLeader,
                maxFetchBytes,
                minFetchBytes,
                maxWaitMs,
                tableLooseFilterMap);
    }
}
