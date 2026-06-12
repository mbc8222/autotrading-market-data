package com.autotrading.autotradingmarketdata.raw;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 고빈도 WS 행을 비차단으로 받아 배치 적재로 비우는 바운디드 버퍼 (모놀리스 이식).
 * WS 수신 스레드는 {@link #offer}만(절대 차단 안 함), 전용 워커가 {@link #drain}으로 batch 적재한다.
 * 상한 초과 시 drop(누적 카운트) — DB가 못 따라와도 WS 수신/연결을 막지 않는다.
 */
public abstract class BatchBuffer<T> {

    private final Logger log = LogManager.getLogger(getClass());

    private final Queue<T> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicLong dropped = new AtomicLong(0);
    private final AtomicLong offered = new AtomicLong(0);
    private final int max;

    protected BatchBuffer(int max) {
        this.max = max;
    }

    /** 비차단 적재. 상한 초과 시 drop. */
    public void offer(T item) {
        if (count.get() >= max) {
            long d = dropped.incrementAndGet();
            if (d == 1 || d % 100_000 == 0) {
                log.warn("[{}] 버퍼 상한({}) 초과 — drop(누적 {}). DB 적재 지연 의심.",
                        getClass().getSimpleName(), max, d);
            }
            return;
        }
        queue.add(item);
        count.incrementAndGet();
        offered.incrementAndGet();
    }

    /** 최대 {@code limit}건을 꺼낸다(배치 크기 제한). */
    public List<T> drain(int limit) {
        List<T> out = new ArrayList<>(Math.min(limit, Math.max(0, count.get())));
        T item;
        while (out.size() < limit && (item = queue.poll()) != null) {
            count.decrementAndGet();
            out.add(item);
        }
        return out;
    }

    public int size() {
        return count.get();
    }

    /** 버퍼 상한(용량). */
    public int capacity() {
        return max;
    }

    public long droppedCount() {
        return dropped.get();
    }

    public long offeredCount() {
        return offered.get();
    }
}
