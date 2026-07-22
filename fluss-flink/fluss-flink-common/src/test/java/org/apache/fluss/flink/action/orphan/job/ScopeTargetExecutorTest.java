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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScopeTargetExecutorTest {

    @Test
    void doesNotCloseResourcesBeforeInterruptIgnoringWorkerExits() throws Exception {
        Connection connection = mock(Connection.class);
        Admin inventoryAdmin = mock(Admin.class);
        Admin workerAdmin0 = mock(Admin.class);
        Admin workerAdmin1 = mock(Admin.class);
        when(connection.getAdmin()).thenReturn(inventoryAdmin);
        AtomicInteger adminIndex = new AtomicInteger();
        when(connection.createAdmin())
                .thenAnswer(
                        ignored -> adminIndex.getAndIncrement() == 0 ? workerAdmin0 : workerAdmin1);

        IOException primary = new IOException("primary");
        AtomicInteger invocation = new AtomicInteger();
        AtomicReference<Admin> stubbornAdmin = new AtomicReference<Admin>();
        CountDownLatch stubbornStarted = new CountDownLatch(1);
        CountDownLatch releaseStubborn = new CountDownLatch(1);
        CountDownLatch stubbornExited = new CountDownLatch(1);
        CountDownLatch stubbornAdminClosed = new CountDownLatch(1);
        CountDownLatch connectionClosed = new CountDownLatch(1);
        doAnswer(
                        invocationOnMock -> {
                            if (invocationOnMock.getMock() == stubbornAdmin.get()) {
                                stubbornAdminClosed.countDown();
                            }
                            return null;
                        })
                .when(workerAdmin0)
                .close();
        doAnswer(
                        invocationOnMock -> {
                            if (invocationOnMock.getMock() == stubbornAdmin.get()) {
                                stubbornAdminClosed.countDown();
                            }
                            return null;
                        })
                .when(workerAdmin1)
                .close();
        doAnswer(
                        ignored -> {
                            connectionClosed.countDown();
                            return null;
                        })
                .when(connection)
                .close();

        Admin retainedInventoryAdmin = connection.getAdmin();
        Throwable failure;
        try {
            failure =
                    catchThrowable(
                            () -> {
                                try (ScopeTargetExecutor executor =
                                        ScopeTargetExecutor.create(
                                                connection,
                                                2,
                                                admin ->
                                                        input -> {
                                                            if (invocation.getAndIncrement() == 0) {
                                                                stubbornStarted.await(
                                                                        10, TimeUnit.SECONDS);
                                                                throw primary;
                                                            }
                                                            stubbornAdmin.set(admin);
                                                            stubbornStarted.countDown();
                                                            try {
                                                                while (true) {
                                                                    try {
                                                                        if (releaseStubborn.await(
                                                                                10,
                                                                                TimeUnit
                                                                                        .MILLISECONDS)) {
                                                                            return emptyResult();
                                                                        }
                                                                    } catch (
                                                                            InterruptedException
                                                                                    ignored) {
                                                                        // Deliberately ignore
                                                                        // cancellation.
                                                                    }
                                                                }
                                                            } finally {
                                                                stubbornExited.countDown();
                                                            }
                                                        },
                                                25L)) {
                                    executor.forEachCompleted(nullInputs(2), ignored -> {});
                                }
                            });

            assertThat(failure).isSameAs(primary);
            assertThat(failure.getSuppressed())
                    .singleElement()
                    .isInstanceOfSatisfying(
                            IOException.class,
                            timeout ->
                                    assertThat(timeout)
                                            .hasMessageContaining(
                                                    "Timed out closing Scope target executor"));
            assertThat(stubbornExited.getCount()).isEqualTo(1L);
            verify(stubbornAdmin.get(), never()).close();
            verify(connection, never()).close();
        } finally {
            releaseStubborn.countDown();
        }

        assertThat(stubbornExited.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(stubbornAdminClosed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(connectionClosed.await(10, TimeUnit.SECONDS)).isTrue();
        verify(stubbornAdmin.get(), times(1)).close();
        verify(workerAdmin0, times(1)).close();
        verify(workerAdmin1, times(1)).close();
        verify(connection, times(1)).close();
        verify(inventoryAdmin, never()).close();
        assertThat(retainedInventoryAdmin).isSameAs(inventoryAdmin);
    }

    @Test
    void interruptedCloseDefersResourcesUntilWorkerExit() throws Exception {
        Connection connection = mock(Connection.class);
        Admin inventoryAdmin = mock(Admin.class);
        Admin workerAdmin = mock(Admin.class);
        when(connection.getAdmin()).thenReturn(inventoryAdmin);
        when(connection.createAdmin()).thenReturn(workerAdmin);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch workerExited = new CountDownLatch(1);
        CountDownLatch adminClosed = new CountDownLatch(1);
        CountDownLatch connectionClosed = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            adminClosed.countDown();
                            return null;
                        })
                .when(workerAdmin)
                .close();
        doAnswer(
                        ignored -> {
                            connectionClosed.countDown();
                            return null;
                        })
                .when(connection)
                .close();
        ScopeTargetEnumeration.Worker worker =
                input -> {
                    workerStarted.countDown();
                    try {
                        while (true) {
                            try {
                                if (releaseWorker.await(10, TimeUnit.MILLISECONDS)) {
                                    return emptyResult();
                                }
                            } catch (InterruptedException ignored) {
                                // Deliberately ignore cancellation.
                            }
                        }
                    } finally {
                        workerExited.countDown();
                    }
                };

        Admin retainedInventoryAdmin = connection.getAdmin();
        ScopeTargetExecutor executor =
                ScopeTargetExecutor.create(connection, 1, ignored -> worker, 25L);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        Future<?> enumeration =
                caller.submit(
                        () -> {
                            executor.forEachCompleted(nullInputs(1), ignored -> {});
                            return null;
                        });
        try {
            assertThat(workerStarted.await(10, TimeUnit.SECONDS)).isTrue();
            Thread.currentThread().interrupt();
            Throwable closeFailure = catchThrowable(executor::close);
            boolean interruptRestored = Thread.currentThread().isInterrupted();
            Thread.interrupted();

            assertThat(closeFailure).isInstanceOf(InterruptedException.class);
            assertThat(interruptRestored).isTrue();
            assertThat(workerExited.getCount()).isEqualTo(1L);
            verify(workerAdmin, never()).close();
            verify(connection, never()).close();
        } finally {
            Thread.interrupted();
            releaseWorker.countDown();
            caller.shutdown();
        }

        assertThat(workerExited.await(10, TimeUnit.SECONDS)).isTrue();
        enumeration.get(10, TimeUnit.SECONDS);
        assertThat(adminClosed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(connectionClosed.await(10, TimeUnit.SECONDS)).isTrue();
        verify(workerAdmin, times(1)).close();
        verify(connection, times(1)).close();
        verify(inventoryAdmin, never()).close();
        assertThat(retainedInventoryAdmin).isSameAs(inventoryAdmin);
    }

    @Test
    void productionFactoryOwnsOneDistinctAdminPerWorkerThread() throws Exception {
        Connection connection = mock(Connection.class);
        Admin inventoryAdmin = mock(Admin.class);
        Admin workerAdmin0 = mock(Admin.class);
        Admin workerAdmin1 = mock(Admin.class);
        AtomicInteger adminIndex = new AtomicInteger();
        when(connection.getAdmin()).thenReturn(inventoryAdmin);
        when(connection.createAdmin())
                .thenAnswer(
                        ignored -> adminIndex.getAndIncrement() == 0 ? workerAdmin0 : workerAdmin1);

        Admin retainedInventoryAdmin = connection.getAdmin();
        Set<Admin> workerAdmins = ConcurrentHashMap.<Admin>newKeySet();
        CountDownLatch twoStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (ScopeTargetExecutor executor =
                ScopeTargetExecutor.create(
                        connection,
                        2,
                        admin -> {
                            workerAdmins.add(admin);
                            return input -> {
                                twoStarted.countDown();
                                release.await(10, TimeUnit.SECONDS);
                                return emptyResult();
                            };
                        })) {
            Future<?> future =
                    caller.submit(
                            () -> {
                                executor.forEachCompleted(nullInputs(2), ignored -> {});
                                return null;
                            });
            assertThat(twoStarted.await(10, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            future.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            caller.shutdownNow();
        }

        assertThat(retainedInventoryAdmin).isSameAs(inventoryAdmin);
        assertThat(workerAdmins).containsExactlyInAnyOrder(workerAdmin0, workerAdmin1);
        verify(workerAdmin0, times(1)).close();
        verify(workerAdmin1, times(1)).close();
        verify(inventoryAdmin, never()).close();
        verify(connection, times(2)).createAdmin();
        verify(connection, times(1)).getAdmin();
        verify(connection, times(1)).close();
    }

    @Test
    void productionFactoryClosesAdminWhenWorkerConstructionFails() throws Exception {
        Connection connection = mock(Connection.class);
        Admin inventoryAdmin = mock(Admin.class);
        Admin workerAdmin = mock(Admin.class);
        IllegalStateException factoryFailure = new IllegalStateException("factory");
        IOException closeFailure = new IOException("close");
        when(connection.getAdmin()).thenReturn(inventoryAdmin);
        when(connection.createAdmin()).thenReturn(workerAdmin);
        doThrow(closeFailure).when(workerAdmin).close();

        Admin retainedInventoryAdmin = connection.getAdmin();
        Throwable failure =
                catchThrowable(
                        () -> {
                            try (ScopeTargetExecutor executor =
                                    ScopeTargetExecutor.create(
                                            connection,
                                            1,
                                            admin -> {
                                                assertThat(admin).isSameAs(workerAdmin);
                                                throw factoryFailure;
                                            })) {
                                executor.forEachCompleted(nullInputs(1), ignored -> {});
                            }
                        });

        assertThat(failure).isSameAs(factoryFailure);
        assertThat(failure.getSuppressed()).containsExactly(closeFailure);
        assertThat(retainedInventoryAdmin).isSameAs(inventoryAdmin);
        verify(workerAdmin, times(1)).close();
        verify(inventoryAdmin, never()).close();
        verify(connection, times(1)).createAdmin();
        verify(connection, times(1)).getAdmin();
        verify(connection, times(1)).close();
    }

    @Test
    void overlapsConfiguredTargetsAndReplaysEveryResultOnce() throws Exception {
        List<ScopeTargetEnumeration.Result> expected =
                Arrays.asList(emptyResult(), emptyResult(), emptyResult(), emptyResult());
        AtomicInteger nextResult = new AtomicInteger();
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        CyclicBarrier twoWorkers = new CyclicBarrier(2);

        ScopeTargetEnumeration.Worker worker =
                input -> {
                    ScopeTargetEnumeration.Result result =
                            expected.get(nextResult.getAndIncrement());
                    int active = running.incrementAndGet();
                    maximum.accumulateAndGet(active, Math::max);
                    try {
                        twoWorkers.await(10, TimeUnit.SECONDS);
                        return result;
                    } finally {
                        running.decrementAndGet();
                    }
                };
        List<ScopeTargetEnumeration.Result> completed =
                new ArrayList<ScopeTargetEnumeration.Result>();

        try (ScopeTargetExecutor executor = ScopeTargetExecutor.testing(2, () -> worker)) {
            executor.forEachCompleted(nullInputs(4), completed::add);
        }

        assertThat(completed).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(maximum.get()).isEqualTo(2);
    }

    @Test
    void invokesEveryResultConsumerOnCallingThread() throws Exception {
        Thread caller = Thread.currentThread();
        List<Thread> callbackThreads = new ArrayList<Thread>();

        try (ScopeTargetExecutor executor =
                ScopeTargetExecutor.testing(3, () -> input -> emptyResult())) {
            executor.forEachCompleted(
                    nullInputs(6), result -> callbackThreads.add(Thread.currentThread()));
        }

        assertThat(callbackThreads).hasSize(6).allMatch(thread -> thread == caller);
    }

    @Test
    void drainsResultsInCompletionOrder() throws Exception {
        ScopeTargetEnumeration.Result firstResult = emptyResult();
        ScopeTargetEnumeration.Result secondResult = emptyResult();
        AtomicInteger invocation = new AtomicInteger();
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        ScopeTargetEnumeration.Worker worker =
                input -> {
                    if (invocation.getAndIncrement() == 0) {
                        secondStarted.await(10, TimeUnit.SECONDS);
                        releaseFirst.await(10, TimeUnit.SECONDS);
                        return firstResult;
                    }
                    secondStarted.countDown();
                    return secondResult;
                };
        List<ScopeTargetEnumeration.Result> completed =
                new ArrayList<ScopeTargetEnumeration.Result>();

        try (ScopeTargetExecutor executor = ScopeTargetExecutor.testing(2, () -> worker)) {
            executor.forEachCompleted(
                    nullInputs(2),
                    result -> {
                        completed.add(result);
                        if (result == secondResult) {
                            releaseFirst.countDown();
                        }
                    });
        } finally {
            releaseFirst.countDown();
        }

        assertThat(completed).containsExactly(secondResult, firstResult);
    }

    @Test
    void concurrencyOnePreservesInputOrderAndOneWorkerContext() throws Exception {
        ScopeTargetEnumeration.Result first = emptyResult();
        ScopeTargetEnumeration.Result second = emptyResult();
        ScopeTargetEnumeration.Result third = emptyResult();
        List<ScopeTargetEnumeration.Result> workerResults =
                new ArrayList<ScopeTargetEnumeration.Result>(Arrays.asList(first, second, third));
        AtomicInteger workerCreations = new AtomicInteger();
        AtomicInteger nextResult = new AtomicInteger();
        List<ScopeTargetEnumeration.Result> completed =
                new ArrayList<ScopeTargetEnumeration.Result>();

        try (ScopeTargetExecutor executor =
                ScopeTargetExecutor.testing(
                        1,
                        () -> {
                            workerCreations.incrementAndGet();
                            return input -> workerResults.get(nextResult.getAndIncrement());
                        })) {
            executor.forEachCompleted(nullInputs(3), completed::add);
        }

        assertThat(completed).containsExactly(first, second, third);
        assertThat(workerCreations.get()).isEqualTo(1);
    }

    @Test
    void closesEveryCreatedWorkerContextExactlyOnce() throws Exception {
        ScopeTargetExecutor.WorkerContext context0 = mock(ScopeTargetExecutor.WorkerContext.class);
        ScopeTargetExecutor.WorkerContext context1 = mock(ScopeTargetExecutor.WorkerContext.class);
        AtomicInteger contextIndex = new AtomicInteger();
        AtomicInteger running = new AtomicInteger();
        CountDownLatch twoStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        ScopeTargetEnumeration.Worker worker =
                input -> {
                    running.incrementAndGet();
                    twoStarted.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                        return emptyResult();
                    } finally {
                        running.decrementAndGet();
                    }
                };
        when(context0.worker()).thenReturn(worker);
        when(context1.worker()).thenReturn(worker);

        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (ScopeTargetExecutor executor =
                ScopeTargetExecutor.testingContexts(
                        2, () -> contextIndex.getAndIncrement() == 0 ? context0 : context1)) {
            Future<?> future =
                    caller.submit(
                            () -> {
                                executor.forEachCompleted(nullInputs(2), ignored -> {});
                                return null;
                            });
            assertThat(twoStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(running.get()).isEqualTo(2);
            release.countDown();
            future.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            caller.shutdownNow();
        }

        verify(context0, times(1)).close();
        verify(context1, times(1)).close();
    }

    @Test
    void preservesPrimaryFailureAndSuppressesContextCloseFailure() throws Exception {
        IOException primary = new IOException("primary");
        IOException close = new IOException("close");
        ScopeTargetExecutor.WorkerContext context = mock(ScopeTargetExecutor.WorkerContext.class);
        when(context.worker())
                .thenReturn(
                        input -> {
                            throw primary;
                        });
        doThrow(close).when(context).close();

        Throwable failure =
                catchThrowable(
                        () -> {
                            try (ScopeTargetExecutor executor =
                                    ScopeTargetExecutor.testingContexts(1, () -> context)) {
                                executor.forEachCompleted(nullInputs(1), ignored -> {});
                            }
                        });

        assertThat(failure).isSameAs(primary);
        assertThat(failure.getSuppressed()).containsExactly(close);
        verify(context, times(1)).close();
    }

    @Test
    void replaysPartialResultBeforeRethrowingOriginalFailure() throws Exception {
        IOException primary = new IOException("primary");
        ScopeTargetEnumeration.Result partial = emptyResult();
        ScopeTargetEnumeration.Worker worker =
                input -> {
                    throw new ScopeTargetEnumeration.EnumerationException(partial, primary);
                };
        List<ScopeTargetEnumeration.Result> completed =
                new ArrayList<ScopeTargetEnumeration.Result>();

        Throwable failure =
                catchThrowable(
                        () -> {
                            try (ScopeTargetExecutor executor =
                                    ScopeTargetExecutor.testing(1, () -> worker)) {
                                executor.forEachCompleted(nullInputs(1), completed::add);
                            }
                        });

        assertThat(failure).isSameAs(primary);
        assertThat(completed).containsExactly(partial);
    }

    @Test
    void cancelsOutstandingWorkerAfterFatalFailure() throws Exception {
        IOException primary = new IOException("primary");
        AtomicInteger invocation = new AtomicInteger();
        CountDownLatch otherStarted = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        ScopeTargetEnumeration.Worker worker =
                input -> {
                    if (invocation.getAndIncrement() == 0) {
                        otherStarted.await(10, TimeUnit.SECONDS);
                        throw primary;
                    }
                    otherStarted.countDown();
                    try {
                        new CountDownLatch(1).await(10, TimeUnit.SECONDS);
                        return emptyResult();
                    } catch (InterruptedException expected) {
                        interrupted.countDown();
                        throw expected;
                    }
                };

        Throwable failure =
                catchThrowable(
                        () -> {
                            try (ScopeTargetExecutor executor =
                                    ScopeTargetExecutor.testing(2, () -> worker)) {
                                executor.forEachCompleted(nullInputs(2), ignored -> {});
                            }
                        });

        assertThat(failure).isSameAs(primary);
        assertThat(interrupted.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private static ScopeTargetEnumeration.Result emptyResult() {
        return ScopeTargetEnumeration.Result.empty(ScopeIdentity.global());
    }

    private static List<ScopeTargetEnumeration.Input> nullInputs(int count) {
        return new ArrayList<ScopeTargetEnumeration.Input>(
                Arrays.asList(new ScopeTargetEnumeration.Input[count]));
    }
}
