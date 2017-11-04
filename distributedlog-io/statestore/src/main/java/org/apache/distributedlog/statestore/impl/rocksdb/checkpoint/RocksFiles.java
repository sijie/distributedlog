/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.statestore.impl.rocksdb.checkpoint;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Manage all the rocksdb files.
 */
class RocksFiles {

    private final BookKeeper bk;
    private final ScheduledExecutorService executor;
    private final int numReplicas;

    private final Map<String, RocksFileInfo> completedSstFiles;
    private final Map<String, CompletableFuture<RocksFileInfo>> inprogressSstFiles;

    RocksFiles(BookKeeper bk,
               ScheduledExecutorService executor,
               int numReplicas) {
        this.bk = bk;
        this.executor = executor;
        this.numReplicas = numReplicas;
        this.completedSstFiles = Collections.synchronizedMap(Maps.newHashMap());
        this.inprogressSstFiles = Collections.synchronizedMap(Maps.newHashMap());
    }

    @VisibleForTesting
    synchronized RocksFileInfo getSstFileInfo(String name) {
        return completedSstFiles.get(name);
    }

    CompletableFuture<RocksFileInfo> copySstFile(String checkpointName, String name, File file) {
        final CompletableFuture<RocksFileInfo> future;
        synchronized (this) {
            RocksFileInfo fileInfo = getSstFileInfo(name);
            if (null != fileInfo) {
                fileInfo.link(checkpointName);
                return FutureUtils.value(fileInfo);
            }
            // no sst file is available, schedule a copy task
            CompletableFuture<RocksFileInfo> localFuture = inprogressSstFiles.get(name);
            if (null != localFuture) {
                return localFuture.thenApply(f -> {
                    f.link(checkpointName);
                    return f;
                });
            }
            future = FutureUtils.createFuture();
            inprogressSstFiles.put(name, future);
        }

        LedgerCopier copier = new LedgerCopier(
            bk,
            name,
            file,
            executor,
            numReplicas,
            numReplicas,
            numReplicas);
        executor.submit(copier);
        copier.future().whenCompleteAsync(new FutureEventListener<RocksFileInfo>() {
            @Override
            public void onSuccess(RocksFileInfo fileInfo) {
                synchronized (RocksFiles.this) {
                    completedSstFiles.put(name, fileInfo);
                    inprogressSstFiles.remove(name, future);
                }
                future.complete(fileInfo);
            }

            @Override
            public void onFailure(Throwable throwable) {
                synchronized (RocksFiles.this) {
                    inprogressSstFiles.remove(name, future);
                }
                future.completeExceptionally(throwable);
            }
        }, executor);
        return future.thenApply(f -> {
            f.link(checkpointName);
            return f;
        });
    }

    CompletableFuture<RocksFileInfo> deleteSstFile(String checkpointName, String name) {
        synchronized (this) {
            RocksFileInfo fileInfo = getSstFileInfo(name);
            if (null != fileInfo) {
                fileInfo.unlink(checkpointName);
                if (fileInfo.getLinks().isEmpty()) {
                    return deleteSstFile(fileInfo);
                }
            }
            return FutureUtils.value(fileInfo);
        }
    }

    private CompletableFuture<RocksFileInfo> deleteSstFile(RocksFileInfo fi) {
        return bk
            .newDeleteLedgerOp()
            .withLedgerId(fi.getLedgerId())
            .execute()
            .thenApply(ignored -> fi);
    }



}
