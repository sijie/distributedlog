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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

/**
 * Unit test for checkpoints.
 */
public class TestRocksCheckpoints extends TestRocksdbCheckpointBase {

    private File checkpointDir;
    private RocksCheckpoints checkpoints;

    @Override
    protected void doSetup() {
        super.doSetup();
        checkpointDir = new File(baseDir, "checkpoints");
        checkpointDir.mkdirs();
        checkpoints = new RocksCheckpoints(
            dbCheckpointer,
            checkpointDir,
            rocksFiles,
            ioScheduler);
    }

    @Test
    public void testCheckpoint() throws Exception {
        writeKvs(100);
        store.flush();

        String checkpointName = runtime.getMethodName();
        RocksCheckpoint checkpoint = result(checkpoints.checkpoint(checkpointName));

        verifyCheckpoint(
            new File(checkpointDir, checkpointName),
            checkpoint);
    }

    @Test
    public void testCheckpointTwiceSequential() throws Exception {
        String checkpointName = runtime.getMethodName();
        RocksCheckpoint checkpoint = result(checkpoints.checkpoint(checkpointName));
        assertEquals(checkpoint, checkpoints.getCompletedCheckpoint(checkpointName));
        verifyCheckpoint(
            new File(checkpointDir, checkpointName),
            checkpoint);
        RocksCheckpoint checkpoint2 = result(checkpoints.checkpoint(checkpointName));
        assertTrue(checkpoint == checkpoint2);
    }

    @Test
    public void testCheckpointTwiceInParallel() throws Exception {
        String checkpointName = runtime.getMethodName();
        CountDownLatch latch = new CountDownLatch(1);
        ioScheduler.submit(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
            }
        });
        CompletableFuture<RocksCheckpoint> future1 = checkpoints.checkpoint(checkpointName);
        CompletableFuture<RocksCheckpoint> future2 = checkpoints.checkpoint(checkpointName);
        latch.countDown();
        RocksCheckpoint checkpoint1 = result(future1);
        RocksCheckpoint checkpoint2 = result(future2);
        assertTrue(checkpoint1 == checkpoint2);
        verifyCheckpoint(
            new File(checkpointDir, checkpointName),
            checkpoint1);
    }

    @Test
    public void testDeleteNullCheckpoint() throws Exception {
        String checkpointName = runtime.getMethodName();
        result(checkpoints.deleteCheckpoint(checkpointName));
        assertNull(checkpoints.getCompletedCheckpoint(checkpointName));
        assertFalse(new File(checkpointDir, checkpointName).exists());
    }

    @Test
    public void testDeleteCheckpoint() throws Exception {
        writeKvs(100);
        store.flush();

        String checkpointName = runtime.getMethodName();
        RocksCheckpoint checkpoint = result(checkpoints.checkpoint(checkpointName));
        assertEquals(checkpoint, checkpoints.getCompletedCheckpoint(checkpointName));

        verifyCheckpoint(
            new File(checkpointDir, checkpointName),
            checkpoint);

        result(checkpoints.deleteCheckpoint(checkpointName));
        assertNull(checkpoints.getCompletedCheckpoint(checkpointName));
        assertFalse(new File(checkpointDir, checkpointName).exists());
    }

}
