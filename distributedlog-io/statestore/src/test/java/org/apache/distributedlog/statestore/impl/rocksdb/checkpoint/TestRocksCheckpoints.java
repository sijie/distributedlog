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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.io.FileUtils;
import org.apache.distributedlog.statestore.exceptions.StateStoreException;
import org.apache.distributedlog.statestore.impl.rocksdb.RocksdbKVStore;
import org.junit.Test;
import org.rocksdb.RocksDBException;

/**
 * Unit test for checkpoints.
 */
public class TestRocksCheckpoints extends TestRocksdbCheckpointBase {

    private File checkpointDir;
    private RocksCheckpoints checkpoints;

    @Override
    protected void doSetup() throws Exception {
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

    private RocksCheckpoint checkpoint() throws Exception {
        return checkpoint(runtime.getMethodName());
    }

    private RocksCheckpoint checkpoint(String checkpointName) throws Exception {
        writeKvs(100);
        store.flush();

        RocksCheckpoint checkpoint = result(checkpoints.checkpoint(checkpointName));

        verifyCheckpoint(
            new File(checkpointDir, checkpointName),
            checkpoint);
        return checkpoint;
    }

    @Test
    public void testRestoreCheckpointToExistDir() throws Exception {
        checkpoint();
        File destDir = new File(baseDir, "restore-" + runtime.getMethodName());
        assertTrue(destDir.mkdir());
        try {
            result(checkpoints.restoreCheckpoint(runtime.getMethodName(), destDir));
            fail("Should fail to restore a checkpoint to an exist dir");
        } catch (IOException ioe) {
            assertEquals("Dir " + destDir + " already exists", ioe.getMessage());
        }
    }

    @Test
    public void testRestoreNotExistCheckpoint() throws Exception {
        File destDir = new File(baseDir, "restore-" + runtime.getMethodName());
        try {
            result(checkpoints.restoreCheckpoint(runtime.getMethodName() + "-1", destDir));
            fail("Should fail to restore a not exist checkpoint");
        } catch (IOException ioe) {
            assertEquals("Checkpoint " + runtime.getMethodName() + "-1 is not found", ioe.getMessage());
        }
    }

    @Test
    public void testRestoreCheckpointMissingLocalDir() throws Exception {
        RocksCheckpoint checkpoint = checkpoint();
        File srcDir = new File(checkpointDir, runtime.getMethodName());
        FileUtils.deleteDirectory(srcDir);
        File destDir = new File(baseDir, "restore-" + runtime.getMethodName());
        result(checkpoints.restoreCheckpoint(runtime.getMethodName(), destDir));
        verifyCheckpoint(
            destDir,
            new File(checkpointDir, runtime.getMethodName()),
            checkpoint);

        RocksdbKVStore<String, String> aStore = openStore(destDir);
        try {
            readKvsAndVerify(aStore, 100);
        } finally {
            aStore.close();
        }
    }

    @Test
    public void testRestoreCheckpoint() throws Exception {
        RocksCheckpoint checkpoint = checkpoint();
        File destDir = new File(baseDir, "restore-" + runtime.getMethodName());
        result(checkpoints.restoreCheckpoint(runtime.getMethodName(), destDir));
        verifyCheckpoint(
            destDir,
            new File(checkpointDir, runtime.getMethodName()),
            checkpoint);
        RocksdbKVStore<String, String> aStore = openStore(destDir);
        try {
            readKvsAndVerify(aStore, 100);
        } finally {
            aStore.close();
        }
    }

    @Test
    public void testRestoreCheckpointCorruptedLocalState() throws Exception {
        RocksCheckpoint checkpoint = checkpoint();
        store.close();

        File srcDir = new File(checkpointDir, runtime.getMethodName());
        for (File file : srcDir.listFiles()) {
            long oldLength = file.length();
            FileChannel out = new FileOutputStream(file, true).getChannel();
            out.truncate(oldLength / 2);
            out.force(true);
            out.close();
            assertEquals(oldLength / 2, file.length());
        }
        try {
            openStore(srcDir);
            fail("Should fail to restart with corrupted files");
        } catch (StateStoreException e) {
            assertTrue(e.getCause() instanceof RocksDBException);
        }

        File destDir = new File(baseDir, "restore-" + runtime.getMethodName());
        result(checkpoints.restoreCheckpoint(runtime.getMethodName(), destDir));
        verifyCheckpoint(
            destDir,
            new File(checkpointDir, runtime.getMethodName()),
            checkpoint);
        RocksdbKVStore<String, String> aStore = openStore(destDir);
        try {
            readKvsAndVerify(aStore, 100);
        } finally {
            aStore.close();
        }
    }

}
