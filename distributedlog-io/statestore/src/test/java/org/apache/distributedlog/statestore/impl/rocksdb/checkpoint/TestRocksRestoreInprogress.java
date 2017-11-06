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

import static org.apache.distributedlog.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.statestore.exceptions.StateStoreException;
import org.apache.distributedlog.statestore.impl.rocksdb.RocksdbKVStore;
import org.junit.Test;
import org.rocksdb.RocksDBException;

/**
 * Unit test for restore checkpoint inprogress.
 */
@Slf4j
public class TestRocksRestoreInprogress extends TestRocksdbCheckpointBase {

    private static final int NUM_PAIRS = 100;

    private File checkpointDir;
    private RocksCheckpoint checkpoint;

    @Override
    public void doSetup() throws Exception {
        super.doSetup();
        writeKvs(NUM_PAIRS);
        store.flush();

        File checkpointsDir = new File(baseDir, "checkpoints");
        assertTrue(checkpointsDir.mkdir());
        this.checkpointDir = new File(checkpointsDir, runtime.getMethodName());

        // create a checkpoint
        RocksCheckpointInprogress inprogress = new RocksCheckpointInprogress(
            dbCheckpointer,
            runtime.getMethodName(),
            checkpointDir,
            rocksFiles,
            ioScheduler);
        ioScheduler.submit(inprogress);
        checkpoint = result(inprogress.future());
        log.info("Saved a checkpoint at dir {} : {}", checkpointDir, checkpoint);
    }

    @Test
    public void testRestoreToNotExistDir() throws Exception {
        File restoreDir = new File(baseDir + "restore-" + runtime.getMethodName());
        RocksRestoreInprogress inprogress = new RocksRestoreInprogress(
            bk,
            checkpoint,
            restoreDir,
            ioScheduler);
        ioScheduler.submit(inprogress);
        try {
            result(inprogress.future());
        } catch (IOException ioe) {
            assertEquals("Dir " + restoreDir + " doesn't exist", ioe.getMessage());
        }
    }

    @Test
    public void testRestoreToEmptyDir() throws Exception {
        File restoreDir = new File(baseDir + "restore-" + runtime.getMethodName());
        restoreDir.mkdir();
        RocksRestoreInprogress inprogress = new RocksRestoreInprogress(
            bk,
            checkpoint,
            restoreDir,
            ioScheduler);
        ioScheduler.submit(inprogress);
        result(inprogress.future());

        verifyCheckpoint(restoreDir, checkpointDir, checkpoint);
        RocksdbKVStore<String, String> aStore = openStore(restoreDir);
        try {
            readKvsAndVerify(aStore, NUM_PAIRS);
        } finally {
            aStore.close();
        }
    }

    private Map<File, Long> getModifiedTimestamps(File dir) {
        Map<File, Long> modifiedTs = Maps.newHashMap();
        File[] files = dir.listFiles();
        for (File file : files) {
            modifiedTs.put(file, file.lastModified());
        }
        return modifiedTs;
    }

    @Test
    public void testRestoreToDirWithExtraFiles() throws Exception {
        File restoreDir = checkpointDir;
        Map<File, Long> modifiedTs = getModifiedTimestamps(restoreDir);

        for (int i = 0; i < 10; i++) {
            File file = new File(restoreDir, "extra-file-" + i);
            file.createNewFile();
            assertTrue(file.exists());
        }

        RocksRestoreInprogress inprogress = new RocksRestoreInprogress(
            bk,
            checkpoint,
            restoreDir,
            ioScheduler);
        ioScheduler.submit(inprogress);
        result(inprogress.future());

        for (int i = 0; i < 10; i++) {
            File file = new File(restoreDir, "extra-file-" + i);
            assertFalse(file.exists());
        }
        // get modification time again, no file should be touched.
        Map<File, Long> newModifiedTs = getModifiedTimestamps(restoreDir);
        assertTrue(Maps.difference(modifiedTs, newModifiedTs).areEqual());

        verifyCheckpoint(restoreDir, checkpointDir, checkpoint);
        RocksdbKVStore<String, String> aStore = openStore(restoreDir);
        try {
            readKvsAndVerify(aStore, NUM_PAIRS);
        } finally {
            aStore.close();
        }
    }

    @Test
    public void testRestoreToDirWithCorruptedFile() throws Exception {
        store.close();

        File restoreDir = checkpointDir;
        for (File file : restoreDir.listFiles()) {
            long oldLength = file.length();
            FileChannel out = new FileOutputStream(file, true).getChannel();
            out.truncate(oldLength / 2);
            out.force(true);
            out.close();
            assertEquals(oldLength / 2, file.length());
        }
        try {
            openStore(restoreDir);
            fail("Should fail to restart with corrupted files");
        } catch (StateStoreException e) {
            assertTrue(e.getCause() instanceof RocksDBException);
        }

        RocksRestoreInprogress inprogress = new RocksRestoreInprogress(
            bk,
            checkpoint,
            restoreDir,
            ioScheduler);
        ioScheduler.submit(inprogress);
        result(inprogress.future());

        verifyCheckpoint(restoreDir, checkpointDir, checkpoint);

        RocksdbKVStore<String, String> aStore = openStore(restoreDir);
        try {
            readKvsAndVerify(aStore, NUM_PAIRS);
        } finally {
            aStore.close();
        }
    }

}
