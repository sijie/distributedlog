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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.rocksdb.RocksDBException;

/**
 * Unit test for checkpoint inpgoress.
 */
@Slf4j
public class TestRocksCheckpointInprogress extends TestRocksdbCheckpointBase {

    @Test
    public void testCheckpoint() throws Exception {
        writeKvs(100);
        store.flush();

        File checkpointDir = new File(baseDir, "checkpoint-" + runtime.getMethodName());

        // checkpoint test
        {
            RocksCheckpointInprogress inprogress = new RocksCheckpointInprogress(
                dbCheckpointer,
                runtime.getMethodName(),
                checkpointDir,
                rocksFiles,
                ioScheduler);
            ioScheduler.submit(inprogress);
            RocksCheckpoint checkpoint = result(inprogress.future());

            verifyCheckpoint(checkpointDir, checkpoint);
        }

        // checkpoint twice
        {
            RocksCheckpointInprogress inprogress = new RocksCheckpointInprogress(
                dbCheckpointer,
                runtime.getMethodName(),
                checkpointDir,
                rocksFiles,
                ioScheduler);
            ioScheduler.submit(inprogress);
            try {
                result(inprogress.future());
                fail("should fail when a checkpoint is already created");
            } catch (IOException e) {
                // expected
                assertTrue(e.getCause() instanceof RocksDBException);
                RocksDBException rdbe = (RocksDBException) e.getCause();
                assertEquals("Directory exists", rdbe.getMessage());
            }
        }

    }

    @Test
    public void testCheckpointToDifferentDirs() throws Exception {
        writeKvs(100);
        store.flush();

        // checkpoint test
        String checkpointName1 = runtime.getMethodName() + "-1";
        RocksCheckpoint checkpoint1;
        String checkpointName2 = runtime.getMethodName() + "-2";
        RocksCheckpoint checkpoint2;
        {
            File checkpointDir = new File(baseDir, checkpointName1);
            RocksCheckpointInprogress inprogress = new RocksCheckpointInprogress(
                dbCheckpointer,
                checkpointName1,
                checkpointDir,
                rocksFiles,
                ioScheduler);
            ioScheduler.submit(inprogress);
            RocksCheckpoint checkpoint = result(inprogress.future());

            verifyCheckpoint(checkpointDir, checkpoint);
            checkpoint1 = checkpoint;
        }

        // checkpoint twice by into different directory
        {
            File checkpointDir = new File(baseDir, checkpointName2);
            RocksCheckpointInprogress inprogress = new RocksCheckpointInprogress(
                dbCheckpointer,
                checkpointName2,
                checkpointDir,
                rocksFiles,
                ioScheduler);
            ioScheduler.submit(inprogress);
            RocksCheckpoint checkpoint = result(inprogress.future());

            verifyCheckpoint(checkpointDir, checkpoint);
            checkpoint2 = checkpoint;
        }

        assertEquals(checkpoint1.getFiles().size(), checkpoint2.getFiles().size());
        // verify sst files
        for (RocksFileInfo fi1 : checkpoint1.getFiles().values()) {
            if (fi1.getName().endsWith(".sst")) {
                RocksFileInfo fi2 = checkpoint2.getFiles().get(fi1.getName());
                assertTrue(fi1.getLinks().contains(checkpointName1));
                assertTrue(fi2.getLinks().contains(checkpointName2));
                assertEquals(fi1, fi2);
            } else {
                String name = new File(fi1.getName()).getName();
                RocksFileInfo fi2 = checkpoint2.getFiles().get(checkpointName2 + "/" + name);
                assertNotNull("File " + name + " is null", fi2);
                assertEquals(fi1.getName(), fi1.getName());
                assertEquals(fi1.getLength(), fi2.getLength());
                assertTrue(fi1.getLedgerId() != fi2.getLedgerId());
                assertEquals(1, fi1.numLinks());
                assertEquals(1, fi2.numLinks());
                assertTrue(fi1.getLinks().contains(checkpointName1));
                assertTrue(fi2.getLinks().contains(checkpointName2));
            }
        }
    }

}
