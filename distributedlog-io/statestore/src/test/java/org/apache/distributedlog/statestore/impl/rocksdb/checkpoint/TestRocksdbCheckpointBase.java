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
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils;
import org.apache.distributedlog.statestore.impl.rocksdb.TestRocksdbBase;
import org.rocksdb.Checkpoint;

/**
 * Test Base for rocksdb checkpoint related test cases.
 */
public class TestRocksdbCheckpointBase extends TestRocksdbBase {

    protected Checkpoint dbCheckpointer;
    protected RocksFiles rocksFiles;

    @Override
    protected void doSetup() throws Exception {
        dbCheckpointer = Checkpoint.create(store.getDb());
        rocksFiles = new RocksFiles(
            bk,
            ioScheduler,
            3);
    }

    @Override
    protected void doTeardown() {
        RocksUtils.close(dbCheckpointer);
    }

    protected void verifyFileCopied(RocksFileInfo fi) throws Exception {
        org.apache.bookkeeper.client.BookKeeper bkc =
            org.apache.bookkeeper.client.BookKeeper.forConfig(clientConf).build();
        LedgerHandle lh = bkc.openLedgerNoRecovery(
            fi.getLedgerId(),
            DigestType.CRC32,
            new byte[0]);
        assertEquals(fi.getLength(), lh.getLength());
        assertTrue(lh.isClosed());
        bkc.close();
    }

    protected void verifyLedgerDeleted(RocksFileInfo fi) throws Exception {
        try {
            result(bk.newOpenLedgerOp()
                .withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32)
                .withLedgerId(fi.getLedgerId())
                .withPassword(new byte[0])
                .execute());
            fail("ledger " + fi.getLedgerId() + " should be deleted");
        } catch (BKNoSuchLedgerExistsException e) {
            assertTrue(true);
        }
    }

    protected void verifyCheckpoint(File checkpointDir, RocksCheckpoint checkpoint) throws Exception {
        verifyCheckpoint(checkpointDir, checkpointDir, checkpoint);
    }

    protected void verifyCheckpoint(File restoreDir,
                                    File checkpointDir,
                                    RocksCheckpoint checkpoint) throws Exception {
        File[] children = restoreDir.listFiles();
        assertEquals(children.length, checkpoint.getFiles().size());
        for (File file : children) {
            String name;
            if (file.getName().endsWith(".sst")) {
                name = file.getName();
            } else {
                name = checkpointDir.getName() + "/" + file.getName();
            }
            RocksFileInfo fi = checkpoint.getFiles().get(name);
            assertNotNull("File " + name + " doesn't exist", fi);
            assertEquals(name, fi.getName());
            assertEquals(file.length(), fi.getLength());
            assertTrue(fi.getLedgerId() > 0);
        }
    }

}
