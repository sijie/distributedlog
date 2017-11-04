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

import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.distributedlog.TestDistributedLogBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link RocksFiles}.
 */
public class TestRocksFiles extends TestDistributedLogBase {

    @Rule
    public final TestName runtime = new TestName();

    private ClientConfiguration clientConf;
    private BookKeeper bk;
    private File tempDir;
    private ScheduledExecutorService ioScheduler;
    private RocksFiles sstFiles;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        this.tempDir = Files.createTempDir();
        this.clientConf = new ClientConfiguration();
        this.clientConf.setZkServers(zkServers);
        this.bk = BookKeeper.newBuilder(clientConf).build();
        this.ioScheduler = Executors.newSingleThreadScheduledExecutor();
        this.sstFiles = new RocksFiles(
            bk,
            ioScheduler,
            3);
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (null != ioScheduler) {
            ioScheduler.shutdown();
        }
        if (null != bk) {
            this.bk.close();
        }
        if (null != tempDir) {
            FileUtils.deleteDirectory(tempDir);
        }
        super.teardown();
    }

    private File createFileAndWriteContent() throws Exception {
        File file = new File(tempDir, runtime.getMethodName() + ".sst");
        BufferedWriter bw = new BufferedWriter(
            new OutputStreamWriter(
                new FileOutputStream(file)
            )
        );
        for (int i = 0; i < 100; i++) {
            bw.write("line [" + i + "]: this is a test line");
            bw.newLine();
        }
        bw.flush();
        bw.close();
        return file;
    }

    private void verifyFileCopied(RocksFileInfo fi) throws Exception {
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

    private void verifyLedgerDeleted(RocksFileInfo fi) throws Exception {
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

    @Test
    public void testGetSstFileNotExists() throws Exception {
        String name = runtime.getMethodName() + ".sst";
        File file = createFileAndWriteContent();
        RocksFileInfo fi = result(sstFiles.copySstFile("ckpt", name, file));
        assertEquals(fi.getName(), name);
        assertEquals(fi.getLength(), file.length());
        assertTrue(fi.getLedgerId() > 0);
        verifyFileCopied(fi);

        RocksFileInfo deletedFi = result(sstFiles.deleteSstFile("ckpt", name));
        assertEquals(fi, deletedFi);
        verifyLedgerDeleted(deletedFi);
    }

    @Test
    public void testGetSstFileExists() throws Exception {
        // create the sst file
        String name = runtime.getMethodName() + ".sst";
        File file = createFileAndWriteContent();
        RocksFileInfo fi = result(sstFiles.copySstFile("checkpoint1", name, file));
        assertEquals(fi.getName(), name);
        assertEquals(fi.getLength(), file.length());
        assertTrue(fi.getLedgerId() > 0);
        assertEquals("" + fi, 1, fi.numLinks());
        verifyFileCopied(fi);
        assertNotNull(sstFiles.getSstFileInfo(name));

        // attempt to get the sst file
        RocksFileInfo getFi = result(sstFiles.copySstFile("checkpoint2", name, file));
        assertEquals(fi, getFi);
        assertEquals(2, fi.numLinks());

        // attempt to delete the sst file
        RocksFileInfo deleteFi = result(sstFiles.deleteSstFile("checkpoint1", name));
        assertEquals(fi, deleteFi);
        assertEquals(getFi, deleteFi);
        assertEquals(1, deleteFi.numLinks());
        verifyFileCopied(deleteFi);

        // delete the sst file again. the ledger is deleted when refCnt goes down to zero
        RocksFileInfo deleteFi2 = result(sstFiles.deleteSstFile("checkpoint2", name));
        assertEquals(deleteFi, deleteFi2);
        assertEquals(0, deleteFi2.numLinks());
        verifyLedgerDeleted(deleteFi2);
    }

}
