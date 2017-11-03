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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
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
 * Test case for {@link LedgerSaver} and {@link LedgerCopier}.
 */
@Slf4j
public class TestLedgerCopierSaver extends TestDistributedLogBase {

    @Rule
    public final TestName runtime = new TestName();

    protected ClientConfiguration clientConf;
    protected BookKeeper bk;
    protected File tempDir;
    protected ScheduledExecutorService ioScheduler;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        this.tempDir = Files.createTempDir();
        this.clientConf = new ClientConfiguration();
        this.clientConf.setZkServers(zkServers);
        this.bk = BookKeeper.newBuilder(clientConf).build();
        this.ioScheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (null != ioScheduler) {
            this.ioScheduler.shutdown();
        }
        if (null != bk) {
            this.bk.close();
        }
        if (null != tempDir) {
            FileUtils.deleteDirectory(tempDir);
        }
        super.teardown();
    }

    @Test
    public void testEmptyLedger() throws Exception {
        File emptyFile = new File(tempDir, runtime.getMethodName());
        assertTrue(emptyFile.createNewFile());

        LedgerCopier copier = new LedgerCopier(
            bk,
            emptyFile,
            ioScheduler,
            3,
            3,
            3);
        ioScheduler.submit(copier);
        RocksFileInfo info = result(copier.future());
        assertEquals(emptyFile.getName(), info.getName());
        assertEquals(-1L, info.getLedgerId());
        assertEquals(0L, info.getLength());

        // save the ledger
        File otherDir = new File(tempDir, "other");
        assertTrue(otherDir.mkdirs());

        LedgerSaver saver = new LedgerSaver(
            bk,
            info,
            otherDir,
            ioScheduler);
        ioScheduler.submit(saver);
        RocksFileInfo savedInfo = result(saver.future());
        assertEquals(info, savedInfo);
        File savedFile = new File(otherDir, info.getName());
        assertTrue(savedFile.exists());
        assertEquals(0L, savedFile.length());
    }

    private File createFileAndWriteContent() throws Exception {
        File file = new File(tempDir, runtime.getMethodName());
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

    private void readFileAndVerifyContent(File srcFile, File destFile) throws Exception {
        assertEquals(srcFile.length(), destFile.length());
        byte[] srcBuf = new byte[4096];
        byte[] destBuf = new byte[4096];
        InputStream srcIn = new FileInputStream(srcFile);
        InputStream destIn = new FileInputStream(destFile);
        long remainingBytes = srcFile.length();
        while (remainingBytes > 0) {
            int numBytesToRead = remainingBytes > 4096 ? 4096 : (int) remainingBytes;
            assertEquals(numBytesToRead, srcIn.read(srcBuf, 0, numBytesToRead));
            assertEquals(numBytesToRead, destIn.read(destBuf, 0, numBytesToRead));
            assertArrayEquals(srcBuf, destBuf);
            remainingBytes -= numBytesToRead;
        }
    }

    @Test
    public void testCopySave() throws Exception {
        File srcFile = createFileAndWriteContent();

        log.info("Copy file {} to bookkeeper", srcFile);

        LedgerCopier copier = new LedgerCopier(
            bk,
            srcFile,
            ioScheduler,
            3,
            3,
            3);
        ioScheduler.submit(copier);
        RocksFileInfo info = result(copier.future());
        assertEquals(srcFile.getName(), info.getName());
        assertTrue(info.getLedgerId() > -1L);
        assertEquals(srcFile.length(), info.getLength());

        log.info("Verified ledger {} at bookkeeper", info.getLedgerId());

        org.apache.bookkeeper.client.BookKeeper bkc =
            org.apache.bookkeeper.client.BookKeeper.forConfig(clientConf).build();
        LedgerHandle lh = bkc.openLedgerNoRecovery(
            info.getLedgerId(),
            DigestType.CRC32,
            new byte[0]);
        assertEquals(info.getLength(), lh.getLength());
        assertTrue(lh.isClosed());

        // save the ledger
        File otherDir = new File(tempDir, "other");
        assertTrue(otherDir.mkdirs());
        File savedFile = new File(otherDir, info.getName());
        assertFalse(savedFile.exists());

        log.info("Save ledger {} to file {}", info.getLedgerId(), savedFile);

        LedgerSaver saver = new LedgerSaver(
            bk,
            info,
            otherDir,
            ioScheduler);
        ioScheduler.submit(saver);
        RocksFileInfo savedInfo = result(saver.future());
        assertEquals(info, savedInfo);
        assertTrue(savedFile.exists());
        assertEquals(srcFile.length(), savedFile.length());

        log.info("Verified copied file {} with src file {}", savedFile, srcFile);

        readFileAndVerifyContent(srcFile, savedFile);
    }

}
