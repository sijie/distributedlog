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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import org.junit.Test;

/**
 * Unit test of {@link RocksFiles}.
 */
public class TestRocksFiles extends TestRocksdbCheckpointBase {

    private RocksFiles sstFiles;

    @Override
    protected void doSetup() {
        this.sstFiles = new RocksFiles(
            bk,
            ioScheduler,
            3);
    }

    private File createFileAndWriteContent() throws Exception {
        File file = new File(baseDir, runtime.getMethodName() + ".sst");
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
