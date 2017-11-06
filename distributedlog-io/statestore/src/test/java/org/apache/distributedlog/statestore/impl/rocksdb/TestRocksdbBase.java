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

package org.apache.distributedlog.statestore.impl.rocksdb;

import static org.junit.Assert.assertEquals;

import com.google.common.io.Files;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.common.coder.StringUtf8Coder;
import org.apache.distributedlog.statestore.api.KVMulti;
import org.apache.distributedlog.statestore.api.StateStoreSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Test Base for rocksdb related tests.
 */
public abstract class TestRocksdbBase extends TestDistributedLogBase {

    @Rule
    public final TestName runtime = new TestName();

    // local db dirs
    protected File baseDir;
    protected File dbDir;
    protected StateStoreSpec spec;
    protected RocksdbKVStore<String, String> store;
    // bk settings
    protected ClientConfiguration clientConf;
    protected BookKeeper bk;
    // others
    protected ScheduledExecutorService ioScheduler;

    @Before
    public void setup() throws Exception {
        super.setup();
        // setup local db
        baseDir = Files.createTempDir();
        dbDir = new File(baseDir, "db");
        spec = StateStoreSpec.newBuilder()
            .name(runtime.getMethodName())
            .keyCoder(StringUtf8Coder.of())
            .valCoder(StringUtf8Coder.of())
            .localStateStoreDir(dbDir)
            .stream(runtime.getMethodName())
            .build();
        store = new RocksdbKVStore<>();
        store.init(spec);

        // setup bk
        clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkServers);
        bk = BookKeeper.newBuilder(clientConf).build();

        // io scheduler
        ioScheduler = Executors.newSingleThreadScheduledExecutor();

        // setup
        doSetup();
    }

    protected void doSetup() throws Exception {}

    @After
    public void teardown() throws Exception {
        doTeardown();
        if (null != ioScheduler) {
            ioScheduler.shutdown();
        }
        if (null != store) {
            store.close();
        }
        if (null != dbDir) {
            FileUtils.deleteDirectory(dbDir);
        }
        super.teardown();
    }

    protected void doTeardown() {}

    protected String getKey(int i) {
        return String.format("key-06d", i);
    }

    protected String getValue(int i) {
        return String.format("value-06d", i);
    }

    protected void writeKvs(int numPairs) {
        KVMulti<String, String> multi = store.multi();
        for (int i = 0; i < numPairs; i++) {
            multi.put(getKey(i), getValue(i));
        }
        multi.execute();
    }

    protected void readKvsAndVerify(RocksdbKVStore<String, String> db, int numPairs) {
        for (int i = 0; i < numPairs; i++) {
            String key = getKey(i);
            String value = db.get(key);
            assertEquals(getValue(i), value);
        }
    }

    protected static RocksdbKVStore<String, String> openStore(File dir) throws Exception {
        StateStoreSpec spec = StateStoreSpec.newBuilder()
            .name(dir.getName())
            .keyCoder(StringUtf8Coder.of())
            .valCoder(StringUtf8Coder.of())
            .localStateStoreDir(dir)
            .stream(dir.getName())
            .build();
        RocksdbKVStore<String, String> store = new RocksdbKVStore<>();
        store.init(spec);
        return store;
    }

}
