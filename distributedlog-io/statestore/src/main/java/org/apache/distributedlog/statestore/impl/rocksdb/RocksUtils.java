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

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.statestore.proto.AbortedCheckpointCommand;
import org.apache.distributedlog.statestore.proto.AbortingCheckpointCommand;
import org.apache.distributedlog.statestore.proto.BeginCheckpointCommand;
import org.apache.distributedlog.statestore.proto.CheckpointCommand;
import org.apache.distributedlog.statestore.proto.CommitCheckpointCommand;
import org.apache.distributedlog.statestore.proto.CopyFileCommand;
import org.apache.distributedlog.statestore.proto.DeletedCheckpointCommand;
import org.apache.distributedlog.statestore.proto.DeletingCheckpointCommand;
import org.apache.distributedlog.statestore.proto.FileCommand;
import org.apache.distributedlog.statestore.proto.NopCommand;
import org.apache.distributedlog.statestore.proto.RocksFileInfo;
import org.rocksdb.AbstractImmutableNativeReference;

/**
 * Utils for interacting with rocksdb classes.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RocksUtils {

    public static CheckpointCommand NOP_CMD = CheckpointCommand.newBuilder()
        .setNopCmd(NopCommand.newBuilder().build())
        .build();

    public static void close(AbstractImmutableNativeReference ref) {
        if (null == ref) {
            return;
        }
        ref.close();
    }

    public static ByteBuf newLogRecordBuf(CheckpointCommand command) throws IOException {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(command.getSerializedSize());
        try {
            command.writeTo(new ByteBufOutputStream(buf));
        } catch (IOException e) {
            buf.release();
            throw e;
        }
        return buf;
    }

    public static CheckpointCommand newCheckpointCommand(ByteBuf recordBuf) throws IOException {
        try {
            return CheckpointCommand.parseFrom(recordBuf.nioBuffer());
        } catch (InvalidProtocolBufferException e) {
            log.error("Found a corrupted record on replaying log stream", e);
            throw new IOException("Found a corrupted record on replaying log stream", e);
        }
    }

    public static CheckpointCommand newBeginCheckpointCommand(String checkpointName,
                                                              File[] files) {
        BeginCheckpointCommand.Builder cmdBuilder = BeginCheckpointCommand.newBuilder();
        for (File file : files) {
            String name;
            if (file.getName().endsWith(".sst")) {
                name = file.getName();
            } else {
                name = checkpointName + "/" + file.getName();
            }
            cmdBuilder.addFiles(name);
        }
        return CheckpointCommand.newBuilder()
            .setCheckpointName(checkpointName)
            .setBeginCmd(cmdBuilder)
            .build();
    }

    public static CheckpointCommand newCommitCheckpointCommand(String checkpointName) {
        return CheckpointCommand.newBuilder()
            .setCheckpointName(checkpointName)
            .setCommitCmd(CommitCheckpointCommand.newBuilder().build())
            .build();
    }

    public static CheckpointCommand newAbortingCheckpointCommand(String checkpointName) {
        return CheckpointCommand.newBuilder()
            .setCheckpointName(checkpointName)
            .setAbortingCmd(AbortingCheckpointCommand.newBuilder().build())
            .build();
    }

    public static CheckpointCommand newAbortedCheckpointCommand(String checkpointName) {
        return CheckpointCommand.newBuilder()
            .setCheckpointName(checkpointName)
            .setAbortedCmd(AbortedCheckpointCommand.newBuilder().build())
            .build();
    }

    public static CheckpointCommand newDeletingCheckpointCommand(String checkpointName) {
        return CheckpointCommand.newBuilder()
            .setCheckpointName(checkpointName)
            .setDeletingCmd(DeletingCheckpointCommand.newBuilder().build())
            .build();
    }

    public static CheckpointCommand newDeletedCheckpointCommand(String checkpointName) {
        return CheckpointCommand.newBuilder()
            .setCheckpointName(checkpointName)
            .setDeletedCmd(DeletedCheckpointCommand.newBuilder().build())
            .build();
    }

    public static CheckpointCommand newFileCommand(String checkpointName,
                                                   String fileName,
                                                   long ledgerId,
                                                   FileCommand.State state) {
        return CheckpointCommand.newBuilder()
            .setCheckpointName(checkpointName)
            .setFileCmd(FileCommand.newBuilder()
                .setFi(RocksFileInfo.newBuilder()
                    .setFileName(fileName)
                    .setLedgerId(ledgerId))
                .setState(state))
            .build();
    }

}
