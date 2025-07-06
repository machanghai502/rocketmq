/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

public class RunningFlags {

    // 00000000 00000000 00000000 00000001
    // 不可读
    private static final int NOT_READABLE_BIT = 1;

    // 无符号左移1位
    //  00000000 00000000 00000000 00000010
    // 不可写
    private static final int NOT_WRITEABLE_BIT = 1 << 1;

    // 无符号左移2位
    // 00000000 00000000 00000000 00000100
    // 写 queue 错误
    private static final int WRITE_LOGICS_QUEUE_ERROR_BIT = 1 << 2;

    // 无符号左移3位
    // 00000000 00000000 00000000 00001000
    // 写 index 错误
    private static final int WRITE_INDEX_FILE_ERROR_BIT = 1 << 3;

    // 无符号左移4位
    // 00000000 00000000 00000000 00010000
    // 磁盘已满
    private static final int DISK_FULL_BIT = 1 << 4;

    // 几种状态组合标记位，如果是对应的状态，对应的位设置成1
    private volatile int flagBits = 0;

    public RunningFlags() {
    }

    public int getFlagBits() {
        return flagBits;
    }

    public boolean getAndMakeReadable() {
        boolean result = this.isReadable();
        if (!result) {
            this.flagBits &= ~NOT_READABLE_BIT;
        }
        return result;
    }

    public boolean isReadable() {
        // 00000000 & 00000001 == 000000000
        if ((this.flagBits & NOT_READABLE_BIT) == 0) {
            return true;
        }

        return false;
    }

    public boolean getAndMakeNotReadable() {
        boolean result = this.isReadable();
        if (result) {
            this.flagBits |= NOT_READABLE_BIT;
        }
        return result;
    }

    public boolean getAndMakeWriteable() {
        boolean result = this.isWriteable();
        if (!result) {
            this.flagBits &= ~NOT_WRITEABLE_BIT;
        }
        return result;
    }

    public boolean isWriteable() {
        if ((this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | DISK_FULL_BIT | WRITE_INDEX_FILE_ERROR_BIT)) == 0) {
            return true;
        }

        return false;
    }

    //for consume queue, just ignore the DISK_FULL_BIT
    public boolean isCQWriteable() {
        if ((this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | WRITE_INDEX_FILE_ERROR_BIT)) == 0) {
            return true;
        }

        return false;
    }

    public boolean getAndMakeNotWriteable() {
        boolean result = this.isWriteable();
        if (result) {
            this.flagBits |= NOT_WRITEABLE_BIT;
        }
        return result;
    }

    public void makeLogicsQueueError() {
        this.flagBits |= WRITE_LOGICS_QUEUE_ERROR_BIT;
    }

    public boolean isLogicsQueueError() {
        if ((this.flagBits & WRITE_LOGICS_QUEUE_ERROR_BIT) == WRITE_LOGICS_QUEUE_ERROR_BIT) {
            return true;
        }

        return false;
    }

    public void makeIndexFileError() {
        this.flagBits |= WRITE_INDEX_FILE_ERROR_BIT;
    }

    public boolean isIndexFileError() {
        if ((this.flagBits & WRITE_INDEX_FILE_ERROR_BIT) == WRITE_INDEX_FILE_ERROR_BIT) {
            return true;
        }

        return false;
    }

    // 标记磁盘满了
    public boolean getAndMakeDiskFull() {
        // 判断是否磁盘满了，
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        // 设置磁盘满了
        // 按位或赋值运算符
        // this.flagBits = this.flagBits | DISK_FULL_BIT;
        this.flagBits |= DISK_FULL_BIT;
        return result;
    }

    public boolean getAndMakeDiskOK() {
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        // 按位取反运算符，对操作数的每一位取反（0 变 1，1 变 0）
        //  00010000 --> 11101111
        // 按位与赋值运算符 即设置flagBits对应位0，其他1保留
        this.flagBits &= ~DISK_FULL_BIT;
        return result;
    }
}
