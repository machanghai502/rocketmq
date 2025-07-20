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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

// Index文件类，代表一个index文件
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 1个HashSlot的大小
    private static int hashSlotSize = 4;
    // 一个index条目的大小
    private static int indexSize = 20;
    //
    private static int invalidIndex = 0;
    // 一个index索引文件最大Hash Slot数量，默认500w个
    private final int hashSlotNum;
    // 一个index索引文件最大的Index条目数量，默认2000w个
    private final int indexNum;
    // Index文件对应的MappedFile
    private final MappedFile mappedFile;
    // mappedFile关联的内存映射文件通道
    private final FileChannel fileChannel;
    // mappedFile关联的内存映射文件缓冲区
    private final MappedByteBuffer mappedByteBuffer;
    // Index文件头
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        // 40 + 500w * 4 + 2000W * 20
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    // 为key构建消息索引
    // key格式：{topic}"#"{key}
    // 消息的CommitLogOffset，全局物理偏移量
    // 消息的storeTimestamp
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // key 的 hashCode
            int keyHash = indexKeyHashMethod(key);
            // hashSlot位置数组计算，hash槽下标
            int slotPos = keyHash % this.hashSlotNum;
            // 计算key对应的hashSlot字节偏移：40 + 数组下标 * 一个hashSlot的4字节
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {
                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                // 获取Key的HashCode对应的Index条目的Index数组索引下标
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 计算index索引条目的timeDif
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                // 并且换算成秒
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 头部字节长度+哈希槽数量×单个哈希槽大小（4个字节）+当前Index条目个数×单个Index条目大小（20个字节）
                // 40 + 500w * 4 + 已有index条目数量 * 20字节
                // 计算新添加条目的起始物理偏移量
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                // 将条目信息存储在Index文件中
                // 写入hashCode、CommitLog消息物理量、时间差（单位秒）、
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // 存储当前Hash槽中的值，
                // 哈希冲突链式解决方案的关键实现，哈希槽中存储的是该哈希码对应的最新Index条目的下标，新的Index条目最后4个字节存储该哈希码上一个条目的Index下标
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // hashSlot写入，
                // hashSlot的内容，在key的hashCode对应数组下标处，写入当前index的数量，即新维护的index索引的对应的数组下标
                // 将相同的HashCode中覆盖写入最新的Index条目的下标索引位置
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 更新IndexFile头信息
                // 维护当前index文件的相关数据的统计记录
                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }


    /**
     * 根据消息Key获取
     * @param phyOffsets 记录符合命中查找条件的消息phyOffset，同一个HashCode会对应多个Index条目，这里并没有比较key是否相等。
     * @param key 需要查找的key
     * @param maxNum
     * @param begin 开始时间，毫秒 必传项目，如果不指定，需要传入0
     * @param end 结束时间， 毫秒 必传项目，如果不指定，需要传入Integer.Max
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            // 获取Key对应的HashCode
            int keyHash = indexKeyHashMethod(key);
            // keyHash对哈希槽数量取余，定位到哈希码对应的哈希槽下标
            int slotPos = keyHash % this.hashSlotNum;
            // 哈希槽的字节偏移地址为IndexHeader（40字节）加上下标乘以每个哈希槽的大小（4字节）
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                // 获取HashSlot中储存的index条目下标
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                // 如果对应的哈希槽中存储的数据小于1或大于当前索引条目个数，表示该哈希码没有对应的条目
                // ？？ 如果对应的槽中没有写入数据，slotValue是什么？？
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    // 当前slotValue定位到的是该哈希槽最新的一个Item条目
                    // 会存在哈希冲突，所以根据最新的一个Item条目，
                    //
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        // 根据Index索引条目下标获取对应的keyHash
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        // 根据Index索引条目下标获取对应的phyOffset
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        // 根据Index索引条目下标获取对应的timeDiff
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        //
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        // 转换成毫秒
                        timeDiff *= 1000L;

                        // 当前Index条目对应的消息的存储时间
                        // Index文件中存储的第一条消息的storeTimestamp + 时间差。
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        // 如果keyHash相等且消息时间符合范围，则当前phyOffset查找命中
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        // 验证条目的前一个Index索引
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
