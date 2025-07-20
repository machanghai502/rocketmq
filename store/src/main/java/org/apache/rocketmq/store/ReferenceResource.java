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

import java.util.concurrent.atomic.AtomicLong;

// 资源引用抽象类
// 比如对资源MappFile对象的使用（引用）
public abstract class ReferenceResource {
    // 引用计数
    protected final AtomicLong refCount = new AtomicLong(1);
    // 指示状态：是否关闭或者停止的标记，默认是true
    protected volatile boolean available = true;
    // 是否清理xx完成
    protected volatile boolean cleanupOver = false;
    // 第一次shutdown的时间戳
    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    // 什么是文件可用？
    public boolean isAvailable() {
        return this.available;
    }

    // 关闭或者停止该引用资源对象
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    // 是否对象资源的引用，即将引用计数？？
    public void release() {
        // 减少一次引用，如果引用计数依然大于0。则说明还有引用，直接返回
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        // 如果引用计数为0了，资源无被引用，可以进行清理操作
        synchronized (this) {
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    // 资源引用是否清除完毕
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
