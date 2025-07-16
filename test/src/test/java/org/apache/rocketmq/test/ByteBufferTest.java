package org.apache.rocketmq.test;

import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.test.util.MQWait;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ByteBufferTest {

    @Test
    public void testSlice() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(10);
        System.out.println("limit:" + byteBuffer.limit());
        System.out.println("position:" + byteBuffer.position());
        System.out.println("capacity:" + byteBuffer.capacity());
        byteBuffer.put("1".getBytes());
        byteBuffer.put("2".getBytes());
        byteBuffer.put("3".getBytes());
        byteBuffer.put("4".getBytes());
        byteBuffer.put("5".getBytes());
        byteBuffer.put("6".getBytes());

        System.out.println("============after put ==========");
        System.out.println("limit:" + byteBuffer.limit());
        System.out.println("position:" + byteBuffer.position());
        System.out.println("capacity:" + byteBuffer.capacity());

        System.out.println("===after slice()========");

        ByteBuffer sliceByteBuffer = byteBuffer.slice();
        System.out.println("limit:" + sliceByteBuffer.limit());
        System.out.println("position:" + sliceByteBuffer.position());
        System.out.println("capacity:" + sliceByteBuffer.capacity());


        sliceByteBuffer.put("7".getBytes());

        System.out.println("===after sliceByteBuffer put()========");

        System.out.println("sliceByteBuffer limit:" + sliceByteBuffer.limit());
        System.out.println("sliceByteBuffer position:" + sliceByteBuffer.position());
        System.out.println("sliceByteBuffer capacity:" + sliceByteBuffer.capacity());

        // sliceByteBuffer 添加，不影响原始byteBuffer的position。两个byteBuffer是独立的。
        System.out.println("byteBuffer limit:" + byteBuffer.limit());
        System.out.println("byteBuffer position:" + byteBuffer.position());
        System.out.println("byteBuffer capacity:" + byteBuffer.capacity());


//        sliceByteBuffer.position(2);
//
//        System.out.println("===after position(2)========");
//        System.out.println("position:" + sliceByteBuffer.position());
//        sliceByteBuffer.put("7".getBytes());
//        System.out.println("limit:" + sliceByteBuffer.limit());
//        System.out.println("position:" + sliceByteBuffer.position());
//        System.out.println("capacity:" + sliceByteBuffer.capacity());


        int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;
        int factor = (int) Math.ceil(mappedFileSizeConsumeQueue / (ConsumeQueue.CQ_STORE_UNIT_SIZE * 1.0));
        int aa =  (int) (factor * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        System.out.println(aa);

    }
}
