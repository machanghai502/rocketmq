package org.apache.rocketmq.test;

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

        sliceByteBuffer.position(2);

        System.out.println("===after position(2)========");
        System.out.println("position:" + sliceByteBuffer.position());
        sliceByteBuffer.put("7".getBytes());
        System.out.println("limit:" + sliceByteBuffer.limit());
        System.out.println("position:" + sliceByteBuffer.position());
        System.out.println("capacity:" + sliceByteBuffer.capacity());

    }
}
