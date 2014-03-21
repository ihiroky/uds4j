package net.ihiroky.uds4j;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides the buffer for I/O operations.
 */
class ByteBufferPool {

    private ByteBuffer[] buffers_;
    private int head_;
    private int count_;

    private static final Logger LOG = Logger.getLogger(ByteBufferPool.class.getName());

    private static Method cleanerMethod_;
    private static Method cleanMethod_;

    static {
        try {
            Class<?> directBufferClass = Class.forName("sun.nio.ch.DirectBuffer");
            Method cleanerMethod = directBufferClass.getDeclaredMethod("cleaner");
            Class<?> cleanerClass = Class.forName("sun.misc.Cleaner");
            Method cleanMethod = cleanerClass.getDeclaredMethod("clean");

            cleanerMethod_ = cleanerMethod;
            cleanMethod_ = cleanMethod;
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to reflect the release method for the direct buffer", e);
        }
    }

    ByteBufferPool() {
        int size = 1;
        while (size < Native.IOV_MAX) {
            size <<= 1;
        }

        buffers_ = new ByteBuffer[size];
    }

    ByteBuffer search(int size) {
        int head = head_;
        if (count_ == 0) {
            return ByteBuffer.allocateDirect(size);
        }

        ByteBuffer[] buffers = buffers_;
        int mask = buffers.length - 1;
        int i = head;
        int tail = (head + count_) % mask;
        int minimum = i;
        ByteBuffer b = buffers[i];
        if (b.capacity() < size) {
            i = (head + 1) & mask;
            while (i != tail) {
                b = buffers[i];
                if (b.capacity() >= size) {
                    buffers[i] = buffers[head];
                    break;
                }
                if (b.capacity() < minimum) {
                    minimum = i;
                }
                i = (i + 1) & mask;
            }

            if (i == tail) {
                // Release the minimum buffer to avoid the cache growing.
                b = buffers[minimum];
                buffers[minimum] = buffers[head];
                buffers[head] = null;
                head_ = (head + 1) & mask;
                count_--;
                release(b);
                return ByteBuffer.allocateDirect(size);
            }
        }
        buffers[head] = null;
        head_ = (head + 1) & mask;
        count_--;
        b.rewind().limit(size);
        return b;
    }

    void offerFirst(ByteBuffer buffer) {
        if (count_ == Native.IOV_MAX) {
            release(buffer);
            return;
        }

        ByteBuffer[] buffers = buffers_;
        int mask = buffers.length - 1;
        int h = (head_ - 1) & mask;
        buffers[h] = buffer;
        head_ = h;
        count_++;
    }

    void offerLast(ByteBuffer buffer) {
        if (count_ == Native.IOV_MAX) {
            release(buffer);
            return;
        }

        ByteBuffer[] buffers = buffers_;
        int mask = buffers.length - 1;
        int t = (head_ + count_) & mask;
        buffers[t] = buffer;
        count_++;
    }

    int count() {
        return count_;
    }

    void clear() {
        ByteBuffer[] buffers = buffers_;
        for (int i = head_; buffers[i] != null;) {
            release(buffers[i]);
            i = (i + 1) & buffers.length;
            buffers[i] = null;
        }
        head_ = 0;
        count_ = 0;
    }

    private static void release(ByteBuffer buffer) {
        Method cleanerMethod = cleanerMethod_;
        if (cleanerMethod == null) {
            return;
        }

        if (buffer.isDirect()) {
            try {
                Object cleanerObject = cleanerMethod.invoke(buffer);
                cleanMethod_.invoke(cleanerObject);
            } catch (Throwable t) {
                LOG.log(Level.SEVERE, "Failed to release the direct buffer.", t);
            }
        }
    }
}
