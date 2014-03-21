package net.ihiroky.uds4j;

import java.nio.ByteBuffer;

/**
 * Provides the thread local buffer for readv/writev.
 */
final class IOVecBuffer {

    private final Native.IOVec.ByReference ioVecHead_;
    private final Native.IOVec[] ioVecArray_;
    private final ByteBufferPool bufferPool_;

    private IOVecBuffer() {
        ioVecHead_ = new Native.IOVec.ByReference();
        ioVecArray_ = (Native.IOVec[]) ioVecHead_.toArray(Native.IOV_MAX);
        bufferPool_ = new ByteBufferPool();
    }

    private static final ThreadLocal<IOVecBuffer> INSTANCE = new ThreadLocal<IOVecBuffer>() {
        @Override
        protected IOVecBuffer initialValue() {
            return new IOVecBuffer();
        }
    };

    static IOVecBuffer getInstance() {
        return INSTANCE.get();
    }

    static synchronized IOVecBuffer newInstance() {
        IOVecBuffer instance = INSTANCE.get();
        if (instance == null) {
            instance = new IOVecBuffer();
            INSTANCE.set(instance);
        }
        return instance;
    }

    Native.IOVec.ByReference headReference() {
        return ioVecHead_;
    }

    void set(int i, ByteBuffer buffer) {
        if (buffer.isDirect()) {
            Native.IOVec vec = ioVecArray_[i];
            vec.iovBase_ = buffer;
            vec.iovLen_ = buffer.remaining();
            return;
        }

        int remaining = buffer.remaining();
        ByteBuffer directBuffer = bufferPool_.search(remaining);
        int position = buffer.position();
        directBuffer.put(buffer);
        directBuffer.flip();
        buffer.position(position);

        Native.IOVec vec = ioVecArray_[i];
        vec.iovBase_ = directBuffer;
        vec.iovLen_ = remaining;
    }

    void clear(int i, ByteBuffer buffer) {
        Native.IOVec vec = ioVecArray_[i];
        if (buffer.isDirect()) {
            vec.iovBase_ = null;
            return;
        }

        ByteBuffer directBuffer = vec.iovBase_;
        if (directBuffer != null) {
            vec.iovBase_ = null;
            bufferPool_.offerLast(directBuffer);
        }
    }

    Native.IOVec get(int i) {
        return ioVecArray_[i];
    }

    int getPooledBuffers() {
        return bufferPool_.count();
    }
}
