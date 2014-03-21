package net.ihiroky.uds4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SelectionKey;

/**
 * An implementation of {@link java.nio.channels.GatheringByteChannel} and
 * {@link java.nio.channels.ScatteringByteChannel} for unix domain sockets.
 *
 * TODO send/recv credential
 */
public abstract class ReadWriteUnixDomainChannel extends AbstractUnixDomainChannel
        implements GatheringByteChannel, ScatteringByteChannel {

    /**
     * Constructs a new instance.
     *
     * @param fd a file discriptor
     * @throws java.io.IOException if an I/O error occurs
     */
    protected ReadWriteUnixDomainChannel(int fd) throws IOException {
        this(fd, SelectionKey.OP_READ | SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT);
    }

    /**
     * Constructs a new instance.
     *
     * @param fd a file discriptor
     * @param validOps an interest set of {@link java.nio.channels.SelectionKey} used by the instance.
     * @throws java.io.IOException if an I/O error occurs
     */
    protected ReadWriteUnixDomainChannel(int fd, int validOps) throws IOException {
        super(fd, validOps);
    }

    /**
     * Ensures that the state of this instance is readable.
     *
     * @return true if the state of this instance is readable.
     * @throws IOException if an I/O error occurs
     */
    protected abstract boolean ensureReadOpen() throws IOException;

    /**
     * Ensures that the state of this instance is writable.
     *
     * @return true if the state of this instance is writable.
     * @throws IOException if an I/O error occurs
     */
    protected abstract boolean ensureWriteOpen() throws IOException;

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if (length > Native.IOV_MAX) {
            throw new IOException("The length must be less than " + Native.IOV_MAX);
        }

        if (!ensureReadOpen()) {
            return -1L;
        }

        boolean fail = true;
        long read = 0L;
        IOVecBuffer vec = IOVecBuffer.getInstance();
        try {
            for (int i = 0; i < length; i++) {
                vec.set(i, dsts[i + offset]);
            }

            begin();

            read = Native.readv(fd_, vec.headReference(), length).longValue();
            if (read == -1) {
                throw new IOException(Native.getLastError());
            }

            long left = read;
            for (int i = 0; i < length && left > 0; i++) {
                ByteBuffer dst = dsts[i];
                int position = dst.position();
                int remaining = dst.remaining();
                int n = (left > remaining) ? remaining : (int) left;
                if (dst.isDirect()) {
                    dst.position(position + n);
                } else {
                    ByteBuffer iovBase = vec.get(i).iovBase_;
                    iovBase.limit(n);
                    dst.put(iovBase);
                }
                left -= n;
            }
            return read;
        } finally {
            end(read > 0L);
            if (fail) {
                for (int i = 0; i < length; i++) {
                    vec.clear(i, dsts[i + offset]);
                }
            }
        }
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if (length > Native.IOV_MAX) {
            throw new IOException("The length must be less than " + Native.IOV_MAX);
        }

        if (!ensureWriteOpen()) {
            throw new ClosedChannelException();
        }

        long written = 0L;
        boolean fail = true;
        IOVecBuffer vec = IOVecBuffer.getInstance();
        try {
            for (int i = 0; i < length; i++) {
                vec.set(i, srcs[i + offset]);
            }

            begin();

            written = Native.writev(fd_, vec.headReference(), length).longValue();
            if (written == -1) {
                throw new IOException(Native.getLastError());
            }

            long left = written;
            for (int i = 0; i < length && left > 0; i++) {
                ByteBuffer src = srcs[i + offset];
                vec.clear(i, src);
                int position = src.position();
                int remaining = src.remaining();
                int n = (left > remaining) ? remaining : (int) left;
                src.position(position + n);
                left -= n;
            }

            fail = false;
            return written;
        } finally {
            end(written > 0L);
            if (fail) {
                for (int i = 0; i < length; i++) {
                    vec.clear(i, srcs[i + offset]);
                }
            }
        }
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!ensureReadOpen()) {
            return -1;
        }
        int read = 0;
        try {
            begin();
            read = Native.read(fd_, dst, dst.remaining());
            if (read == -1) {
                if (Native.errno() == Native.EAGAIN) {
                    return 0;
                }
                throw new IOException(Native.getLastError());
            }
        } finally {
            end(read > 0);
        }
        if (read == 0) { // EOF according to 'man 2 read'
            return -1;
        }
        dst.position(dst.position() + read);
        return read;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!ensureWriteOpen()) {
            throw new ClosedChannelException();
        }
        int written = 0;
        try {
            begin();
            written = Native.write(fd_, src, src.remaining());
            if (written == -1) {
                throw new IOException(Native.getLastError());
            }
        } finally {
            end(written > 0);
        }

        src.position(src.position() + written);
        return written;
    }
}
