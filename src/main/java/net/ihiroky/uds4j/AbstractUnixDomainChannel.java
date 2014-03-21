package net.ihiroky.uds4j;

import com.sun.jna.ptr.IntByReference;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NetworkChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A selectable channel for unix domain sockets.
 *
 * This channel supports the following socket options: SO_SNDBUF, SO_PASSCRED (unimplemented)
 *
 * The implementations of this class accept {@link UnixDomainSocketAddress} only
 * as {@link java.net.SocketAddress}. The implementations throw {@link java.lang.ClassCastException}
 * on attempts to use any operations.
 */
public abstract class AbstractUnixDomainChannel extends AbstractChannel {

    /**
     * The socket address that this channel's socket is bound.
     */
    protected UnixDomainSocketAddress localAddress_;

    private static final int INT_BYTES = Integer.SIZE / Byte.SIZE;

    private static final Set<SocketOption<?>> SUPPORTED_OPTIONS =
            Collections.unmodifiableSet(new HashSet<SocketOption<?>>(Arrays.<SocketOption<?>>asList(
                    StandardSocketOptions.SO_SNDBUF, SocketOptions.SO_PASSCRED)));

    /**
     * Constructs the instance of this class.
     * @param fd the file descriptor
     * @param validOps acceptable operations of {@link java.nio.channels.SelectionKey}
     * @throws java.io.IOException if an I/O error occurs
     */
    protected AbstractUnixDomainChannel(int fd, int validOps) throws IOException {
        super(fd, validOps);
    }

    /**
     * Opens a new file descriptor with AF_UNIX and the specified type.
     * @param type the type (SOCK_TREAM or SOCK_DGRAM defiend in {@link net.ihiroky.uds4j.Native})
     * @return the new file descriptor
     * @throws java.io.IOException if an I/O error occurs
     */
    protected static int open(int type) throws IOException {
        int fd = Native.socket(Native.AF_UNIX, type, Native.PROTOCOL);
        if (fd == -1) {
            throw new IOException(Native.getLastError());
        }
        return fd;
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (localAddress_ == null) {
                AddressBuffer buffer = AddressBuffer.getInstance();
                Native.SockAddrUn sun = buffer.getAddress();
                sun.sunFamily_ = Native.AF_UNIX;
                // TODO Check result for unbound socket.
                if (Native.getsockname(fd_, sun, buffer.getSize()) == -1) {
                    throw new IOException(Native.getLastError());
                }
                localAddress_ = new UnixDomainSocketAddress(sun.getSunPath());
            }
        }
        return localAddress_;
    }


    @Override
    public <T> NetworkChannel setOption(SocketOption<T> name, T value) throws IOException {
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (value == null) {
            throw new NullPointerException("value");
        }

        // direct buffer is required ?

        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (name.equals(StandardSocketOptions.SO_SNDBUF))                    {
                ByteBuffer buffer = ByteBuffer.allocate(INT_BYTES).order(ByteOrder.nativeOrder());
                buffer.putInt((Integer) value);
                if (Native.setsockopt(fd_, Native.SOL_SOCKET, Native.SO_SNDBUF, buffer, buffer.capacity()) == -1) {
                    throw new IOException(Native.getLastError());
                }
                return this;
            }
            if (name.equals(SocketOptions.SO_PASSCRED)) {
                ByteBuffer buffer = ByteBuffer.allocate(INT_BYTES).order(ByteOrder.nativeOrder());
                boolean on = (Boolean) value;
                buffer.putInt(on ? 1 : 0);
                if (Native.setsockopt(fd_, Native.SOL_SOCKET, Native.SO_PASSCRED, buffer, buffer.capacity()) == -1) {
                    throw new IOException(Native.getLastError());
                }
                return this;
            }
        }
        throw new UnsupportedOperationException(name.name());
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        if (name == null) {
            throw new NullPointerException("name");
        }

        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (name.equals(StandardSocketOptions.SO_SNDBUF)) {
                ByteBuffer buffer = ByteBuffer.allocate(INT_BYTES).order(ByteOrder.nativeOrder());
                IntByReference sizeRef = AddressBuffer.getInstance().getSize();
                if (Native.getsockopt(fd_, Native.SOL_SOCKET, Native.SO_SNDBUF, buffer, sizeRef) == -1) {
                    throw new IOException(Native.getLastError());
                }
                return name.type().cast(buffer.getInt());
            }
            if (name.equals(SocketOptions.SO_PASSCRED)) {
                ByteBuffer buffer = ByteBuffer.allocate(INT_BYTES).order(ByteOrder.nativeOrder());
                IntByReference sizeRef = AddressBuffer.getInstance().getSize();
                if (Native.getsockopt(fd_, Native.SOL_SOCKET, Native.SO_PASSCRED, buffer, sizeRef) == -1) {
                    throw new IOException(Native.getLastError());
                }
                return name.type().cast(buffer.getInt() != 0);
            }
        }
        throw new UnsupportedOperationException(name.name());
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return SUPPORTED_OPTIONS;
    }
}
