package net.ihiroky.uds4j;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectionKey;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Arrays;

/**
 * A selectable channel for datagram oriented unix domain sockets.
 *
 * This channel supports non-blocking connection like {@link java.nio.channels.DatagramChannel}.
 */
public final class DatagramUnixDomainChannel extends ReadWriteUnixDomainChannel {

    private UnixDomainSocketAddress remoteAddress_;

    private DatagramUnixDomainChannel(int fd) throws IOException {
        this(fd, SelectionKey.OP_READ | SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT);
    }

    private DatagramUnixDomainChannel(int fd, int validOps) throws IOException {
        super(fd, validOps);
    }

    @Override
    protected boolean ensureReadOpen() throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (!isConnected()) {
                throw new NotYetConnectedException();
            }
        }
        return true;
    }

    @Override
    protected boolean ensureWriteOpen() throws IOException {
        return true;
    }

    /**
     * Opens a new channel.
     *
     *  @return the new channel
     * @throws java.io.IOException if an I/O error occurs
     */
    public static DatagramUnixDomainChannel open() throws IOException {
        int fd = open(Native.SOCK_DGRAM);
        return new DatagramUnixDomainChannel(fd);
    }

    /**
     * Creates an pair of connected sockets.
     *
     * @return the pair of connected sockets
     * @throws java.io.IOException if an I/O error occurs
     */
    public static DatagramUnixDomainChannel[] pair() throws IOException {
        int[] sockets = {-1, -1};
        Native.socketpair(Native.AF_UNIX, Native.SOCK_DGRAM, Native.PROTOCOL, sockets);
        int ops = SelectionKey.OP_READ | SelectionKey.OP_WRITE;
        return new DatagramUnixDomainChannel[] {
                new DatagramUnixDomainChannel(sockets[0], ops),
                new DatagramUnixDomainChannel(sockets[1], ops)
        };
    }

    /**
     * Connects this channel to a channel specified by the remote.
     *
     * @param remote the remote address which the the channel has
     * @return this channel
     * @throws java.nio.channels.ClosedChannelException if this channel is closed
     * @throws java.nio.channels.AlreadyConnectedException if this channel is already connected
     * @throws java.io.IOException if an I/O error occurs
     */
    public DatagramUnixDomainChannel connect(SocketAddress remote) throws IOException {
        if (remote == null) {
            throw new NullPointerException("remote");
        }
        if (!(remote instanceof UnixDomainSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        }

        UnixDomainSocketAddress remoteAddress = (UnixDomainSocketAddress) remote;
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (remoteAddress_ != null) {
                throw new AlreadyConnectedException();
            }

            Native.SockAddrUn sun = AddressBuffer.getInstance().getAddress();
            sun.sunFamily_ = Native.AF_UNIX;
            sun.setSunPath(remoteAddress.getPath());
            if (Native.connect(fd_, sun, sun.size()) == -1) {
                throw new IOException(Native.getLastError());
            }
            remoteAddress_ = (UnixDomainSocketAddress) remote;
        }
        return this;
    }

    /**
     * Disconnects this channel's socket.
     *
     * @return this channel
     * @throws java.io.IOException if an I/O error occurs
     */
    public DatagramUnixDomainChannel disconnect() throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (remoteAddress_ == null) {
                return this;
            }

            // Ref: http://timesinker.blogspot.jp/2010/02/unconnect-udp-socket.html
            Native.SockAddrUn sun = AddressBuffer.getInstance().getAddress();
            sun.clear();
            sun.sunFamily_ = Native.AF_UNSPEC;
            Arrays.fill(sun.sunPath_, (byte) 0);
            if (Native.connect(fd_, sun, sun.size()) == -1) {
                throw new IOException(Native.getLastError());
            }
            remoteAddress_ = null;
        }
        return this;
    }

    /**
     * Returns true if this channel is connected.
     *
     * @return true if this channel is connected
     * @throws java.io.IOException if an I/O error occurs
     */
    public boolean isConnected() throws IOException {
        synchronized (stateLock_) {
            return remoteAddress_ != null;
        }
    }

    /**
     * Returns the address of the peer connected to the remote channel.
     *
     * @return the address of the peer connected to the remote channel
     * @throws java.io.IOException if an I/O error occurs
     */
    public SocketAddress getRemoteAddress() throws IOException {
        synchronized (stateLock_) {
            if (remoteAddress_ == null) {
                AddressBuffer buffer = AddressBuffer.getInstance();
                Native.SockAddrUn sun = buffer.getAddress();
                sun.sunFamily_ = Native.AF_UNIX;
                if (Native.getpeername(fd_, sun, buffer.getSize()) == -1) {
                    throw new IOException(Native.getLastError());
                }
                remoteAddress_ = new UnixDomainSocketAddress(sun.getSunPath());
            }
        }
        return remoteAddress_;
    }

    @Override
    public DatagramUnixDomainChannel bind(SocketAddress local) throws IOException {
        UnixDomainSocketAddress uds = (UnixDomainSocketAddress) local;
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (localAddress_ != null) {
                throw new AlreadyBoundException();
            }

            Native.SockAddrUn sun = AddressBuffer.getInstance().getAddress();
            sun.clear();
            sun.sunFamily_ = Native.AF_UNIX;
            sun.setSunPath(uds.getPath());
            if (Native.bind(fd_, sun, sun.size()) == -1) {
                throw new IOException(Native.getLastError());
            }
            localAddress_ = uds;
        }
        return this;
    }

    /**
     * Sends a datagram via this channel.
     *
     * @param src the buffer containing the datagram to be sent
     * @param target the address to which the datagram is to be sent
     * @return the number of bytes sent
     * @throws java.nio.channels.ClosedChannelException if this channel is closed
     * @throws java.io.IOException if an I/O error occurs
     */
    public int send(ByteBuffer src, SocketAddress target) throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (isConnected() && !remoteAddress_.equals(target)) {
                throw new IOException("The target is not equals to the connected address.");
            }
        }

        UnixDomainSocketAddress sa = (UnixDomainSocketAddress) target;
        int sent = 0;
        Native.SockAddrUn sun = AddressBuffer.getInstance().getAddress();
        sun.sunFamily_ = Native.AF_UNIX;
        sun.setSunPath(sa.getPath());
        try {
            begin();
            sent = Native.sendto(fd_, src, src.remaining(), 0, sun, sun.size());
        } finally {
            end(sent > 0);
        }
        if (sent == -1) {
            throw new IOException(Native.getLastError());
        }
        src.position(src.position() + sent);
        return sent;
    }

    /**
     * Receives a datagram via this channel.
     *
     * @param dst the buffer into which the datagram is to be transferred
     * @return the source address of the datagram
     * @throws java.nio.channels.ClosedChannelException if this channel is closed
     * @throws java.io.IOException if an I/O error occurs
     */
    public SocketAddress receive(ByteBuffer dst) throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
        }

        int received = 0;
        AddressBuffer addressBuffer = AddressBuffer.getInstance();
        Native.SockAddrUn sun = addressBuffer.getAddress();
        sun.sunFamily_ = Native.AF_UNIX;
        try {
            begin();
            received = Native.recvfrom(fd_, dst, dst.remaining(), 0, sun, addressBuffer.getSize());
        } finally {
            end(received > 0);
        }
        if (received == -1) {
            if (Native.errno() == Native.EAGAIN) {
                return null;
            }
            throw new IOException(Native.getLastError());
        }
        String sunPath = sun.getSunPath();
        dst.position(dst.position() + received);
        return received >= 0 ? new UnixDomainSocketAddress(sunPath) : null;
    }
}
