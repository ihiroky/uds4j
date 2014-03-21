package net.ihiroky.uds4j;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.NoConnectionPendingException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectionKey;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A selectable channel for connecting unix domain sockets.
 *
 * This channel supports non-blocking connection like {@link java.nio.channels.SocketChannel}.
 */
public class ClientUnixDomainChannel extends ReadWriteUnixDomainChannel {

    private volatile ConnectionState connectionState_;
    private int shutdownState_;
    private UnixDomainSocketAddress remoteAddress_;

    private static final int SHUTDOWN_INPUT = 1;
    private static final int SHUTDOWN_OUTPUT = 1 << 1;

    /**
     * The connection state of this channel.
     */
    enum ConnectionState {
        /** The state that represents the connect() is not called. */
        INITIAL,

        /** The state that represents the connect() is called but the connection operation is not completed. */
        CONNECTING,

        /** The state that represents the connection operatio is completed. */
        CONNECTED,
    }

    ClientUnixDomainChannel(int fd, boolean connected) throws IOException {
        super(fd);
        connectionState_ = connected ? ConnectionState.CONNECTED : ConnectionState.INITIAL;
    }

    private ClientUnixDomainChannel(int fd, int validOps) throws IOException {
        super(fd, validOps);
        connectionState_ = ConnectionState.INITIAL;
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
            if ((shutdownState_ & SHUTDOWN_INPUT) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean ensureWriteOpen() throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (!isConnected()) {
                throw new NotYetConnectedException();
            }
            if ((shutdownState_ & SHUTDOWN_OUTPUT) != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Opens a new channel.
     *
     *  @return the new channel
     * @throws java.io.IOException if an I/O error occurs
     */
    public static ClientUnixDomainChannel open() throws IOException {
        int fd = open(Native.SOCK_STREAM);
        return new ClientUnixDomainChannel(fd, false);
    }

    /**
     * Creates an pair of connected sockets.
     *
     * @return the pair of connected sockets
     * @throws java.io.IOException if an I/O error occurs
     */
    public static final List<ClientUnixDomainChannel> pair() throws IOException {
        int[] sockets = {-1, -1};
        int n = Native.socketpair(Native.AF_UNIX, Native.SOCK_STREAM, Native.PROTOCOL, sockets);
        if (n == -1) {
            throw new IOException(Native.getLastError());
        }
        int ops = SelectionKey.OP_READ | SelectionKey.OP_WRITE;
        return Collections.unmodifiableList(Arrays.asList(
                new ClientUnixDomainChannel(sockets[0], ops), new ClientUnixDomainChannel(sockets[1], ops)));
    }

    private boolean connect() throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                return false;
            }
        }

        synchronized (blockingLock()) {
            if (connectionState_ == ConnectionState.CONNECTED) {
                return true;
            }
            connectionState_ = ConnectionState.CONNECTING;
            Native.SockAddrUn sun = AddressBuffer.getInstance().getAddress();
            sun.sunFamily_ = Native.AF_UNIX;
            sun.setSunPath(remoteAddress_.getPath());
            try {
                begin();
                if (Native.connect(fd_, sun, sun.size()) == -1) {
                    switch (Native.errno()) {
                        case Native.EISCONN:
                        connectionState_ = ConnectionState.CONNECTED;
                            return true;
                        case Native.EALREADY:
                            return false;
                        default:
                            throw new IOException(Native.getLastError());
                    }
                }
                connectionState_ = ConnectionState.CONNECTED;
            } finally {
                end(connectionState_ == ConnectionState.CONNECTED);
            }
        }
        return true;
    }

    /**
     * Connects this channel to a channel specified by the remote.
     *
     * @param remote the remote address which the the channel has
     * @return true if the connection was established, false if this channel
     *         is in non-blocking mode and the connection operation is in progress
     *
     * @throws java.nio.channels.ClosedChannelException if this channel is closed
     * @throws java.nio.channels.AlreadyConnectedException if this channel is already connected
     * @throws java.nio.channels.ConnectionPendingException if the connection operation is in progress
     * @throws java.io.IOException if an I/O error occurs
     */
    public boolean connect(SocketAddress remote) throws IOException {
        if (remote == null) {
            throw new NullPointerException("remote");
        }

        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (connectionState_ == ConnectionState.CONNECTED) {
                throw new AlreadyConnectedException();
            }
            if (connectionState_ == ConnectionState.CONNECTING) {
                throw new ConnectionPendingException();
            }
        }
        remoteAddress_ = (UnixDomainSocketAddress) remote;
        return connect();
    }

    /**
     * Finishes the process of connecting a channel.
     *
     * @return true if this channel is connected
     * @throws java.io.IOException if an I/O error occurs
     */
    public boolean finishConnect() throws IOException {
        switch (connectionState_) {
            case INITIAL:
                throw new NoConnectionPendingException();
            case CONNECTING:
                return connect();
            case CONNECTED:
                return true;
            default:
                throw new AssertionError("Invalid state: " + connectionState_);
        }
    }

    /**
     * Returns true if this channel is connected.
     *
     * @return true if this channel is connected
     */
    public boolean isConnected() {
        return connectionState_ == ConnectionState.CONNECTED;
    }

    /**
     * Returns true if the connection operation of this channel is in progress.
     *
     * @return true if the connection operation of this channel is in progress.
     */
    public boolean isConnectionPending() {
        return connectionState_ == ConnectionState.CONNECTING;
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
    public ClientUnixDomainChannel bind(SocketAddress local) throws IOException {
        if (!(local instanceof UnixDomainSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        }

        UnixDomainSocketAddress uds = (UnixDomainSocketAddress) local;
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (connectionState_ == ConnectionState.CONNECTING) {
                throw new ConnectionPendingException();
            }
            if (localAddress_ != null) {
                throw new AlreadyBoundException();
            }

            Native.SockAddrUn sun = AddressBuffer.getInstance().getAddress();
            sun.clear();
            sun.setSunPath(uds.getPath());
            sun.sunFamily_ = Native.AF_UNIX;
            if (Native.bind(fd_, sun, sun.size()) == -1) {
                throw new IOException(Native.getLastError());
            }
            localAddress_ = uds;
        }
        return this;
    }

    /**
     * Shutdowns the connection for reading without closing this channel.
     *
     * @return this instance
     * @throws java.nio.channels.ClosedChannelException if this channel is closed
     * @throws java.nio.channels.NotYetConnectedException if this channel is not yet connected
     * @throws java.io.IOException if some other I/O error occurs
     */
    public ClientUnixDomainChannel shutdownInput() throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (!isConnected()) {
                throw new NotYetConnectedException();
            }
            if ((shutdownState_ & SHUTDOWN_INPUT) == 0) {
                if (Native.shutdown(fd_, Native.SHUT_RD) == -1) {
                    throw new IOException(Native.getLastError());
                }
                shutdownState_ |= SHUTDOWN_INPUT;
            }
        }
        return this;
    }

    /**
     * Shutdowns the connection for writing without closing this channel.
     *
     * @return this instance
     * @throws java.nio.channels.ClosedChannelException if this channel is closed
     * @throws java.nio.channels.NotYetConnectedException if this channel is not yet connected
     * @throws java.io.IOException if some other I/O error occurs
     */
    public ClientUnixDomainChannel shutdownOutput() throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (!isConnected()) {
                throw new NotYetConnectedException();
            }
            if ((shutdownState_ & SHUTDOWN_OUTPUT) == 0) {
                if (Native.shutdown(fd_, Native.SHUT_WR) == -1) {
                    throw new IOException(Native.getLastError());
                }
                shutdownState_ |= SHUTDOWN_OUTPUT;
            }
        }
        return this;
    }
}
