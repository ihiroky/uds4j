package net.ihiroky.uds4j;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.NotYetBoundException;
import java.nio.channels.SelectionKey;
import java.nio.channels.UnsupportedAddressTypeException;

/**
 * A selectable channel for accepting unix domain sockets.
 *
 * This channel supports non-blocking connection like {@link java.nio.channels.ServerSocketChannel}.
 */
public class ServerUnixDomainChannel extends AbstractUnixDomainChannel implements NetworkChannel {

    private final Object acceptLock_;
    private UnixDomainSocketAddress localAddress_;

    private static final int DEFAULT_BACKLOG = 64;

    ServerUnixDomainChannel(int fd) throws IOException {
        super(fd, SelectionKey.OP_ACCEPT);
        acceptLock_ = new Object();
    }

    /**
     * Opens a new channel.
     * @return the new channel
     * @throws java.io.IOException if an I/O error occurs
     */
    public static ServerUnixDomainChannel open() throws IOException {
        int fd = open(Native.SOCK_STREAM);
        return new ServerUnixDomainChannel(fd);
    }

    @Override
    public ServerUnixDomainChannel bind(SocketAddress local) throws IOException {
        return bind(local, DEFAULT_BACKLOG);
    }

    /**
     * Binds the channel's socket to a local address and configures the socket to
     * listen for connections.
     *
     * @param local the local address
     * @param backlog the maximum number of pending connections
     * @return this instance
     * @throws java.nio.channels.AlreadyBoundException if the socket is already bound
     * @throws java.nio.channels.UnsupportedAddressTypeException if the type of the given address is not supported
     * @throws java.nio.channels.ClosedChannelException if this channel is closed
     * @throws java.io.IOException if some other I/O error occurs
      */
    public synchronized ServerUnixDomainChannel bind(SocketAddress local, int backlog) throws IOException {
        if (!(local instanceof UnixDomainSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        }

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
            if (Native.listen(fd_, backlog) == -1) {
                throw new IOException(Native.getLastError());
            }
            localAddress_ = uds;
        }
        return this;
    }

    /**
     * Accepts a connection made to this channel's socket.
     *
     * @return the socket channel for the new connection,
     *         or null if this channel is in non-blocking mode
     *         and no connection is available to be accepted
     * @throws java.nio.channels.ClosedChannelException if this channel is closed
     * @throws java.nio.channels.NotYetBoundException if this channel's socket has not yet been bound
     * @throws java.io.IOException if some other I/O error occurs
     */
    public synchronized ClientUnixDomainChannel accept() throws IOException {
        synchronized (stateLock_) {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (localAddress_ == null) {
                throw new NotYetBoundException();
            }
        }

        int client = -1;
        synchronized (acceptLock_) {
            AddressBuffer buffer = AddressBuffer.getInstance();
            Native.SockAddrUn sun = buffer.getAddress();
            sun.clear();
            sun.sunFamily_ = Native.AF_UNIX;
            try {
                begin();
                client = Native.accept(fd_, sun, buffer.getSize());
            } finally {
                end(client != -1);
            }
            if (client == -1) {
                throw new IOException(Native.getLastError());
            }
        }

        ClientUnixDomainChannel channel = new ClientUnixDomainChannel(client, true);

        // call get*Address() to cache the addresses.
        channel.getLocalAddress();
        channel.getRemoteAddress();
        return channel;
    }
}
