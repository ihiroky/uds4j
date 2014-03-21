package net.ihiroky.uds4j;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.SelectorProvider;

/**
 * A skeletal implementation of {@link java.nio.channels.spi.AbstractSelectableChannel}
 * to hold a file descriptor. A sub class of this class can be registered to
 * {@link net.ihiroky.uds4j.EPollSelector}.
 *
 */
public abstract class AbstractChannel extends AbstractSelectableChannel implements NetworkChannel {

    /** the file descriptor. */
    protected final int fd_;

    /** A lock object to guard the state of this channel. */
    protected final Object stateLock_;

    private final int validOps_;

    private static final SelectorProvider SELECTOR_PROVIDER = new EPollSelectorProvider();

    /**
     * Constructs the instance of this class.
     *
     * @param fd the file descriptor
     * @param validOps acceptable operations of {@link java.nio.channels.SelectionKey}
     */
    protected AbstractChannel(int fd, int validOps) {
        super(SELECTOR_PROVIDER);

        fd_ = fd;
        validOps_ = validOps;
        stateLock_ = new Object();
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        synchronized (stateLock_) {
            if (Native.shutdown(fd_, Native.SHUT_RDWR) == -1) {
                throw new IOException(Native.getLastError());
            }
            if (Native.close(fd_) == -1) {
                throw new IOException(Native.getLastError());
            }
        }
    }

    @Override
    protected void implConfigureBlocking(boolean block) throws IOException {
        synchronized (stateLock_) {
            int flags = Native.fcntl(fd_, Native.F_GETFL, 0);
            if (block) {
                flags |= Native.O_NONBLOCK;
            } else {
                flags &= ~Native.O_NONBLOCK;
            }
            Native.fcntl(fd_, Native.F_SETFL, flags);
        }
    }

    @Override
    public int validOps() {
        return validOps_;
    }

    @Override
    public String toString() {
        return "{" + fd_ + ", " + (isOpen() ? "open" : "closed") + "}";
    }
}
