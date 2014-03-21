package net.ihiroky.uds4j;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectionKey;

/**
 * A token representing the registration of a {@link net.ihiroky.uds4j.AbstractChannel}
 * with a {@link net.ihiroky.uds4j.EPollSelector}.
 */
public class EPollSelectionKey extends AbstractSelectionKey {

    private final AbstractChannel channel_;
    private final EPollSelector selector_;
    private volatile int readyOps_;
    private volatile int interestOps_;

    private transient int hashCode_;

    private static final int HASH_FACTOR = 31;

    EPollSelectionKey(AbstractChannel channel, EPollSelector selector, int interestOps) {
        channel_ = channel;
        selector_ = selector;
        interestOps_ = interestOps;
    }

    @Override
    public AbstractChannel channel() {
        return channel_;
    }

    @Override
    public Selector selector() {
        return selector_;
    }

    @Override
    public int interestOps() {
        return interestOps_;
    }

    @Override
    public EPollSelectionKey interestOps(int ops) {
        interestOps_ = ops;
        selector_.register(channel_, ops, attachment());
        return this;
    }

    @Override
    public int readyOps() {
        return readyOps_;
    }

    @Override
    public int hashCode() {
        if (hashCode_ != 0) {
            return hashCode_;
        }

        int hashCode = channel_.hashCode() * HASH_FACTOR + selector_.hashCode();
        hashCode_ = hashCode;
        return hashCode;
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof EPollSelectionKey) {
            EPollSelectionKey that = (EPollSelectionKey) object;
            return this.channel_ == that.channel_ && this.selector_ == that.selector_;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("fd:").append(channel_.fd_).append(", interest:");
        appendOps(b, interestOps_);
        b.append(", ready:");
        appendOps(b, readyOps_);
        return b.toString();
    }

    private static void appendOps(StringBuilder b, int ops) {
        b.append('[');
        int n = b.length();
        if ((ops & SelectionKey.OP_READ) != 0) {
            b.append("OP_READ");
        }
        if ((ops & SelectionKey.OP_WRITE) != 0) {
            if (b.length() > n) {
                b.append(", ");
            }
            b.append("OP_WRITE");
        }
        if ((ops & SelectionKey.OP_ACCEPT) != 0) {
            if (b.length() > n) {
                b.append(", ");
            }
            b.append("OP_ACCEPT");
        }
        if ((ops & SelectionKey.OP_CONNECT) != 0) {
            if (b.length() > n) {
                b.append(", ");
            }
            b.append("OP_CONNECT");
        }
        b.append(']');
    }

    boolean isInterestedInRead() {
        return (interestOps_ & SelectionKey.OP_READ) != 0;
    }

    void updateReadyOps(int readyOps) {
        readyOps_ = readyOps;
    }
}
