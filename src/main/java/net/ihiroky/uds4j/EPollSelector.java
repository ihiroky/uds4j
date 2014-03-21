package net.ihiroky.uds4j;

import com.sun.jna.ptr.LongByReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AbstractSelector;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A multiplexer of SelectableChannel objects. This is for Linux 2.6+ kernels
 * that uses the epoll event notification facility.↲
 */
public final class EPollSelector extends AbstractSelector {

    private final int fd_;
    private final int eventFd_;
    private final Set<SelectionKey> keySet_;
    private final Map<Integer, EPollSelectionKey> fdKeyMap_;
    private final SelectedKeySet selectedKeySet_;
    private final Object lock_;

    private static Logger logger_ = LoggerFactory.getLogger(EPollSelector.class);
    private static final int EVENT_BUFFER_SIZE = 1024; // TODO getrlimit RLIMIT_NOFILE

    private final Native.EPollEvent.ByReference tmpEvent_;
    private final Native.EPollEvent eventsHead_;
    private final Native.EPollEvent[] eventBuffer_;
    private final LongByReference eventFdBuffer_;

    private  EPollSelector(int eventBufferSize) throws IOException {
        super(null);

        if (eventBufferSize <= 0) {
            throw new IllegalArgumentException("The eventBuffer must be positive.");
        }

        int fd = Native.epoll_create(eventBufferSize);
        if (fd == -1) {
            throw new IOException(Native.getLastError());
        }
        int evFd = Native.eventfd(0, Native.EFD_NONBLOCK);
        if (evFd == -1) {
            throw new IOException(Native.getLastError());
        }

        Native.EPollEvent.ByReference tmpEvent =
                new Native.EPollEvent.ByReference(evFd, Native.EPOLLIN | Native.EPOLLET);
        Native.EPollEvent eventsHead = new Native.EPollEvent();
        Native.EPollEvent[] eventBuffer = (Native.EPollEvent[]) eventsHead.toArray(eventBufferSize);
        LongByReference eventFdBuffer = new LongByReference();
        Native.epoll_ctl(fd, Native.EPOLL_CTL_ADD, evFd, tmpEvent);

        fd_ = fd;
        eventFd_ = evFd;
        tmpEvent_ = tmpEvent;
        eventsHead_ = eventsHead;
        eventBuffer_ = eventBuffer;
        eventFdBuffer_ = eventFdBuffer;
        keySet_ = new HashSet<SelectionKey>();
        fdKeyMap_ = new ConcurrentHashMap<Integer, EPollSelectionKey>();
        selectedKeySet_ = new SelectedKeySet(eventBufferSize);
        lock_ = new Object();
    }

    /**
     * Opens a selector. An invocation of this method behaves
     * in exactly the same way as the invocation {@code open(1024)}.
     *
     * @return the selector
     * @throws java.io.IOException If an I/O error occurs
     */
    public static EPollSelector open() throws IOException {
        return new EPollSelector(EVENT_BUFFER_SIZE);
    }

    /**
     * Opens a selector.
     *
     * @param eventBufferSize the size of the array of epoll event buffer to receive the result of epoll().
     * @return the selector
     * @throws java.io.IOException If an I/O error occurs
     */
    public static EPollSelector open(int eventBufferSize) throws IOException {
        return new EPollSelector(eventBufferSize);
    }

    @Override
    protected void implCloseSelector() throws IOException {
        synchronized (lock_) {
            if (Native.close(fd_) == -1) {
                throw new IOException(Native.getLastError());
            }
        }
    }

    @Override
    protected SelectionKey register(AbstractSelectableChannel ch, int ops, Object att) {

        AbstractChannel channel = (AbstractChannel) ch;
        EPollSelectionKey key = new EPollSelectionKey(channel, this, ops);
        key.attach(att);

        int events = 0;
        if ((ops & (SelectionKey.OP_ACCEPT | SelectionKey.OP_READ)) != 0) {
            events |= (Native.EPOLLIN | Native.EPOLLET);
        }
        if ((ops & SelectionKey.OP_CONNECT) != 0) {
            events |= Native.EPOLLOUT;
        }
        if ((ops & SelectionKey.OP_WRITE) != 0) {
            events |= (Native.EPOLLOUT | Native.EPOLLET);
        }

        synchronized (keySet_) {
            Set<SelectionKey> cancelledKeySet = cancelledKeys();
            synchronized (cancelledKeySet) {
                if (cancelledKeySet.contains(key)) {
                    throw new CancelledKeyException();
                }
            }

            Native.EPollEvent.ByReference ev = tmpEvent_;
            ev.clear();
            ev.update(channel.fd_, events);
            if (keySet_.contains(key)) {
                if (events != 0) {
                    if (Native.epoll_ctl(fd_, Native.EPOLL_CTL_MOD, channel.fd_, ev) == -1) {
                        throw new RuntimeException(Native.getLastError());
                    }
                    logger_.debug("[register] Mod the key: {}", key);
                } else {
                    remove(key);
                }
            } else {
                int cfd = channel.fd_;
                if (Native.epoll_ctl(fd_, Native.EPOLL_CTL_ADD, cfd, ev) == -1) {
                    throw new RuntimeException(Native.getLastError());
                }
                keySet_.add(key);
                fdKeyMap_.put(cfd, key);
                logger_.debug("[register] Add new key: {}", key);
            }
        }

        return key;
    }

    private void remove(EPollSelectionKey key) {

        // Precondition: synchronized by keySet_

        int fd = key.channel().fd_;
        Native.EPollEvent.ByReference ev = tmpEvent_;
        ev.clear();
        ev.update(fd, 0);
        if (Native.epoll_ctl(fd_, Native.EPOLL_CTL_DEL, fd, ev) == -1) {
            throw new RuntimeException(Native.getLastError());
        }

        deregister(key);
        fdKeyMap_.remove(fd);
        keySet_.remove(key);
        logger_.debug("[remove] Remove the key: {}", key);
    }

    @Override
    public Set<SelectionKey> keys() {
        if (!isOpen()) {
            throw new ClosedSelectorException();
        }

        return Collections.unmodifiableSet(keySet_);
    }

    @Override
    public Set<SelectionKey> selectedKeys() {
        if (!isOpen()) {
            throw new ClosedSelectorException();
        }

        return selectedKeySet_;
    }

    private int poll(int timeout) throws IOException {
        synchronized (lock_) {
            if (!isOpen()) {
                throw new ClosedSelectorException();
            }
            synchronized (keySet_) {
                synchronized (selectedKeySet_) {
                    processCancelledKeys();

                    int count;
                    try {
                        begin();
                        count = Native.epoll_wait(fd_, eventsHead_.getPointer(), eventBuffer_.length, timeout);
                    } finally {
                        end();
                    }

                    return updateSelectedKeys(count);
                }
            }
        }
    }

    private void processCancelledKeys() throws IOException {
        Set<SelectionKey> cancelledKeys = cancelledKeys();
        synchronized (cancelledKeys) {
            Set<SelectionKey> keySet = keySet_;
            for (Iterator<SelectionKey> i = cancelledKeys.iterator(); i.hasNext();) {
                SelectionKey key = i.next();
                if (keySet.contains(key)) {
                    remove((EPollSelectionKey) key);
                }
            }
        }
    }

    private int updateSelectedKeys(int count) throws IOException {
        SelectedKeySet selectedKeySet = selectedKeySet_;
        int eventFd = eventFd_;
        int selected = 0;
        for (int i = 0; i < count; i++) {
            Native.EPollEvent event = new Native.EPollEvent(eventBuffer_[i].getPointer());
            int fd = event.data_.fd_;
            if (fd == eventFd) {
                logger_.debug("[updateSelectedKeys] Poll event fd: {}", fd);
                if (Native.eventfd_read(fd, eventFdBuffer_) == 0) {
                    continue;
                }
                // Assume that the errno is set.
                throw new IOException(Native.getLastError());
            }

            EPollSelectionKey key = fdKeyMap_.get(event.data_.fd_);
            int ops = selectedKeySet.add(key) ? 0 : key.readyOps();
            int events = event.events_;
            if ((events & Native.EPOLLIN) != 0) {
                ops |= key.isInterestedInRead() ? SelectionKey.OP_READ : SelectionKey.OP_ACCEPT;
            }
            if ((events & Native.EPOLLOUT) != 0) {
                ops |= ((events & Native.EPOLLET) != 0) ? SelectionKey.OP_WRITE : SelectionKey.OP_CONNECT;
            }
            key.updateReadyOps(ops);
            logger_.debug("[updateSelectedKeys] {}", key);
            selected++;
        }
        return selected;
    }

    @Override
    public int selectNow() throws IOException {
        return poll(0);
    }

    @Override
    public int select(long timeout) throws IOException {
        if (timeout < 0) {
            throw new IllegalArgumentException("The timeout is negative.");
        }
        int timeoutMillis = (timeout <= Integer.MAX_VALUE) ? (int) timeout : Integer.MAX_VALUE;
        return poll((timeoutMillis != 0) ? timeoutMillis : -1);
    }

    @Override
    public int select() throws IOException {
        return poll(-1);
    }

    @Override
    public Selector wakeup() {
        if (Native.eventfd_write(eventFd_, 1L) == 0) {
            return this;
        }
        // Assume that errno is set.
        throw new RuntimeException(Native.getLastError());
    }

    /**
     * The set to hold {@link net.ihiroky.uds4j.EPollSelectionKey} which selected by epoll().
     */
    private static class SelectedKeySet implements Set<SelectionKey> {

        final Set<SelectionKey> keys_;

        SelectedKeySet(int maxKeySize) {
            keys_ = new HashSet<SelectionKey>(maxKeySize);
        }

        @Override
        public Iterator<SelectionKey> iterator() {
            return keys_.iterator();
        }

        @Override
        public Object[] toArray() {
            return keys_.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return keys_.toArray(a);
        }

        @Override
        public boolean add(SelectionKey selectionKey) {
            throw new UnsupportedOperationException();
        }

        private boolean add(EPollSelectionKey selectionKey) {
            return keys_.add(selectionKey);
        }

        @Override
        public boolean remove(Object o) {
            return keys_.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return keys_.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends SelectionKey> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return keys_.retainAll(c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return keys_.removeAll(c);
        }

        @Override
        public void clear() {
            keys_.clear();
        }

        @Override
        public int size() {
            return keys_.size();
        }

        @Override
        public boolean isEmpty() {
            return keys_.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return keys_.contains(o);
        }
    }
}
