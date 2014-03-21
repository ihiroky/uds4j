package net.ihiroky.uds4j;

import com.sun.jna.ptr.IntByReference;

/**
 * Provides the thread local buffer for sockaddr_un.
 */
final class AddressBuffer {

    private final Native.SockAddrUn address_;
    private final IntByReference size_;

    private AddressBuffer() {
        address_ = new Native.SockAddrUn();
        size_ = new IntByReference();
    }

    private static final ThreadLocal<AddressBuffer> INSTANCE = new ThreadLocal<AddressBuffer>() {
        @Override
        protected AddressBuffer initialValue() {
            return new AddressBuffer();
        }
    };

    static AddressBuffer getInstance() {
        return INSTANCE.get();
    }

    Native.SockAddrUn getAddress() {
        return address_;
    }

    IntByReference getSize() {
        return size_;
    }
}
