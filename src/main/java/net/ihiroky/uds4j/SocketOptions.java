package net.ihiroky.uds4j;

import java.net.SocketOption;

/**
 * The unique socket options for unix domain sockets.
 */
public final class SocketOptions {

    private SocketOptions() {
        throw new AssertionError();
    }

    /**
     * Enables the receiving of the credentials of the sending process in an ancillary message.
     */
    public static final SocketOption<Boolean> SO_PASSCRED =
            new SocketOptionImpl<Boolean>("SO_PASSCRED", Boolean.class);

    /**
     * An implementation of {@link java.net.SocketOption}.
     * @param <T> the type of the value for the option
     */
    private static class SocketOptionImpl<T> implements SocketOption<T> {

        final String name_;
        final Class<T> type_;

        SocketOptionImpl(String name, Class<T> type) {
            name_ = name;
            type_ = type;
        }

        @Override
        public String name() {
            return null;
        }

        @Override
        public Class<T> type() {
            return null;
        }
    }
}
