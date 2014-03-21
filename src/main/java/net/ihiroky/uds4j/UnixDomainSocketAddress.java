package net.ihiroky.uds4j;

import java.net.SocketAddress;
import java.nio.charset.Charset;

/**
 * This class implements an unix domain socket address (path to the socket file).
 */
public final class UnixDomainSocketAddress extends SocketAddress {

    private final String path_;

    static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private static final long serialVersionUID = -8928645008416192272L;

    /**
     * Constructs a new instance.
     * @param path the path to the socket file
     */
    public UnixDomainSocketAddress(String path) {
        if (path == null) {
            throw new NullPointerException("path");
        }
        if (path.length() == 0) {
            throw new IllegalArgumentException("The path must not be empty.");
        }

        path_ = path;
    }

    byte[] getPath() {
        return path_.getBytes(DEFAULT_CHARSET);
    }

    @Override
    public String toString() {
        return path_;
    }
}
