package net.ihiroky.uds4j;

import java.io.IOException;
import java.net.ProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;

/**
 * A provider class for selectors and selectable channels. The only {@link #openSelector()}
 * can return a new instance. The other methods throw {@link java.lang.UnsupportedOperationException}
 */
public class EPollSelectorProvider extends SelectorProvider {

    @Override
    public DatagramChannel openDatagramChannel() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DatagramChannel openDatagramChannel(ProtocolFamily family) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pipe openPipe() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AbstractSelector openSelector() throws IOException {
        return EPollSelector.open();
    }

    @Override
    public ServerSocketChannel openServerSocketChannel() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        throw new UnsupportedOperationException();
    }
}
