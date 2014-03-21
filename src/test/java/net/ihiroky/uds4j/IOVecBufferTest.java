package net.ihiroky.uds4j;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 *
 */
public class IOVecBufferTest {

    private IOVecBuffer sut_;

    @Before
    public void setUp() throws Exception {
        sut_ = IOVecBuffer.getInstance();
    }

    @Test
    public void testDirect() throws Exception {
        ByteBuffer direct = ByteBuffer.allocateDirect(3);
        int beforePoolSize = sut_.getPooledBuffers();
        sut_.set(10, direct);

        Native.IOVec vec = sut_.get(10);
        assertThat(vec.iovBase_, is(sameInstance(direct)));
        assertThat(vec.iovLen_, is(direct.remaining()));
        assertThat(sut_.getPooledBuffers(), is(beforePoolSize));
        sut_.clear(10, direct);
    }

    @Test
    public void testIndirect() throws Exception {
        ByteBuffer indirect = ByteBuffer.allocate(3);
        int beforePoolSize = sut_.getPooledBuffers();
        indirect.put(new byte[]{0, 1, 2}).flip();

        sut_.set(10, indirect); // In this case, no suitable buffer is found. So Allocate new one.

        Native.IOVec vec = sut_.get(10);
        assertThat(vec.iovBase_.isDirect(), is(true));
        assertThat(vec.iovBase_, is(indirect));
        assertThat(vec.iovLen_, is(indirect.remaining()));
        sut_.clear(10, indirect);
        assertThat(sut_.getPooledBuffers(), is(beforePoolSize + 1));
    }
}
