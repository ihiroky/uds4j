package net.ihiroky.uds4j;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 *
 */
public class ByteBufferPoolTest {

    private ByteBufferPool sut_;

    @Before
    public void setUp() throws Exception {
        sut_ = new ByteBufferPool();
    }
    @After
    public void tearDown() throws Exception {
        sut_.clear();
    }

    @Test
    public void testSearchIfEmptyThenReturnsNewBuffer() throws Exception {
        ByteBuffer buffer = sut_.search(10);

        assertThat(buffer.capacity(), is(10));
        assertThat(sut_.count(), is(0));
    }

    @Test
    public void testSearchSuitableBuffer() throws Exception {
        ByteBuffer b0 = sut_.search(1);
        ByteBuffer b1 = sut_.search(3);
        ByteBuffer b2 = sut_.search(2);
        sut_.offerLast(b0);
        sut_.offerLast(b1);
        sut_.offerLast(b2);

        ByteBuffer b = sut_.search(2);

        assertThat(b, is(sameInstance(b1)));
        assertThat(b.limit(), is(2));
        assertThat(b.remaining(), is(2));
        assertThat(sut_.search(0), is(sameInstance(b0)));
        assertThat(sut_.search(1), is(sameInstance(b2)));
    }

    @Test
    public void testSearchReleaseMinimumBuffer() throws Exception {
        ByteBuffer b0 = sut_.search(0);
        ByteBuffer b1 = sut_.search(1);
        sut_.offerLast(b0);
        sut_.offerLast(b1);

        ByteBuffer b = sut_.search(2);

        assertThat(sut_.count(), is(1));
        assertThat(b, is(not(sameInstance(b0))));
        assertThat(b, is(not(sameInstance(b1))));
    }

    @Test
    public void testOfferFirst() throws Exception {
        ByteBuffer b0 = sut_.search(0);
        ByteBuffer b1 = sut_.search(0);
        ByteBuffer b2 = sut_.search(0);

        sut_.offerFirst(b0);
        sut_.offerFirst(b1);
        sut_.offerFirst(b2);

        assertThat(sut_.search(0), is(b2));
        assertThat(sut_.search(0), is(b1));
        assertThat(sut_.search(0), is(b0));
    }

    @Test
    public void testOfferFirstExceedsSizeLimit() throws Exception {
        ByteBuffer b0 = ByteBuffer.allocateDirect(0);
        ByteBuffer b1 = ByteBuffer.allocateDirect(0);

        for (int i = 0; i < Native.IOV_MAX; i++) {
            sut_.offerFirst(b0);
        }
        sut_.offerFirst(b1);

        assertThat(sut_.count(), is(1024));
    }

    @Test
    public void testOfferLast() throws Exception {
        ByteBuffer b0 = sut_.search(0);
        ByteBuffer b1 = sut_.search(0);
        ByteBuffer b2 = sut_.search(0);

        sut_.offerLast(b0);
        sut_.offerLast(b1);
        sut_.offerLast(b2);

        assertThat(sut_.search(0), is(b0));
        assertThat(sut_.search(0), is(b1));
        assertThat(sut_.search(0), is(b2));
    }

    @Test
    public void testOfferLastExceedsSizeLimit() throws Exception {
        ByteBuffer b0 = ByteBuffer.allocateDirect(0);
        ByteBuffer b1 = ByteBuffer.allocateDirect(0);

        for (int i = 0; i < Native.IOV_MAX; i++) {
            sut_.offerLast(b0);
        }
        sut_.offerLast(b1);

        assertThat(sut_.count(), is(1024));
    }
}
