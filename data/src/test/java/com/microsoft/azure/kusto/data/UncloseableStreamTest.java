package com.microsoft.azure.kusto.data;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class UncloseableStreamTest {
    private UncloseableStream stream;
    private static final byte[] bytes = "0123456789".getBytes(StandardCharsets.UTF_8);

    @BeforeEach
    void setUp() throws IOException {
        InputStream mockStream = mock(ByteArrayInputStream.class, withSettings().useConstructor(bytes).defaultAnswer(InvocationOnMock::callRealMethod));
        doThrow(new IOException("Stream was closed")).when(mockStream).close();
        stream = new UncloseableStream(mockStream);
    }

    @Test
    void WhenClose_ThenInnerStreamIsNotClosed() throws IOException {
        stream.close();
        verify(stream.getInnerStream(), never()).close();
    }

    @Test
    void WhenReadThenClose_ThenInnerStreamIsNotClosed() throws IOException {
        int amount = 3;
        byte[] buffer = new byte[amount];
        int read = stream.read(buffer);
        assertEquals(amount, read);
        for (int i = 0; i < amount; i++) {
            assertEquals(bytes[i], buffer[i]);
        }

        stream.close();

        read = stream.read(buffer);
        assertEquals(amount, read);
        for (int i = 0; i < amount; i++) {
            assertEquals(bytes[amount + i], buffer[i]);
        }

        verify(stream.getInnerStream(), times(2)).read(buffer);
        verify(stream.getInnerStream(), never()).close();
    }

    @Test
    void WhenSkipThenClose_ThenInnerStreamIsNotClosed() throws IOException {
        int amount = 3;

        int skipped = (int)stream.skip(amount);
        assertEquals(amount, skipped);

        stream.close();

        byte[] buffer = new byte[amount];
        int read = stream.read(buffer);
        assertEquals(amount, read);
        for (int i = 0; i < amount; i++) {
            assertEquals(bytes[skipped + i], buffer[i]);
        }

        verify(stream.getInnerStream(), times(1)).skip(amount);
        verify(stream.getInnerStream(), times(1)).read(buffer);
        verify(stream.getInnerStream(), never()).close();
    }

    @Test
    void WhenAvailableThenClose_ThenInnerStreamIsNotClosed() throws IOException {
        int total = bytes.length;

        assertEquals(total, stream.available());

        stream.close();

        assertEquals(total, stream.available());

        int amount = 3;
        byte[] buffer = new byte[amount];
        int read = stream.read(buffer);
        assertEquals(amount, read);
        for (int i = 0; i < amount; i++) {
            assertEquals(bytes[i], buffer[i]);
        }

        assertEquals(total - amount, stream.available());

        verify(stream.getInnerStream(), times(3)).available();
        verify(stream.getInnerStream(), times(1)).read(buffer);
        verify(stream.getInnerStream(), never()).close();
    }

    @Test
    void WhenResetThenClose_ThenInnerStreamIsNotClosed() throws IOException {
        int amount = 3;
        byte[] buffer = new byte[amount];
        int read = stream.read(buffer);
        assertEquals(amount, read);
        for (int i = 0; i < amount; i++) {
            assertEquals(bytes[i], buffer[i]);
        }

        stream.close();
        stream.reset();

        read = stream.read(buffer);
        assertEquals(amount, read);
        for (int i = 0; i < amount; i++) {
            assertEquals(bytes[i], buffer[i]);
        }

        verify(stream.getInnerStream(), times(2)).read(buffer);
        verify(stream.getInnerStream(), times(1)).reset();
        verify(stream.getInnerStream(), never()).close();
    }

    @Test
    void WheMarkThenClose_ThenInnerStreamIsNotClosed() throws IOException {
        int amount = 3;
        byte[] buffer = new byte[amount];
        int read = stream.read(buffer);
        assertEquals(amount, read);
        for (int i = 0; i < amount; i++) {
            assertEquals(bytes[i], buffer[i]);
        }

        stream.close();
        stream.mark(amount);
        stream.reset();

        read = stream.read(buffer);
        assertEquals(amount, read);
        for (int i = 0; i < amount; i++) {
            assertEquals(bytes[amount + i], buffer[i]);
        }

        verify(stream.getInnerStream(), times(2)).read(buffer);
        verify(stream.getInnerStream(), times(1)).reset();
        verify(stream.getInnerStream(), times(1)).mark(amount);
        verify(stream.getInnerStream(), never()).close();
    }


    @Test
    void TestMarkSupported_MatchesInnerMarkSupported() throws IOException {
        assertEquals(stream.markSupported(), stream.getInnerStream().markSupported());
        stream.close();
        assertEquals(stream.markSupported(), stream.getInnerStream().markSupported());

        verify(stream.getInnerStream(), times(4)).markSupported();
        verify(stream.getInnerStream(), never()).close();
    }
}