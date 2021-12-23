package com.microsoft.azure.kusto.data;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class UncloseableStreamTest {

    private static UncloseableStream stream;

    @BeforeAll
    static void setUp() throws IOException {
        InputStream mockStream = mock(InputStream.class);
        when(mockStream.read()).thenReturn(1);
        doThrow(new IOException("Stream was closed")).when(mockStream).close();
        stream = new UncloseableStream(mockStream);
    }

    @Test
    void close() throws IOException {
        stream.close();
        verify(stream.getInnerStream(), never()).close();
    }

    @Test
    void read() throws IOException {
        stream.read();
        verify(stream.getInnerStream(), times(1)).read();
    }

    @Test
    void skip() throws IOException {
        stream.skip(0);
        verify(stream.getInnerStream(), times(1)).skip(0);
    }

    @Test
    void available() throws IOException {
        stream.available();
        verify(stream.getInnerStream(), times(1)).available();
    }

    @Test
    void mark() {
        stream.mark(0);
        verify(stream.getInnerStream(), times(1)).mark(0);
    }

    @Test
    void reset() throws IOException {
        stream.reset();
        verify(stream.getInnerStream(), times(1)).reset();
    }

    @Test
    void markSupported() {
        stream.markSupported();
        verify(stream.getInnerStream(), times(1)).markSupported();
    }
}