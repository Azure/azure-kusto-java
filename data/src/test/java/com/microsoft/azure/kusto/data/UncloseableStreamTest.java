package com.microsoft.azure.kusto.data;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        //noinspection ResultOfMethodCallIgnored ignore for test
        stream.read();
        verify(stream.getInnerStream(), times(1)).read();
    }

    @Test
    void skip() throws IOException {
        //noinspection ResultOfMethodCallIgnored ignore for test
        stream.skip(0);
        verify(stream.getInnerStream(), times(1)).skip(eq((long)0));
    }

    @Test
    void available() throws IOException {
        //noinspection ResultOfMethodCallIgnored ignore for test
        stream.available();
        verify(stream.getInnerStream(), times(1)).available();
    }

    @Test
    void mark() {
        stream.mark(0);
        verify(stream.getInnerStream(), times(1)).mark(eq(0));
    }

    @Test
    void reset() throws IOException {
        stream.reset();
        verify(stream.getInnerStream(), times(1)).reset();
    }

    @Test
    void markSupported() {
        //noinspection ResultOfMethodCallIgnored ignore for test
        stream.markSupported();
        verify(stream.getInnerStream(), times(1)).markSupported();
    }
}