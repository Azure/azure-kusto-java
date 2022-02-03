// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class allows parent resources (HttpClient and HttpResponse) to be closed when the stream is closed.
 * This becomes necessary when passing an InputStream externally, as postToStreamingOutput does. The parent resources
 * must be closed, but this cannot happen until the InputStream is consumed.
 * It takes a stream, and simply forwards the call to all of its methods, except for the {@code close()} method - which does nothing.
 */
public class CloseParentResourcesStream extends InputStream {
    private final InputStream innerStream;
    private final CloseableHttpResponse httpResponse;

    public CloseParentResourcesStream(CloseableHttpResponse httpResponse) throws IOException {
        this.innerStream = httpResponse.getEntity().getContent();
        this.httpResponse = httpResponse;
    }

    /**
     * Closes all parent resources once the stream is closed
     */
    @Override
    public void close() throws IOException {
        innerStream.close();
        httpResponse.close();
    }

    @Override
    public int read() throws IOException {
        return innerStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return innerStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return innerStream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return innerStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return innerStream.available();
    }

    @Override
    public synchronized void mark(int readlimit) {
        innerStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        innerStream.reset();
    }

    @Override
    public boolean markSupported() {
        return innerStream.markSupported();
    }
}