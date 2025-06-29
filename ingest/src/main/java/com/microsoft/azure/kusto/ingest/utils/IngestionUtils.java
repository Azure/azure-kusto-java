package com.microsoft.azure.kusto.ingest.utils;

import com.azure.core.implementation.ByteBufferCollector;
import com.azure.core.util.FluxUtil;
import com.microsoft.azure.kusto.data.exceptions.ExceptionUtils;
import com.microsoft.azure.kusto.ingest.ResettableFileInputStream;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.univocity.parsers.csv.CsvRoutines;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;

public class IngestionUtils {
    private IngestionUtils() {
        // Hide the default constructor, since this is a utils class
    }

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int STREAM_COMPRESS_BUFFER_SIZE = 16 * 1024;

    @NotNull
    public static StreamSourceInfo fileToStream(FileSourceInfo fileSourceInfo, boolean resettable)
            throws IngestionClientException, FileNotFoundException {
        String filePath = fileSourceInfo.getFilePath();
        File file = new File(filePath);
        if (file.length() == 0) {
            String message = "Empty file: " + file.getName();
            log.error(message);
            throw new IngestionClientException(message);
        }
        InputStream stream = new FileInputStream(filePath);
        if (resettable) {
            stream = new ResettableFileInputStream((FileInputStream) stream);
        }

        CompressionType compression = getCompression(filePath);
        return new StreamSourceInfo(stream, false, fileSourceInfo.getSourceId(), compression);
    }

    @NotNull
    public static StreamSourceInfo resultSetToStream(ResultSetSourceInfo resultSetSourceInfo) throws IOException, IngestionClientException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        new CsvRoutines().write(resultSetSourceInfo.getResultSet(), byteArrayOutputStream);
        byteArrayOutputStream.flush();
        if (byteArrayOutputStream.size() <= 0) {
            String message = "Empty ResultSet.";
            log.error(message);
            throw new IngestionClientException(message);
        }

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        return new StreamSourceInfo(byteArrayInputStream, false, resultSetSourceInfo.getSourceId(), null);
    }

    public static byte[] readBytesFromInputStream(InputStream inputStream, int bytesToRead) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numBytesRead;
        int currOffset = 0;
        byte[] data = new byte[bytesToRead];

        while (bytesToRead > 0 && (bytesToRead <= data.length - currOffset) && (numBytesRead = inputStream.read(data, currOffset, bytesToRead)) != -1) {
            buffer.write(data, currOffset, numBytesRead);
            currOffset += numBytesRead;
            bytesToRead -= numBytesRead;
        }

        return buffer.toByteArray();
    }

    public static CompressionType getCompression(String fileName) {
        if (fileName.endsWith(".gz")) {
            return CompressionType.gz;
        }
        if (fileName.endsWith(".zip")) {
            return CompressionType.zip;
        }

        return null;
    }

    public static Mono<ByteArrayInputStream> compressStream(InputStream uncompressedStream, boolean leaveOpen) {
        EmbeddedChannel encoder = new EmbeddedChannel(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
        Flux<ByteBuffer> byteBuffers = FluxUtil.toFluxByteBuffer(uncompressedStream);

        return byteBuffers
                .switchIfEmpty(Mono.error(new IngestionClientException("Empty stream.")))
                .reduce(new ByteBufferCollector(), (byteBufferCollector, byteBuffer) -> {
                    encoder.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer));
                    ByteBuf compressedByteBuf;
                    while ((compressedByteBuf = encoder.readOutbound()) != null) {
                        byteBufferCollector.write(compressedByteBuf.nioBuffer());
                        compressedByteBuf.release();
                    }
                    return byteBufferCollector;
                })
                .map(byteBufferCollector -> {
                    encoder.finish();
                    ByteBuf compressedByteBuf;
                    while ((compressedByteBuf = encoder.readOutbound()) != null) {
                        byteBufferCollector.write(compressedByteBuf.nioBuffer());
                        compressedByteBuf.release();
                    }
                    return byteBufferCollector;
                })
                .map(ByteBufferCollector::toByteArray)
                .doFinally(ignore -> {
                    encoder.finishAndReleaseAll();
                    if (!leaveOpen) {
                        try {
                            uncompressedStream.close();
                        } catch (IOException e) {
                            String msg = ExceptionUtils.getMessageEx(e);
                            log.error(msg, e);
                            throw new IngestionClientException(msg, e);
                        }
                    }
                }).map(ByteArrayInputStream::new);
    }

    /**
     * Converts an InputStream to a Mono of byte array.
     *
     * @param inputStream the InputStream to convert
     * @return a Mono containing the byte array read from the InputStream
     */
    public static Mono<byte[]> toByteArray(InputStream inputStream) {
        return Mono.create(sink -> {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int bytesRead;

            try {
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    byteArrayOutputStream.write(buffer, 0, bytesRead);
                }
                sink.success(byteArrayOutputStream.toByteArray());
            } catch (IOException e) {
                sink.error(e);
            } finally {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    sink.error(e);
                }
            }
        });
    }

    public static class IntegerHolder {
        int value;

        public int increment() {
            return value++;
        }

        public void add(int length) {
            value += length;
        }

        public int getValue() {
            return value;
        }
    }
}
