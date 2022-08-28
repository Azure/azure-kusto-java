package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.univocity.parsers.csv.CsvRoutines;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;

public class IngestionUtils {
    private IngestionUtils() {
        // Hide the default constructor, since this is a utils class
    }

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @NotNull
    public static StreamSourceInfo fileToStream(FileSourceInfo fileSourceInfo, boolean resettable) throws IngestionClientException, FileNotFoundException {
        String filePath = fileSourceInfo.getFilePath();
        File file = new File(filePath);
        if (file.length() == 0) {
            String message = "Empty file.";
            log.error(message);
            throw new IngestionClientException(message);
        }
        InputStream stream = new FileInputStream(filePath);
        if (resettable) {
            stream = new ResettableFileInputStream((FileInputStream) stream);
        }

        return new StreamSourceInfo(stream, false, fileSourceInfo.getSourceId(), getCompression(filePath));
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
        return new StreamSourceInfo(byteArrayInputStream, false, resultSetSourceInfo.getSourceId());
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

    static CompressionType getCompression(String fileName) {
        if (fileName.endsWith(".gz")) {
            return CompressionType.gz;
        }
        if (fileName.endsWith(".zip")) {
            return CompressionType.zip;
        }

        return null;
    }
}
