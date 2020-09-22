package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.univocity.parsers.csv.CsvRoutines;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;

public class IngestionUtils {
    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @NotNull
    public static StreamSourceInfo fileToStream(FileSourceInfo fileSourceInfo) throws IngestionClientException, FileNotFoundException {
        String filePath = fileSourceInfo.getFilePath();
        File file = new File(filePath);
        if (file.length() == 0) {
            String message = "Empty file.";
            log.error(message);
            throw new IngestionClientException(message);
        }
        InputStream stream = new FileInputStream(filePath);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false, fileSourceInfo.getSourceId());
        streamSourceInfo.setCompressionType(AzureStorageClient.getCompression(filePath));
        return streamSourceInfo;
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
}
