package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Utils {

    private static final int COMPRESSED_FILE_MULTIPLIER = 11;
    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static long estimateBlobRawSize(AzureStorageClient azureStorageClient, String blobPath) throws StorageException, URISyntaxException {
        long blobSize = azureStorageClient.getBlobSize(blobPath);

        return azureStorageClient.isCompressed(blobPath) ?
                blobSize * COMPRESSED_FILE_MULTIPLIER : blobSize;
    }

    public static long estimateFileRawSize(AzureStorageClient azureStorageClient, String filePath) {
        File file = new File(filePath);
        long fileSize = file.length();

        return azureStorageClient.isCompressed(filePath) ?
                fileSize * COMPRESSED_FILE_MULTIPLIER : fileSize;
    }

    public static long resultSetToCsv(ResultSet resultSet, Writer writer, boolean includeHeaderAsFirstRow)
            throws IngestionClientException {
        final String LINE_SEPARATOR = System.getProperty("line.separator");

        try {
            String columnSeparator = "";

            ResultSetMetaData metaData = resultSet.getMetaData();
            int numberOfColumns = metaData.getColumnCount();

            if (includeHeaderAsFirstRow) {
                for (int column = 0; column < numberOfColumns; column++) {
                    writer.write(columnSeparator);
                    writer.write(metaData.getColumnLabel(column + 1));

                    columnSeparator = ",";
                }

                writer.write(LINE_SEPARATOR);
            }

            int numberOfRecords = 0;
            long numberOfChars = 0;

            // Get all rows.
            while (resultSet.next()) {
                numberOfChars += writeResultSetRow(resultSet, writer, numberOfColumns);
                writer.write(LINE_SEPARATOR);
                // Increment row count
                numberOfRecords++;
            }

            log.debug("Number of chars written from column values: {}", numberOfChars);

            long totalNumberOfChars = numberOfChars + numberOfRecords * LINE_SEPARATOR.length();

            log.debug("Wrote resultset to file. CharsCount: {}, ColumnCount: {}, RecordCount: {}"
                    , numberOfChars, numberOfColumns, numberOfRecords);

            return totalNumberOfChars;
        } catch (Exception ex) {
            String msg = "Unexpected error when writing result set to temporary file.";
            log.error(msg, ex);
            throw new IngestionClientException(msg);
        } finally {
            try {
                writer.close();
            } catch (IOException e) { /* ignore */
            }
        }
    }

    private static int writeResultSetRow(ResultSet resultSet, Writer writer, int numberOfColumns)
            throws IOException, SQLException {
        int numberOfChars = 0;
        String columnString;
        String columnSeparator = "";

        for (int i = 1; i <= numberOfColumns; i++) {
            writer.write(columnSeparator);
            writer.write('"');
            columnString = resultSet.getObject(i).toString().replace("\"", "\"\"");
            writer.write(columnString);
            writer.write('"');

            columnSeparator = ",";
            numberOfChars += columnString.length();
        }

        return numberOfChars
                + numberOfColumns * 2 * columnSeparator.length() // 2 " per column
                + numberOfColumns - 1 // last column doesn't have a separator
                ;
    }
}