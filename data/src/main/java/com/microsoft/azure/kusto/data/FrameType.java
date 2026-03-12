// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

/**
 * Enumerates the frame types in a Kusto V2 query response.
 * <p>
 * A V2 response is a JSON array of frames. The first frame is always a {@link #DataSetHeader}
 * and the last is always a {@link #DataSetCompletion}. In between are table frames, either as
 * single {@link #DataTable} frames (non-progressive) or as sequences of
 * {@link #TableHeader}/{@link #TableFragment}/{@link #TableProgress}/{@link #TableCompletion}
 * frames (progressive mode).
 *
 * @see <a href="https://learn.microsoft.com/en-us/kusto/api/rest/response-v2">V2 Response Format</a>
 */
public enum FrameType {
    DataSetHeader,
    DataTable,
    TableHeader,
    TableFragment,
    TableProgress,
    TableCompletion,
    DataSetCompletion
}
