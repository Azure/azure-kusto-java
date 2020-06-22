// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import java.util.Date;
import java.util.UUID;

public class IngestionFailureInfo {
    public UUID OperationId;
    public String Database;
    public String Table;
    public Date FailedOn;
    public UUID IngestionSourceId;
    public String IngestionSourcePath;
    public String Details;
    public FailureStatusValue FailureStatus;
    public UUID RootActivityId;
    public Boolean OriginatesFromUpdatePolicy;

    public enum FailureStatusValue {
        Unknown(0),
        Permanent(1),
        Transient(2),
        Exhausted(3);

        private final int value;

        FailureStatusValue(int v) {
            value = v;
        }

        public int getValue() {
            return value;
        }
    }
}

