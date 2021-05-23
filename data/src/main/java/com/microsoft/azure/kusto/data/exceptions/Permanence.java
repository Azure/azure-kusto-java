package com.microsoft.azure.kusto.data.exceptions;

public enum Permanence {
    TRUE,
    FALSE,
    UNKNOWN;

    public static Permanence fromBool(boolean bool)
    {
        return bool ? Permanence.TRUE : Permanence.FALSE;
    }
}
