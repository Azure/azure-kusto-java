package com.microsoft.azure.kusto.data.exceptions;

public enum TriState {
    TRUE,
    FALSE,
    DONT_KNOW;

    public static TriState fromBool(boolean bool)
    {
        return bool ? TriState.TRUE : TriState.FALSE;
    }
}
