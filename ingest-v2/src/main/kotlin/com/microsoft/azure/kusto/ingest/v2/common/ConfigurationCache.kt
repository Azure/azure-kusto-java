package com.microsoft.azure.kusto.ingest.v2.common

//import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import java.time.Duration

interface ConfigurationCache {
    val refreshInterval: Duration

    /** Gets the configuration response data. */
//    fun getConfiguration(): ConfigurationResponse
}