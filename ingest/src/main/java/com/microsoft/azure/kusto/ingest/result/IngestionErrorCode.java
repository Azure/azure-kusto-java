// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

public enum IngestionErrorCode {
    /// <summary>
    /// Unknown error occurred
    /// </summary>
    Unknown,
    
    /// <summary>
    /// Low memory condition.
    /// </summary>
    Stream_LowMemoryCondition,
        
    /// <summary>
    /// Wrong number of fields.
    /// </summary>
    Stream_WrongNumberOfFields,
    
    /// <summary>
    /// Input stream/record/field too large.
    /// </summary>
    Stream_InputStreamTooLarge,
    
    /// <summary>
    /// No data streams to ingest
    /// </summary>
    Stream_NoDataToIngest,
    
    /// <summary>
    /// Invalid csv format - closing quote missing.
    /// </summary>
    Stream_ClosingQuoteMissing,
    
    /// <summary>
    /// Failed to download source from Azure storage - source not found
    /// </summary>
    Download_SourceNotFound,
    
    /// <summary>
    /// Failed to download source from Azure storage - access condition not satisfied
    /// </summary>
    Download_AccessConditionNotSatisfied,
    
    /// <summary>
    /// Failed to download source from Azure storage - access forbidden
    /// </summary>
    Download_Forbidden,
    
    /// <summary>
    /// Failed to download source from Azure storage - account not found
    /// </summary>
    Download_AccountNotFound,
    
    /// <summary>
    /// Failed to download source from Azure storage - bad request
    /// </summary>
    Download_BadRequest,
    
    /// <summary>
    /// Failed to download source from Azure storage - not transient error
    /// </summary>
    Download_NotTransient,
    
    /// <summary>
    /// Failed to download source from Azure storage - Unknown error
    /// </summary>
    Download_UnknownError,

    /// <summary>
    /// Access to database is denied.
    /// </summary>
    BadRequest_DatabaseAccessDenied,

    /// <summary>
    /// Authentication to data isn't valid.
    /// </summary>
    BadRequest_InvalidAuthentication,

    /// <summary>
    /// Failed to invoke update policy. Query schema does not match table schema
    /// </summary>
    UpdatePolicy_QuerySchemaDoesNotMatchTableSchema,
    
    /// <summary>
    /// Failed to invoke update policy. Failed descendant transactional update policy
    /// </summary>
    UpdatePolicy_FailedDescendantTransaction,
    
    /// <summary>
    /// Failed to invoke update policy. Ingestion Error occurred
    /// </summary>
    UpdatePolicy_IngestionError,
    
    /// <summary>
    /// Failed to invoke update policy. Unknown error occurred
    /// </summary>
    UpdatePolicy_UnknownError,
    
    /// <summary>
    /// Json pattern was not ingested with jsonMapping parameter
    /// </summary>
    BadRequest_MissingJsonMappingtFailure,
    
    /// <summary>
    /// Blob is invalid or empty zip archive
    /// </summary>
    BadRequest_InvalidOrEmptyBlob,
    
    /// <summary>
    /// Database does not exist
    /// </summary>
    BadRequest_DatabaseNotExist,
    
    /// <summary>
    /// Table does not exist
    /// </summary>
    BadRequest_TableNotExist,
    
    /// <summary>
    /// Invalid kusto identity token
    /// </summary>
    BadRequest_InvalidKustoIdentityToken,

    /// <summary>
    /// Insufficient security permissions to execute request.
    /// </summary>
    Forbidden,

    /// <summary>
    /// Blob path without SAS from Unknown blob storage
    /// </summary>
    BadRequest_UriMissingSas,
    
    /// <summary>
    /// File too large
    /// </summary>
    BadRequest_FileTooLarge,
    
    /// <summary>
    /// No valid reply from ingest command
    /// </summary>
    BadRequest_NoValidResponseFromEngine,
    
    /// <summary>
    /// Access to table is denied
    /// </summary>
    BadRequest_TableAccessDenied,
    
    /// <summary>
    /// Message is exhausted
    /// </summary>
    BadRequest_MessageExhausted,
    
    /// <summary>
    /// Bad request
    /// </summary>
    General_BadRequest,
    
    /// <summary>
    /// Internal server error occurred
    /// </summary>
    General_InternalServerError,
    
    /// <summary>
    /// Failed to invoke update policy. Cyclic update is not allowed
    /// </summary>
    UpdatePolicy_Cyclic_Update_Not_Allowed,
    
    /// <summary>
    /// Failed to invoke update policy. Transactional update policy is not allowed in streaming ingestion
    /// </summary>
    UpdatePolicy_Transactional_Not_Allowed_In_Streaming_Ingestion,

    /// <summary>
    /// Blob is empty.
    /// </summary>
    BadRequest_EmptyBlob,

    /// <summary>
    /// Blob Uri is empty..
    /// </summary>
    BadRequest_EmptyBlobUri,

    /// <summary>
    /// Ingestion properties include both ingestionMapping and ingestionMappingReference, which isn't valid.
    /// </summary>
    BadRequest_DuplicateMapping,

    /// <summary>
    /// Table name is empty or invalid.
    /// </summary>
    BadRequest_InvalidOrEmptyTableName,

    /// <summary>
    /// Database name is empty.
    /// </summary>
    BadRequest_EmptyDatabaseName,

    /// <summary>
    /// Some formats should get ingestion mapping to be ingested and the mapping reference is empty.
    /// </summary>
    BadRequest_EmptyMappingReference,

    /// <summary>
    /// Failed to parse csv mapping.
    /// </summary>
    BadRequest_InvalidCsvMapping,

    /// <summary>
    /// Invalid mapping.
    /// </summary>
    BadRequest_InvalidMapping,

    /// <summary>
    /// Invalid mapping reference.
    /// </summary>
    BadRequest_InvalidMappingReference,
    
    /// <summary>
    /// Mapping reference wasn't found.
    /// </summary>
    BadRequest_MappingReferenceWasNotFound,

    /// <summary>
    /// Azure Data Explorer entity (such as mapping, database, or table) wasn't found.
    /// </summary>
    BadRequest_EntityNotFound,

    /// <summary>
    /// Failed to parse Json mapping.
    /// </summary>
    BadRequest_InvalidJsonMapping,

    /// <summary>
    /// Blob is invalid.
    /// </summary>
    BadRequest_InvalidBlob,

    /// <summary>
    /// Format is not supported
    /// </summary>
    BadRequest_FormatNotSupported,

    /// <summary>
    /// Archive is empty.
    /// </summary>
    BadRequest_EmptyArchive,

    /// <summary>
    /// Archive is invalid.
    /// </summary>
    BadRequest_InvalidArchive,

    /// <summary>
    /// Message is corrupted
    /// </summary>
    BadRequest_CorruptedMessage,
    
    /// <summary>
    /// Inconsistent ingestion mapping
    /// </summary>
    BadRequest_InconsistentMapping,
    
    /// <summary>
    /// Syntax error
    /// </summary>
    BadRequest_SyntaxError,

    /// <summary>
    /// Unexpected character in the input stream.
    /// </summary>
    BadRequest_UnexpectedCharacterInInputStream,

    /// <summary>
    /// Table has zero retention policy and isn't the source table for any update policy.
    /// </summary>
    BadRequest_ZeroRetentionPolicyWithNoUpdatePolicy,

    /// <summary>
    /// Creation time that was specified for ingestion, isn't within the SoftDeletePeriod.
    /// </summary>
    BadRequest_CreationTimeEarlierThanSoftDeletePeriod,

    /// <summary>
    /// Request not supported.
    /// </summary>
    BadRequest_NotSupported,

    /// <summary>
    /// Entity name isn't valid.
    /// </summary>
    BadRequest_EntityNameIsNotValid,

    /// <summary>
    /// Ingestion property is malformed.
    /// </summary>
    BadRequest_MalformedIngestionProperty,

    /// <summary>
    /// Ingestion property isn't supported in this context.
    /// </summary>
    BadRequest_IngestionPropertyNotSupportedInThisContext,

    /// <summary>
    /// Blob URI is invalid.
    /// </summary>
    BadRequest_InvalidBlobUri,

    /// <summary>
    /// Abandoned ingestion.
    /// </summary>
    General_AbandonedIngestion,
    
    /// <summary>
    /// Throttled ingestion.
    /// </summary>
    General_ThrottledIngestion,
    
    /// <summary>
    /// Schema of target table at start time doesn't match the one at commit time.
    /// </summary>
    General_TransientSchemaMismatch,

    /// <summary>
    /// Avro and Json formats must be ingested with ingestionMapping or ingestionMappingReference parameter..
    /// </summary>
    BadRequest_MissingMappingtFailure,

    /// <summary>
    /// The data contains too large values in a dynamic column. HRESULT: 0x80DA000E.
    /// </summary>
    Stream_DynamicPropertyBagTooLarge,

    /// <summary>
    /// Operation has exceeded the retry attempts limit or timespan limit following a recurring transient error.
    /// </summary>
    General_RetryAttemptsExceeded,

    /// <summary>
    /// The operation has been aborted because of timeout.
    /// </summary>
    Timeout,

    /// <summary>
    /// Ingestion operation ran out of memory.
    /// </summary>
    OutOfMemory,

    /// <summary>
    /// Failed to update schema permanently.
    /// </summary>
    Schema_PermanentUpdateFailure,

    /// <summary>
    /// A new error code unknown to the client
    /// </summary>
    Misc,
}
