# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [UNRELEASED]

### Added
- The SDK now provides Reactor Core-based asynchronous APIs for all query, management, streaming query/ingestion (StreamingClient) endpoints,
enabling non-blocking operations. You can read more about Reactor Core and [Mono type here](https://projectreactor.io/docs/core/release/api/).
- `ConnectionStringBuilder` now supports keywords without regards to spaces or case. It now supports `toString()` that prints a canonical connection string, with censored secrets by default.
### Changed
- [BREAKING] All synchronous query/management, streaming query/ingestion (StreamingClient) APIs now delegate to their asynchronous counterparts
internally and block for results.
- [BREAKING] * Make ManagedStreamingQueuingPolicy internal, expose just a factor
* Dont allow users to pass raw data size, provide it only if we have it
- [BREAKING] Removing max keep alive from HttpClientPropertiesBuilder.
### Fixed
- Fixed edge cases in query timeouts.
- Long Queries would time out after 2 minutes. Remove keep alive timeout to fix.

## [6.0.0-ALPHA-01] - 2024-11-27
### Added
- A new policy heuristic for choosing between queuing and streaming in Managed streaming client. A policy can be configured
based on the size, format and compression of data. This will also allow users to stream bigger than 4mb non-compressed data (which was the previous limit).
- Added support for the http protocol, only for connection without authentication.
### Fixed
- Some better error messages

### Changed
- Replaced Apache CloseableHttpClient with configurable azure-core client.
- [BREAKING] HttpClientFactory now accepts clients implementing azure-core HttpClient.
- [BREAKING] HttpClientProperties and HttpClientPropertiesBuilder now use azure-core ProxyOptions.
- Data client now wraps internal HTTP client.
- Moved HTTP request tracing logic into a builder class.
- Moved HTTP request building logic into a builder class.
- [BREAKING] Redirects are disabled by default. Use ClientRequestProperties "client_max_redirect_count" option 
    to enable. Default changed to 0.
- [BREAKING] Added exception to signature of ResourceAlgorithms.postToQueueWithRetries.
- [BREAKING] Removed maxConnectionsPerRoute as it was not easily provided by azure-core.
- [BREAKING] IPv6 addresses must now be enclosed in brackets ( [] ) within URLs.
- Removed the dependency on Apache HTTP client URIBuilder for URL parsing.

## [5.2.0] - 2024-08-27
### Fixed
- Used Msal user prompt old code which is deprecated in the new version coming from last bom update resulted in method not found exception.
### Added
- Proxy planner support for http client
- Introduce a new `supportedProtocols` field in `HttpClientProperties` to allow specifying SSL/TLS protocols.

## [5.1.1] - 2024-07-25
### Fixed
- Fix population of application and client version for tracing

## [5.1.0] - 2024-06-25
### Added
- Azure CLI authentication
- Enhanced the robustness of the ingestion client
### Fixed
- Solved dependency issues

## [5.0.5] - 2024-03-06
### Fixed
- Fixed bugs in how ClientRequestProperties' servertimeout is set/get, and aligned the 3 different ways to set/get this option
- Replaced deprecated OpenTelemetry dependency
- Fixed ConcurrentModificationException bug in ranked storage account buckets

## [5.0.4] - 2024-01-23
### Fixed
- Getting ingestion resources could fail with containers list being empty.
- Added missing headers in commands.
- Throw KustoServiceQueryError when Kusto service result includes OneApiError(s).

### Changed
- Changed binary files data format compression to `false`.
- Enforce valid server timeout range.

## [5.0.3] - 2023-11-27
### Fixed
- IOException is sometimes considered transient.

### Added
- Smarter way for retries with StorageAccounts.
- Support for new PlayFab domain.
- Retries on metadata fetch.

## [5.0.2] - 2023-08-24
### Fixed
- Close the HTTP response properly.

## [5.0.1] - 2023-07-31
### Added
- Automatic retries for queued ingestion.
- Methods `executeQuery`, `executeMgmt` to call with a specific type.

### Fixed
- Timer was used if authentication throws after client was closed.
- Public client credentials (user prompt, device auth) are synchronized - so that users are prompted once.
- Msal version was outdated after some changes and collided with azure-identity msal dependency.

## [5.0.0] - 2023-06-27
### Fixed
- Reverted back to using Java 8.
- Updated BOM version and msal.
- Replaced non-ascii characters in headers to comply with the service.

### Security
- Disabled redirects by default to enhance security.

## [4.0.4] - 2023-02-20
### Added
- Added new Trident endpoint support.

## [4.0.3] - 2023-02-14
### Changed
- Aligned HTTP header sending with the other SDKs.

## [4.0.1] - 2022-12-15
### Added
- Added Trident endpoint support.

## [3.2.1-nifi] - 2022-12-07
### Changed
- Incorporated Jackson removal from version 4.0.0 to support newer environments.

## [4.0.0] - 2022-11-27
### Changed
- [BREAKING] The minimum JDK version for the SDK is now up to JDK 11.
- [BREAKING] Updated to Blob Storage v12 SDK.
- [BREAKING] Removed org.json library. Exception signatures have been changed accordingly.
- Automatically add process name and username to queries.
- Upgraded apache.commons.text to version 1.10.0 to address security issues.
- Improvements to Quickstart documentation.

## [3.2.1] - 2022-11-27
### Changed
- Security fix: Upgraded apache.commons.text to version 1.10.0.

## [3.2.0] - 2022-10-09
### Added
- HTTP Keep-Alive header support.
- Deprecated constructors in favor of new ones.
- Support for special characters in table names.
- Javadocs and other documentation improvements.
- Added the capability to ignore the first record during ingestion.

### Fixed
- Endpoint validation improvement.
- MSAL scopes URL fixed to properly append ".default".

## [3.1.3] - 2022-07-11
### Added
- Introduced `QueuedIngestClient` interface for queued ingestion options.

## [3.1.2] - 2022-06-21
### Fixed
- System properties such as proxies not working for cloudinfo - this usage is now working: java -ea -Dhttp.proxyHost=1.2.34 -Dhttp.proxyPort=8989 -Dhttps.proxyHost=1.2.3.4 -Dhttps.proxyPort=8989
### Changed
- [BREAKING] Change default authority of device authentication to "organizations"

## [3.1.1] - 2022-05-29
### Added
- Option to set client version for tracing.
- Enhanced date/time formatter.

### Fixed
- Acceptance of valid cluster URIs clarified.
- Support for proxy system properties established.

## [3.1.0] - 2022-03-20
### Added
- Shared HTTP client across requests.
- Support for non-oneapi exceptions.
- Increased visibility for specific APIs.
- Ingestion improvements, including optional first record ignore.
- Implementation of validation policies.

### Fixed
- Fixed Quickstart Maven dependency.
- Fixed `getIntegerObject()` return type.

### Improved
- Github actions now triggered on pull requests.

[5.0.4]: https://github.com/user/repo/compare/v5.0.3...v5.0.4
[5.0.3]: https://github.com/user/repo/compare/v5.0.2...v5.0.3
[5.0.2]: https://github.com/user/repo/compare/v5.0.1...v5.0.2
[5.0.1]: https://github.com/user/repo/compare/v5.0.0...v5.0.1
[5.0.0]: https://github.com/user/repo/compare/v4.0.4...v5.0.0
[4.0.4]: https://github.com/user/repo/compare/v4.0.3...v4.0.4
[4.0.3]: https://github.com/user/repo/compare/v4.0.1...v4.0.3
[4.0.1]: https://github.com/user/repo/compare/v4.0.0...v4.0.1
[4.0.0]: https://github.com/user/repo/compare/v3.2.1-nifi...v4.0.0
[3.2.1-nifi]: https://github.com/user/repo/compare/v3.2.1...v3.2.1-nifi
[3.2.1]: https://github.com/user/repo/compare/v3.2.0...v3.2.1
[3.2.0]: https://github.com/user/repo/compare/v3.1.3...v3.2.0
[3.1.3]: https://github.com/user/repo/compare/v3.1.2...v3.1.3
[3.1.2]: https://github.com/user/repo/compare/v3.1.1...v3.1.2
[3.1.1]: https://github.com/user/repo/compare/v3.1.0...v3.1.1
[3.1.0]: https://github.com/user/repo/compare/v3.0.0...v3.1.0
