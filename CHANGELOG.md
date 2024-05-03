* Switched the internal plugin logger from `fmt`  to `zerolog` with default logging level `zerolog.Disabled`
* Added optional parameter of ydb output plugin `LogLevel` (if not defined - plugin logging disabled)
* Added the logic to automatically limit the request sizes for `BulkUpsert`, to avoid the ingestion errors
* Added the saving extra input fields as the (optional) additional JSON-formatted field named `.other`
* Supported the flexible schema parsers like `logfmt` on input
* Fixed the loss of same-time messages by (optionally) adding extra `.hash` field containing the `Cityhash64` computed over the record's data fields

## v1.1.1
* Fixed Dockerfile for build with go1.22

## v1.1.0
* Changed template of static credentials config option
* Fixed the initialization of static credentials for secure/insecure endpoints 

## v1.0.0

### Added

Initial release

### Changed

### Fixed
