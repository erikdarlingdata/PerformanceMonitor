# SRV-WRITE-1: owned set

## Holder
`DarlingOwnedSecrets` (public static, `PerformanceMonitor.Darling.Service/DarlingOwnedSecrets.cs`). `Current` returns a `DarlingOwnedSet(Paths, EnvNames)`; `Set(...)` swaps a volatile reference once at startup. The XML doc says why a static is used. `DarlingConfig.Load` calls `Set(Compute(config, path))`, so the worker, MCP host and web host populate it the same way.

## Capture
`DarlingConfig.Parse` fills `internal [JsonIgnore] List<string> SecretReferencesAsWritten` before any resolution. It uses a reflection walk over string properties and string collections, recursing into this assembly's types and lists. It has a depth cap of 8 and a reference-identity cycle guard, and skips `[JsonIgnore]` properties. A value counts if `DarlingSecretSource.IsReference` accepts it (case-sensitive `env:`/`file:` prefix, as that method does). So the store's `file:` credential reference is kept as written.

## Covered (proved by the census test over a fully populated config)
`Web.Network.Oidc.ClientSecret` and `EncryptedClientSecret`, `MonitoredServer.EncryptedPassword`, `RemediationEncryptedPassword`, `Password`, the `EncryptedToken` fields (mcp, web), `EncryptedPfxPassword`, `PfxPassword`, the `Smtp` `Password` and `EncryptedPassword`. Any future string field is covered automatically.

## Owned set (`Compute`)
- Paths: config directory; the path of each `file:` reference; every string property whose name ends in `Path` or `Directory` (Tls PfxPath/CertPath/KeyPath and similar); `Postgres.DataDirectory`; and, in the data directory's parent, `pg-credential.dpapi`, `pg-admin-credential.dpapi`, `pg-viewer-credential.dpapi`, `pg-mcp-credential.dpapi`, `server.crt`, `server.key`, `pg.log`.
- Env names: `DARLING_CONFIG`, `DARLING_OUTPUT_FORMAT`, `DARLING_STOPPED_MARKER`, `DOTNET_RUNNING_IN_CONTAINER`, `SystemDrive`, `USERPROFILE`, plus each `env:` reference name.

## Tests
`DarlingOwnedSecretsCensusTests`: 6 of 6 pass. They cover the missed entries, as-written capture through `Parse` (resolved value absent), parent-directory files, the holder, a source census of every `GetEnvironmentVariable("X")` literal, and a census of secret-like `[JsonPropertyName]` string properties.
Also run: `DarlingConfigTests`, `WebSecretReference*` and 8 classes that mention `DarlingConfig.cs`. 138 of 138 pass, 1 skipped. `DarlingConfigTests` has 4 failures, all SqlClient platform loads on this Mac (`ConnectionString_EntraServicePrincipal...`, `ConnectionString_ManagedIdentity...`, `ConnectionString_SqlAuth_AzureDatabase...`, `ConnectionString_MirrorsLitePosture_Integrated`).
Not run here: `LivePostgresCollectionHygieneTests` (platform assembly load). No Lite.Tests class references the changed symbols.

## Not covered
- `ScopeFor` is unchanged and is not yet switched to the new set; the add-core lane wires that.
- Paths held in non-`Path`/`Directory`-named properties are not collected, except through `file:` references.
- Env names in code reached through a non-literal argument are not seen by the census.
