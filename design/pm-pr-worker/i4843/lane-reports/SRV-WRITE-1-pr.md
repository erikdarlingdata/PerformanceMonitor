TITLE: Add POST /api/servers: web server onboarding on the viewer role (#4843) ([#PR])

Part of #4843

## What users see change

An admin seat (an OIDC admin, the shared-token seat, or loopback-only mode) can add monitored servers over HTTP with `POST /api/servers`. The body is the same JSON array of server objects the MCP `add_servers` tool takes, and the answer is that tool's own `{requested, added, skipped, collided, failed, results}` envelope. There is no page yet; the Add Server form is a later PR. A read-only seat is refused with 403.

## What changed

- **The grant.** `GRANT INSERT, DELETE ON config.config_monitored_servers TO viewer;` in `DarlingManagedRoles.cs`, mirrored in `tools/provision-roles.sql`. The role description, the provisioning status line and the README write-surface paragraph name it. The DELETE is for the remove route that follows in a later PR; the add route uses only the INSERT.
- **The route.** `MapServers` in `DarlingWebEndpoints.cs`, beside the mute and tag routes. It calls the existing `add_servers` core, so there is one validation path: the same accepted auth modes, the probe from the service before saving, and typed refusals for duplicates, collisions and probe failures.
- **Tests.** `ServerAddRouteTests` (21), `ServerAddViewerRoleLiveTests` (3, live), and a new pin in `DarlingManagedRolesTests`.

## Security

**The grant.**
- INSERT and DELETE only. No UPDATE, because neither core updates a row. A live test proves `UPDATE ... SET is_enabled` and `UPDATE ... SET encrypted_password` both fail 42501 for a role holding exactly the shipped viewer grants.
- `encrypted_password` stays SELECT-carved from viewer, so the web host can write a password blob and never read one back. Two things were checked while building: the INSERT names no `RETURNING`, and its `ON CONFLICT (server_id) DO NOTHING` needs no carved read. The add test connects as a NOSUPERUSER role built by replaying every GRANT/REVOKE in the managed batch that names viewer, adds a server through the real core, and then shows `SELECT encrypted_password` is still 42501.
- The reload-beacon trigger fires as viewer through the existing two-column `config_service` grant; the live test asserts `config_version` advanced.
- The owned-set check compares file identity of each owned file (catching symlinks, case variants and short names) but deliberately does not scan the owned directories for hard links: making one needs local read access to the target, which already defeats the control.
- The WPF read-only probe is unchanged: it asks about `config_alert_log` UPDATE, which this does not touch.

**The gate.** `CanEdit`, the same as every other admin write: the host's group-level gate (`DarlingWebSeat.IsRequestAllowed`) refuses a read-only seat before routing, and the handler checks the seat again because this route carries a credential. All `CanEdit` seats are accepted, including the shared-token and loopback seats, as the design ruled. To restrict later, tighten the handler's check.

**CSRF.** `application/json` is required, otherwise 415. That forces a CORS preflight, as on the mute routes.

**Credentials.**
- The password travels only in the request body. The route never echoes it, never logs it, and keeps it out of error text.
- Every 400 is a fixed sentence. A JSON parser's message can quote the character it stopped at, so a parse failure names nothing of the body.
- The core's caught-exception envelope never reaches the wire. It answers through the fixed 500/503 body, and the failure log carries the sentence, not the request.
- A route-level catch logs only the exception type.
- As a backstop, any submitted password that appears in the core's answer is replaced with `[redacted]` before it is written. The core's answers carry none today.
- Checked the capture paths: nothing in the web host buffers or logs request bodies (no `EnableBuffering`, no HTTP logging), and `read_latency` is not on this route.
- Tests send a recognisable fake secret and assert it appears in no response and no captured log line, across 415, 403, 429, the 20-entry cap, an oversized body, six malformed bodies, a core fault, the core's error envelope, a whole-request refusal and a leaky answer.

**Abuse limits.**
- At most 20 servers per request (400 above that).
- 64 KB body cap.
- One add in flight per process: a second gets 429. The slot is released when the core faults. A core call that never finishes holds the slot until the process restarts, so the 503 text says a restart clears it.
- Duplicate names are the core's typed `duplicate` refusal.

**Audit.** One Information line per added server: `Server added by {principal}: {server}, auth {mode}`. It never contains the secret, and the server text is sanitized before it is logged. Nothing is logged for a refused, duplicate or failed entry.

## What's not changed

- `DELETE /api/servers/{name}` and the Add Server page are separate PRs.
- No change to the core's storage (DPAPI on Windows, `env:`/`file:` references elsewhere) or any MCP tool text.
- No schema change.

## Verified at the head

- **RED without the change:**
  - The two new test files do not compile on dev (`MapServers`, `MaxServersPerAddRequest` and the other new members don't exist there).
  - With the grant line deleted from `DarlingManagedRoles.cs` and everything else kept, `TheShippedViewerGrants_LetTheAddCoreInsertAServer_AndTheBeaconTriggerFire` fails, `DarlingManagedRolesTests.BuildProvisioningSql_GrantsViewerTheNarrowCustomViewsWrite_WithoutWideningTheSchemaGrant` fails, and `ProvisionRolesAclDriftTests.ProvisionRolesSql_GrantsTheReadRolesExactlyWhatManagedProvisioningDoes` fails. With the grant restored all three pass.
  - `WithoutTheServerGrant_TheAddWrite_Fails42501_AndNothingIsSaved` replays every viewer statement except the new one and sees 42501, plus a typed `not_saved` through the core.
- **GREEN at the head (Mac, in-process, live classes against a throwaway TimescaleDB container, since removed):**
  - `ServerAddRouteTests` 21/21.
  - `ServerAddViewerRoleLiveTests` 3/3.
  - `DarlingManagedRolesTests` 27/27.
  - `ProvisionRolesAclDriftTests` 9/9.
  - `ServerTagViewerRoleLiveTests` 2/2.
- **Requested classes plus the whole guard family** (every class ending in Census, Ratchet, Adoption, Discipline, Guard, Scrub, Hygiene, Pin, Convention or Contract followed by Tests, plus `DarlingWebEndpointsTests`, `DarlingWebOidcTests`, `WebExceptionTextCensusTests`, `McpToolsListBudgetTests`, `DarlingMcpServerAdminToolsTests`, `AddServerVerbTests`, `AlertHistoryDismissTests`, `DarlingSecuritySplitLiveTests`, the editor-attribution and store-application-name classes): 97 classes, 1359 tests. The only failure is `LivePostgresCollectionHygieneTests.EveryClassUsingTheSharedStore_IsSerializedOrDocumentsWhyNot`, which is on the Mac-local list (it loads WPF types). The new live class carries the own-store marker comment, as `ServerTagViewerRoleLiveTests` does.
- **Full Darling.Tests, run once:** 22867 tests, 569 failed, 247 skipped. 446 distinct failing tests; 430 of them are on the Mac-local failing list. The other 16 are in 7 classes that are not on that list and touch nothing in this diff; I did not re-run them on dev, so CI decides them:
  - `FinOpsOptimizationViewLiveTests` (9), `FinOpsOptimizationGoldenLiveTests` (2), `FinOpsOptimizationTieBreakTests` (`function max(bytea) does not exist`), `ViewerFinOpsIntervalHonestLiveTests`;
  - `TrendPayloadBudgetLiveTests` (stream read error on the throwaway store);
  - `StoreMetricsLatestSkipScanLiveTests` (a plan-text format difference, `rows=30` against `rows=30.00`);
  - `TopCpuQueriesTextLiveTests`.
  
  Those look like the rig's Postgres version, not the change.
- `--no-incremental` Release build: 0 warnings, 0 errors. `scrubcheck --diff` exit 0.
- Not run: Lite.Tests (not touched, and never run on this Mac).

## Secret references

A server's password may be an `env:NAME` or `file:/path` reference, resolved by the host at connect time. References stay allowed with no setting to turn on. One rule, in the shared core (`DarlingMcpServerAdminTools.ParseEntry`, so the MCP `add_servers` tool and the web route both get it): a reference must not point at Darling's own configuration or secret files.

- **Owned set.** `DarlingOwnedSecrets.Current`: the config directory, the store data directory (for a managed store, the directory the store actually uses, so the default install is covered) and its parent's credential, key and log files, the compose credential directory, the log-hash-key directory, every `file:`/`env:` reference written in the config (captured before resolution), and the service's own environment names.
- **Fails closed.** Until a configuration has loaded, every `env:` and `file:` reference is refused. The `--add-server` command calls `DarlingConfig.Load` before it reaches the core, so it always runs with a populated set.
- **Same object.** The check reads the password from the entry object `ParseEntry` already holds, so the route and the core cannot read different values.
- **Refused as a class** (no real secret path needs them): a path that is not rooted; one with a `..` segment; a `\\?\` or `\\.\` prefix; a UNC root; a leading `~`; a second `:` after the drive letter; a root of `/proc` or `/sys`.
- **Otherwise** the path is resolved through symlinks (capped at 40 hops; a cycle is refused) and compared with each owned path as text (case-insensitive on Windows and macOS). It is then compared by file identity: volume and file ID on Windows, `st_dev`/`st_ino` on Unix. That covers hard links, bind mounts, 8.3 short names and case variants, which share no text with the owned path. A reference to a file that does not exist is checked through its nearest existing parent directory, so a missing file under an owned directory is still refused, and a missing file elsewhere is accepted. A file's identity is also looked up among the files under each owned directory (at most 20,000 files per check). Env names compare ordinally, case-insensitive only on Windows.
- **One sentence** for every case, naming no path and no variable: "That password reference points at this service's own configuration or secrets."
- A literal or empty password is untouched, and an unrelated `env:`/`file:` reference is accepted.

The earlier web-only allowlist (`web.serverAddSecretReferences`) and `DarlingWebSecretReferencePolicy` are deleted, with the setting's validation, sample-config block and route plumbing.

## Handoff

Finished. All four parts of the work are on this branch: the grant and route, the owned set, the core refusal, and the alias-proof comparison. Nothing is pending on the owner.

- **New tests:** `OwnedSecretReferenceRefusalTests` (MCP core refuses owned file and env references; each alias form; symlink and cycle; unrelated references accepted; identical text; literals untouched; env-name census; an unpopulated set refuses every reference; hard link; case variant; a Windows short-name case as a unit test on the comparison). `DarlingOwnedSecretsCensusTests` also pins that a default managed configuration owns the resolved store directory's files, and that the compose credential and log-hash-key directories are owned. `DarlingOwnedSecretsCensusTests` shares its collection because both set the static holder.
- **Run on the Mac:** the requested classes plus the whole guard family, 1171 tests: 5 failed, all expected here (four `DarlingConfigTests` SqlClient connection-string cases and `LivePostgresCollectionHygieneTests`, platform assembly loads). The tool-list budget test passes under its 186,010-byte ceiling; no tool text changed.
- **Not run:** Lite.Tests (none name a changed symbol). Windows-path cases (drive-letter ADS, `\\?\`) run as string forms on the Mac, not against a Windows filesystem; a VM run would confirm them.

## CHANGELOG entry

SECTION: Added
ENTRY: - **Add monitored servers from the web dashboard's API** ([#5167]) - `POST /api/servers` adds servers through the same checks and connection test as the MCP `add_servers` tool, for admin seats only: at most 20 servers per request, one add at a time, and the password is never echoed or logged. An added server's password reference (`env:` or `file:`) cannot point at Darling's own configuration or secret files, on the web route or through MCP. The web role gains INSERT and DELETE on the monitored-server list, and still cannot read the stored password. Re-run `provision-roles.sql` on a bring-your-own store.
REF: [#5167]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5167
