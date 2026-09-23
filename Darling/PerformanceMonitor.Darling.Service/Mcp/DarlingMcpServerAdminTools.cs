/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The server-onboarding MCP tools — <c>add_servers</c> (BULK) and <c>remove_server</c> — the Darling-only WRITE
/// surface that lets an MCP client (an LLM assistant) stand up or tear down FLEET monitoring conversationally:
/// "monitor these twenty servers with this login" writes twenty <c>config.config_monitored_servers</c> rows the
/// running service reconciles into its collection set on the next reload beacon. This is the service-side twin of
/// the Viewer's Add / Add-Multiple dialogs (<c>AddServerDialog</c> / <c>AddMultipleServersDialog</c>), and the
/// direct sibling of the #1600 Custom Views and #1608 alert-tuning MCP write tools.
///
/// <para><b>No divergent second implementation.</b> Every authority is REUSED, never re-invented: the connection
/// probe is <see cref="DarlingServerConnector.ProbeAsync"/> run IN-PROCESS (the MCP host lives inside the service,
/// which holds the network path + credentials — unlike the Viewer, whose dialogs enqueue a <c>test_connect</c>
/// command for the service to run); the case-folded dedupe gate is the shared <see cref="ServerIdHelper.BuildStorageName"/>
/// identity in a <c>HashSet&lt;string&gt;(OrdinalIgnoreCase)</c> exactly as the bulk dialog (#1549) uses; the SQL
/// password is DPAPI-encrypted through <see cref="DarlingSecrets.Protect"/> (the SAME service identity that
/// decrypts it during collection, so it round-trips) and NEVER stored, logged, or echoed in plaintext; and the
/// INSERT mirrors <c>StoreConfigProvider.SeedMonitoredServersAsync</c>'s exact column set + <c>server_id =
/// ServerIdHelper.GetDeterministicHashCode(StorageName)</c> identity, so a tool-written row JOINs the collected
/// data and the service's reconcile matches it.</para>
///
/// <para><b>add_servers</b> processes its JSON array SEQUENTIALLY (mirroring #1549's Darling bulk probe, which is
/// serial to avoid a probe storm): for each server it validates the fields, skips an exact/case-variant DUPLICATE
/// of an existing or earlier-in-batch server (<c>status:"duplicate"</c>), probes the connection (a failure is
/// <c>status:"connection_failed"</c> and does NOT abort the batch), DPAPI-encrypts the SQL password or the service-
/// principal client secret, and INSERTs the row (<c>status:"added"</c>). Windows/integrated and managed-identity
/// auth store no secret; a service principal stores its client secret exactly like a SQL password; the INTERACTIVE
/// Entra modes (MFA / device-code / default-credential) are rejected (<c>status:"invalid"</c>) — they need a broker
/// or a signed-in user and cannot run headless, whereas ServicePrincipal and ManagedIdentity are non-interactive
/// and supported (#3484). A server whose probed connection lands in a database another registration already
/// covers is refused as <c>status:"collides"</c> (#2280). The whole call returns
/// <c>{requested, added, skipped, collided, failed, results:[...]}</c>, and the four counters SUM to
/// <c>requested</c> — every per-row status is mapped to exactly one of them by <see cref="CounterOfStatus"/>
/// (#3541 A14: <c>collides</c> used to land in no counter, so a batch with a collided server summarised as
/// <c>failed: 0</c> and the server went silently unmonitored under a clean summary). <b>remove_server</b> resolves
/// the name against the <c>config.config_monitored_servers</c> DEFINITIONS — the rows it deletes from, projected
/// onto the read tools' (id, storage name, display name) identity — rather than the <c>servers</c> registry the
/// read tools resolve against (#3653 A15/A16: that registry is written on FIRST successful connect, so a server
/// added with a bad password or an unreachable host could not be removed by the tool that added it), with a
/// stricter rule than theirs: an exact match, or a partial match ONLY when it is unique — an ambiguous partial is
/// refused with the candidates named, because a first-wins partial on a DELETE removes whichever sibling sorts
/// first. It then DELETEs the definition row and says whether a registry row (history) exists beside it.</para>
///
/// <para><b>Security.</b> These tools connect (like every MCP tool) as the least-privilege <c>mcp</c> role, granted
/// (see <see cref="DarlingManagedRoles"/>) INSERT/UPDATE/DELETE on <c>config.config_monitored_servers</c> — a single
/// non-secret-key table (the DPAPI password blob is written but the <c>encrypted_password</c> column is SELECT-
/// carved from <c>mcp</c>, so a token-holder can WRITE a credential but never READ one back) — and nothing else, so
/// it still cannot reach the <c>config_command</c> service-credential pivot or the carved secret columns. The
/// <c>config_monitored_servers</c> write fires the existing <c>trg_bump_monitored_servers → config_bump_version</c>
/// trigger (SECURITY INVOKER), which the #1608 <c>config_service</c> beacon column-grant already covers, so the
/// running service hot-reloads the monitored set within one sweep. <b>The SQL password transits the MCP endpoint in
/// the request JSON</b>; on a LAN deployment front the endpoint with the documented TLS reverse proxy.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpServerAdminTools
{
    /// <summary>The connect-and-probe seam — <see cref="DefaultProbeAsync"/> in production
    /// (<see cref="DarlingServerConnector.ProbeAsync"/> in-process), a stub in the unit tests so the store-write /
    /// dedupe / encrypt / result-aggregation logic can be exercised WITHOUT a real SQL Server.</summary>
    internal delegate Task<ConnectionProbeResult> ServerProbe(MonitoredServer server, CancellationToken cancellationToken);

    private static Task<ConnectionProbeResult> DefaultProbeAsync(MonitoredServer server, CancellationToken cancellationToken) =>
        DarlingServerConnector.ProbeAsync(server, null, cancellationToken);

    [McpServerTool(Name = "add_servers"), Description(
        "Adds servers (a JSON ARRAY) to the monitored fleet: each entry is validated, probed live, and written to " +
        "the shared central store immediately if new and reachable, no confirm step; the running service picks it " +
        "up within one sweep, visible to every client. Entries process in order; one failed connection or a " +
        "duplicate does not stop the rest. Returns requested/added/skipped/collided/failed counts that sum to " +
        "requested; only added servers are monitored, so check skipped/collided/failed before calling a batch " +
        "done. A password or client secret is encrypted at rest and never returned. <<GUIDE>> " +
        "Adds one or more database servers to the fleet the Darling service monitors — BULK onboarding: pass a JSON " +
        "ARRAY of server objects and each is validated, connection-tested, and (if new and reachable) saved to the " +
        "central monitoring store, which the running service picks up within one collection sweep (no restart). " +
        "Each object: host (REQUIRED); display_name (optional, defaults to host); database (optional — set it only " +
        "to monitor a single database, e.g. one Azure SQL Database); engine (\"sqlserver\" default, or \"postgres\" " +
        "for PostgreSQL / Amazon Aurora PostgreSQL); auth (\"Windows\" for integrated security, \"SQL\" for a SQL " +
        "login, or the non-interactive Microsoft Entra modes \"ServicePrincipal\" (Entra app/client id + secret) " +
        "and \"ManagedIdentity\" for Entra-authenticated Azure SQL — default \"Windows\"; a PostgreSQL target " +
        "REQUIRES \"SQL\"); username + password (REQUIRED for \"SQL\" auth — and for \"ServicePrincipal\", where " +
        "username is the application/client id and password is the client secret; for \"ManagedIdentity\" both are " +
        "optional — set username to a user-assigned identity's client id, omit it for system-assigned; ignored for " +
        "\"Windows\"); port (optional, PostgreSQL only — omit for 5432); " +
        "encrypt_mode (\"Optional\"|\"Mandatory\"|\"Strict\", default \"Mandatory\"); " +
        "trust_server_certificate (bool, default false — set true to accept a self-signed server cert, and " +
        "typically REQUIRED for Aurora, which presents an RDS CA a stock trust store does not know); " +
        "read_only_intent (bool, default false); multi_subnet_failover (bool, default false). Servers are processed " +
        "IN ORDER, one at a time. A case-variant or exact duplicate of an already-monitored server (or an earlier " +
        "entry in the same array) is skipped as status \"duplicate\". A server that fails to connect is recorded as " +
        "status \"connection_failed\" and does NOT stop the rest of the batch. The INTERACTIVE Microsoft Entra modes " +
        "(MFA / device-code / default-credential) are rejected (status \"invalid\") — they need a broker or a " +
        "signed-in user and cannot run headless; ServicePrincipal and ManagedIdentity are non-interactive and are " +
        "supported. A server whose probed connection lands in a database that ANOTHER monitored server already " +
        "covers (it names one database but connects to a different one, e.g. a wrong Initial Catalog) is refused as " +
        "status \"collides\" — adding it would store one database's history under two identities and alert twice. " +
        "A SQL password or service-principal client secret is encrypted at rest (DPAPI, the service identity) and " +
        "is never returned. Returns {requested:N, added:N, skipped:N, collided:N, failed:N, results:[{server, " +
        "status:\"added\"|\"duplicate\"|\"collides\"|\"connection_failed\"|\"invalid\", detail}]}. requested is " +
        "the number of entries you sent and the four counters SUM to it — every entry lands in exactly one: " +
        "\"added\" → added, \"duplicate\" → skipped, \"collides\" → collided, \"connection_failed\" and " +
        "\"invalid\" → failed. Only added servers are monitored; read the other three counters before treating " +
        "the batch as done. An added server's detail reports what the probe found — for a PostgreSQL target that " +
        "includes writer-vs-reader, Aurora-vs-not, and how many of the PostgreSQL collectors apply to it. NOTE: the " +
        "password travels to this endpoint in the request; on a LAN use the documented TLS reverse proxy. " +
        "servers_json is a JSON ARRAY of server objects to add (see above for the per-object fields), e.g. " +
        "[{\"host\":\"sql01\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"...\",\"encrypt_mode\":\"Mandatory\"," +
        "\"trust_server_certificate\":true},{\"host\":\"aurora.cluster-abc.us-east-1.rds.amazonaws.com\",\"engine\":" +
        "\"postgres\",\"auth\":\"SQL\",\"username\":\"darling_monitor\",\"password\":\"...\",\"trust_server_certificate\":true}].")]
    public static Task<string> AddServers(
        NpgsqlDataSource postgres,
        [Description("A JSON ARRAY of server objects to add (see the tool description for the per-object fields and an example).")] string servers_json) =>
        AddServersAsync(postgres, servers_json, DefaultProbeAsync, CancellationToken.None);

    /// <summary>The testable core of <c>add_servers</c>: validates + dedupes + probes (through the injected
    /// <paramref name="probe"/> seam) + encrypts + INSERTs, aggregating a per-server result. Structural validation
    /// runs BEFORE any store access, and when NO structurally-valid candidate remains the store is never opened —
    /// so a call whose entries are all invalid (bad field, MFA auth) returns without a connection or a probe.</summary>
    internal static async Task<string> AddServersAsync(
        NpgsqlDataSource postgres, string servers_json, ServerProbe probe, CancellationToken cancellationToken)
    {
        try
        {
            var (entries, invalidResults, wholeError) = ParseRequest(servers_json);
            if (wholeError != null)
            {
                return Outcome("invalid", wholeError);
            }

            var results = new List<ServerResult>(invalidResults);

            /* No structurally-valid candidate → never open the store (validate-before-write); the aggregate below
               reports only the per-entry invalids. */
            if (entries.Count == 0)
            {
                return Aggregate(results);
            }

            /* Seed the case-folded dedupe gate from the authoritative store rows FIRST, then partition the batch —
               a duplicate (of an existing server OR an earlier entry in this batch, first occurrence wins) is
               skipped WITHOUT a probe, exactly as the bulk dialog (#1549) does. */
            var existingKeys = await LoadExistingStorageKeysAsync(postgres, cancellationToken);
            var (ready, duplicates) = PartitionDuplicates(entries, existingKeys);
            results.AddRange(duplicates);

            /* #2280: the identities claimed so far — the store's, plus every entry this batch is about to add.
               The gate above compares DECLARED identities; the check inside the loop compares each entry's ACTUAL
               database (what the server just told the probe) against this set, which is what catches two
               registrations resolving to one database while claiming different ones. */
            var claimed = new HashSet<string>(existingKeys, StringComparer.OrdinalIgnoreCase);
            foreach (var entry in ready)
            {
                claimed.Add(entry.StorageKey);
            }

            foreach (var entry in ready)
            {
                /* Validate the connection IN-PROCESS (the service holds the network path + credentials). A failure
                   is recorded and the batch CONTINUES — one unreachable server never aborts the rest. */
                var probeResult = await probe(entry.ProbeConfig, cancellationToken);
                if (!probeResult.Success)
                {
                    results.Add(new ServerResult(entry.Order, entry.DisplayName, AddStatus.ConnectionFailed,
                        string.IsNullOrWhiteSpace(probeResult.Error)
                            ? "Could not connect to the server."
                            : $"Could not connect: {probeResult.Error}"));
                    continue;
                }

                /* #2280: the probe just asked the server which database it actually reached. If that is a
                   DIFFERENT database from the one this entry names, and some other registration already claims
                   that one, then adding this would give one real database two identities and two full copies of
                   every collected row — the #2220 field report, prevented at the point of creation instead of
                   reported at every connect by #2277's tripwire.

                   Compared against the ACTUAL database and only when it differs from the declared one: an entry
                   that names what it reached is the normal case and is already covered by the declared gate
                   above, so re-checking it would just re-detect that gate's own decision. */
                var collision = ActualIdentityCollision(entry, probeResult.ConnectedDatabase, claimed);
                if (collision is not null)
                {
                    results.Add(new ServerResult(entry.Order, entry.DisplayName, AddStatus.Collides, collision));
                    continue;
                }

                /* DPAPI-encrypt the SQL password for storage (the service identity encrypts here and decrypts it
                   during collection, so it round-trips); Windows-auth servers store no secret. The plaintext never
                   leaves this method — it is not logged, not echoed in a result. */
                var encryptedPassword = ProtectPasswordForStorage(entry.PlaintextPassword);
                await InsertServerAsync(postgres, entry, encryptedPassword, cancellationToken);
                results.Add(new ServerResult(entry.Order, entry.DisplayName, AddStatus.Added, DescribeProbe(probeResult)));
            }

            return Aggregate(results);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("add_servers", ex);
        }
    }

    [McpServerTool(Name = "remove_server"), Description(
        "Deletes a monitored server's definition from the shared central store (config_monitored_servers) " +
        "immediately, no confirm step; the service drops it from collection within one sweep. Already-collected " +
        "historical data is NOT deleted. Matches by exact name first (display or storage name/address, " +
        "case-insensitive); a partial match is honored only if exactly one definition contains it, otherwise " +
        "nothing is deleted and candidates are listed for re-issue. Matches against DEFINITIONS, not the " +
        "connected registry, so an unconnected server can still be removed. <<GUIDE>> " +
        "Removes a monitored server from the fleet by name (its display name or storage name / address, as " +
        "list_servers reports them). The name is resolved against the monitored-server DEFINITIONS in the central " +
        "store (config_monitored_servers — the table add_servers writes and this tool deletes from), NOT against the " +
        "registry of servers that have connected, so a server that was added but has never connected can be removed " +
        "by the tool that added it. Matching is exact first (case-insensitive, against the storage name and the " +
        "display name); a PARTIAL match is honored ONLY when exactly one defined server contains the text. When " +
        "the name is ambiguous — an exact name shared by two definitions, or a fragment such as \"-01\" that " +
        "several servers contain — NOTHING is deleted and the response is {status:\"ambiguous\", candidates:[{server, " +
        "display_name, ever_connected}], message}; re-issue with one candidate's full name. Deletes the server's " +
        "definition from the central monitoring store; the running service drops it from its collection set within " +
        "one sweep. Already-collected historical data is NOT deleted. Returns {status:\"removed\", server, " +
        "display_name, matched_by:\"exact\"|\"partial\", matched_in:\"config_monitored_servers\", ever_connected} on " +
        "success — ever_connected says whether the connected-servers registry (populated on a server's first " +
        "successful connection) also had a row for it, i.e. whether any history exists under its id; " +
        "{status:\"ambiguous\", ...} as above; or {status:\"not_found\", ...} when no definition matches the name. A " +
        "not_found whose matched_in is \"servers\" means the name IS a connected server but has no definition in the " +
        "store to delete (it is defined in darling.json, or its definition was already removed); the message otherwise " +
        "lists the servers that are defined. " +
        "server_name is matched as list_servers / get_alert_history report it.")]
    public static async Task<string> RemoveServer(
        NpgsqlDataSource postgres,
        [Description("The name of the monitored server to remove — its display name or storage name / address. A partial name is accepted only when it matches exactly one server.")] string server_name)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(server_name))
            {
                return Outcome("invalid", "server_name is required.");
            }

            /* The rows this DELETE targets — the definitions in config_monitored_servers — projected onto the same
               (server_id, storage name, display name) identity the read tools resolve against, so the matching rule
               below is the one #3541 A14 wrote and the caller's vocabulary (list_servers' names) is unchanged.

               Why the definitions and not the servers registry (#3653 A15/A16): the registry is written by the
               worker on a server's FIRST successful connection. A server that add_servers defined a minute ago with
               a wrong password, or one whose host is unreachable from the service, has a definition and no registry
               row — and the pre-#3653 tool, resolving against the registry, answered not_found for the very server
               its sibling had just added; the only way to undo a bad add_servers was psql. Resolving against the
               table the DELETE runs on closes that gap by construction: whatever the tool can name, it can remove.
               The registry is still consulted, but only to DISCLOSE (ever_connected on every candidate) and to give
               the darling.json-defined server — a registry row with no store definition — an answer that names
               the real reason nothing was deleted.

               NOT the read resolver's rule, then or now. Its first-wins partial match is the right convenience
               for a read — an agent that lands on the wrong sibling sees its name in the payload and re-asks. On a
               DELETE the payload IS the damage: "-01" against "-01"/"-02" removed whichever sorted first, and
               said so only after the fact (#3541 A14). So the partial match survives, as documented, but only when
               it is UNIQUE; anything else is refused with the candidates named. */
            var definitions = await LoadDefinitionsForRemovalAsync(postgres);
            var target = ResolveForRemoval(definitions.Select(d => d.Server).ToList(), server_name);
            var everConnected = definitions.ToDictionary(d => d.Server.ServerId, d => d.EverConnected);

            if (target.Candidates.Count == 0)
            {
                /* No definition matches. Two honest answers, and the registry tells them apart: a name the
                   connected-servers registry DOES resolve (by the same rule, so a partial that is unique there is
                   honored as a match) is a server defined outside the store — darling.json — or one whose
                   definition was already deleted; naming that is the difference between "typo" and "wrong tool".
                   Otherwise the miss lists what IS defined, through the resolver's own listing (its miss message
                   over the definition rows; with zero candidates it cannot resolve to anything, so reusing it cannot
                   pick a server this method declined to). */
                var registry = await DarlingServerResolver.LoadEnabledAsync(postgres);
                var connectedOnly = ResolveForRemoval(registry, server_name);
                if (connectedOnly.Candidates.Count > 0)
                {
                    return JsonSerializer.Serialize(new
                    {
                        status = "not_found",
                        matched_in = "servers",
                        matched_by = connectedOnly.MatchedBy,
                        candidates = connectedOnly.Candidates.Select(c => new { server = c.ServerName, display_name = c.DisplayName }),
                        message = $"'{server_name}' matches {connectedOnly.Candidates.Count} server(s) in the connected-servers registry but none of the " +
                                  "monitored-server definitions in the central store, so there is nothing for this tool to delete: the server is " +
                                  "defined in darling.json (remove it there and restart the service), or its store definition was already removed. " +
                                  "Nothing was changed.",
                    }, McpHelpers.JsonOptions);
                }

                if (definitions.Count == 0)
                {
                    return Outcome("not_found", $"Could not resolve server '{server_name}': no monitored-server definitions exist in the central store (servers defined in darling.json are not removable through this tool).");
                }

                /* The read resolver's miss is the `invalid` envelope since #3739; this verb's outcome is
                   `not_found` (a delete of something that is not there is a different word from a bad
                   parameter), so the SENTENCE is lifted out of the resolver's envelope and re-wrapped in this
                   one rather than nesting JSON inside JSON. */
                var (_, missMessage) = DarlingServerResolver.ResolveOrError(definitions.Select(d => d.Server).ToList(), server_name);
                return Outcome("not_found", missMessage is null ? $"Could not resolve server '{server_name}'." : McpHelpers.ErrorMessageOf(missMessage));
            }

            if (target.Candidates.Count > 1)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "ambiguous",
                    message = $"'{server_name}' matches {target.Candidates.Count} defined servers " +
                              $"({(target.MatchedBy == "exact" ? "the same name on more than one definition" : "as a partial name")}); " +
                              "nothing was removed. Re-issue remove_server with ONE candidate's full name.",
                    matched_by = target.MatchedBy,
                    matched_in = "config_monitored_servers",
                    candidates = target.Candidates.Select(c => new { server = c.ServerName, display_name = c.DisplayName, ever_connected = everConnected[c.ServerId] }),
                }, McpHelpers.JsonOptions);
            }

            var resolved = target.Candidates[0];
            var connected = everConnected[resolved.ServerId];

            await using var command = postgres.CreateCommand("DELETE FROM config_monitored_servers WHERE server_id = $1");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = resolved.ServerId });
            var affected = await command.ExecuteNonQueryAsync();

            return affected > 0
                ? JsonSerializer.Serialize(new
                {
                    status = "removed",
                    server = resolved.ServerName,
                    display_name = resolved.DisplayName,
                    matched_by = target.MatchedBy,
                    matched_in = "config_monitored_servers",
                    ever_connected = connected,
                    note = connected
                        ? "The definition is deleted; the running service drops the server from collection within one sweep. Its connected-servers registry row and already-collected history are kept."
                        : "The definition is deleted. This server had never connected (no connected-servers registry row), so no history exists under its id and nothing else references it.",
                }, McpHelpers.JsonOptions)
                : Outcome("not_found",
                    $"'{resolved.ServerName}' was defined when this call resolved it but its definition was gone by the time the delete ran (a concurrent remove); nothing was changed.");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("remove_server", ex);
        }
    }

    /* ─────────────────────────────── pure parse + validate (no I/O) ─────────────────────────────── */

    /// <summary>One structurally-valid server ready for dedupe → probe → insert. <see cref="StorageKey"/> is the
    /// shared case-folded identity (<see cref="ServerIdHelper.BuildStorageName"/>); <see cref="ProbeConfig"/> is the
    /// <see cref="MonitoredServer"/> the in-process probe connects with (carrying the plaintext password for the
    /// connect); <see cref="PlaintextPassword"/> is retained ONLY until the post-probe DPAPI encrypt, never stored
    /// or echoed. <see cref="Order"/> is the entry's index in the input array, so the aggregated results echo input
    /// order.</summary>
    internal sealed record ParsedServerEntry(
        int Order, string DisplayName, string StorageKey, MonitoredServer ProbeConfig, string? PlaintextPassword);

    /// <summary>One per-server outcome (a <see cref="AddStatus"/> value) with a human-readable <see cref="Detail"/>;
    /// <see cref="Order"/> restores input order in the aggregate.</summary>
    internal sealed record ServerResult(int Order, string Server, string Status, string Detail);

    /// <summary>
    /// The per-row <c>status</c> vocabulary of <c>add_servers</c> — the five outcomes an entry can have, as
    /// constants so a new one cannot be introduced as a bare literal at a result site without also being placed in
    /// <see cref="CounterOfStatus"/> (the test census walks these fields and demands each has a counter).
    /// </summary>
    internal static class AddStatus
    {
        /// <summary>Probed, reachable, INSERTed — the server is now monitored.</summary>
        public const string Added = "added";

        /// <summary>A case-variant or exact duplicate of an existing server or an earlier entry in the batch; skipped
        /// without a probe (#1549).</summary>
        public const string Duplicate = "duplicate";

        /// <summary>Probed, but its connection reached a database another registration already covers (#2280); NOT
        /// added.</summary>
        public const string Collides = "collides";

        /// <summary>The in-process probe could not connect; NOT added, the batch continued.</summary>
        public const string ConnectionFailed = "connection_failed";

        /// <summary>Structurally invalid (a bad field, an unsupported auth mode); NOT added, never probed.</summary>
        public const string Invalid = "invalid";
    }

    /// <summary>
    /// Which summary counter each per-row status is counted under — the whole of the "writes report what happened"
    /// contract for this tool (#3541 A14). Every status maps to exactly one counter and every result is counted
    /// once, so <c>added + skipped + collided + failed == requested</c> by construction rather than by luck.
    ///
    /// <para>The defect this replaces: the counters were three ad-hoc <c>Count(...)</c> filters naming four of the
    /// five statuses, and the fifth — <see cref="AddStatus.Collides"/>, added by #2280 after the counters were
    /// written — fell into none of them. A batch of three with one collision summarised as
    /// <c>{added: 2, skipped: 0, failed: 0}</c>: an agent reading the summary saw a clean run, and the collided
    /// server was silently not monitored. A map the aggregator REFUSES to serialize without makes the next new
    /// status a hard error at the first call instead of a silent hole.</para>
    ///
    /// <para><c>collided</c> is its own counter rather than a kind of <c>failed</c> because the remedy differs:
    /// a failed entry is retried after fixing the connection or the fields; a collided one must NOT be retried as
    /// sent — it needs a different Initial Catalog or no registration at all, and folding it into <c>failed</c>
    /// would invite exactly the retry the refusal exists to prevent.</para>
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> CounterOfStatus = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        [AddStatus.Added] = "added",
        [AddStatus.Duplicate] = "skipped",
        [AddStatus.Collides] = "collided",
        [AddStatus.ConnectionFailed] = "failed",
        [AddStatus.Invalid] = "failed",
    };

    /// <summary>
    /// The outcome of matching a <c>remove_server</c> name against the definitions: the rows it matched and HOW.
    /// Zero candidates is a miss, one is the row to delete, more than one is a refusal.
    /// </summary>
    internal sealed record RemovalTarget(IReadOnlyList<DarlingServerResolver.RegisteredServer> Candidates, string MatchedBy);

    /// <summary>
    /// One <c>config_monitored_servers</c> row as <c>remove_server</c> resolves it (#3653 A15/A16): projected onto
    /// the read tools' <see cref="DarlingServerResolver.RegisteredServer"/> identity — the storage name rebuilt
    /// from the definition's identity columns through the SAME <see cref="ServerIdHelper.BuildStorageName"/> the
    /// worker uses when it registers the server on first connect, so a definition and its registry row carry one
    /// name and one id — plus whether that registry row exists at all.
    /// </summary>
    internal sealed record ServerDefinition(DarlingServerResolver.RegisteredServer Server, bool EverConnected);

    /// <summary>
    /// The definitions read behind <c>remove_server</c>: every row of the table the DELETE targets, with its
    /// identity columns (the same five <see cref="ExistingServersSql"/> reads for the dedupe gate, #2218's engine
    /// and port included) and the display name, plus an EXISTS against the connected-servers registry so the tool
    /// can say whether the server it is about to remove ever connected. No <c>is_enabled</c> filter: a disabled
    /// definition is still a definition, and removing one is what an operator who disabled it first would expect.
    /// Exposed const so Darling.Tests can pin the dialect ungated ($-free, no bare now(), no N'' literals).
    /// </summary>
    public const string DefinitionsForRemovalSql = @"
SELECT d.server_id, d.name, d.host, d.database, d.read_only_intent, d.engine, d.port,
       EXISTS (SELECT 1 FROM servers s WHERE s.server_id = d.server_id) AS ever_connected
FROM config_monitored_servers d
ORDER BY d.host, d.database";

    private static async Task<List<ServerDefinition>> LoadDefinitionsForRemovalAsync(NpgsqlDataSource postgres)
    {
        var definitions = new List<ServerDefinition>();
        await using var command = postgres.CreateCommand(DefinitionsForRemovalSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var serverId = reader.GetInt32(0);
            var displayName = reader.IsDBNull(1) ? null : reader.GetString(1);
            var host = reader.GetString(2);
            var database = reader.IsDBNull(3) ? null : reader.GetString(3);
            var readOnlyIntent = !reader.IsDBNull(4) && reader.GetBoolean(4);
            var engine = reader.IsDBNull(5) ? null : reader.GetString(5);
            var port = reader.IsDBNull(6) ? 0 : reader.GetInt32(6);
            var storageName = ServerIdHelper.BuildStorageName(host, database, readOnlyIntent, engine, port);
            definitions.Add(new ServerDefinition(
                new DarlingServerResolver.RegisteredServer(serverId, storageName, displayName),
                reader.GetBoolean(7)));
        }

        return definitions;
    }

    /// <summary>
    /// The matching rule for a DELETE, over the same registry rows the read resolver uses: every exact match
    /// (storage name OR display name, case-insensitive, trimmed) if there are any; otherwise every partial
    /// (<c>Contains</c>) match. The CALLER decides what a count other than one means — this only refuses to
    /// choose among equals.
    ///
    /// <para><b>Why not the read resolver's rule.</b> <see cref="DarlingServerResolver"/> is first-wins on a partial
    /// match, ordered by storage name. For a read that is a convenience: the answer names the server it resolved
    /// to, and a caller who meant the other one re-asks having lost nothing. For a delete the same rule removed
    /// <c>-01</c> when the caller typed <c>-01</c> meaning <c>-01</c>, and removed it just the same when the caller
    /// typed a fragment that <c>-01</c> and <c>-02</c> both contain — a coin the caller did not know was being
    /// flipped. The read tools keep their rule; this write does not borrow it.</para>
    ///
    /// <para><b>Why partial matching survives at all.</b> The description has promised it since the tool shipped
    /// ("resolved the same way the read tools resolve server_name"), display names are what an operator knows a
    /// server by, and a unique partial is unambiguous — refusing it would be refusing something the tool CAN
    /// honor. The rule the contract asks for is "refuse what you cannot honor", and what cannot be honored here is
    /// a choice, not a fragment.</para>
    ///
    /// <para><b>Exact matches are collected, not first-taken.</b> <c>display_name</c> is not unique in the registry,
    /// so two registrations can share one display name exactly; picking the first would be the same coin under a
    /// better-looking name. Two rows matching exactly is reported as ambiguous with <c>matched_by: "exact"</c>, and
    /// the caller disambiguates on the storage name, which IS unique.</para>
    /// </summary>
    internal static RemovalTarget ResolveForRemoval(IReadOnlyList<DarlingServerResolver.RegisteredServer> servers, string serverName)
    {
        var name = (serverName ?? string.Empty).Trim();
        if (name.Length == 0)
        {
            return new RemovalTarget(Array.Empty<DarlingServerResolver.RegisteredServer>(), "none");
        }

        var exact = servers
            .Where(s => string.Equals(s.ServerName, name, StringComparison.OrdinalIgnoreCase)
                     || string.Equals(s.DisplayName, name, StringComparison.OrdinalIgnoreCase))
            .ToList();
        if (exact.Count > 0)
        {
            return new RemovalTarget(exact, "exact");
        }

        var partial = servers
            .Where(s => s.ServerName.Contains(name, StringComparison.OrdinalIgnoreCase)
                     || (s.DisplayName?.Contains(name, StringComparison.OrdinalIgnoreCase) ?? false))
            .ToList();

        return new RemovalTarget(partial, partial.Count == 0 ? "none" : "partial");
    }

    /// <summary>
    /// PURE structural validation of the <c>servers_json</c> request — no store, no probe, no crypto — so the
    /// validate-before-write behavior is unit-testable without a live SQL Server. Returns the structurally-valid
    /// <c>Entries</c> (with their dedupe keys + probe configs built), the per-entry <c>Invalid</c> results (a bad
    /// field or an unsupported auth mode — recorded, the batch continues past them), and a non-null
    /// <c>WholeError</c> when the whole payload is unusable (not JSON, not an array, or empty), for which the caller
    /// returns a single <c>{status:"invalid"}</c> without opening the store.
    /// </summary>
    internal static (List<ParsedServerEntry> Entries, List<ServerResult> Invalid, string? WholeError) ParseRequest(string servers_json)
    {
        var entries = new List<ParsedServerEntry>();
        var invalid = new List<ServerResult>();

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(servers_json);
        }
        catch (JsonException ex)
        {
            return (entries, invalid, $"servers_json is not valid JSON: {ex.Message}");
        }

        if (root is not JsonArray array)
        {
            return (entries, invalid, "servers_json must be a JSON array of server objects (e.g. [{\"host\":\"sql01\"}]).");
        }

        if (array.Count == 0)
        {
            return (entries, invalid, "servers_json must be a non-empty JSON array — provide at least one server object.");
        }

        for (var i = 0; i < array.Count; i++)
        {
            var (entry, result) = ParseEntry(i, array[i]);
            if (entry != null)
            {
                entries.Add(entry);
            }
            else
            {
                invalid.Add(result!);
            }
        }

        return (entries, invalid, null);
    }

    /// <summary>Parses + validates ONE array element into a ready entry, or an <c>invalid</c> result naming the
    /// problem. The service honors Windows, SQL, and the two non-interactive Entra modes (ServicePrincipal,
    /// ManagedIdentity); the interactive Entra modes (MFA/device-code/default-credential) are rejected — they
    /// cannot run headless (#3484).</summary>
    private static (ParsedServerEntry? Entry, ServerResult? Result) ParseEntry(int index, JsonNode? node)
    {
        if (node is not JsonObject obj)
        {
            return (null, new ServerResult(index, $"(entry {index + 1})", AddStatus.Invalid, "Each entry must be a JSON object."));
        }

        var host = TryGetString(obj, "host");
        var label = string.IsNullOrWhiteSpace(host) ? $"(entry {index + 1})" : host!.Trim();

        ServerResult Invalid(string message) => new(index, label, AddStatus.Invalid, message);

        if (string.IsNullOrWhiteSpace(host))
        {
            return (null, Invalid("host is required."));
        }

        host = host!.Trim();
        var displayName = TryGetString(obj, "display_name") is { Length: > 0 } dn ? dn.Trim() : host;
        var databaseRaw = TryGetString(obj, "database");
        var database = string.IsNullOrWhiteSpace(databaseRaw) ? null : databaseRaw!.Trim();

        /* Auth: Windows (integrated), SQL, or the two NON-INTERACTIVE Microsoft Entra modes — ServicePrincipal
           and ManagedIdentity (#3484). Absent defaults to Windows. The interactive Entra modes (MFA,
           device-code, default-credential) are refused: they need a broker or a signed-in user and cannot run
           unattended. */
        var authRaw = TryGetString(obj, "auth");
        var authTrim = authRaw?.Trim();
        string storeAuth;
        if (string.IsNullOrWhiteSpace(authTrim) || authTrim.Equals("Windows", StringComparison.OrdinalIgnoreCase))
        {
            storeAuth = ServerStoreAuth.Integrated;
        }
        else if (authTrim.Equals("SQL", StringComparison.OrdinalIgnoreCase))
        {
            storeAuth = ServerStoreAuth.Sql;
        }
        else if (authTrim.Equals("ServicePrincipal", StringComparison.OrdinalIgnoreCase))
        {
            storeAuth = ServerStoreAuth.ServicePrincipal;
        }
        else if (authTrim.Equals("ManagedIdentity", StringComparison.OrdinalIgnoreCase))
        {
            storeAuth = ServerStoreAuth.ManagedIdentity;
        }
        else
        {
            return (null, Invalid(
                "auth must be \"Windows\", \"SQL\", \"ServicePrincipal\", or \"ManagedIdentity\". The interactive " +
                "Microsoft Entra modes (MFA, device-code, default-credential) are not supported for a headless " +
                "collector — they need a broker or a signed-in user. Use ServicePrincipal (client id + secret) or " +
                "ManagedIdentity, both non-interactive, for Entra-authenticated Azure SQL targets."));
        }

        string? username = null;
        string? plaintextPassword = null;
        if (storeAuth == ServerStoreAuth.Sql || storeAuth == ServerStoreAuth.ServicePrincipal)
        {
            /* Service principal takes the same two fields as SQL auth — the application/client id as username,
               the client secret as the password (DPAPI-encrypted after a successful probe, exactly like a SQL
               password) — so the requirement and the storage are identical; only the wording differs. */
            var isSp = storeAuth == ServerStoreAuth.ServicePrincipal;
            username = TryGetString(obj, "username");
            if (string.IsNullOrWhiteSpace(username))
            {
                return (null, Invalid(isSp
                    ? "username is required for ServicePrincipal authentication (the Entra application/client id)."
                    : "username is required for SQL authentication."));
            }

            username = username!.Trim();
            plaintextPassword = TryGetString(obj, "password");
            if (string.IsNullOrEmpty(plaintextPassword))
            {
                return (null, Invalid(isSp
                    ? "password is required for ServicePrincipal authentication (the client secret)."
                    : "password is required for SQL authentication."));
            }
        }
        else if (storeAuth == ServerStoreAuth.ManagedIdentity)
        {
            /* Managed identity carries no secret. A user-assigned identity may name its client id in username;
               a system-assigned identity omits it. Nothing is required and nothing is stored as a secret. */
            username = TryGetString(obj, "username") is { Length: > 0 } miClientId ? miClientId.Trim() : null;
        }

        /* encrypt_mode + trust_server_certificate are deliberately EXPOSED (per Erik) — a headless caller sets the
           TLS posture explicitly. Optional with the fail-closed MonitoredServer defaults (Mandatory / no trust). */
        var (encryptMode, encryptError) = ResolveEncryptMode(TryGetString(obj, "encrypt_mode"));
        if (encryptError != null)
        {
            return (null, Invalid(encryptError));
        }

        var (trustCert, trustError) = ResolveBool(obj, "trust_server_certificate", false);
        if (trustError != null)
        {
            return (null, Invalid(trustError));
        }

        var (readOnlyIntent, roError) = ResolveBool(obj, "read_only_intent", false);
        if (roError != null)
        {
            return (null, Invalid(roError));
        }

        var (multiSubnet, msError) = ResolveBool(obj, "multi_subnet_failover", false);
        if (msError != null)
        {
            return (null, Invalid(msError));
        }

        var (engine, engineError) = ResolveEngine(TryGetString(obj, "engine"));
        if (engineError != null)
        {
            return (null, Invalid(engineError));
        }

        /* The same rule DarlingConfig.Validate enforces for a file entry, enforced here so the two onboarding
           paths cannot disagree: PostgreSQL has no integrated-auth path, and defaulting auth to Windows means
           an entry that just says {"host": ..., "engine": "postgres"} would otherwise be accepted and then
           fail at every connect. */
        if (engine != EngineSqlServer && storeAuth != ServerStoreAuth.Sql)
        {
            return (null, Invalid(
                "a PostgreSQL target requires auth \"SQL\" with a username and password " +
                "(integrated/Kerberos auth is not supported for PostgreSQL targets)."));
        }

        var (port, portError) = ResolvePort(obj);
        if (portError != null)
        {
            return (null, Invalid(portError));
        }

        var probeConfig = new MonitoredServer
        {
            Name = displayName,
            Host = host,
            Database = database,
            Auth = storeAuth,
            Username = username,
            /* Plaintext for the probe's connect (ResolvePassword's dev-plaintext fallback); the stored blob is the
               DPAPI encryption of this, produced only AFTER a successful probe. */
            Password = plaintextPassword,
            EncryptMode = encryptMode,
            TrustServerCertificate = trustCert,
            ReadOnlyIntent = readOnlyIntent,
            MultiSubnetFailover = multiSubnet,
            Engine = engine,
            Port = port,
        };

        /* #2218: the FULL identity, matching what the store derives — engine and port included, so a PostgreSQL
           entry does not collide with a SQL Server one on the same host. */
        var storageKey = ServerIdHelper.BuildStorageName(host, database, readOnlyIntent, engine, port);
        return (new ParsedServerEntry(index, displayName, storageKey, probeConfig, plaintextPassword), null);
    }

    /// <summary>
    /// The #2280 check: why this entry must not be added, or null when it may be.
    ///
    /// <para>Compares the identity this entry would have if keyed on the database the server ACTUALLY reached
    /// against the identities already claimed. Only fires when the actual database DIFFERS from the declared one
    /// — an entry that reached what it named is the ordinary case and the declared gate has already ruled on it,
    /// so re-checking would only re-detect that gate's decision under a more confusing name.</para>
    ///
    /// <para><b>Silent when the probe did not report a database</b> (a stub probe, or a target that returned
    /// none): unknown is not the same as colliding, and refusing on an absent value would block registrations
    /// for a reason nobody could act on.</para>
    ///
    /// <para><b>Keyed on the FULL identity, not on (host, database).</b> A read-only-intent registration
    /// alongside a read-write one for the same database is legitimate and <c>read_only_intent</c> is part of the
    /// identity, so comparing without it would refuse a valid pair. Same for engine and port after #2218.</para>
    ///
    /// <para>Note the asymmetry this cannot see: existing rows record only the database they DECLARE, so this
    /// catches "the new one lands where an existing one lives" and not "both mis-resolve to a database neither
    /// names". Closing that needs the actual database persisted per row; #2277's tripwire reports it at connect
    /// for both in the meantime, which is why this is a guard and not the whole answer.</para>
    /// </summary>
    internal static string? ActualIdentityCollision(
        ParsedServerEntry entry, string? connectedDatabase, ISet<string> claimedKeys)
    {
        if (entry is null || claimedKeys is null || string.IsNullOrWhiteSpace(connectedDatabase))
        {
            return null;
        }

        var declared = entry.ProbeConfig.Database;
        if (!string.IsNullOrWhiteSpace(declared)
            && string.Equals(declared.Trim(), connectedDatabase.Trim(), StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var actualKey = ServerIdHelper.BuildStorageName(
            entry.ProbeConfig.Host, connectedDatabase.Trim(), entry.ProbeConfig.ReadOnlyIntent,
            entry.ProbeConfig.Engine, entry.ProbeConfig.Port);

        if (string.Equals(actualKey, entry.StorageKey, StringComparison.OrdinalIgnoreCase)
            || !claimedKeys.Contains(actualKey))
        {
            return null;
        }

        var declaredText = string.IsNullOrWhiteSpace(declared) ? "no database" : $"database '{declared}'";
        return $"Not added: this registration names {declaredText} but its connection lands in " +
               $"'{connectedDatabase.Trim()}', which another monitored server already covers. Adding it would " +
               "store that one database's history under two identities and alert twice for every incident. " +
               "Point it at the database you meant (check Initial Catalog), or monitor the existing " +
               "registration instead.";
    }

    /// <summary>
    /// PURE dedupe partition — the case-folded <see cref="ServerIdHelper.BuildStorageName"/> gate seeded with the
    /// existing store keys, first-occurrence-wins within the batch (the #1549 idiom). Returns the <c>Ready</c>
    /// entries to probe + insert and the <c>Duplicates</c> as ready-to-report results. Unit-testable without a
    /// store or probe.
    /// </summary>
    internal static (List<ParsedServerEntry> Ready, List<ServerResult> Duplicates) PartitionDuplicates(
        IReadOnlyList<ParsedServerEntry> entries, IEnumerable<string> existingKeys)
    {
        var seen = new HashSet<string>(existingKeys, StringComparer.OrdinalIgnoreCase);
        var ready = new List<ParsedServerEntry>();
        var duplicates = new List<ServerResult>();

        foreach (var entry in entries)
        {
            if (seen.Add(entry.StorageKey))
            {
                ready.Add(entry);
            }
            else
            {
                duplicates.Add(new ServerResult(entry.Order, entry.DisplayName, AddStatus.Duplicate,
                    "Already monitored (or a duplicate of an earlier entry in this batch); skipped."));
            }
        }

        return (ready, duplicates);
    }

    /* ─────────────────────────────── store I/O ─────────────────────────────── */

    /// <summary>Reads the identity fields of every existing monitored server so the dedupe gate can be seeded from
    /// the authoritative set (mirrors the bulk dialog's <c>LoadExistingKeysAsync</c>). Non-secret columns only.
    ///
    /// <para>#2218 added <c>engine</c> and <c>port</c> to the identity, so they have to be read here too. Without
    /// them the gate keys on a NARROWER identity than the product does, and a PostgreSQL instance on a host that
    /// already has a SQL Server registration reads as a duplicate and is refused — a valid pair rejected because
    /// the gate could not see what distinguishes them.</para></summary>
    public const string ExistingServersSql =
        "SELECT host, database, read_only_intent, engine, port FROM config_monitored_servers";

    /// <summary>The INSERT — column set + shape mirrored from <c>StoreConfigProvider.SeedMonitoredServersAsync</c>
    /// (the seed authority), so a tool-added row is byte-identical to a seeded one. <c>capture_plans</c> and
    /// <c>alert_delivery_mode_override</c> default to NULL (inherit the globals), <c>monthly_cost_usd</c>/
    /// <c>excluded_databases</c> are the neutral defaults, and <c>is_enabled</c> is TRUE (collection starts at
    /// once). ON CONFLICT DO NOTHING guards a race with a concurrent writer — the dedupe gate is the primary
    /// guard.</summary>
    public const string InsertServerSql = @"
INSERT INTO config_monitored_servers (
    server_id, name, host, database, auth, username, encrypted_password, encrypt_mode,
    trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases,
    monthly_cost_usd, capture_plans, alert_delivery_mode_override, engine, port, is_enabled, created_at, modified_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NULL, NULL, $15, $16, TRUE, $14, $14)
ON CONFLICT (server_id) DO NOTHING";

    private static async Task<List<string>> LoadExistingStorageKeysAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var keys = new List<string>();
        await using var command = postgres.CreateCommand(ExistingServersSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var host = reader.GetString(0);
            var database = reader.IsDBNull(1) ? null : reader.GetString(1);
            var readOnlyIntent = !reader.IsDBNull(2) && reader.GetBoolean(2);
            var engine = reader.IsDBNull(3) ? null : reader.GetString(3);
            var port = reader.IsDBNull(4) ? 0 : reader.GetInt32(4);
            keys.Add(ServerIdHelper.BuildStorageName(host, database, readOnlyIntent, engine, port));
        }

        return keys;
    }

    private static async Task InsertServerAsync(
        NpgsqlDataSource postgres, ParsedServerEntry entry, string? encryptedPassword, CancellationToken cancellationToken)
    {
        var config = entry.ProbeConfig;
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await using var command = postgres.CreateCommand(InsertServerSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerIdHelper.GetDeterministicHashCode(entry.StorageKey) }); // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = config.Name });                                            // $2
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = config.Host });                                            // $3
        AddNullableText(command, config.Database);                                                                                    // $4
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = config.Auth });                                            // $5
        AddNullableText(command, config.Username);                                                                                    // $6
        AddNullableText(command, encryptedPassword);                                                                                  // $7
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = config.EncryptMode });                                     // $8
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = config.TrustServerCertificate });                            // $9
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = config.ReadOnlyIntent });                                     // $10
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = config.MultiSubnetFailover });                                // $11
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = Array.Empty<string>() }); // $12
        command.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = 0m });                                                     // $13
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now });                           // $14
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = config.Engine });                                           // $15
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = config.Port });                                                // $16
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /* ─────────────────────────────── helpers ─────────────────────────────── */

    /// <summary>Prepares a SQL password for storage: an <c>env:</c>/<c>file:</c> secret REFERENCE (#1804) is
    /// stored VERBATIM — a reference is a pointer, not a secret; the secret stays in the mounted file or
    /// environment variable, which is the whole Linux/compose contract and needs no DPAPI (#2087: before this,
    /// <c>add_servers</c> refused ALL SQL-auth passwords off-Windows, dead-ending the designed onboarding path
    /// for compose deployments — the store-authoritative control plane means darling.json edits do not add
    /// servers after first seed). A LITERAL password is DPAPI-encrypted and therefore Windows-only, with the
    /// refusal now pointing at references as the cross-platform alternative. Null for Windows auth (no secret).
    /// The plaintext is not logged and never leaves this method.</summary>
    internal static string? ProtectPasswordForStorage(string? plaintextPassword)
    {
        if (string.IsNullOrEmpty(plaintextPassword))
        {
            return null;
        }

        if (DarlingSecretSource.IsReference(plaintextPassword))
        {
            return plaintextPassword;
        }

        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Storing a LITERAL SQL-auth password requires Windows (DPAPI). On Linux, pass an env:/file: " +
                "secret reference instead (e.g. file:/run/secrets/sql_password) — it is stored as-is and " +
                "resolved at connect time (#1804).");
        }

        return DarlingSecrets.Protect(plaintextPassword);
    }

    /// <summary>
    /// Builds the <c>{requested, added, skipped, collided, failed, results}</c> envelope, results in input order,
    /// every result counted under the ONE counter <see cref="CounterOfStatus"/> names for its status.
    ///
    /// <para>A status the map does not know is thrown on, not dropped: the caller's catch turns it into the tool's
    /// error envelope, which is a loud wrong answer where the old shape gave a quiet one. It cannot fire in
    /// production while the census test holds (every <see cref="AddStatus"/> constant is mapped), and if a future
    /// status is added as a literal and the test is skipped, the first real call says so instead of summarising
    /// the batch short.</para>
    /// </summary>
    internal static string Aggregate(List<ServerResult> results)
    {
        var ordered = results.OrderBy(r => r.Order).ToList();
        var counters = new Dictionary<string, int>(StringComparer.Ordinal)
        {
            ["added"] = 0,
            ["skipped"] = 0,
            ["collided"] = 0,
            ["failed"] = 0,
        };

        foreach (var result in ordered)
        {
            if (!CounterOfStatus.TryGetValue(result.Status, out var counter))
            {
                throw new InvalidOperationException(
                    $"add_servers produced status '{result.Status}' for '{result.Server}', which no summary counter accounts for.");
            }

            counters[counter]++;
        }

        return JsonSerializer.Serialize(new
        {
            requested = ordered.Count,
            added = counters["added"],
            skipped = counters["skipped"],
            collided = counters["collided"],
            failed = counters["failed"],
            results = ordered.Select(r => new { server = r.Server, status = r.Status, detail = r.Detail }),
        }, McpHelpers.JsonOptions);
    }

    /// <summary>The probed facts for an <c>added</c> server, from the same describer the
    /// <c>--test-connection</c> CLI line uses (<see cref="DarlingServerConnector.DescribeProbeFacts"/>), so the
    /// two cannot drift and a PostgreSQL target reads as one here too.</summary>
    private static string DescribeProbe(ConnectionProbeResult probe) =>
        $"Connected — {DarlingServerConnector.DescribeProbeFacts(probe)}.";

    /// <summary>The canonical <c>engine</c> values, as written to the store.</summary>
    internal const string EngineSqlServer = "sqlserver";
    internal const string EnginePostgres = "postgres";

    /// <summary>
    /// Validates the optional <c>engine</c>; absent → <see cref="EngineSqlServer"/>, the
    /// <see cref="MonitoredServer.Engine"/> default.
    /// <para>Deliberately STRICTER than <see cref="MonitoredServer.TargetEngine"/>, which resolves anything
    /// unrecognized to SQL Server so that one typo in darling.json cannot take the whole fleet down. That is
    /// the right call for a file read at startup and the wrong one here: onboarding is a single deliberate act,
    /// and silently turning <c>"postgress"</c> into a SQL Server target would hand back <c>connection_failed</c>
    /// against a Postgres port with nothing pointing at the typo. Accepts the same aliases the parser does, so
    /// the two never disagree about a value they both accept.</para>
    /// </summary>
    internal static (string Engine, string? Error) ResolveEngine(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return (EngineSqlServer, null);
        }

        return raw.Trim().ToLowerInvariant() switch
        {
            "sqlserver" or "sql" or "mssql" => (EngineSqlServer, null),
            "postgres" or "postgresql" or "pg" or "aurora-postgresql" or "aurora" => (EnginePostgres, null),
            _ => (EngineSqlServer,
                "engine must be \"sqlserver\" (default) or \"postgres\" — also accepted: \"postgresql\", " +
                "\"pg\", \"aurora\", \"aurora-postgresql\"."),
        };
    }

    /// <summary>
    /// Validates the optional <c>port</c>; absent or 0 → the driver's default. Consumed only by the PostgreSQL
    /// connection builder (a SQL Server target carries its port in the host string), and range-checked here to
    /// match <c>DarlingConfig.Validate</c> rather than failing later inside Npgsql.
    /// </summary>
    internal static (int Port, string? Error) ResolvePort(JsonObject obj)
    {
        var node = obj["port"];
        if (node is null)
        {
            return (0, null);
        }

        if (!TryGetInt(node, out var port))
        {
            return (0, "port must be a number.");
        }

        if (port is not 0 && port is < 1 or > 65535)
        {
            return (0, $"port must be between 1 and 65535 (got {port}).");
        }

        return (port, null);
    }

    private static bool TryGetInt(JsonNode node, out int value)
    {
        try
        {
            value = node.GetValue<int>();
            return true;
        }
        catch (Exception ex) when (ex is FormatException or InvalidOperationException or OverflowException)
        {
            /* A JSON string ("5432") is a plausible thing for a caller to send; accept it rather than
               refusing on a type technicality. */
            if (node.GetValueKind() == JsonValueKind.String
                && int.TryParse(node.GetValue<string>(), System.Globalization.NumberStyles.Integer,
                    System.Globalization.CultureInfo.InvariantCulture, out value))
            {
                return true;
            }

            value = 0;
            return false;
        }
    }

    /// <summary>Validates the optional <c>encrypt_mode</c> ("Optional"/"Mandatory"/"Strict"); absent → the
    /// fail-closed "Mandatory" default (matching <see cref="MonitoredServer.EncryptMode"/> + the connection builder).</summary>
    private static (string Mode, string? Error) ResolveEncryptMode(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return ("Mandatory", null);
        }

        return raw.Trim().ToUpperInvariant() switch
        {
            "OPTIONAL" => ("Optional", null),
            "MANDATORY" => ("Mandatory", null),
            "STRICT" => ("Strict", null),
            _ => ("Mandatory", "encrypt_mode must be \"Optional\", \"Mandatory\", or \"Strict\"."),
        };
    }

    /// <summary>Reads an optional boolean field; absent → <paramref name="fallback"/>, a non-boolean value → error.</summary>
    private static (bool Value, string? Error) ResolveBool(JsonObject obj, string field, bool fallback)
    {
        if (obj[field] is not JsonNode node)
        {
            return (fallback, null);
        }

        if (node is JsonValue value && value.TryGetValue<bool>(out var b))
        {
            return (b, null);
        }

        return (fallback, $"{field} must be true or false.");
    }

    private static string? TryGetString(JsonObject obj, string key) =>
        obj[key] is JsonValue value && value.TryGetValue<string>(out var s) ? s : null;

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)value ?? DBNull.Value });

    /// <summary>A small <c>{status, message}</c> envelope for a non-data outcome (invalid / not_found) — the same
    /// shape <see cref="DarlingMcpAlertTools"/> / <see cref="DarlingMcpCustomViewTools"/> use, so an MCP client can
    /// branch on the outcome kind. A successful add-batch / removal returns its own data-bearing shape, not this.</summary>
    private static string Outcome(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);

    /// <summary>The two <c>config.config_monitored_servers.auth</c> values the Darling service connect path honors
    /// — the service-side twin of the Viewer's <c>ServerStoreCredential</c> constants (the Viewer project is not
    /// referenced here, so the values are restated; they are pinned equal to the store's DDL default 'integrated').</summary>
    private static class ServerStoreAuth
    {
        public const string Integrated = "integrated";
        public const string Sql = "sql";

        /// <summary>Microsoft Entra service principal — client id in username, client secret in the DPAPI blob;
        /// non-interactive, so it is honored headless (#3484). Pinned equal to <see cref="MonitoredServer.UsesServicePrincipal"/>.</summary>
        public const string ServicePrincipal = "serviceprincipal";

        /// <summary>Azure managed identity — non-interactive and secret-less (#3484). Pinned equal to
        /// <see cref="MonitoredServer.UsesManagedIdentity"/>.</summary>
        public const string ManagedIdentity = "managedidentity";
    }
}
