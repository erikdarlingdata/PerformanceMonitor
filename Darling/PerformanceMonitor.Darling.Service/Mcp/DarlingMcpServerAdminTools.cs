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
/// <c>status:"connection_failed"</c> and does NOT abort the batch), DPAPI-encrypts the SQL password, and INSERTs
/// the row (<c>status:"added"</c>). Windows/integrated auth stores no secret; Entra/MFA/Service-Principal/Managed-
/// Identity auth is rejected (<c>status:"invalid"</c>) — interactive MFA is nonsensical headless, the same belt the
/// bulk dialog applies. The whole call returns <c>{added, skipped, failed, results:[...]}</c>. <b>remove_server</b>
/// resolves a name through the SAME <see cref="DarlingServerResolver"/> the read tools use and DELETEs the
/// <c>config.config_monitored_servers</c> row.</para>
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
        "Adds one or more database servers to the fleet the Darling service monitors — BULK onboarding: pass a JSON " +
        "ARRAY of server objects and each is validated, connection-tested, and (if new and reachable) saved to the " +
        "central monitoring store, which the running service picks up within one collection sweep (no restart). " +
        "Each object: host (REQUIRED); display_name (optional, defaults to host); database (optional — set it only " +
        "to monitor a single database, e.g. one Azure SQL Database); engine (\"sqlserver\" default, or \"postgres\" " +
        "for PostgreSQL / Amazon Aurora PostgreSQL); auth (\"Windows\" for integrated security or " +
        "\"SQL\" for a SQL login — default \"Windows\"; a PostgreSQL target REQUIRES \"SQL\"); username + password " +
        "(REQUIRED for \"SQL\" auth, ignored for " +
        "\"Windows\"); port (optional, PostgreSQL only — omit for 5432); " +
        "encrypt_mode (\"Optional\"|\"Mandatory\"|\"Strict\", default \"Mandatory\"); " +
        "trust_server_certificate (bool, default false — set true to accept a self-signed server cert, and " +
        "typically REQUIRED for Aurora, which presents an RDS CA a stock trust store does not know); " +
        "read_only_intent (bool, default false); multi_subnet_failover (bool, default false). Servers are processed " +
        "IN ORDER, one at a time. A case-variant or exact duplicate of an already-monitored server (or an earlier " +
        "entry in the same array) is skipped as status \"duplicate\". A server that fails to connect is recorded as " +
        "status \"connection_failed\" and does NOT stop the rest of the batch. Microsoft Entra / MFA / Service " +
        "Principal / Managed Identity auth is rejected (status \"invalid\") — the service connects with Windows or " +
        "SQL authentication only. A SQL password is encrypted at rest (DPAPI, the service identity) and is never " +
        "returned. Returns {added:N, skipped:N, failed:N, results:[{server, status:\"added\"|\"duplicate\"|" +
        "\"connection_failed\"|\"invalid\", detail}]}, where an added server's detail reports what the probe found " +
        "— for a PostgreSQL target that includes writer-vs-reader, Aurora-vs-not, and how many of the PostgreSQL " +
        "collectors apply to it. NOTE: the password travels to this endpoint in the request; " +
        "on a LAN use the documented TLS reverse proxy.")]
    public static Task<string> AddServers(
        NpgsqlDataSource postgres,
        [Description("A JSON ARRAY of server objects to add (see the tool description for the per-object fields), e.g. [{\"host\":\"sql01\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"...\",\"encrypt_mode\":\"Mandatory\",\"trust_server_certificate\":true},{\"host\":\"aurora.cluster-abc.us-east-1.rds.amazonaws.com\",\"engine\":\"postgres\",\"auth\":\"SQL\",\"username\":\"darling_monitor\",\"password\":\"...\",\"trust_server_certificate\":true}].")] string servers_json) =>
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
                    results.Add(new ServerResult(entry.Order, entry.DisplayName, "connection_failed",
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
                    results.Add(new ServerResult(entry.Order, entry.DisplayName, "collides", collision));
                    continue;
                }

                /* DPAPI-encrypt the SQL password for storage (the service identity encrypts here and decrypts it
                   during collection, so it round-trips); Windows-auth servers store no secret. The plaintext never
                   leaves this method — it is not logged, not echoed in a result. */
                var encryptedPassword = ProtectPasswordForStorage(entry.PlaintextPassword);
                await InsertServerAsync(postgres, entry, encryptedPassword, cancellationToken);
                results.Add(new ServerResult(entry.Order, entry.DisplayName, "added", DescribeProbe(probeResult)));
            }

            return Aggregate(results);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("add_servers", ex);
        }
    }

    [McpServerTool(Name = "remove_server"), Description(
        "Removes a monitored SQL Server from the fleet by name (the display name or address, resolved the same way " +
        "the read tools resolve server_name — exact match first, then partial, against the storage name and the " +
        "display name). Deletes the server's definition from the central monitoring store; the running service " +
        "drops it from its collection set within one sweep. Already-collected historical data is NOT deleted. " +
        "Returns {status:\"removed\", server} on success, or {status:\"not_found\", ...} when no monitored server " +
        "matches the name.")]
    public static async Task<string> RemoveServer(
        NpgsqlDataSource postgres,
        [Description("The name of the monitored server to remove — its display name or address (as list_servers / get_alert_history report it).")] string server_name)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(server_name))
            {
                return Outcome("invalid", "server_name is required.");
            }

            /* Resolve through the SAME resolver the read tools use, against the servers registry (the id it returns
               is the shared-identity server_id that keys config_monitored_servers). A miss returns the resolver's
               available-servers listing — surfaced as not_found here. */
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
            if (error != null)
            {
                return Outcome("not_found", error);
            }

            await using var command = postgres.CreateCommand("DELETE FROM config_monitored_servers WHERE server_id = $1");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = resolved.ServerId });
            var affected = await command.ExecuteNonQueryAsync();

            return affected > 0
                ? JsonSerializer.Serialize(new { status = "removed", server = resolved.ServerName }, McpHelpers.JsonOptions)
                : Outcome("not_found",
                    $"'{resolved.ServerName}' is registered but has no monitored-server definition to remove (it may have been added only via darling.json, or already removed).");
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

    /// <summary>One per-server outcome (<c>added</c> / <c>duplicate</c> / <c>connection_failed</c> / <c>invalid</c>)
    /// with a human-readable <see cref="Detail"/>; <see cref="Order"/> restores input order in the aggregate.</summary>
    internal sealed record ServerResult(int Order, string Server, string Status, string Detail);

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
    /// problem. Windows/SQL are the only auth modes the service can honor; every other value (including the
    /// Entra/MFA/Service-Principal/Managed-Identity modes) is rejected — interactive MFA is nonsensical headless.</summary>
    private static (ParsedServerEntry? Entry, ServerResult? Result) ParseEntry(int index, JsonNode? node)
    {
        if (node is not JsonObject obj)
        {
            return (null, new ServerResult(index, $"(entry {index + 1})", "invalid", "Each entry must be a JSON object."));
        }

        var host = TryGetString(obj, "host");
        var label = string.IsNullOrWhiteSpace(host) ? $"(entry {index + 1})" : host!.Trim();

        ServerResult Invalid(string message) => new(index, label, "invalid", message);

        if (string.IsNullOrWhiteSpace(host))
        {
            return (null, Invalid("host is required."));
        }

        host = host!.Trim();
        var displayName = TryGetString(obj, "display_name") is { Length: > 0 } dn ? dn.Trim() : host;
        var databaseRaw = TryGetString(obj, "database");
        var database = string.IsNullOrWhiteSpace(databaseRaw) ? null : databaseRaw!.Trim();

        /* Auth: Windows (integrated) or SQL only. Absent defaults to Windows (the MonitoredServer default). Any
           other value — Entra / MFA / ServicePrincipal / ManagedIdentity, or a typo — is refused with the SAME
           message, so a headless caller learns the service's two supported modes. */
        var authRaw = TryGetString(obj, "auth");
        string storeAuth;
        if (string.IsNullOrWhiteSpace(authRaw) || authRaw.Trim().Equals("Windows", StringComparison.OrdinalIgnoreCase))
        {
            storeAuth = ServerStoreAuth.Integrated;
        }
        else if (authRaw.Trim().Equals("SQL", StringComparison.OrdinalIgnoreCase))
        {
            storeAuth = ServerStoreAuth.Sql;
        }
        else
        {
            return (null, Invalid(
                "auth must be \"Windows\" or \"SQL\". Microsoft Entra / MFA / Service Principal / Managed Identity " +
                "are not supported for headless onboarding — the Darling service connects with Windows (integrated) " +
                "or SQL authentication only."));
        }

        string? username = null;
        string? plaintextPassword = null;
        if (storeAuth == ServerStoreAuth.Sql)
        {
            username = TryGetString(obj, "username");
            if (string.IsNullOrWhiteSpace(username))
            {
                return (null, Invalid("username is required for SQL authentication."));
            }

            username = username!.Trim();
            plaintextPassword = TryGetString(obj, "password");
            if (string.IsNullOrEmpty(plaintextPassword))
            {
                return (null, Invalid("password is required for SQL authentication."));
            }
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
                duplicates.Add(new ServerResult(entry.Order, entry.DisplayName, "duplicate",
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

    /// <summary>Builds the <c>{added, skipped, failed, results}</c> envelope, results in input order.</summary>
    private static string Aggregate(List<ServerResult> results)
    {
        var ordered = results.OrderBy(r => r.Order).ToList();
        return JsonSerializer.Serialize(new
        {
            added = ordered.Count(r => r.Status == "added"),
            skipped = ordered.Count(r => r.Status == "duplicate"),
            failed = ordered.Count(r => r.Status is "connection_failed" or "invalid"),
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
    }
}
