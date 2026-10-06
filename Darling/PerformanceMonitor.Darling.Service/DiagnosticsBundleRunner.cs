/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using DiagnosticsBundleExitCode = PerformanceMonitor.Darling.Service.DarlingCliCommands.DiagnosticsBundleExitCode;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Reads every bundle section and drives <see cref="DiagnosticsBundle.Assemble"/>. Reads only: it opens the store
/// through readers that already exist (the statement history is the MCP tool's) and runs a few small catalog reads of its own.
/// </summary>
internal static class DiagnosticsBundleRunner
{
    /// <summary>The registry's identifier columns, for the name set (the same table the service's config provider reads).</summary>
    internal const string RegistryNamesSql = @"
SELECT name, host, database, username, excluded_databases
FROM config_monitored_servers";

    /// <summary>Both spellings the collected-data tables use for a server, for the name set.</summary>
    internal const string CollectServersSql = @"
SELECT server_id, server_name, display_name
FROM servers
ORDER BY server_id";

    /// <summary>Every store login (the system roles excluded), for the name set: a login with no connection string in the config still appears in statement sections.</summary>
    internal const string StoreRolesSql = @"
SELECT rolname FROM pg_roles WHERE rolname !~ '^pg_'";

    /// <summary>The store's own database names.</summary>
    internal const string StoreDatabasesSql = @"
SELECT datname FROM pg_database";

    /// <summary>
    /// The monitored database inventory: the distinct database names the periodic per-database state snapshot saw in
    /// the last 24 hours. The table is chunked by collection_time and indexed on (server_id, collection_time), so a
    /// bound of one day reads the newest chunk only; the read adds no index. A table that cannot be read is a failed name source.
    /// </summary>
    internal const string DatabaseInventorySql = @"
SELECT DISTINCT database_name
FROM collect.database_states
WHERE collection_time >= (now() AT TIME ZONE 'UTC') - INTERVAL '24 hours'
AND   database_name IS NOT NULL";

    /// <summary>The store-side twin of <see cref="DatabaseInventorySql"/> for PostgreSQL targets, bounded the same way.</summary>
    internal const string PgDatabaseInventorySql = @"
SELECT DISTINCT database_name
FROM collect.pg_database_stats
WHERE collection_time >= (now() AT TIME ZONE 'UTC') - INTERVAL '24 hours'
AND   database_name IS NOT NULL";

    /// <summary>
    /// Whether the statement-history tables exist. Names match V163 as merged; the section reads
    /// <c>not_present</c> when the tables are absent, so the verb works on a store below that schema. This is the
    /// only read of the history tables the bundle makes itself: the statements come from
    /// <see cref="DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistoryUncut"/>.
    /// </summary>
    internal const string StatementHistoryPresentSql = @"
SELECT to_regclass('collect.store_statement_history') IS NOT NULL AND to_regclass('collect.store_statement_captures') IS NOT NULL";

    /// <summary>
    /// How many rows the history lists: the 25 that spent the most time in the window, one row per role and statement. The
    /// tool ranks that way, so a statement two roles ran takes two of the 25 rows, and the section can name fewer than 25
    /// distinct statements. 25 is the cap the section has always had. The tool's default page is 20.
    /// </summary>
    internal const int StatementHistoryTop = 25;

    /// <summary>The outcome of building and writing a bundle.</summary>
    internal sealed record Outcome(
        int ExitCode, string? Text, IReadOnlyList<BundleLeak> Leaks, BundleAliaser Aliaser, string Message, IReadOnlyList<string>? Warnings = null);

    /// <summary>
    /// The fleet roll-up with its tag forest reduced to a count. A tag name is text the operator chose (often a client's
    /// name), and the bundle has no use for it.
    /// </summary>
    internal static JsonNode? ShapeFleetOverview(JsonNode? fleet)
    {
        if (fleet is JsonObject obj && obj["tags"] is JsonArray tags)
        {
            obj["tags"] = new JsonObject { ["count"] = tags.Count };
        }

        return fleet;
    }

    /// <summary>
    /// The slow-read <c>statement_summary</c> label is cut to 40 characters by the tool before the bundle sees it, and a
    /// database name cut to a prefix matches no known name. Each read's <c>statements</c> array carries the same labels
    /// whole, so the summary is dropped.
    /// </summary>
    internal static JsonNode? DropStatementSummaries(JsonNode? slowReads)
    {
        if (slowReads is JsonObject o && o["reads"] is JsonArray reads)
        {
            foreach (var read in reads.OfType<JsonObject>())
            {
                read.Remove("statement_summary");
            }
        }

        return slowReads;
    }

    /// <summary>
    /// The health rows' <c>last_error</c> and <c>output_finding</c>, cut to the lengths the MCP tool cuts them to, after
    /// they have been aliased. The rows are read uncut, so a name that straddles the cut is aliased whole first.
    /// </summary>
    internal static JsonNode? CutHealthTexts(JsonNode? collection)
    {
        if (collection is JsonObject o && o["health_by_server"] is JsonArray perServer)
        {
            foreach (var entry in perServer.OfType<JsonObject>())
            {
                if (entry["health"] is JsonObject health && health["collectors"] is JsonArray collectors)
                {
                    foreach (var row in collectors.OfType<JsonObject>())
                    {
                        CutField(row, "last_error", "last_error_truncated", DarlingMcpDataTools.ErrorMessagePreviewLength);
                        CutField(row, "output_finding", "output_finding_truncated", DarlingMcpDataTools.OutputFindingPreviewLength);
                    }
                }
            }
        }

        return collection;
    }

    private static void CutField(JsonObject row, string field, string flag, int length)
    {
        if (row[field] is JsonValue v && v.TryGetValue<string>(out var text) && text.Length > length)
        {
            row[field] = McpHelpers.Truncate(text, length);
            if (row.ContainsKey(flag))
            {
                row[flag] = true;
            }
        }
    }

    /// <summary>Statement text in a store-statements section, cut to the preview length after it has been aliased.</summary>
    internal static JsonNode? CompactStatementTexts(JsonNode? section)
    {
        void Cut(JsonNode? node)
        {
            switch (node)
            {
                case JsonObject o:
                    foreach (var key in o.Select(p => p.Key).ToList())
                    {
                        if (key == "query" && o[key] is JsonValue v && v.TryGetValue<string>(out var text))
                        {
                            o[key] = StoreStatementStats.CompactStatementText(text, DarlingMcpStoreQueryStatsTools.PreviewLength);
                        }
                        else
                        {
                            Cut(o[key]);
                        }
                    }

                    break;
                case JsonArray a:
                    foreach (var item in a)
                    {
                        Cut(item);
                    }

                    break;
            }
        }

        Cut(section);
        return section;
    }

    /// <summary>The service log's message text, cut to the entry cap after aliasing.</summary>
    internal static JsonNode? CapServiceLogMessages(JsonNode? section)
    {
        if (section is JsonObject o && o["entries"] is JsonArray entries)
        {
            foreach (var entry in entries.OfType<JsonObject>())
            {
                if (entry["message"] is JsonValue v && v.TryGetValue<string>(out var text))
                {
                    entry["message"] = DiagnosticsBundleServiceLog.CutAtWhitespace(text, DiagnosticsBundleServiceLog.MaxEntryChars);
                }
            }
        }

        return section;
    }

    /// <summary>
    /// Builds the bundle text. <paramref name="postgres"/> is null when the store could not be reached
    /// (<paramref name="storeError"/> then says why), which yields the reduced bundle and exit 2.
    /// </summary>
    internal static async Task<Outcome> BuildAsync(
        DiagnosticsBundleOptions options,
        DarlingConfig config,
        string? connectionString,
        NpgsqlDataSource? postgres,
        Exception? storeError,
        string? storeReason,
        CancellationToken cancellationToken,
        NameSourceSql? nameSources = null)
    {
        var aliaser = new BundleAliaser();
        var warnings = new List<string>(DiagnosticsBundle.SeedFromConfig(aliaser, config, connectionString));
        var sections = new List<BundleSection>();
        string scope = "fleet";

        if (postgres is not null)
        {
            var seeded = await SeedFromStoreAsync(aliaser, postgres, cancellationToken, nameSources);
            if (seeded.FailedNameSource is not null)
            {
                /* The registry, the collected-data server list, the store's roles and databases and the monitored database
                   inventories are where names are learned. With the store up and one unreadable, the bundle cannot prove
                   it removed every name. */
                return new Outcome(DiagnosticsBundleExitCode.LeakGuard, null, Array.Empty<BundleLeak>(), aliaser,
                    seeded.FailedNameSource + ": a name source could not be read, so the bundle cannot prove it removed every name.");
            }
        }

        string? scopeServerName = null;
        if (postgres is not null && !string.IsNullOrWhiteSpace(options.ServerName))
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, options.ServerName, cancellationToken);
            if (error is not null)
            {
                return new Outcome(DiagnosticsBundleExitCode.ConfigError, null, Array.Empty<BundleLeak>(), aliaser,
                    "--server did not match one monitored server.");
            }

            scopeServerName = resolved.ServerName;
            scope = aliaser.AddServer(resolved.ServerId, resolved.ServerName);
        }

        sections.Add(new BundleSection("config_shape", DiagnosticsBundle.BuildConfigShape(config), false));

        if (postgres is null)
        {
            /* No sentence: an error message and the store's own help text quote paths and addresses nobody registered.
               The reason is a fixed code, and the two codes a driver gives (a SQLSTATE, a socket error name) are enums. */
            var unreachable = new JsonObject
            {
                ["status"] = "store_unreachable",
                ["error_class"] = storeError is null ? "NotConfigured" : SlowReadLog.ErrorClassOf(storeError),
                ["reason"] = DiagnosticsBundle.StoreReasonCode(storeReason, storeError),
            };
            if (DiagnosticsBundle.SqlStateOf(storeError) is { } sqlState)
            {
                unreachable["sql_state"] = sqlState;
            }

            if (DiagnosticsBundle.SocketErrorOf(storeError) is { } socketError)
            {
                unreachable["socket_error"] = socketError;
            }

            if (!string.IsNullOrWhiteSpace(options.ServerName))
            {
                const string ignored = "--server was ignored: the store is unreachable, so the server could not be looked up and no section is limited to it.";
                unreachable["server_scope"] = ignored;
                warnings.Add(ignored);
            }

            sections.Add(new BundleSection("store_unreachable", unreachable, true));
        }
        else
        {
            var hours = options.Hours;
            var days = Math.Max(1, (int)Math.Ceiling(hours / 24.0));
            var ct = cancellationToken;
            sections.Add(await DiagnosticsBundle.RunSectionAsync("store", c => StoreSectionAsync(postgres, config, c), ct));
            sections.Add((await DiagnosticsBundle.RunSectionAsync("collection",
                c => CollectionSectionAsync(postgres, scopeServerName, hours, days, c), ct)) with { AfterAlias = CutHealthTexts });
            sections.Add(await DiagnosticsBundle.RunSectionAsync("stall_probes",
                async c => DiagnosticsBundle.ParseReader(await DarlingMcpStallProbeTools.GetCollectorStallProbes(postgres, scopeServerName, days, 50, c)), ct));
            sections.Add(await DiagnosticsBundle.RunSectionAsync("slow_reads",
                async c => DropStatementSummaries(DiagnosticsBundle.ParseReader(await DarlingMcpSlowReadTools.GetSlowReads(postgres, hours, scopeServerName, null, null, 100, true, c))), ct));
            sections.Add(await DiagnosticsBundle.RunSectionAsync("read_latency",
                async c => DiagnosticsBundle.ParseReader(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours, null, null, 200, c)), ct));
            sections.Add((await DiagnosticsBundle.RunSectionAsync("store_statements",
                c => StoreStatementsSectionAsync(postgres, hours, c), ct)) with { AfterAlias = CompactStatementTexts });
            sections.Add(await DiagnosticsBundle.RunSectionAsync("store_log",
                async c => DiagnosticsBundle.ParseReader(await DarlingMcpStoreLogTools.GetStoreLog(postgres, hours, 50, null, c)), ct));
        }

        var logDirectory = options.LogDirectory ?? DarlingFileLoggerProvider.DefaultLogDirectory();
        var searched = options.LogDirectory is null ? "default" : "--log-dir";

        /* With the store down, the registry (--add-server entries live only there) cannot be read, so servers known only to
           it cannot be aliased. The service log then goes out as counts only, unless the caller asks for the text. */
        var includeText = postgres is not null || options.IncludeLogText;
        if (postgres is null && options.IncludeLogText)
        {
            const string registryOnly = "--include-log-text with the store unreachable: servers known only to the store registry cannot be aliased, so the service log text may name them. Read the file before you attach it.";
            warnings.Add(registryOnly);
        }

        var logSection = await DiagnosticsBundle.RunSectionAsync("service_log",
            _ => Task.FromResult<JsonNode?>(DiagnosticsBundleServiceLog.Read(logDirectory, searched, DateTime.Now.AddHours(-options.Hours), includeText)), cancellationToken);
        sections.Add(logSection with { AfterAlias = CapServiceLogMessages });

        var failedSections = sections.Count(s => s.Failed && s.Name != "store_unreachable");
        var exit = postgres is null ? DiagnosticsBundleExitCode.StoreUnreachable
            : failedSections > 0 ? DiagnosticsBundleExitCode.PartialBundle : DiagnosticsBundleExitCode.Ok;

        var manifest = DiagnosticsBundle.BuildManifest(options.Hours, scope, exit);
        if (warnings.Count > 0)
        {
            manifest["notes"] = new JsonArray(warnings.Select(w => (JsonNode?)JsonValue.Create(w)).ToArray());
        }

        var (text, leaks, overCap) = DiagnosticsBundle.Assemble(sections, aliaser, manifest);
        if (overCap)
        {
            return new Outcome(DiagnosticsBundleExitCode.OutputError, null, leaks, aliaser, "The bundle could not be held under the size cap.");
        }

        if (text is null)
        {
            return new Outcome(DiagnosticsBundleExitCode.LeakGuard, null, leaks, aliaser, "A known name or secret survived aliasing.");
        }

        return new Outcome(exit, text, leaks, aliaser, string.Empty, warnings);
    }

    /// <summary>
    /// The SQL of each store name source. The defaults are the product's reads; a test substitutes one to make exactly
    /// that source fail.
    /// </summary>
    internal sealed record NameSourceSql(
        string Registry = RegistryNamesSql,
        string Servers = CollectServersSql,
        string Roles = StoreRolesSql,
        string Databases = StoreDatabasesSql,
        string Inventory = DatabaseInventorySql,
        string PgInventory = PgDatabaseInventorySql);

    /// <summary>What reading the store's name sources gave: the first source that failed (null when none did).</summary>
    internal sealed record SeedResult(string? FailedNameSource);

    /// <summary>
    /// Reads every store name source. While the store is up, a source that cannot be read leaves names known only to it
    /// unaliased and unknown to the verifier, so the first failure is returned by name and the caller refuses.
    /// </summary>
    internal static async Task<SeedResult> SeedFromStoreAsync(
        BundleAliaser aliaser, NpgsqlDataSource postgres, CancellationToken ct, NameSourceSql? sql = null)
    {
        sql ??= new NameSourceSql();
        string? failedNameSource = null;

        if (!await TryReadAsync(postgres, sql.Registry, reader =>
        {
            aliaser.AddName(AliasKind.Server, reader.IsDBNull(0) ? null : reader.GetString(0));
            aliaser.AddName(AliasKind.Host, reader.IsDBNull(1) ? null : reader.GetString(1));
            aliaser.AddName(AliasKind.Database, reader.IsDBNull(2) ? null : reader.GetString(2));
            aliaser.AddName(AliasKind.Login, reader.IsDBNull(3) ? null : reader.GetString(3));
            if (!reader.IsDBNull(4))
            {
                foreach (var excluded in reader.GetFieldValue<string[]>(4))
                {
                    aliaser.AddName(AliasKind.Database, excluded);
                }
            }
        }, ct))
        {
            failedNameSource ??= "config_monitored_servers";
        }

        /* Servers by id first so the alias order follows server_id; display names share their server's alias. */
        if (!await TryReadAsync(postgres, sql.Servers, reader =>
        {
            var id = reader.GetInt32(0);
            var name = reader.IsDBNull(1) ? null : reader.GetString(1);
            aliaser.AddServer(id, name);
            if (!reader.IsDBNull(2))
            {
                aliaser.AddNameLike(reader.GetString(2), name);
            }
        }, ct))
        {
            failedNameSource ??= "servers";
        }

        if (!await TryReadAsync(postgres, sql.Roles, reader =>
        {
            var role = reader.GetString(0);
            if (!BundleAliaser.ProductRoles.Contains(role, StringComparer.OrdinalIgnoreCase))
            {
                aliaser.AddName(AliasKind.Login, role);
            }
        }, ct))
        {
            failedNameSource ??= "pg_roles";
        }

        if (!await TryReadAsync(postgres, sql.Databases, reader => aliaser.AddName(AliasKind.Database, reader.GetString(0)), ct))
        {
            failedNameSource ??= "pg_database";
        }

        if (!await TryReadAsync(postgres, sql.Inventory, reader => aliaser.AddName(AliasKind.Database, reader.GetString(0)), ct))
        {
            failedNameSource ??= "collect.database_states";
        }

        if (!await TryReadAsync(postgres, sql.PgInventory, reader => aliaser.AddName(AliasKind.Database, reader.GetString(0)), ct))
        {
            failedNameSource ??= "collect.pg_database_stats";
        }

        return new SeedResult(failedNameSource);
    }

    /// <summary>One bounded read for the name set. Returns false when the read failed; the caller refuses, since a name source that cannot be read leaves its names unaliased.</summary>
    private static async Task<bool> TryReadAsync(NpgsqlDataSource postgres, string sql, Action<NpgsqlDataReader> each, CancellationToken ct)
    {
        try
        {
            await using var command = postgres.CreateCommand(sql);
            command.CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                each(reader);
            }

            return true;
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException or InvalidCastException)
        {
            return false;
        }
    }

    private static async Task<JsonNode?> StoreSectionAsync(NpgsqlDataSource postgres, DarlingConfig config, CancellationToken ct)
    {
        long stored;
        await using (var command = postgres.CreateCommand(DarlingWebEndpoints.QueryStoreWideSchemaVersionSql))
        {
            command.CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds;
            stored = Convert.ToInt64(await command.ExecuteScalarAsync(ct) ?? 0L, CultureInfo.InvariantCulture);
        }

        var host = DiagnosticsBundle.ParseReader(
            await DarlingMcpStoreHostTools.GetStoreHost(postgres, config.Postgres, new StoreHostProfileCache(TimeSpan.FromMinutes(1)), ct));
        return new JsonObject
        {
            ["schema_version_stored"] = stored,
            ["schema_version_compiled"] = StorageVersion.SchemaVersion,
            ["host"] = host,
            ["password_key"] = await PasswordKeyInfoAsync(postgres, ct),
        };
    }

    /// <summary>The published password key's id and the service's key state (#5366), read from the store. A store without the
    /// tables reads <c>not_present</c>; any other failure reads <c>unavailable</c> with the error's class only, so this
    /// member never fails the store section.</summary>
    private static async Task<JsonNode?> PasswordKeyInfoAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(ct);
            var published = await PasswordKeyTables.ReadCurrentAsync(connection, ct);
            var state = await PasswordKeyTables.ReadNewestServiceStateAsync(connection, ct);
            return DiagnosticsBundle.BuildPasswordKeyInfo(published, state);
        }
        catch (PostgresException ex) when (ex.SqlState == "42P01")
        {
            return new JsonObject { ["status"] = "not_present" };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new JsonObject { ["status"] = "unavailable", ["error_class"] = SlowReadLog.ErrorClassOf(ex) };
        }
    }

    private static async Task<JsonNode?> CollectionSectionAsync(NpgsqlDataSource postgres, string? serverName, int hours, int days, CancellationToken ct)
    {
        var health = new JsonArray();
        var omitted = 0;
        var servers = await DarlingServerResolver.LoadEnabledAsync(postgres, ct);
        if (!string.IsNullOrWhiteSpace(serverName))
        {
            servers = servers.Where(s => string.Equals(s.ServerName, serverName, StringComparison.OrdinalIgnoreCase)).ToList();
        }

        var used = 0;
        foreach (var server in servers)
        {
            if (used >= DiagnosticsBundle.HealthFanOutBudgetBytes)
            {
                omitted++;
                continue;
            }

            var json = await DarlingMcpDataTools.GetCollectionHealthUncut(postgres, server.ServerName, ct);
            used += System.Text.Encoding.UTF8.GetByteCount(json);
            health.Add(new JsonObject { ["server_name"] = server.ServerName, ["health"] = DiagnosticsBundle.ParseReader(json) });
        }

        var fleet = ShapeFleetOverview(DiagnosticsBundle.ParseReader(await DarlingMcpFleetTools.GetFleetOverview(postgres, hours_back: 1, detail: "summary", cancellationToken: ct)));
        var slowest = DiagnosticsBundle.ParseReader(await DarlingMcpDataTools.GetCollectionLog(
            postgres, server_name: string.IsNullOrWhiteSpace(serverName) ? "*" : serverName, hours_back: hours, limit: 50, as_of: null,
            collector_name: null, min_duration_ms: 1, status: null, full_text: true, logger: null, cancellationToken: ct));
        var cost = DiagnosticsBundle.ParseReader(await DarlingMcpCollectorCostTools.GetCollectorCost(postgres, days, null, ct));
        return new JsonObject
        {
            ["health_by_server"] = health,
            ["servers_omitted"] = omitted,
            ["fleet_overview"] = fleet,
            ["slowest_runs"] = slowest,
            ["collector_cost"] = cost,
        };
    }

    private static async Task<JsonNode?> StoreStatementsSectionAsync(NpgsqlDataSource postgres, int hours, CancellationToken ct)
    {
        var cumulative = DiagnosticsBundle.ParseReader(
            await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(postgres, null, "total_time", 25, true, ct));
        var section = new JsonObject
        {
            ["cumulative"] = cumulative,
            ["history"] = await StatementHistoryAsync(postgres, hours, ct),
        };

        /* A tool's error answer is a member, one level down, so the section's own status would still read ok while the member
           failed (#5097). A section that throws reads error in its body, in the manifest and in the exit code; this one reads
           the same when either member answered an error, keeping both members with the tool's own error in the one that failed.
           The exit code (RunSectionAsync) and the manifest (Assemble) both take a section's failure from this status. */
        if (MemberFailed(section))
        {
            section.Insert(0, "status", "error");
        }

        return section;
    }

    /// <summary>
    /// The statement history, read through <c>get_store_query_history</c> (#5097): the answer of the tool's ranked mode, the
    /// 25 rows that spent the most time in the window (one per role and statement), becomes the member as the tool wrote it,
    /// with each statement's text whole (<see cref="DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistoryUncut"/>), because
    /// the bundle aliases the text before <see cref="CompactStatementTexts"/> cuts it. A refusal or an error the tool answers
    /// is the member as it is, with its <c>status</c>. The existence probe stays: a store before V163 reads <c>not_present</c>
    /// with the reason below, and never reaches the tool. Never reads the diff-state baseline table.
    /// </summary>
    /// <remarks>
    /// The window is the bundle's <c>--hours</c>, clamped to the tool's range (1 to <see cref="DarlingMcpStoreQueryHistoryTools.MaxHours"/>).
    /// <c>--hours</c> stops at 168 today, so the clamp is a guard for the day that range widens; when it acts, the member
    /// carries <c>hours_back_clamped_from</c> with the value that was asked for.
    /// </remarks>
    internal static async Task<JsonNode?> StatementHistoryAsync(NpgsqlDataSource postgres, int hours, CancellationToken ct)
    {
        bool present;
        await using (var probe = postgres.CreateCommand(StatementHistoryPresentSql))
        {
            probe.CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds;
            present = (await probe.ExecuteScalarAsync(ct)) is true;
        }

        if (!present)
        {
            return new JsonObject { ["status"] = "not_present", ["reason"] = "This store has no statement-history tables yet." };
        }

        var hoursBack = Math.Clamp(hours, 1, DarlingMcpStoreQueryHistoryTools.MaxHours);
        var answer = DiagnosticsBundle.ParseReader(
            await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistoryUncut(postgres, hoursBack, StatementHistoryTop, ct));
        if (hoursBack != hours && answer is JsonObject history)
        {
            history["hours_back_clamped_from"] = hours;
        }

        return answer;
    }

    /// <summary>
    /// Whether either member of the section, <c>cumulative</c> or <c>history</c>, is the tool's error answer. A member is nested
    /// under the section, so the section's own status does not show it; <see cref="StoreStatementsSectionAsync"/> then gives the
    /// section the status a section that throws gets (<c>error</c>), so the exit code and the manifest both count it as failed,
    /// as they did when a failed read threw. A precondition or a refusal is not an error: it is how a store that cannot serve
    /// the read yet answers, and it is not counted.
    /// </summary>
    internal static bool MemberFailed(JsonNode? section) =>
        section is JsonObject o
        && (DiagnosticsBundle.StatusOf(o["history"]) == "error" || DiagnosticsBundle.StatusOf(o["cumulative"]) == "error");
}
