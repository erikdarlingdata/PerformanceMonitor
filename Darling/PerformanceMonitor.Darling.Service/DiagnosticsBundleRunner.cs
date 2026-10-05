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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using DiagnosticsBundleExitCode = PerformanceMonitor.Darling.Service.DarlingCliCommands.DiagnosticsBundleExitCode;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Reads every bundle section and drives <see cref="DiagnosticsBundle.Assemble"/>. Reads only: it opens the store
/// through readers that already exist and runs three small catalog and history reads of its own.
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

    /// <summary>The store's own database names.</summary>
    internal const string StoreDatabasesSql = @"
SELECT datname FROM pg_database";

    /// <summary>
    /// The monitored database inventory: the distinct database names the periodic per-database state snapshot saw in
    /// the last 24 hours. The table is chunked by collection_time and indexed on (server_id, collection_time), so a
    /// bound of one day reads the newest chunk only; the read adds no index, and a store without the table answers
    /// from the other inventory below.
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
    /// Whether the statement-history tables exist. Names are PROVISIONAL until #5097 part 3 merges; the section reads
    /// <c>not_present</c> when the tables are absent, so the verb works on a store below that schema.
    /// </summary>
    internal const string StatementHistoryPresentSql = @"
SELECT to_regclass('collect.store_statement_history') IS NOT NULL AND to_regclass('collect.store_statement_captures') IS NOT NULL";

    internal const string StatementCapturesSql = @"
SELECT capture_time, interval_seconds, dealloc_delta, statements_seen, statements_kept, hidden_statements, outcome
FROM collect.store_statement_captures
WHERE capture_time >= $1
ORDER BY capture_time DESC
LIMIT 500";

    internal const string StatementHistoryTopSql = @"
SELECT queryid
FROM collect.store_statement_history
WHERE capture_time >= $1
GROUP BY queryid
ORDER BY SUM(delta_total_exec_ms) DESC, queryid
LIMIT 25";

    internal const string StatementHistoryRowsSql = @"
SELECT capture_time, interval_seconds, role_name, queryid, delta_calls, delta_total_exec_ms, delta_rows,
       delta_shared_blks_hit, delta_shared_blks_read, delta_temp_blks_written, max_exec_ms,
       first_seen, entry_restarted, reset_in_interval
FROM collect.store_statement_history
WHERE capture_time >= $1
AND   queryid = ANY($2)
ORDER BY queryid, capture_time DESC
LIMIT 1500";

    /// <summary>The text of the listed statements, withheld where the shared sensitive-statement filter names it.</summary>
    internal static readonly string StatementTextSql = @"
SELECT queryid, " + PgSensitiveStatementFilter.SqlPredicate("query") + @" AS query
FROM " + PgSchemaGenerator.ConfigSchema + "." + StoreStatementStats.FunctionName + @"()
WHERE queryid = ANY($1)";

    /// <summary>The outcome of building and writing a bundle.</summary>
    internal sealed record Outcome(int ExitCode, string? Text, IReadOnlyList<BundleLeak> Leaks, BundleAliaser Aliaser, string Message);

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
        string? storeErrorSentence,
        CancellationToken cancellationToken)
    {
        var aliaser = new BundleAliaser();
        DiagnosticsBundle.SeedFromConfig(aliaser, config, connectionString);
        var sections = new List<BundleSection>();
        string scope = "fleet";
        var failedSections = 0;

        if (postgres is not null)
        {
            await SeedFromStoreAsync(aliaser, postgres, cancellationToken);
        }

        int? scopeServerId = null;
        string? scopeServerName = null;
        if (postgres is not null && !string.IsNullOrWhiteSpace(options.ServerName))
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, options.ServerName, cancellationToken);
            if (error is not null)
            {
                return new Outcome(DiagnosticsBundleExitCode.ConfigError, null, Array.Empty<BundleLeak>(), aliaser,
                    "--server did not match one monitored server.");
            }

            scopeServerId = resolved.ServerId;
            scopeServerName = resolved.ServerName;
            scope = aliaser.AddServer(resolved.ServerId, resolved.ServerName);
        }

        sections.Add(new BundleSection("config_shape", DiagnosticsBundle.BuildConfigShape(config), false));

        if (postgres is null)
        {
            sections.Add(new BundleSection("store_unreachable", new JsonObject
            {
                ["status"] = "store_unreachable",
                ["error_class"] = storeError is null ? "NotConfigured" : SlowReadLog.ErrorClassOf(storeError),
                ["message"] = storeErrorSentence ?? storeError?.Message ?? "The store could not be opened.",
            }, true));
        }
        else
        {
            var hours = options.Hours;
            var days = Math.Max(1, (int)Math.Ceiling(hours / 24.0));
            var ct = cancellationToken;
            sections.Add(await DiagnosticsBundle.RunSectionAsync("store", c => StoreSectionAsync(postgres, config, c), ct));
            sections.Add(await DiagnosticsBundle.RunSectionAsync("collection",
                c => CollectionSectionAsync(postgres, scopeServerName, hours, days, c), ct));
            sections.Add(await DiagnosticsBundle.RunSectionAsync("stall_probes",
                async c => DiagnosticsBundle.ParseReader(await DarlingMcpStallProbeTools.GetCollectorStallProbes(postgres, scopeServerName, days, 50, c)), ct));
            sections.Add(await DiagnosticsBundle.RunSectionAsync("slow_reads",
                async c => DiagnosticsBundle.ParseReader(await DarlingMcpSlowReadTools.GetSlowReads(postgres, hours, scopeServerName, null, null, 100, true, c)), ct));
            sections.Add(await DiagnosticsBundle.RunSectionAsync("read_latency",
                async c => DiagnosticsBundle.ParseReader(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours, null, null, 200, c)), ct));
            sections.Add(await DiagnosticsBundle.RunSectionAsync("store_statements",
                c => StoreStatementsSectionAsync(postgres, hours, c), ct));
            sections.Add(await DiagnosticsBundle.RunSectionAsync("store_log",
                async c => DiagnosticsBundle.ParseReader(await DarlingMcpStoreLogTools.GetStoreLog(postgres, hours, 50, null, c)), ct));
        }

        var logDirectory = options.LogDirectory ?? DarlingFileLoggerProvider.DefaultLogDirectory();
        var searched = options.LogDirectory is null ? "default" : "--log-dir";
        sections.Add(await DiagnosticsBundle.RunSectionAsync("service_log",
            _ => Task.FromResult<JsonNode?>(DiagnosticsBundleServiceLog.Read(logDirectory, searched, DateTime.Now.AddHours(-options.Hours))), cancellationToken));

        failedSections = sections.Count(s => s.Failed && s.Name != "store_unreachable");
        var exit = postgres is null ? DiagnosticsBundleExitCode.StoreUnreachable
            : failedSections > 0 ? DiagnosticsBundleExitCode.PartialBundle : DiagnosticsBundleExitCode.Ok;

        _ = scopeServerId;
        var manifest = DiagnosticsBundle.BuildManifest(options.Hours, scope, exit);
        var (text, leaks, overCap) = DiagnosticsBundle.Assemble(sections, aliaser, manifest);
        if (overCap)
        {
            return new Outcome(DiagnosticsBundleExitCode.OutputError, null, leaks, aliaser, "The bundle could not be held under the size cap.");
        }

        if (text is null)
        {
            return new Outcome(DiagnosticsBundleExitCode.LeakGuard, null, leaks, aliaser, "A known name or secret survived aliasing.");
        }

        return new Outcome(exit, text, leaks, aliaser, string.Empty);
    }

    private static async Task SeedFromStoreAsync(BundleAliaser aliaser, NpgsqlDataSource postgres, CancellationToken ct)
    {
        await TryReadAsync(postgres, RegistryNamesSql, reader =>
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
        }, ct);

        /* Servers by id first so the alias order follows server_id; display names share their server's alias. */
        await TryReadAsync(postgres, CollectServersSql, reader =>
        {
            var id = reader.GetInt32(0);
            var name = reader.IsDBNull(1) ? null : reader.GetString(1);
            aliaser.AddServer(id, name);
            if (!reader.IsDBNull(2))
            {
                aliaser.AddNameLike(reader.GetString(2), name);
            }
        }, ct);

        await TryReadAsync(postgres, StoreDatabasesSql, reader => aliaser.AddName(AliasKind.Database, reader.GetString(0)), ct);
        await TryReadAsync(postgres, DatabaseInventorySql, reader => aliaser.AddName(AliasKind.Database, reader.GetString(0)), ct);
        await TryReadAsync(postgres, PgDatabaseInventorySql, reader => aliaser.AddName(AliasKind.Database, reader.GetString(0)), ct);
    }

    /// <summary>One bounded read for the name set. A failure adds nothing; the harvest pass and the verifier cover what it would have named.</summary>
    private static async Task TryReadAsync(NpgsqlDataSource postgres, string sql, Action<NpgsqlDataReader> each, CancellationToken ct)
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
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException or InvalidCastException)
        {
            /* A table the store does not have yet (below the schema that adds it) or a read the role may not make. */
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
        };
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

            var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, server.ServerName, false, null, ct);
            used += System.Text.Encoding.UTF8.GetByteCount(json);
            health.Add(new JsonObject { ["server_name"] = server.ServerName, ["health"] = DiagnosticsBundle.ParseReader(json) });
        }

        var fleet = DiagnosticsBundle.ParseReader(await DarlingMcpFleetTools.GetFleetOverview(postgres, hours_back: 1, detail: "summary", cancellationToken: ct));
        var slowest = DiagnosticsBundle.ParseReader(await DarlingMcpDataTools.GetCollectionLog(
            postgres, server_name: string.IsNullOrWhiteSpace(serverName) ? "*" : serverName, hours_back: hours, limit: 50, as_of: null,
            collector_name: null, min_duration_ms: 1, status: null, full_text: false, logger: null, cancellationToken: ct));
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
            await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(postgres, null, "total_time", 25, false, ct));
        return new JsonObject
        {
            ["cumulative"] = cumulative,
            ["history"] = await StatementHistoryAsync(postgres, hours, ct),
        };
    }

    /// <summary>
    /// Part 3's statement history. Table and column names are provisional until #5097 part 3 merges. Never reads the
    /// diff-state baseline table. Absent tables report <c>not_present</c>.
    /// </summary>
    private static async Task<JsonNode?> StatementHistoryAsync(NpgsqlDataSource postgres, int hours, CancellationToken ct)
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

        var since = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-hours), DateTimeKind.Unspecified);
        var captures = new JsonArray();
        await using (var command = postgres.CreateCommand(StatementCapturesSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(since);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                captures.Add(new JsonObject
                {
                    ["capture_time"] = reader.GetDateTime(0).ToString("o", CultureInfo.InvariantCulture),
                    ["interval_seconds"] = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                    ["dealloc_delta"] = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                    ["statements_seen"] = reader.GetInt32(3),
                    ["statements_kept"] = reader.GetInt32(4),
                    ["hidden_statements"] = reader.GetInt32(5),
                    ["outcome"] = reader.GetString(6),
                });
            }
        }

        var queryIds = new List<long>();
        await using (var command = postgres.CreateCommand(StatementHistoryTopSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(since);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                queryIds.Add(reader.GetInt64(0));
            }
        }

        var texts = new Dictionary<long, string>();
        if (queryIds.Count > 0)
        {
            try
            {
                await using var command = postgres.CreateCommand(StatementTextSql);
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.AddWithValue(queryIds.ToArray());
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var raw = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                    texts[reader.GetInt64(0)] = StoreStatementStats.CompactStatementText(DarlingMcpStoreQueryStatsTools.ShownText(raw), 240);
                }
            }
            catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
            {
                /* The text reader is gated on the extension; without it the history still stands, with no text. */
            }
        }

        var rows = new JsonArray();
        if (queryIds.Count > 0)
        {
            await using var command = postgres.CreateCommand(StatementHistoryRowsSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(since);
            command.Parameters.AddWithValue(queryIds.ToArray());
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var id = reader.GetInt64(3);
                rows.Add(new JsonObject
                {
                    ["capture_time"] = reader.GetDateTime(0).ToString("o", CultureInfo.InvariantCulture),
                    ["interval_seconds"] = reader.GetInt32(1),
                    ["role_name"] = reader.GetString(2),
                    ["queryid"] = id.ToString(CultureInfo.InvariantCulture),
                    ["delta_calls"] = reader.GetInt64(4),
                    ["delta_total_exec_ms"] = reader.GetDouble(5),
                    ["delta_rows"] = reader.GetInt64(6),
                    ["delta_shared_blks_hit"] = reader.GetInt64(7),
                    ["delta_shared_blks_read"] = reader.GetInt64(8),
                    ["delta_temp_blks_written"] = reader.GetInt64(9),
                    ["max_exec_ms_cumulative"] = reader.IsDBNull(10) ? null : reader.GetDouble(10),
                    ["first_seen"] = reader.GetBoolean(11),
                    ["entry_restarted"] = reader.GetBoolean(12),
                    ["reset_in_interval"] = reader.GetBoolean(13),
                    ["query"] = texts.TryGetValue(id, out var text) ? text : null,
                });
            }
        }

        return new JsonObject { ["status"] = "ok", ["captures"] = captures, ["top_statement_rows"] = rows };
    }
}
