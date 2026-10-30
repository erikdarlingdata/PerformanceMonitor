/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198: get_collection_health has no row to drop (every collector on the server is one row, and a health
/// read must never hide a failing/stale/disabled/erroring one by leaving it off the page), so its default-size
/// cut is per-field instead. This seeds every SQL Server catalog collector - the realistic per-server shape -
/// mostly HEALTHY-and-boring, plus six deliberately NOT-boring rows that must never compact even though five
/// of the six band HEALTHY, and measures the tool method's own UTF-8 byte count. Its own file/seeding per the
/// #4198 common brief: not shared with any other lane's tonight.
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionHealthPayloadBudgetLiveTests
{
    private const string ServerName = "darling-collection-health-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private readonly ITestOutputHelper _output;
    public CollectionHealthPayloadBudgetLiveTests(ITestOutputHelper output) => _output = output;

    /// <summary>No-rig pin: the web viewer's /api/read row must keep today's full per-collector payload, not
    /// the new compact default. Source-scanned rather than rig-driven so it runs everywhere, including CI legs
    /// with no Postgres rig.</summary>
    [Fact]
    public void WebViewerRow_PassesFullDetailTrue()
    {
        var path = FindRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        var source = System.IO.File.ReadAllText(path);
        Assert.Contains(
            "[\"get_collection_health\"] = (c, pg, an) => DarlingMcpDataTools.GetCollectionHealth(pg, Server(c), full_detail: true, collector_name: Str(c, \"collector_name\"), hours_back: Hours(c, 24), cancellationToken: c.RequestAborted)",
            source, StringComparison.Ordinal);
    }

    private static string FindRepoFile(params string[] relativeParts)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8; i++)
        {
            var candidate = System.IO.Path.Combine(new[] { dir }.Concat(relativeParts).ToArray());
            if (System.IO.File.Exists(candidate)) return candidate;
            dir = System.IO.Path.GetDirectoryName(dir) ?? dir;
        }
        throw new System.IO.FileNotFoundException(System.IO.Path.Combine(relativeParts));
    }

    [Fact]
    public async Task DefaultCall_StaysUnderBudget_AndNeverCompactsANonBoringCollector()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live get_collection_health budget test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var sqlServerCollectors = CollectorCatalog.All
                .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer)
                .Select(d => d.Name)
                .ToArray();
            Assert.True(sqlServerCollectors.Length > 30, "expected a realistic SQL Server catalog width");

            /* six collectors that must NEVER compact, five of them despite banding HEALTHY: */
            var neverCompact = new[] { "wait_stats", "memory_grant_stats", "query_store_health", "database_scoped_config", "plan_cache_stats", "tempdb_stats" };
            Assert.All(neverCompact, name => Assert.Contains(name, sqlServerCollectors));

            foreach (var name in sqlServerCollectors)
            {
                switch (name)
                {
                    case "wait_stats":
                        /* FAILING band: recent ERROR runs, never a success, so HealthStatus itself excludes it. */
                        for (var i = 0; i < 5; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 6), "ERROR", 120, null,
                                "Login failed for user 'darling_monitor'.", ct);
                        break;

                    case "memory_grant_stats":
                        /* WARNING band via a 30% error rate - HealthStatus alone would already exclude this
                           one, but it also exercises errors > 0 beside a fresh fresh success. */
                        for (var i = 0; i < 7; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 20 - 1), "SUCCESS", 80, 40, null, ct);
                        for (var i = 0; i < 3; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 30 - 2), "ERROR", 90, null, "Timeout expired.", ct);
                        break;

                    case "query_store_health":
                        /* HEALTHY band (a fresh success, 0 current errors) but PermissionDeniedCount > 0 from
                           an OLDER denial this window - the case HealthStatus alone would miss and the reason
                           IsCollectionHealthCompactEligible checks PermissionDeniedCount directly. */
                        await InsertLogRowAsync(connection, name, now.AddDays(-6), "PERMISSIONS", 50, null,
                            "permission denied for function pg_read_file", ct);
                        await InsertLogRowAsync(connection, name, now.AddDays(-6).AddHours(-1), "PERMISSIONS", 50, null,
                            "permission denied for function pg_read_file", ct);
                        for (var i = 0; i < 4; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 12), "SUCCESS", 60, 12, null, ct);
                        break;

                    case "database_scoped_config":
                        /* HEALTHY band, RowsStored = 0, and NOT an event collector - FormatOutputFinding's
                           "needs a look" reading, which a compact row must never hide. */
                        for (var i = 0; i < 8; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 18), "SUCCESS", 30, 0, null, ct);
                        break;

                    case "deadlocks":
                        /* Event collector at rest: RowsStored = 0 but IsEventCollector is true, so this one
                           SHOULD compact - the boring-empty case #1852/#3754 protect deliberately as healthy. */
                        for (var i = 0; i < 8; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 18), "SUCCESS", 15, 0, null, ct);
                        break;

                    case "plan_cache_stats":
                        /* #4198 follow-up: HEALTHY band (a fresh, productive success) but SessionMissingCount > 0
                           - runs whose XE session was missing. Classify() never takes the count as a parameter, so
                           nothing about HealthStatus hints at it; IsCollectionHealthCompactEligible checks it
                           directly, which is why this row must fail the predicate on session_missing ALONE. */
                        for (var i = 0; i < 2; i++)
                            await InsertLogRowAsync(connection, name, now.AddDays(-1).AddHours(-i), "SESSION_MISSING", 40, null, null, ct);
                        for (var i = 0; i < 6; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 24 - 1), "SUCCESS", 100 + i * 15, 50 + i * 5, null, ct);
                        break;

                    case "tempdb_stats":
                        /* #4198 follow-up: HEALTHY band (1/250 = 0.4% abandon rate, under the 0.5% WARNING
                           cutoff) but AbandonedCount > 0 - a cycle the wall-clock budget gave up on, storing
                           nothing and advancing no watermark. IsCollectionHealthCompactEligible checks the COUNT,
                           not the rate, so this row fails it on abandoned ALONE even though Classify() reads it
                           HEALTHY. */
                        await InsertLogRowAsync(connection, name, now.AddHours(-3), "ABANDONED", 120000, null, null, ct);
                        for (var i = 0; i < 249; i++)
                            await InsertLogRowAsync(connection, name, now.AddMinutes(-i * 15 - 5), "SUCCESS", 90, 20, null, ct);
                        break;

                    default:
                        /* The realistic majority: plainly healthy and productive. */
                        for (var i = 0; i < 6; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 24 - 1), "SUCCESS", 100 + i * 15, 50 + i * 5, null, ct);
                        break;
                }
            }

            var defaultJson = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName);
            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
            var fullJson = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, full_detail: true);
            var fullBytes = Encoding.UTF8.GetByteCount(fullJson);
            _output.WriteLine($"get_collection_health: default {defaultBytes:N0} bytes, full_detail=true {fullBytes:N0} bytes, budget {McpResponseBudget.DefaultBytes:N0}.");

            /* #4198 ruling item 6: a default call must land at 80% of the budget or less, to leave room for a
               larger fleet than this fixture's collectors. Expressed off McpResponseBudget.DefaultBytes, the
               same expression Lite's CollectionHealthPayloadBudgetToolTests uses, so the two SKUs' ceilings can
               never drift apart by one being hand-typed and the other derived. */
            var ceiling = McpResponseBudget.DefaultBytes * 4 / 5;
            Assert.True(defaultBytes <= ceiling,
                $"default get_collection_health is {defaultBytes:N0} bytes, over the {ceiling:N0}-byte (80% of {McpResponseBudget.DefaultBytes:N0}) ceiling.");
            Assert.True(defaultBytes < fullBytes, "the default call should be smaller than full_detail=true.");

            using var defaultDoc = JsonDocument.Parse(defaultJson);
            var defaultRows = defaultDoc.RootElement.GetProperty("collectors").EnumerateArray()
                .ToDictionary(r => r.GetProperty("collector").GetString()!, r => r);
            Assert.Equal(sqlServerCollectors.Length, defaultRows.Count);

            foreach (var name in neverCompact)
            {
                Assert.True(defaultRows[name].TryGetProperty("errors", out _), $"{name} must keep at least partial detail by default (it is not boring-healthy).");
                Assert.False(defaultRows[name].TryGetProperty("compact", out _), $"{name} must not be marked compact.");
                /* #4198's second tier: a row that needs a look gets partial_detail, never the full ~30-field
                   shape, and never the compact marker -- reusing `compact` here would teach a `compact != true`
                   scan to skip a row that needs a look, exactly what #4268's marker exists to prevent. */
                Assert.True(defaultRows[name].TryGetProperty("partial_detail", out var partial) && partial.GetBoolean(),
                    $"{name} must carry partial_detail: true.");
                Assert.False(defaultRows[name].TryGetProperty("avg_duration_ms", out _),
                    $"{name} is over budget by default and should not carry full-detail-only fields like avg_duration_ms.");
            }

            /* #4198 follow-up: a HEALTHY row failing the predicate on session-missing runs, abandoned cycles, or
               a denial that a later success cleared must still say WHY in the partial shape - not just that
               something needs a look. Each reason field is nonzero/non-null on the row whose only issue it is. */
            Assert.True(defaultRows["plan_cache_stats"].GetProperty("session_missing").GetInt64() > 0,
                "plan_cache_stats' only issue is session-missing runs; the partial row must say how many.");
            Assert.True(defaultRows["tempdb_stats"].GetProperty("abandoned").GetInt64() > 0,
                "tempdb_stats' only issue is abandoned cycles; the partial row must say how many.");
            Assert.Equal(JsonValueKind.String, defaultRows["query_store_health"].GetProperty("last_denied_at").ValueKind);

            Assert.True(defaultRows["deadlocks"].TryGetProperty("compact", out var deadlocksCompact) && deadlocksCompact.GetBoolean(),
                "an event collector resting at zero rows should compact.");
            Assert.True(defaultRows.Values.Count(r => r.TryGetProperty("compact", out _)) >= sqlServerCollectors.Length - neverCompact.Length,
                "every boring-healthy collector should compact.");

            using var fullDoc = JsonDocument.Parse(fullJson);

            /* #6180: full_detail asks for ~30 fields on every collector, which is past the response target on a store this size.
               It still answers within the target: the leading whole rows that fit, with the cut counted and the way to the rest named. */
            Assert.True(fullBytes <= McpResponseBudget.DefaultBytes,
                $"full_detail get_collection_health is {fullBytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");
            Assert.True(fullDoc.RootElement.GetProperty("truncated").GetBoolean(), "full_detail on this store is over the budget, so it must say it was cut.");
            Assert.Equal(defaultRows.Count, fullDoc.RootElement.GetProperty("collectors_total").GetInt32());
            Assert.Equal(fullDoc.RootElement.GetProperty("collectors").GetArrayLength(), fullDoc.RootElement.GetProperty("collectors_returned").GetInt32());
            Assert.True(fullDoc.RootElement.GetProperty("collectors_returned").GetInt32() < defaultRows.Count);
            Assert.Contains("without full_detail", fullDoc.RootElement.GetProperty("size_note").GetString(), StringComparison.Ordinal);
            Assert.False(defaultDoc.RootElement.GetProperty("truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, defaultDoc.RootElement.GetProperty("size_note").ValueKind);

            Assert.All(fullDoc.RootElement.GetProperty("collectors").EnumerateArray(), r =>
            {
                Assert.False(r.TryGetProperty("compact", out _), "full_detail=true must serve every field on every row.");
                Assert.False(r.TryGetProperty("partial_detail", out _), "full_detail=true must serve every field on every row, not the leaner shape.");
            });

            var note = defaultDoc.RootElement.GetProperty("collector_detail_note").GetString();
            Assert.Contains($"of {sqlServerCollectors.Length} collector", note, StringComparison.Ordinal);
            Assert.Contains($"{neverCompact.Length} need a look", note, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #5748, Darling's twin of Lite's <c>McpReadToolBudgetTests.CollectionHealthWorstCaseAnswersUnder75PercentOfTheBudget</c>:
    /// every SQL Server collector has mixed SUCCESS and ERROR runs, so no row can compact and every one is a partial_detail
    /// row. That is the worst case, and each collector added moves it up by one row. A partial row leaves out every key
    /// whose value is null, false or 0, so the answer stays at or under 75% of the default budget (24,576 B).
    /// </summary>
    [Fact]
    public async Task WorstCase_EveryCollectorNeedsALook_AnswersUnder75PercentOfTheBudget()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live get_collection_health budget test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var sqlServerCollectors = CollectorCatalog.All
                .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer)
                .Select(d => d.Name)
                .ToArray();

            foreach (var name in sqlServerCollectors)
            {
                for (var s = 0; s < 10; s++)
                {
                    await InsertLogRowAsync(connection, name, now.AddMinutes(-5 * s), s % 6 == 0 ? "ERROR" : "SUCCESS", 100 + s,
                        s % 6 == 0 ? null : 50 + s, s % 6 == 0 ? "Login failed for user 'darling_monitor'." : null, ct);
                }
            }

            var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName);
            var bytes = Encoding.UTF8.GetByteCount(json);
            var breakdown = CollectionHealthBreakdown(json);
            _output.WriteLine(breakdown);

            Assert.True(bytes <= McpResponseBudget.DefaultBytes * 3 / 4,
                $"get_collection_health's worst case answered {bytes:N0} B, over 75% of the {McpResponseBudget.DefaultBytes:N0} B budget (#5748). {breakdown}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The byte breakdown #5748 asked for: partial rows, split into key names, null/false/0 values and the
    /// rest, and the bytes outside the rows.</summary>
    private static string CollectionHealthBreakdown(string json)
    {
        using var doc = JsonDocument.Parse(json);
        long rows = 0, keyNames = 0, defaultKeys = 0, defaults = 0, other = 0;
        var count = 0;
        string? sample = null;
        foreach (var row in doc.RootElement.GetProperty("collectors").EnumerateArray())
        {
            if (!row.TryGetProperty("partial_detail", out _)) continue;
            count++;
            rows += Encoding.UTF8.GetByteCount(row.GetRawText());
            sample ??= row.GetRawText();
            foreach (var prop in row.EnumerateObject())
            {
                var value = prop.Value;
                var key = Encoding.UTF8.GetByteCount(prop.Name) + 4;
                keyNames += key;
                var raw = Encoding.UTF8.GetByteCount(value.GetRawText());
                var isDefault = value.ValueKind is JsonValueKind.Null or JsonValueKind.False
                    || (value.ValueKind == JsonValueKind.Number && value.GetDouble() == 0);
                if (isDefault) { defaults += raw; defaultKeys += key; } else other += raw;
            }
        }
        var total = Encoding.UTF8.GetByteCount(json);
        return $"total {total:N0} B; {count} partial rows {rows:N0} B (key names+punctuation {keyNames:N0}, of which on null/false/0 values {defaultKeys:N0}; null/false/0 values {defaults:N0}; other values {other:N0}); outside the rows {total - rows:N0} B; sample row {sample}";
    }

    /// <summary>
    /// #4620: whenever a row says <c>regressed_from_productive</c>, it carries the sentence that explains it,
    /// on the default shape and on full_detail alike. Until this issue the partial shape carried the flag
    /// with no sentence, so a WARNING row whose only issue was a regression said nothing about why. Seeds one
    /// row of each regression shape through the real read: a dense collector three zero-row runs after its
    /// last productive one (memory_grant_stats, which has rows whether or not anything runs, so #6453 leaves it out of the activity-sourced set), an activity-only collector 73 hours after its last productive run
    /// (job_history), and a skip regression (wait_stats, PERMISSIONS on every run since it last stored rows).
    /// Beside them, an activity-only collector only 71 hours quiet (waiting_tasks) reads HEALTHY and compacts,
    /// where before #4620 it banded WARNING.
    /// </summary>
    [Fact]
    public async Task ARegressedRow_CarriesItsSentence_OnEveryRowShape()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live get_collection_health row-shape test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

            /* Dense: productive every hour until an hour ago, then three zero-row successes. */
            for (var i = 1; i <= 6; i++)
                await InsertLogRowAsync(connection, "memory_grant_stats", now.AddHours(-i), "SUCCESS", 80, 40, null, ct);
            for (var i = 1; i <= 3; i++)
                await InsertLogRowAsync(connection, "memory_grant_stats", now.AddMinutes(-i), "SUCCESS", 20, 0, null, ct);

            /* Activity-only, last productive 73 hours ago, four zero-row successes since: flagged. */
            await InsertLogRowAsync(connection, "job_history", now.AddHours(-74), "SUCCESS", 80, 12, null, ct);
            await InsertLogRowAsync(connection, "job_history", now.AddHours(-73), "SUCCESS", 80, 12, null, ct);
            foreach (var hoursAgo in new[] { 72.0, 48.0, 24.0, 0.05 })
                await InsertLogRowAsync(connection, "job_history", now.AddHours(-hoursAgo), "SUCCESS", 20, 0, null, ct);

            /* Activity-only, last productive 71 hours ago, four zero-row successes since: HEALTHY. */
            await InsertLogRowAsync(connection, "waiting_tasks", now.AddHours(-72), "SUCCESS", 80, 3, null, ct);
            await InsertLogRowAsync(connection, "waiting_tasks", now.AddHours(-71), "SUCCESS", 80, 3, null, ct);
            foreach (var hoursAgo in new[] { 70.0, 48.0, 24.0, 0.05 })
                await InsertLogRowAsync(connection, "waiting_tasks", now.AddHours(-hoursAgo), "SUCCESS", 20, 0, null, ct);

            /* The skip class (#3819): productive, then PERMISSIONS on every run since. */
            for (var i = 3; i <= 5; i++)
                await InsertLogRowAsync(connection, "wait_stats", now.AddHours(-i), "SUCCESS", 80, 40, null, ct);
            for (var i = 1; i <= 2; i++)
                await InsertLogRowAsync(connection, "wait_stats", now.AddHours(-i), "PERMISSIONS", 20, null, "VIEW SERVER STATE permission was denied", ct);

            var defaultJson = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName);
            var fullJson = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, full_detail: true);

            foreach (var json in new[] { defaultJson, fullJson })
            {
                using var doc = JsonDocument.Parse(json);
                var rows = doc.RootElement.GetProperty("collectors").EnumerateArray()
                    .ToDictionary(r => r.GetProperty("collector").GetString()!, r => r.Clone());

                /* The general claim, over every row the read returned: a regressed row always has its sentence. */
                var regressed = rows
                    .Where(kv => kv.Value.TryGetProperty("regressed_from_productive", out var flag) && flag.GetBoolean())
                    .Select(kv => kv.Key)
                    .OrderBy(n => n, StringComparer.Ordinal)
                    .ToArray();
                Assert.Equal(new[] { "job_history", "memory_grant_stats", "wait_stats" }, regressed);
                foreach (var name in regressed)
                {
                    Assert.True(rows[name].TryGetProperty("regression_finding", out var finding), $"{name} is regressed but carries no regression_finding.");
                    Assert.Equal(JsonValueKind.String, finding.ValueKind);
                    Assert.False(string.IsNullOrWhiteSpace(finding.GetString()), name);
                    Assert.Equal(CollectorHealthClassifier.Warning, rows[name].GetProperty("status").GetString());
                }

                Assert.Contains("has recorded SUCCESS with zero rows on 3 runs since", rows["memory_grant_stats"].GetProperty("regression_finding").GetString()!, StringComparison.Ordinal);
                Assert.Contains("has recorded SUCCESS with zero rows on 4 runs since", rows["job_history"].GetProperty("regression_finding").GetString()!, StringComparison.Ordinal);
                Assert.Contains("has reported PERMISSIONS since", rows["wait_stats"].GetProperty("regression_finding").GetString()!, StringComparison.Ordinal);

                Assert.Equal(CollectorHealthClassifier.Healthy, rows["waiting_tasks"].GetProperty("status").GetString());
            }

            /* On the default call the regressed rows are partial rows, which is the shape that used to drop the
               sentence, and the quiet activity-only row compacts. */
            using var defaultDoc = JsonDocument.Parse(defaultJson);
            var defaultRows = defaultDoc.RootElement.GetProperty("collectors").EnumerateArray()
                .ToDictionary(r => r.GetProperty("collector").GetString()!, r => r.Clone());
            foreach (var name in new[] { "job_history", "memory_grant_stats", "wait_stats" })
            {
                Assert.True(defaultRows[name].TryGetProperty("partial_detail", out var partial) && partial.GetBoolean(), $"{name} must be a partial row by default.");
            }

            Assert.True(defaultRows["waiting_tasks"].TryGetProperty("compact", out var compact) && compact.GetBoolean(),
                "an activity-only collector 71 hours quiet is healthy and should compact.");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertLogRowAsync(
        NpgsqlConnection connection, string collectorName, DateTime collectionTime, string status,
        int durationMs, int? rowsCollected, string? errorMessage, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue(durationMs);
        command.Parameters.AddWithValue((object?)rowsCollected ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)errorMessage ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
