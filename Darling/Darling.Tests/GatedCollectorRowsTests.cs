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
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection. */

/// <summary>
/// #5249: <c>get_collection_health</c> lists the PostgreSQL collectors the server's engine does not collect, which
/// write no log row and so used to be missing from the web PG Overview. The pure helper, its wiring through the real
/// tool, the roll-up identity (the new rows feed nothing) and the page.
/// </summary>
public sealed class GatedCollectorRowsTests
{
    private sealed class FakeCollector : ICollectorSchemaInfo
    {
        public FakeCollector(string name, CollectorTargetEngine engine = CollectorTargetEngine.PostgreSql)
        {
            Name = name;
            TargetEngine = engine;
        }

        public CollectorTargetEngine TargetEngine { get; }
        public string Name { get; }
        public string TargetTable => Name;
        public bool IncludesCollectionId => true;
        public string PrefixIdColumnName => "collection_id";
        public string PrefixTimeColumnName => "collection_time";
        public IReadOnlyList<CollectorColumn> PayloadColumns => Array.Empty<CollectorColumn>();
        public bool YieldsOnLockTimeout => false;
        public TimeSpan? PerItemWallClockBudget => null;
        public IReadOnlyList<string> StateKeys => Array.Empty<string>();
        public bool AppliesTo(CollectorTargetInfo target) => true;
    }

    private static (string Collector, string Status, string Message)[] Decode(IReadOnlyList<object> rows) =>
        rows.Select(r =>
        {
            using var doc = JsonDocument.Parse(JsonSerializer.Serialize(r));
            var e = doc.RootElement;
            Assert.Equal(new[] { "collector", "status", "message" }, e.EnumerateObject().Select(p => p.Name).ToArray());
            return (e.GetProperty("collector").GetString()!, e.GetProperty("status").GetString()!, e.GetProperty("message").GetString()!);
        }).ToArray();

    private static readonly ICollectorSchemaInfo[] Catalog =
    {
        new FakeCollector("zz_gated"),
        new FakeCollector("aa_gated"),
        new FakeCollector("mm_collected"),
        new FakeCollector("bb_logged_and_gated"),
        new FakeCollector("sql_only", CollectorTargetEngine.SqlServer),
    };

    private static string? Message(string name) => name is "zz_gated" or "aa_gated" or "bb_logged_and_gated" or "sql_only" ? "gap: " + name : null;

    [Fact]
    public void AGatedCollectorAppears_ACollectedOneDoesNot_AndAGatedOneThatLoggedIsNotDuplicated()
    {
        var rows = Decode(DarlingGatedCollectorRows.Rows(
            Catalog, MonitoredEngineKind.Postgres, new HashSet<string> { "bb_logged_and_gated" }, Message));

        Assert.Equal(
            new[] { ("aa_gated", "not_collected", "gap: aa_gated"), ("zz_gated", "not_collected", "gap: zz_gated") },
            rows);
    }

    [Fact]
    public void TheRowsAreOrderedByCollectorNameOrdinal_NotByCatalogOrder()
    {
        var catalog = new ICollectorSchemaInfo[] { new FakeCollector("b_x"), new FakeCollector("B_y"), new FakeCollector("a_z"), new FakeCollector("_w") };
        var rows = Decode(DarlingGatedCollectorRows.Rows(catalog, MonitoredEngineKind.Postgres, new HashSet<string>(), n => "m " + n));

        Assert.Equal(new[] { "B_y", "_w", "a_z", "b_x" }, rows.Select(r => r.Collector).ToArray());
    }

    [Theory]
    [InlineData(MonitoredEngineKind.SqlServer)]
    [InlineData(null)]
    [InlineData("a-token-from-a-newer-build")]
    public void AServerThatIsNotKnownToBePostgreSql_GetsNoRows(string? engineKind)
    {
        Assert.Empty(DarlingGatedCollectorRows.Rows(Catalog, engineKind, new HashSet<string>(), Message));
    }

    [Fact]
    public void TheRealCatalog_ListsExactlyWhatTheCapabilityAnswerSays_OnStockAndOnAurora()
    {
        foreach (var kind in new[] { MonitoredEngineKind.Postgres, MonitoredEngineKind.AuroraPostgres })
        {
            var listed = Decode(DarlingGatedCollectorRows.Rows(
                CollectorCatalog.All, kind, new HashSet<string>(),
                n => CollectorEngineCapability.NotCollectedMessage("alpha-a", 0, kind, n))).Select(r => r.Collector).ToArray();
            var expected = CollectorCatalog.All
                .Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql && !CollectorEngineCapability.IsCollectedOnEngineKind(d, kind))
                .Select(d => d.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();

            Assert.NotEmpty(expected);
            Assert.Equal(expected, listed);
        }
    }

    // ───────────────────────── the page ─────────────────────────

    private static JsonElement RunHarness()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-gated-collectors-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the gated-collectors harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the gated-collectors harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ThePgOverview_PaintsANotCollectedRowNeutrally_WithItsSentence_AndAddsNoColumnWithoutOne()
    {
        var run = RunHarness();

        var with = run.GetProperty("withGated");
        var heads = with.GetProperty("heads").EnumerateArray().Select(h => h.GetString()).ToArray();
        Assert.Equal("Why not collected", heads[^1]);
        var rows = with.GetProperty("rows").EnumerateArray().ToArray();
        Assert.Equal(2, rows.Length);

        var gated = rows[1].EnumerateArray().ToArray();
        Assert.Equal("pg_wait_stats", gated[0].GetProperty("text").GetString());
        Assert.Equal("not_collected", gated[1].GetProperty("text").GetString());
        Assert.Contains("sev-Unknown", gated[1].GetProperty("cls").GetString(), StringComparison.Ordinal);
        Assert.DoesNotContain("sev-Critical", gated[1].GetProperty("cls").GetString(), StringComparison.Ordinal);
        Assert.DoesNotContain("sev-Warning", gated[1].GetProperty("cls").GetString(), StringComparison.Ordinal);
        Assert.Contains("does not run on that engine", gated[^1].GetProperty("text").GetString(), StringComparison.Ordinal);

        Assert.Equal("sev-Healthy", rows[0].EnumerateArray().ToArray()[1].GetProperty("cls").GetString());

        foreach (var arm in new[] { "withoutGated", "sqlServerWithout" })
        {
            var h = run.GetProperty(arm).GetProperty("heads").EnumerateArray().Select(x => x.GetString()).ToArray();
            Assert.DoesNotContain("Why not collected", h);
        }
    }

    [Fact]
    public void TheCollectionLogPanelsEmptySentence_StaysAsItWas()
    {
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.Contains("so an absence here is the gate working.", tabs, StringComparison.Ordinal);
    }

    [Fact]
    public void TheToolIsCalledFromOneLine()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        Assert.Equal(1, source.Split("DarlingGatedCollectorRows.AppendAsync(", StringSplitOptions.None).Length - 1);
    }
}

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared store. */

/// <summary>The same facts through the real tool against a store.</summary>
public sealed class GatedCollectorRowsLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Rig(NpgsqlConnection Connection, NpgsqlDataSource Postgres);

    private static async Task RegisterAsync(NpgsqlConnection c, int id, string name, string? kind, int? major, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, postgres_major_version, created_date, modified_date)
            VALUES ($1, $2, $2, TRUE, $3, $4, $5, $5)
            ON CONFLICT (server_id) DO UPDATE SET engine_kind = EXCLUDED.engine_kind
            """, c);
        cmd.Parameters.AddWithValue(id);
        cmd.Parameters.AddWithValue(name);
        cmd.Parameters.AddWithValue(kind is null ? DBNull.Value : kind);
        cmd.Parameters.AddWithValue(major.HasValue ? major.Value : DBNull.Value);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task LogAsync(NpgsqlConnection c, int id, string server, string collector, int minutesAgo, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected)
            VALUES ($1, $2, $3, $4, $5, 'SUCCESS', 100, 10)
            """, c);
        cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-minutesAgo), DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(id);
        cmd.Parameters.AddWithValue(server);
        cmd.Parameters.AddWithValue(collector);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task SetKindAsync(NpgsqlConnection c, int id, string? kind, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("UPDATE servers SET engine_kind = $2 WHERE server_id = $1", c);
        cmd.Parameters.AddWithValue(id);
        cmd.Parameters.AddWithValue(kind is null ? DBNull.Value : kind);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(string Json, JsonDocument Doc)> HealthAsync(NpgsqlDataSource postgres, string server, bool full, CancellationToken ct)
    {
        var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, server, full_detail: full, cancellationToken: ct);
        return (json, JsonDocument.Parse(json));
    }

    private static string[] Names(JsonElement collectors) =>
        collectors.EnumerateArray().Select(r => r.GetProperty("collector").GetString()!).ToArray();

    /// <summary>The payload with <c>collectors</c> and the memo's age taken out: everything a roll-up could have read.</summary>
    private static string Rollups(JsonDocument doc)
    {
        var map = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var p in doc.RootElement.EnumerateObject())
        {
            if (p.Name is "collectors" or "collection_health_age_seconds") continue;
            map[p.Name] = p.Value.GetRawText();
        }

        return string.Join("\n", map.Select(kv => kv.Key + "=" + kv.Value));
    }

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the gated-collector health pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AStockPostgresServer_ListsItsKindGatedCollectors_AfterTheLogRows_ByName_AndNotOnesThatLogged()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            const int id = 524_901;
            const string server = "gated-alpha-a";
            await RegisterAsync(connection, id, server, MonitoredEngineKind.Postgres, 15, ct);
            // Log order is today's order (the read sorts it); pg_wait_stats is gated on stock PostgreSQL but DID log here.
            foreach (var name in new[] { "pg_database_stats", "pg_lock_stats", "pg_wait_stats" })
                await LogAsync(connection, id, server, name, 5, ct);

            var expectedGated = CollectorCatalog.All
                .Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql && !CollectorEngineCapability.IsCollectedOnEngineKind(d, MonitoredEngineKind.Postgres))
                .Select(d => d.Name).Where(n => n != "pg_wait_stats").OrderBy(n => n, StringComparer.Ordinal).ToArray();
            Assert.Contains("pg_cpu_utilization", expectedGated);

            foreach (var full in new[] { false, true })
            {
                var (_, doc) = await HealthAsync(postgres, server, full, ct);
                using (doc)
                {
                    var collectors = doc.RootElement.GetProperty("collectors");
                    var names = Names(collectors);
                    var logged = names.Take(3).ToArray();
                    Assert.Equal(new[] { "pg_database_stats", "pg_lock_stats", "pg_wait_stats" }.OrderBy(n => n, StringComparer.Ordinal).ToArray(), logged.OrderBy(n => n, StringComparer.Ordinal).ToArray());
                    Assert.Equal(expectedGated, names.Skip(3).ToArray());
                    Assert.Equal(1, names.Count(n => n == "pg_wait_stats"));

                    foreach (var row in collectors.EnumerateArray().Skip(3))
                    {
                        // The same three keys in the full, compact and partial shapes alike: no counts, no times.
                        Assert.Equal(new[] { "collector", "status", "message" }, row.EnumerateObject().Select(p => p.Name).ToArray());
                        Assert.Equal("not_collected", row.GetProperty("status").GetString());
                        var name = row.GetProperty("collector").GetString()!;
                        Assert.Equal(CollectorEngineCapability.NotCollectedMessage(server, 0, MonitoredEngineKind.Postgres, name), row.GetProperty("message").GetString());
                    }
                }
            }
        });
    }

    [Fact]
    public async Task AnAuroraServer_ListsTheSampler_NotTheAuroraOnlyReaders()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            const int id = 524_902;
            const string server = "gated-alpha-b";
            await RegisterAsync(connection, id, server, MonitoredEngineKind.AuroraPostgres, 16, ct);
            await LogAsync(connection, id, server, "pg_database_stats", 5, ct);

            var (_, doc) = await HealthAsync(postgres, server, false, ct);
            using (doc)
            {
                var names = Names(doc.RootElement.GetProperty("collectors"));
                Assert.Contains("pg_wait_sampling", names);
                Assert.DoesNotContain("pg_wait_stats", names);
                Assert.DoesNotContain("pg_cpu_utilization", names);
            }
        });
    }

    [Fact]
    public async Task TheGatedRowsFeedNoRollup_EveryOtherKeyIsIdenticalWithAndWithoutThem_AndStoredCaveatsStayIntact()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            const int id = 524_903;
            const string server = "gated-alpha-c";
            await RegisterAsync(connection, id, server, MonitoredEngineKind.Postgres, 15, ct);
            foreach (var name in new[] { "pg_database_stats", "pg_lock_stats", "pg_session_states" })
                for (var i = 0; i < 4; i++)
                    await LogAsync(connection, id, server, name, 3 + i * 30, ct);
            await CollectionCaveatStore.ApplyPassAsync(
                postgres, id, new[] { new CollectionCaveatStore.UnreadFamily("waits", "timeout") }, DateTime.UtcNow, null, ct);

            foreach (var full in new[] { false, true })
            {
                await SetKindAsync(connection, id, MonitoredEngineKind.Postgres, ct);
                var (_, withGated) = await HealthAsync(postgres, server, full, ct);
                await SetKindAsync(connection, id, null, ct);
                var (_, without) = await HealthAsync(postgres, server, full, ct);
                using (withGated)
                using (without)
                {
                    var gatedNames = Names(withGated.RootElement.GetProperty("collectors"));
                    var plainNames = Names(without.RootElement.GetProperty("collectors"));
                    Assert.True(gatedNames.Length > plainNames.Length, "the PostgreSQL arm must have appended rows");
                    Assert.Equal(plainNames, gatedNames.Take(plainNames.Length).ToArray());

                    // Same log rows in the same order, byte for byte.
                    Assert.Equal(
                        string.Join(",", without.RootElement.GetProperty("collectors").EnumerateArray().Select(r => r.GetRawText())),
                        string.Join(",", withGated.RootElement.GetProperty("collectors").EnumerateArray().Take(plainNames.Length).Select(r => r.GetRawText())));

                    // sweep_pressure, alert_read_health, the counts note, stored_caveats and every other key.
                    Assert.Equal(Rollups(without), Rollups(withGated));
                    Assert.Single(withGated.RootElement.GetProperty("stored_caveats").EnumerateArray());
                    Assert.Single(without.RootElement.GetProperty("stored_caveats").EnumerateArray());
                }
            }
        });
    }

    [Fact]
    public async Task ASqlServerServer_IsUnchanged_AndAPostgresServerWithNoLogRowsStillAnswersUnavailable()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            const int sqlId = 524_904;
            const string sqlServer = "gated-alpha-d";
            await RegisterAsync(connection, sqlId, sqlServer, MonitoredEngineKind.SqlServer, null, ct);
            await LogAsync(connection, sqlId, sqlServer, "wait_stats", 5, ct);

            var (_, doc) = await HealthAsync(postgres, sqlServer, false, ct);
            using (doc)
            {
                Assert.Equal(new[] { "wait_stats" }, Names(doc.RootElement.GetProperty("collectors")));
            }

            const int pgId = 524_905;
            const string pgServer = "gated-alpha-e";
            await RegisterAsync(connection, pgId, pgServer, MonitoredEngineKind.Postgres, 15, ct);
            var empty = await DarlingMcpDataTools.GetCollectionHealth(postgres, pgServer, cancellationToken: ct);
            using var emptyDoc = JsonDocument.Parse(empty);
            Assert.Equal("unavailable", emptyDoc.RootElement.GetProperty("status").GetString());
            Assert.False(emptyDoc.RootElement.TryGetProperty("collectors", out _));
        });
    }

    [Fact]
    public async Task TheResponseSize_OnAPostgresServerWithEveryCollectorLogged_StaysUnderTheBudgetCeiling()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            const int id = 524_906;
            const string server = "gated-alpha-f";
            await RegisterAsync(connection, id, server, MonitoredEngineKind.Postgres, 15, ct);
            var collectors = CollectorCatalog.All
                .Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql && CollectorEngineCapability.IsCollectedOnEngineKind(d, MonitoredEngineKind.Postgres))
                .Select(d => d.Name).ToArray();
            foreach (var name in collectors)
                await LogAsync(connection, id, server, name, 5, ct);

            var sizes = new List<string>();
            foreach (var full in new[] { false, true })
            {
                await SetKindAsync(connection, id, null, ct);
                var (before, b) = await HealthAsync(postgres, server, full, ct);
                b.Dispose();
                await SetKindAsync(connection, id, MonitoredEngineKind.Postgres, ct);
                var (after, a) = await HealthAsync(postgres, server, full, ct);
                a.Dispose();
                var beforeBytes = Encoding.UTF8.GetByteCount(before);
                var afterBytes = Encoding.UTF8.GetByteCount(after);
                sizes.Add($"full_detail={full}: {beforeBytes} -> {afterBytes} bytes (+{afterBytes - beforeBytes})");
                Console.WriteLine($"get_collection_health PG response, full_detail={full}: {beforeBytes} -> {afterBytes} bytes (+{afterBytes - beforeBytes})");
                if (!full) Assert.True(afterBytes <= McpResponseBudget.DefaultBytes * 4 / 5, $"{afterBytes} bytes is over the ceiling");
            }

            Console.WriteLine("get_collection_health PG response: " + string.Join("; ", sizes));
            Assert.All(sizes, s => Assert.Matches(@"\d+ -> \d+ bytes \(\+[1-9]\d*\)", s));
        });
    }
}
