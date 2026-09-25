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
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V132 / #3653 A7: <c>cntr_type</c> on <c>collect.perfmon_stats</c> — the Windows performance-counter type id
/// the DMV reports, stored beside the raw value so the store can say which perfmon rows are counts and which
/// are levels. Until this rung the collector differenced every counter it read, so <c>Total Server Memory (KB)</c>
/// was delta'd like <c>Batch Requests/sec</c> and a FALLING level presented to the shared calculator as a counter
/// reset (#3540 A7: "a falling gauge = fake counter reset"); the viewers classified by a name-suffix proxy (#3702)
/// because the row could not tell them. The rung, the passthrough refresh, the viewer probe's top arm, the
/// Darling readers that now select the type, and the MCP payloads that publish it.
///
/// <para>The "I am the top rung" claims this class carried moved to <c>PgNumbackendsAndSampledMsRungTests</c>
/// (V133) when that rung landed, the same handoff this class received from <c>NotificationRoutesRungTests</c>
/// (V131). What stays here is the one-rung-behind half: a store carrying this and not V133 maps to 132, which
/// is the honest answer for it and what makes the upgrade banner correct in both directions.</para>
///
/// <para>The collector's write shape (a gauge row is the raw value with NULL delta and NULL interval and no
/// delta call; a rate row is unchanged plus the type) is pinned value-by-value in
/// <c>Lite.Tests/PerfmonStatsCollectorDefinitionTests</c>, and the shaping helper's type rule in
/// <c>Lite.Tests/DeltaSeriesShapingTests</c> — both run off Windows. What is here is the PostgreSQL side.</para>
/// </summary>
public sealed class PerfmonCounterTypeRungTests
{
    private const int RungVersion = 132;
    private const int PreviousVersion = 131;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V133 appended
    /// its own — so the invariant that outlives the handoff is that the ordinal is FIXED: a later rung
    /// appends after it and never shifts it.</summary>
    private const int ProbeOrdinal = 107;

    private const string Table = "perfmon_stats";
    private const string Column = "cntr_type";

    /// <summary>The one expression every trend read spells for the point's type: a type only where the instance
    /// rows summed into the point agree on one, so a mixed-type family (the wait-statistics object's instances
    /// are a rate, a gauge and an average under one counter name) reads NULL and falls to the proxy rather than
    /// being classified by whichever instance's id sorts first.</summary>
    private const string AgreedTypeExpression = "CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END AS cntr_type";

    private static PgMigrations.Migration V132 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("perfmon-counter-type", V132.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* One below the top since V133 landed; the "RungVersion == StorageVersion.SchemaVersion" half of
           the top-arm claim moved to PgNumbackendsAndSampledMsRungTests with the top. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion, "V132 is expected to sit below the ladder's top now that V133 has landed");
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// One nullable, default-less <c>ADD COLUMN IF NOT EXISTS</c> on the one table, schema-qualified, in the type
    /// the collector declares (rendered by the generator's own mapping so this pin cannot disagree with
    /// <c>PgSchemaGeneratorTests</c> about what "the type" is); the passthrough refreshed for the V14 reason; no
    /// backfill, no index, no table, no data movement; and the rung doc carrying the argument.
    /// </summary>
    [Fact]
    public void TheRungAddsOneNullableIntegerColumn_RefreshesThePassthrough_AndNothingElse()
    {
        var sql = V132.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        var declared = PerfmonStatsCollector.Instance.PayloadColumns[^1];
        Assert.Equal(Column, declared.Name);
        var rendered = PgSchemaGenerator.TypeFor(declared);
        Assert.Equal("integer", rendered);

        Assert.Contains($"ALTER TABLE collect.{Table}\n    ADD COLUMN IF NOT EXISTS {Column} {rendered};", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "ALTER TABLE"));
        Assert.Single(Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS"));
        Assert.Contains($"CREATE OR REPLACE VIEW collect.v_{Table} AS SELECT * FROM collect.{Table};", sql, StringComparison.Ordinal);
        /* One view statement (the rung's SQL comment quotes the idiom's name, so the count is on the statement form). */
        Assert.Single(Regex.Matches(sql, "CREATE OR REPLACE VIEW collect\\."));

        Assert.DoesNotContain("DEFAULT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DELETE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TABLE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DROP ", sql, StringComparison.Ordinal);

        /* The view IS a passthrough — refreshing it with SELECT * is the right idiom (a resolving view would need
           its definition re-emitted, the V51/V121/V128 shape). */
        Assert.Contains($"v_{Table}", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain($"v_{Table}", PgSchemaGenerator.PayloadResolvingViews);

        /* A fresh store gets the column from the generated CREATE TABLE, LAST — the positional COPY writer and an
           upgraded store's ALTER agree on where it sits. */
        var generated = PgSchemaGenerator.CreateTable(PerfmonStatsCollector.Instance);
        var lines = generated.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0).ToList();
        var closing = lines.FindIndex(l => l.StartsWith(")", StringComparison.Ordinal));
        Assert.True(closing > 0, generated);
        Assert.Equal($"{Column} {rendered}", lines[closing - 1]);
        Assert.StartsWith("sample_interval_seconds ", lines[closing - 2], StringComparison.Ordinal);
        Assert.EndsWith($", {Column}) FROM STDIN (FORMAT BINARY)", PgCollectorRowWriter.CopyCommandFor(PerfmonStatsCollector.Instance), StringComparison.Ordinal);

        /* The rung doc carries the argument in the words the next reader will look for. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var start = source.IndexOf("/// V132 —", StringComparison.Ordinal);
        var end = source.IndexOf("private const string V132Sql", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the V132 rung doc is missing or sits after its constant");
        var doc = source[start..end];
        foreach (var phrase in new[]
        {
            "#3653", "rule 8", "FALLING level", "counter reset", "Nullable, no DEFAULT, no backfill",
            "compressed-chunk shape", "compressed hypertable", "passthrough view is refreshed", "PERF_AVERAGE_BULK",
            "PerformanceMonitor.Common.PerfmonCounterTypes", "PerfmonCounterTypeRungTests",
        })
        {
            Assert.Contains(phrase, doc, StringComparison.Ordinal);
        }

        /* No table, no collector: the censuses did not move. */
        /* 72 since V136 (#3691) added pg_database_size_stats; this rung itself added none. Restated as the current
           census figure rather than as "unchanged from before", which is the claim the line makes. */
        Assert.Equal(72, TimescaleSupport.HypertableCount);
        Assert.Equal(7, PerfmonStatsCollector.Instance.PayloadColumns.Count);
    }

    /* ---- the probe (three sites, one rung behind the top) -------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map has an arm for it one rung behind
    /// the top. The probe asks the question, the caller reads the answer, the map has the parameter — a sentinel
    /// present at only some of them shifts every LATER ordinal onto the wrong column, and a missing arm maps a
    /// store that stopped here one rung short.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndAFullyMigratedStoreMapsToTheLaddersTop()
    {
        Assert.Contains(
            $"table_name = '{Table}'\n                                                     AND   column_name = '{Column}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPerfmonCounterType", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* This rung's sentinel sits strictly BELOW the last argument now that V133 has appended its own; the
           "is the last argument" claim moved to PgNumbackendsAndSampledMsRungTests with the top. */
        Assert.True(ProbeOrdinal < arity - 1, "V132's sentinel is expected to sit below the top rung's now that V133 has landed");

        /* Every sentinel true = a fully-migrated store, which must map to exactly the ladder's top. Stated
           against StorageVersion rather than this rung's number, so it survives every later rung. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns THIS rung's version. */
        var thisArm = viewer.IndexOf("if (hasPerfmonCounterType)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf(PreviousArmSource, StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V132 sentinel arm — a store that stopped here would map to 131");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V132 arm sits below the previous rung's, so a store that stopped here maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table is named in the probe line and nowhere in the arm's prose. */
        Assert.DoesNotContain(Table, viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>The previous rung's arm as the viewer spells it — V131's sentinel.</summary>
    private const string PreviousArmSource = "if (hasNotificationRoutes)";

    /* ---- the readers ----------------------------------------------------------------------------------- */

    /// <summary>
    /// Every Darling perfmon read selects the type: the two trend reads (MCP and viewer) with the one agreed-type
    /// expression, byte-identical to each other and to every copy in Lite's trend reads, and the latest-snapshot read
    /// with the row's own. The row types keep NULL as null — a gauge row stores no delta and no interval, and a 0
    /// manufactured in their place would be #3642's fabricated zero on a level that has no delta.
    /// </summary>
    [Fact]
    public void EveryDarlingPerfmonRead_SelectsTheType_AndTheRowsKeepNullAsNull()
    {
        var mcpTrend = DarlingTrendReader.PerfmonTrendSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var viewerTrend = ViewerDataService.PerfmonTrendsSql(1).Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains(AgreedTypeExpression, mcpTrend, StringComparison.Ordinal);
        Assert.Contains(AgreedTypeExpression, viewerTrend, StringComparison.Ordinal);

        var lite = RepoFile.ReadRepoFile("Lite", "Services", "LocalDataService.Perfmon.cs");
        /* Lite's single-counter read holds it once; its bucketed batched read (#4234) holds it twice, per collection
           inside and per bucket outside, as both Darling twins do. */
        Assert.Equal(3, Regex.Matches(lite, Regex.Escape(AgreedTypeExpression)).Count);

        var latest = DarlingDataReader.LatestPerfmonStatsSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("collection_time,\n    cntr_type\nFROM v_perfmon_stats", latest, StringComparison.Ordinal);
        Assert.Contains("collection_time,\n    cntr_type\nFROM v_perfmon_stats", lite.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        /* The SUMs and the MAX are as they were — the type rides beside them, it does not change them. */
        foreach (var sql in new[] { mcpTrend, viewerTrend })
        {
            Assert.Contains("CAST(SUM(cntr_value) AS bigint) AS cntr_value", sql, StringComparison.Ordinal);
            Assert.Contains("CAST(SUM(delta_cntr_value) AS bigint) AS delta_cntr_value", sql, StringComparison.Ordinal);
            Assert.Contains("CAST(MAX(sample_interval_seconds) AS bigint) AS sample_interval_seconds", sql, StringComparison.Ordinal);
        }

        /* The row shapes: nullable where a gauge stores nothing, the type nullable where the row predates it. */
        Assert.Equal(typeof(long?), typeof(DarlingTrendReader.PerfmonTrendPoint).GetProperty("DeltaValue")!.PropertyType);
        Assert.Equal(typeof(long?), typeof(DarlingTrendReader.PerfmonTrendPoint).GetProperty("SampleIntervalSeconds")!.PropertyType);
        Assert.Equal(typeof(int?), typeof(DarlingTrendReader.PerfmonTrendPoint).GetProperty("CntrType")!.PropertyType);
        Assert.Equal(typeof(long?), typeof(DarlingDataReader.PerfmonRow).GetProperty("DeltaValue")!.PropertyType);
        Assert.Equal(typeof(int?), typeof(DarlingDataReader.PerfmonRow).GetProperty("CntrType")!.PropertyType);
        Assert.Equal(typeof(long?), typeof(PerfmonTrendPoint).GetProperty("DeltaValue")!.PropertyType);
        Assert.Equal(typeof(int?), typeof(PerfmonTrendPoint).GetProperty("CntrType")!.PropertyType);

        /* The three reader bodies, scoped to the perfmon method each file holds (the files hold other readers
           whose delta columns are non-nullable and coerce legitimately): the delta ordinal is read null-or-value. */
        foreach (var (file, method, nullRead, coercion) in new[]
        {
            ("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingTrendReader.cs", "GetPerfmonTrendAsync(", "reader.IsDBNull(2) ? null : reader.GetInt64(2)", "reader.IsDBNull(2) ? 0 : reader.GetInt64(2)"),
            ("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingDataReader.cs", "GetLatestPerfmonStatsAsync(", "reader.IsDBNull(3) ? null : reader.GetInt64(3)", "reader.IsDBNull(3) ? 0 : reader.GetInt64(3)"),
            ("Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.Perfmon.cs", "GetPerfmonTrendsByCountersAsync(", "reader.IsDBNull(3) ? null : reader.GetInt64(3)", "reader.IsDBNull(3) ? 0 : reader.GetInt64(3)"),
        })
        {
            var text = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(file.Split('/')));
            var head = text.IndexOf(method, StringComparison.Ordinal);
            Assert.True(head >= 0, $"{file}: {method} not found");
            var body = CSharpSourceWalker.BraceBalanced(text, text.IndexOf('{', head));
            Assert.Contains(nullRead, body, StringComparison.Ordinal);
            Assert.DoesNotContain(coercion, body, StringComparison.Ordinal);
        }
    }

    /// <summary>Both viewers' chart bodies take the series' type from the trend rows and hand it to the shaping
    /// helper with the counter's name — the type decides, the proxy is the fallback — and hand the raw value
    /// through so a gauge can plot as its level. The chart bodies are WPF-bound and cannot run here; the shaping
    /// itself is pinned value-by-value in <c>Lite.Tests/DeltaSeriesShapingTests</c>.</summary>
    [Theory]
    [InlineData("Darling", "Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Perfmon.cs")]
    [InlineData("Lite", "Lite/Controls/ServerTab.Pickers.cs")]
    public void BothPerfmonCharts_ClassifyByTheStoredType_WithTheProxyAsTheFallback(string sku, string relativePath)
    {
        var raw = RepoFile.ReadRepoFile(relativePath.Split('/'));
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);
        var head = stripped.IndexOf("Task UpdatePerfmonChartFromPickerAsync()", StringComparison.Ordinal);
        Assert.True(head >= 0, $"{sku}: the UpdatePerfmonChartFromPickerAsync declaration was not found in {relativePath}");
        var body = CSharpSourceWalker.BraceBalanced(stripped, stripped.IndexOf('{', head));

        Assert.Contains("var seriesType = trend.Select(t => t.CntrType).LastOrDefault(t => t.HasValue);", body, StringComparison.Ordinal);
        Assert.Contains("DeltaSeriesShaping.BasisFor(counterName, seriesType)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("DeltaSeriesShaping.BasisFor(counterName)", body, StringComparison.Ordinal);
        Assert.Contains("new DeltaSample(t.CollectionTime, t.DeltaValue, t.SampleIntervalSeconds, t.Value)", body, StringComparison.Ordinal);
    }

    /// <summary>The Compose catalog no longer claims the type is not stored, and explains why it compiles no row
    /// guard on it (the write path makes one redundant for new rows and no predicate can separate old ones).</summary>
    [Fact]
    public void TheComposeCatalog_StatesWhyThereIsNoTypeGuard()
    {
        var catalog = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Compose", "MeasureCatalog.cs");
        Assert.DoesNotContain("cntr_type is not stored", catalog, StringComparison.Ordinal);
        Assert.Contains("No cntr_type row guard is compiled here, on purpose", catalog, StringComparison.Ordinal);
        Assert.Contains("perfmon_value_delta over a gauge aggregates only NULLs", catalog, StringComparison.Ordinal);
    }
}

/// <summary>
/// The live half of V132 (#3653 A7), gated on <c>DARLING_TEST_PG</c>: the real collector writes a gauge row and a
/// rate row through the real binary COPY writer against the migrated store; the viewer trend read, the MCP trend
/// read, the latest-snapshot read and both MCP tools read them back beside a planted pre-rung row and a planted
/// mixed-type family; and the shaping helper turns the gauge's rows into levels. Serialized against every other
/// live class because it shares the store.
/// </summary>
[Collection("live-postgres")]
public sealed class PerfmonCounterTypeLivePostgresTests
{
    private const int ServerId = -132132;
    private const string ServerName = "perfmon-cntr-type-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheCollectorWritesAGaugeAsItsLevel_AndEveryReaderClassifiesByTheStoredType_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the perfmon counter-type round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* The column is what the rung says it is. */
            using (var describe = new NpgsqlCommand(
                "SELECT data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'perfmon_stats' AND column_name = 'cntr_type'", connection))
            using (var reader = await describe.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct), "perfmon_stats has no cntr_type column after MigrateAsync");
                Assert.Equal("integer", reader.GetString(0));
                Assert.Equal("YES", reader.GetString(1));
                Assert.True(reader.IsDBNull(2));
            }

            var t1 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-15);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            /* t1 and t2 through the REAL writer: the collector's WritePayload over the binary COPY, with a real
               delta calculator, exactly as DarlingCollectorRunner drives it. The gauge FALLS between the two
               passes — the case that used to read as a counter reset. */
            var deltas = new DarlingDeltaCalculator();
            await WriteThroughTheCollectorAsync(connection, deltas, t1, new[]
            {
                new PerfmonStatsCollector.Row("SQLServer:Memory Manager", "Total Server Memory (KB)", "", 8_388_608, PerfmonCounterTypes.PerfCounterLargeRawCount),
                new PerfmonStatsCollector.Row("SQLServer:SQL Statistics", "Batch Requests/sec", "", 5_000, PerfmonCounterTypes.PerfCounterBulkCount),
            }, ct);
            await WriteThroughTheCollectorAsync(connection, deltas, t2, new[]
            {
                new PerfmonStatsCollector.Row("SQLServer:Memory Manager", "Total Server Memory (KB)", "", 8_000_000, PerfmonCounterTypes.PerfCounterLargeRawCount),
                new PerfmonStatsCollector.Row("SQLServer:SQL Statistics", "Batch Requests/sec", "", 5_900, PerfmonCounterTypes.PerfCounterBulkCount),
            }, ct);

            /* A pre-rung row (NULL type, a stored delta) and a mixed-type family (two instances, a rate and a
               gauge), planted directly — the shapes the writer no longer produces but the store holds. */
            await PlantAsync(connection, ct, t2, "SQLServer:General Statistics", "Legacy Counter", "", 120, 20, 300, null);
            await PlantAsync(connection, ct, t3, "SQLServer:Wait Statistics", "Lock waits", "Waits started per second", 700, 70, 300, PerfmonCounterTypes.PerfCounterBulkCount);
            await PlantAsync(connection, ct, t3, "SQLServer:Wait Statistics", "Lock waits", "Waits in progress", 3, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);

            /* What the writer stored. */
            using (var stored = new NpgsqlCommand(
                "SELECT counter_name, collection_time, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type FROM perfmon_stats WHERE server_id = $1 AND collection_time <= $2 ORDER BY counter_name, collection_time", connection))
            {
                stored.Parameters.AddWithValue(ServerId);
                stored.Parameters.AddWithValue(DarlingMcpTestData.Naive(t2));
                using var reader = await stored.ExecuteReaderAsync(ct);
                var rows = new List<(string Counter, long Value, long? Delta, int? Interval, int? Type)>();
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.GetString(0), reader.GetInt64(2),
                        reader.IsDBNull(3) ? null : reader.GetInt64(3),
                        reader.IsDBNull(4) ? null : reader.GetInt32(4),
                        reader.IsDBNull(5) ? null : reader.GetInt32(5)));
                }

                Assert.Equal(new (string, long, long?, int?, int?)[]
                {
                    ("Batch Requests/sec", 5_000, 0, 0, PerfmonCounterTypes.PerfCounterBulkCount),          // first sighting: the (0, 0) marker
                    ("Batch Requests/sec", 5_900, 900, 300, PerfmonCounterTypes.PerfCounterBulkCount),      // delta 900 over the measured 300 s
                    ("Legacy Counter", 120, 20, 300, null),
                    ("Total Server Memory (KB)", 8_388_608, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount),   // a level: no delta, no interval
                    ("Total Server Memory (KB)", 8_000_000, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount),   // it FELL, and that is the reading
                }, rows);
            }

            /* The viewer's batched trend read and the chart's classification. */
            await using (var viewer = new ViewerDataService(cs!))
            {
                var trends = await viewer.GetPerfmonTrendsByCountersAsync(ServerId,
                    new List<string> { "Total Server Memory (KB)", "Batch Requests/sec", "Legacy Counter", "Lock waits" }, t1.AddMinutes(-1), t3.AddMinutes(1), ct);

                var gauge = trends["Total Server Memory (KB)"];
                Assert.Equal(2, gauge.Count);
                Assert.All(gauge, p => Assert.Equal(PerfmonCounterTypes.PerfCounterLargeRawCount, p.CntrType));
                Assert.All(gauge, p => Assert.Null(p.DeltaValue));
                Assert.All(gauge, p => Assert.Null(p.SampleIntervalSeconds));
                var basis = DeltaSeriesShaping.BasisFor("Total Server Memory (KB)", gauge.Select(p => p.CntrType).LastOrDefault(t => t.HasValue));
                Assert.Equal(DeltaBasis.Level, basis);
                Assert.Equal(new[] { 8_388_608.0, 8_000_000.0 },
                    DeltaSeriesShaping.Shape(gauge.Select(p => new DeltaSample(p.CollectionTime, p.DeltaValue, p.SampleIntervalSeconds, p.Value)).ToList(), basis));

                var rate = trends["Batch Requests/sec"];
                var rateBasis = DeltaSeriesShaping.BasisFor("Batch Requests/sec", rate.Select(p => p.CntrType).LastOrDefault(t => t.HasValue));
                Assert.Equal(DeltaBasis.PerSecond, rateBasis);
                var ys = DeltaSeriesShaping.Shape(rate.Select(p => new DeltaSample(p.CollectionTime, p.DeltaValue, p.SampleIntervalSeconds, p.Value)).ToList(), rateBasis);
                Assert.True(double.IsNaN(ys[0]));   // the first-sighting marker is a line break, not a 0
                Assert.Equal(3.0, ys[1]);           // 900 / 300

                Assert.Null(Assert.Single(trends["Legacy Counter"]).CntrType);
                var mixed = Assert.Single(trends["Lock waits"]);
                Assert.Null(mixed.CntrType);
                Assert.Equal(70, mixed.DeltaValue);
                Assert.Equal(703, mixed.Value);
            }

            /* The MCP trend read and the two tools. */
            var mcpGauge = await DarlingTrendReader.GetPerfmonTrendAsync(postgres, ServerId, "Total Server Memory (KB)", t1.AddMinutes(-1), t3.AddMinutes(1), ct);
            Assert.Equal(2, mcpGauge.Count);
            Assert.All(mcpGauge, p => Assert.Equal(PerfmonCounterTypes.PerfCounterLargeRawCount, p.CntrType));
            Assert.All(mcpGauge, p => Assert.Null(p.DeltaValue));
            Assert.All(mcpGauge, p => Assert.Null(p.SampleIntervalSeconds));

            /* #3960: bucket_minutes: 1 pins per-collection granularity, since this test is about TYPE
               classification, not about bucketing — t1/t3 are 10 minutes apart and the default 24h window's
               auto-sized bucket (10 minutes) could otherwise fold them into one point depending on wall-clock
               alignment against TrendBuckets.OriginSql. */
            using (var trend = JsonDocument.Parse(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Total Server Memory (KB)", ServerName, bucket_minutes: 1)))
            {
                Assert.Equal("gauge", trend.RootElement.GetProperty("counter_kind").GetString());
                Assert.Equal(PerfmonCounterTypes.PerfCounterLargeRawCount, trend.RootElement.GetProperty("cntr_type").GetInt32());
                var points = trend.RootElement.GetProperty("trend").EnumerateArray().ToList();
                Assert.Equal(2, points.Count);
                Assert.All(points, p => Assert.Equal(JsonValueKind.Null, p.GetProperty("delta_value").ValueKind));
                Assert.All(points, p => Assert.Equal(JsonValueKind.Null, p.GetProperty("sample_interval_seconds").ValueKind));
                Assert.Equal(8_000_000, points[1].GetProperty("value").GetInt64());
            }

            using (var trend = JsonDocument.Parse(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Batch Requests/sec", ServerName, bucket_minutes: 1)))
            {
                Assert.Equal("rate", trend.RootElement.GetProperty("counter_kind").GetString());
                var points = trend.RootElement.GetProperty("trend").EnumerateArray().ToList();
                Assert.Equal(0, points[0].GetProperty("sample_interval_seconds").GetInt64());
                Assert.Equal(900, points[1].GetProperty("delta_value").GetInt64());
                Assert.Equal(300, points[1].GetProperty("sample_interval_seconds").GetInt64());
            }

            using (var trend = JsonDocument.Parse(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Lock waits", ServerName, bucket_minutes: 1)))
            {
                Assert.Equal(JsonValueKind.Null, trend.RootElement.GetProperty("counter_kind").ValueKind);
            }

            using (var stats = JsonDocument.Parse(await DarlingMcpDataTools.GetPerfmonStats(postgres, ServerName)))
            {
                /* The latest snapshot is t3's, which holds only the mixed family — two rows, each with its OWN type. */
                var counters = stats.RootElement.GetProperty("counters").EnumerateArray().ToList();
                Assert.Equal(2, counters.Count);
                var started = counters.Single(c => c.GetProperty("instance_name").GetString() == "Waits started per second");
                Assert.Equal("rate", started.GetProperty("counter_kind").GetString());
                Assert.Equal(70, started.GetProperty("delta_value").GetInt64());
                var inProgress = counters.Single(c => c.GetProperty("instance_name").GetString() == "Waits in progress");
                Assert.Equal("gauge", inProgress.GetProperty("counter_kind").GetString());
                Assert.Equal(JsonValueKind.Null, inProgress.GetProperty("delta_value").ValueKind);
                Assert.Equal(3, inProgress.GetProperty("value").GetInt64());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>DarlingCollectorRunner's COPY loop, verbatim in shape: prefix columns through the writer, then
    /// BeginPayload / WritePayload / EndPayload per row, so the collector's positional contract is exercised
    /// against the real table.</summary>
    private static async Task WriteThroughTheCollectorAsync(NpgsqlConnection connection, DarlingDeltaCalculator deltas, DateTime collectionTime,
        IReadOnlyList<PerfmonStatsCollector.Row> rows, System.Threading.CancellationToken ct)
    {
        var definition = PerfmonStatsCollector.Instance;
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName, CollectionTime = collectionTime, Deltas = deltas };
        var writer = new PgCollectorRowWriter();

        using var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        foreach (var row in rows)
        {
            await importer.StartRowAsync(ct);
            writer.Value(CollectionIdGenerator.Next());
            writer.Value(DarlingMcpTestData.Naive(collectionTime)).Value(ServerId).Value(ServerName);
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
        }

        await importer.CompleteAsync(ct);
    }

    private static Task PlantAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at,
        string objectName, string counterName, string instanceName, long cntr, long? delta, int? interval, int? type) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, objectName, counterName, instanceName, cntr,
            delta, interval, type);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM perfmon_stats WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(ServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
