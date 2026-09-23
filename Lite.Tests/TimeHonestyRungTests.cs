/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite v63 / Darling V134 (#3653 item 13, rulings Q7 + Q8 — the "time honesty" rung), the Lite half.
/// <c>cpu_utilization_stats.sample_time</c> is the monitored server's LOCAL wall clock in a store whose every
/// other timestamp is naive UTC, so this side's CPU window used to be a UTC question answered by shifting the
/// bounds by the ONE <c>utc_offset_minutes</c> the store holds now — exact until a DST transition put a sample
/// on the other side of the offset, then an hour wrong, silently. The collector now writes the same instant in
/// UTC beside the local stamp and the window prefers it; the projection keeps the local stamp, because the
/// chart plots the server's own frame. <c>server_properties.utc_offset_minutes</c> is an OFFSET, the one in
/// force at collection, and cannot say which side of a transition an instant fell on; the engine's zone id
/// now sits beside it, NULL where the engine cannot say (every SQL Server before 2022), and
/// <c>get_server_properties</c> publishes the pair and what a NULL zone means.
///
/// <para>The collector shapes (the UTC twin projected last on every arm off <c>SYSUTCDATETIME()</c>, the zone
/// read in its own gated dynamic batch) are pinned in <c>CpuUtilizationCollectorDefinitionTests</c> and
/// <c>ServerPropertiesCollectorDefinitionTests</c>; the Darling side in <c>Darling.Tests/TimeHonestyRungTests</c>.
/// What is here is the DuckDB ladder, the generator, and the Lite reads end to end on a real DuckDB — and,
/// since #3778, the collector's WATERMARK read on this side: the runner reads <c>MAX(sample_time_utc)</c> and
/// <c>MAX(sample_time)</c> in one round trip for the one definition that declares a UTC twin, hands the
/// collector the twin's value where the store has one and the local stamp otherwise, and says which, so the
/// ring-buffer dedup compares in the watermark's frame and the autumn fall-back hour lands
/// (<see cref="WatermarkFrameReadTests"/>; the dedup itself is pinned in <c>CpuUtilizationCollectorDefinitionTests</c>).</para>
/// </summary>
public sealed class TimeHonestyRungTests
{
    /// <summary>The v63 block: both ALTERs, the log line, and the argument in the words the next reader will
    /// look for.</summary>
    [Fact]
    public void TheLiteMigration_StoresBothColumns_AtSchemaVersion63()
    {
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 63);

        var source = Lite.Tests.ParitySource.ReadFile("Lite/Database/DuckDbInitializer.cs");
        var start = source.IndexOf("if (fromVersion < 63)", StringComparison.Ordinal);
        Assert.True(start >= 0, "DuckDbInitializer has no v63 block");
        var block = source[start..];

        Assert.Contains("(\"cpu_utilization_stats\", \"sample_time_utc\", \"TIMESTAMP\")", block, StringComparison.Ordinal);
        Assert.Contains("(\"server_properties\", \"time_zone_id\", \"VARCHAR\")", block, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS {column} {type}", block, StringComparison.Ordinal);
        Assert.Contains("Running migration to v63", block, StringComparison.Ordinal);
        foreach (var phrase in new[]
        {
            "#3653 item 13", "LOCAL wall clock", "watermark", "GetTimeRangeServerLocal", "DST transition",
            "SYSUTCDATETIME()", "COALESCE(sample_time_utc, sample_time - the offset)", "the chart wants the server's frame",
            "CURRENT_TIMEZONE_ID()", "missing BUILT-IN", "only the offset is known", "Both appended at the end",
            "Nothing to backfill", "REQUIRED on this side",
        })
        {
            Assert.Contains(phrase, block, StringComparison.Ordinal);
        }
    }

    /// <summary>The DuckDB generator carries each column into a fresh store's DDL as the trailing nullable
    /// column — the same shape the v63 ALTER gives an upgraded store, so fresh and upgraded databases agree —
    /// and each collector declares it last, which is what makes the appender's positional write land in it.</summary>
    [Theory]
    [InlineData("cpu_utilization", "cpu_utilization_stats", "sample_time_utc", "TIMESTAMP", "other_process_cpu_utilization INTEGER")]
    [InlineData("server_properties", "server_properties", "time_zone_id", "VARCHAR", "utc_offset_minutes INTEGER")]
    public void TheDuckDbGenerator_EmitsTheColumnAsTheTrailingNullableColumn(string collector, string table, string column, string type, string expectedBefore)
    {
        var definition = CollectorCatalog.Find(collector);
        Assert.NotNull(definition);
        Assert.Equal(table, definition!.TargetTable);
        Assert.Equal(column, definition.PayloadColumns[^1].Name);

        var ddl = DuckDbSchemaGenerator.CreateTable(definition);
        var lines = ddl.Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0).ToList();
        var closing = lines.FindLastIndex(l => l.StartsWith(")", StringComparison.Ordinal));
        Assert.True(closing > 0, ddl);
        Assert.Equal($"{column} {type}", lines[closing - 1]);
        Assert.Equal(expectedBefore, lines[closing - 2]);
    }

    /// <summary>The MCP descriptions on BOTH SKUs say what a caller must know to read the clock pair — that the
    /// offset is the one in force at the snapshot, that the zone is 2022+ / Azure only, that a null zone means
    /// only the offset is known — in the same words, and both tool bodies emit the same three keys.</summary>
    [Fact]
    public void BothMcpServerPropertiesTools_PublishTheClockPair_InTheSameWords()
    {
        var lite = Lite.Tests.ParitySource.ReadFile("Lite/Mcp/McpServerInfoTools.cs");
        var darling = Lite.Tests.ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs");

        foreach (var phrase in new[]
        {
            "utc_offset_minutes is the UTC offset in force when the snapshot was collected",
            "time_zone_id is the engine's own time-zone name (CURRENT_TIMEZONE_ID(), SQL Server 2022+ and Azure SQL only)",
            "a null time_zone_id means a pre-2022 engine, where only the offset is known",
            "utc_offset_minutes = row.UtcOffsetMinutes,",
            "time_zone_id = string.IsNullOrEmpty(row.TimeZoneId) ? null : row.TimeZoneId,",
            "time_zone_id is null: a pre-2022 engine (CURRENT_TIMEZONE_ID() is SQL Server 2022+ / Azure SQL only), so only the offset in force at captured_at is known.",
            "time_zone_id is the engine's own zone (CURRENT_TIMEZONE_ID()); utc_offset_minutes is the offset that zone had at captured_at.",
        })
        {
            Assert.Contains(phrase, lite, StringComparison.Ordinal);
            Assert.Contains(phrase, darling, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #3744, the Lite half of the CPU alert gate's identity: the overview read projects the UTC twin BESIDE the
    /// local stamp (trailing, so the pre-#3744 ordinals read what they always read), still orders on the local
    /// stamp (every row has it; a cross-row order on the twin or a COALESCE would compare a pre-rung local stamp
    /// against a post-rung UTC one), carries both onto <c>ServerSummaryItem</c> under names that say which clock
    /// each is, and <c>MainWindow.AlertEngine</c> folds them <c>twin ?? local</c> into the snapshot — the same rule
    /// Darling's <c>ReadLatestCpuAsync</c> applies, pinned there by <c>Darling.Tests/TimeHonestyRungTests</c>. Source
    /// pins because the snapshot construction sits inside a WPF window.
    /// </summary>
    [Fact]
    public void TheOverviewRead_ProjectsTheTwinBesideTheLocalStamp_AndTheSnapshotFoldsThem()
    {
        var overview = Lite.Tests.ParitySource.ReadFile("Lite/Services/LocalDataService.Overview.cs");
        /* #3895: the relation is the hot table first, then the archive view — the same statement over each. */
        Assert.Contains(
            "SELECT sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time, collection_time, sample_time_utc\nFROM \" + relation + @\"",
            overview.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains("foreach (var relation in NewestFirstRelations(\"cpu_utilization_stats\"))", overview, StringComparison.Ordinal);
        Assert.Contains("ORDER BY sample_time DESC\nLIMIT 1", overview.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY sample_time_utc", overview, StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY COALESCE", overview, StringComparison.Ordinal);
        Assert.Contains("cpuSampleTimeUtc = reader.IsDBNull(4) ? null : reader.GetDateTime(4);", overview, StringComparison.Ordinal);
        Assert.Contains("CpuSampleTimeUtc = cpuSampleTimeUtc,", overview, StringComparison.Ordinal);
        Assert.Contains("#3744", overview, StringComparison.Ordinal);

        Assert.Equal(typeof(DateTime?), typeof(ServerSummaryItem).GetProperty("CpuSampleTime")!.PropertyType);
        Assert.Equal(typeof(DateTime?), typeof(ServerSummaryItem).GetProperty("CpuSampleTimeUtc")!.PropertyType);

        var window = Lite.Tests.ParitySource.ReadFile("Lite/MainWindow.AlertEngine.cs");
        Assert.Contains("CpuSampleTimeUtc: summary.CpuSampleTimeUtc ?? summary.CpuSampleTime);", window, StringComparison.Ordinal);
        Assert.Contains("#3744", window, StringComparison.Ordinal);

        /* And the shared gate's side of the contract, in the words both hosts rely on: identity by EQUALITY. */
        var engine = Lite.Tests.ParitySource.ReadFile("PerformanceMonitor.Alerting/AlertEngine.cs");
        Assert.Contains("|| sampleUtc.Value != priorRecord.LastObservedSampleUtc.Value;", engine, StringComparison.Ordinal);
        Assert.DoesNotContain("|| sampleUtc.Value > priorRecord.LastObservedSampleUtc.Value;", engine, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3778, the Lite runner's half: the watermark branch routes a definition with NO <c>UtcWatermarkColumn</c>
    /// through the unchanged <c>GetLastCollectedTimeAsync</c> — whose SQL is the byte-identical string it was, so
    /// every collector but <c>cpu_utilization</c> reads exactly what it read — and only a definition WITH one
    /// through the pair read, whose SQL puts the twin's maximum first and the declared column's second, one
    /// statement, one scan; and the frame the pair read returns rides onto the context as
    /// <c>WatermarkFromUtcColumn</c>. Source pins because the runner's watermark read sits inside a method that
    /// needs a live monitored server; the reads themselves run against a real DuckDB in
    /// <see cref="WatermarkFrameReadTests"/>.
    /// </summary>
    [Fact]
    public void TheRunner_ReadsTheWatermarkPair_OnlyForADefinitionWithAUtcTwin_AndBothReadsSpellTheirSql()
    {
        var runner = Lite.Tests.ParitySource.ReadFile("Lite/Services/RemoteCollectorService.DefinitionRunner.cs").Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains(
            "        else if (definition.UtcWatermarkColumn is null)\n" +
            "        {\n" +
            "            watermark = await GetLastCollectedTimeAsync(serverId, definition.TargetTable, definition.WatermarkColumn, cancellationToken);\n" +
            "        }",
            runner, StringComparison.Ordinal);
        Assert.Contains(
            "            (watermark, watermarkFromUtcColumn) = await GetLastCollectedTimeWithFrameAsync(\n" +
            "                serverId, definition.TargetTable, definition.WatermarkColumn, definition.UtcWatermarkColumn, cancellationToken);",
            runner, StringComparison.Ordinal);
        Assert.Contains("            WatermarkFromUtcColumn = watermarkFromUtcColumn,", runner, StringComparison.Ordinal);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(runner, "GetLastCollectedTimeWithFrameAsync\\("));

        var service = Lite.Tests.ParitySource.ReadFile("Lite/Services/RemoteCollectorService.cs");
        Assert.Contains("cmd.CommandText = $\"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1\";", service, StringComparison.Ordinal);
        Assert.Contains("cmd.CommandText = $\"SELECT MAX({utcColumnName}), MAX({columnName}) FROM {tableName} WHERE server_id = $1\";", service, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3778: Lite's watermark pair read against a real DuckDB through the real <c>RemoteCollectorService</c>
/// method (exposed the way <c>CollectorStateStoreTests</c> exposes the state store), beside the unchanged plain
/// read on the same table. What a store looks like on the morning of the upgrade (pre-rung rows only: the local
/// maximum, frame LOCAL), after the first post-upgrade run (any twin: the twin's maximum, frame UTC), and in
/// the shape that makes the frame matter — an upgrade INSIDE the repeated hour, where a pre-rung row's local
/// stamp exceeds every post-rung row's local stamp, so the plain read's answer and the pair read's answer are
/// different rows in different frames. Scoped per server, because the flag flips per server.
/// </summary>
public sealed class WatermarkFrameReadTests : IClassFixture<SharedDuckDbFixture>
{
    private const string Table = "cpu_utilization_stats";
    private const string Local = "sample_time";
    private const string Utc = "sample_time_utc";

    private readonly DuckDbInitializer _duckDb;
    private readonly WatermarkReads _reads;
    private long _nextId = -3778;

    public WatermarkFrameReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _reads = new WatermarkReads(_duckDb);
    }

    /// <summary>Exposes the runner's two protected watermark reads; only <c>_duckDb</c> is exercised.</summary>
    private sealed class WatermarkReads(DuckDbInitializer duckDb)
        : RemoteCollectorService(duckDb, serverManager: null!, scheduleManager: null!)
    {
        public Task<DateTime?> PlainAsync(int serverId) =>
            GetLastCollectedTimeAsync(serverId, Table, Local, CancellationToken.None);

        public Task<(DateTime? Value, bool FromUtcColumn)> PairAsync(int serverId) =>
            GetLastCollectedTimeWithFrameAsync(serverId, Table, Local, Utc, CancellationToken.None);
    }

    [Fact]
    public async Task AnEmptyStore_ReadsTheFirstRunPair_AndThePlainReadReadsNull()
    {
        Assert.Equal(((DateTime?)null, false), await _reads.PairAsync(1));
        Assert.Null(await _reads.PlainAsync(1));
    }

    [Fact]
    public async Task PreRungRowsOnly_ReadTheLocalMaximum_InTheLocalFrame_SameAsThePlainRead()
    {
        /* The morning of the upgrade at UTC-4: three pre-rung rows, twin NULL. */
        var utc = new DateTime(2026, 7, 1, 16, 0, 0);
        await SeedAsync(1, utc.AddMinutes(-2).AddHours(-4), null);
        await SeedAsync(1, utc.AddMinutes(-1).AddHours(-4), null);
        await SeedAsync(1, utc.AddHours(-4), null);

        Assert.Equal(((DateTime?)utc.AddHours(-4), false), await _reads.PairAsync(1));
        Assert.Equal(utc.AddHours(-4), await _reads.PlainAsync(1));
    }

    [Fact]
    public async Task OnePostRungRow_FlipsTheFrameToUtc_EvenWhenAPreRungLocalStampIsLater_AndOnlyForItsServer()
    {
        /* America/New_York, upgraded INSIDE the repeated hour: the last pre-rung row is 01:59 EDT (05:59 UTC),
           the first post-rung row is 01:05 EST (06:05 UTC). By local stamp the pre-rung row is the newer one;
           by instant it is fifty-four minutes older. */
        var transitionUtc = new DateTime(2026, 11, 1, 6, 0, 0);
        var preRungLocal = transitionUtc.AddMinutes(-1).AddHours(-4);   /* 01:59 */
        var postRungUtc = transitionUtc.AddMinutes(5);                  /* 06:05 */
        var postRungLocal = postRungUtc.AddHours(-5);                   /* 01:05 */
        Assert.True(preRungLocal > postRungLocal, "the fixture must put the pre-rung LOCAL stamp above the post-rung one, or the two reads agree by accident");

        await SeedAsync(1, preRungLocal, null);
        await SeedAsync(1, postRungLocal, postRungUtc);

        /* A different server still on pre-rung rows: its frame does not flip because server 1's did. */
        await SeedAsync(2, preRungLocal, null);

        /* The pair read takes the twin and says so; the plain read, unchanged, still answers the local maximum —
           the pre-rung row — which under the old rule was the watermark every EST-side sample sat below. */
        Assert.Equal(((DateTime?)postRungUtc, true), await _reads.PairAsync(1));
        Assert.Equal(preRungLocal, await _reads.PlainAsync(1));

        Assert.Equal(((DateTime?)preRungLocal, false), await _reads.PairAsync(2));
        Assert.Equal(preRungLocal, await _reads.PlainAsync(2));

        /* A second post-rung row moves the UTC watermark forward; the pre-rung row's later local stamp never
           enters the comparison. */
        await SeedAsync(1, postRungLocal.AddMinutes(1), postRungUtc.AddMinutes(1));
        Assert.Equal(((DateTime?)postRungUtc.AddMinutes(1), true), await _reads.PairAsync(1));
    }

    private async Task SeedAsync(int serverId, DateTime sampleTimeServerLocal, DateTime? sampleTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time_utc)
            VALUES ($1, $2, $3, $4, $5, 50, 5, $6)";
        foreach (var v in new object[] { _nextId--, DateTime.UtcNow, serverId, "WatermarkFrameSrv" + serverId, sampleTimeServerLocal, (object?)sampleTimeUtc ?? DBNull.Value })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}

/// <summary>
/// The Lite reads end to end on a real DuckDB through the real <see cref="LocalDataService"/> and the real MCP
/// tools: the CPU window selecting a post-rung row by its stored UTC instant that the offset-shifted window
/// would have missed, a pre-rung row exactly as before, and the projected stamp still server-local; the
/// properties read carrying the clock pair, NULL zone included, into <c>get_server_properties</c>.
/// </summary>
[Collection("server-time-helper")]
public sealed class TimeHonestyRungReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    /* The server sits in America/New_York. The snapshot the store holds was collected under EDT (UTC-4); the
       post-rung sample under test was taken under EST (UTC-5), on the far side of the fall-back — the exact
       population the offset-shifted window misplaced by an hour. */
    private const int CollectedOffset = -240;
    private const int SampleOffsetAtTheInstant = -300;

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly int _pre2022Id;
    private readonly string _tempDir;
    private long _nextId = -6300;
    private DuckDBConnection? _seedConn;

    public TimeHonestyRungReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _tempDir = Path.Combine(Path.GetTempPath(), "TimeHonestyRungTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _serverManager = new ServerManager(configDir);
        _serverId = Register("TimeHonestySrv");
        _pre2022Id = Register("OlderEngineSrv");
    }

    private int Register(string name)
    {
        var server = new ServerConnection { ServerName = name, DisplayName = name };
        _serverManager.AddServer(server);
        return RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /// <summary>
    /// Four rows against a one-hour window ending now, the store holding the EDT offset (-240):
    /// <list type="bullet">
    /// <item>A — pre-rung (NULL twin), taken 30 minutes ago under EDT: local = now - 30 m - 240 m. Selected by
    /// the fallback arm exactly as the old read selected it.</item>
    /// <item>B — post-rung, taken 30 minutes ago under EST (-300): local = now - 30 m - 300 m, twin = now - 30 m.
    /// Its local stamp sits OUTSIDE the offset-shifted window [now - 60 m - 240 m, now - 240 m] — the old read
    /// missed it — and its twin sits inside the UTC window, so the new read takes it.</item>
    /// <item>C — pre-rung, taken 100 minutes ago under EDT: outside either way.</item>
    /// <item>D — post-rung, taken 100 minutes ago under EST: twin outside the UTC window, and its local stamp
    /// (now - 400 m) outside the shifted one too. Excluded, and by the twin rather than by luck.</item>
    /// </list>
    /// The projected <c>SampleTime</c> is the LOCAL stamp for every returned row — the frame the chart plots
    /// and the MCP tool has always published.
    /// </summary>
    [Fact]
    public async Task TheCpuWindow_PrefersTheStoredUtcInstant_AndFallsBackToTheOffsetForPreRungRows()
    {
        var savedOffset = ServerTimeHelper.UtcOffsetMinutes;
        try
        {
            /* Parked on a value belonging to no server, so a read that reached for the desktop static would
               window on the wrong clock and fail. */
            ServerTimeHelper.UtcOffsetMinutes = 720;

            var now = Truncate(DateTime.UtcNow);
            await SeedServerPropertiesAsync(_serverId, "TimeHonestySrv", CollectedOffset, "Eastern Standard Time");

            var utcA = now.AddMinutes(-30);
            var utcB = now.AddMinutes(-30);
            var utcC = now.AddMinutes(-100);
            var utcD = now.AddMinutes(-100);

            await SeedCpuAsync(_serverId, "TimeHonestySrv", utcA.AddMinutes(CollectedOffset), null, 11);
            await SeedCpuAsync(_serverId, "TimeHonestySrv", utcB.AddMinutes(SampleOffsetAtTheInstant), utcB, 22);
            await SeedCpuAsync(_serverId, "TimeHonestySrv", utcC.AddMinutes(CollectedOffset), null, 33);
            await SeedCpuAsync(_serverId, "TimeHonestySrv", utcD.AddMinutes(SampleOffsetAtTheInstant), utcD, 44);

            var rows = await _dataService.GetCpuUtilizationAsync(_serverId, hoursBack: 1, asOfUtc: now, utcOffsetMinutes: CollectedOffset);

            Assert.Equal(new[] { 22, 11 }, rows.Select(r => r.SqlServerCpu).ToArray());   /* ordered on the local stamp: B's EST stamp sorts first */

            /* The frame of the projected value did not move: each returned SampleTime is the row's LOCAL stamp. */
            Assert.Equal(utcB.AddMinutes(SampleOffsetAtTheInstant), rows[0].SampleTime);
            Assert.Equal(utcA.AddMinutes(CollectedOffset), rows[1].SampleTime);

            /* Why B is the whole point: its local stamp lies outside the window the pre-rung read built
               (GetTimeRangeServerLocal: [asOf - 1 h + offset, asOf + offset]), so that read returned only A. */
            var oldLocalStart = now.AddHours(-1).AddMinutes(CollectedOffset);
            Assert.True(rows[0].SampleTime < oldLocalStart, "the fixture must place B's local stamp where the offset-shifted window could not see it");

            /* The MCP tool comes through the same read with the STORED offset, and emits the local stamps. */
            var json = await McpCpuTools.GetCpuUtilization(_dataService, _serverManager, "TimeHonestySrv", hours_back: 1, as_of: now.ToString("o"));
            using var doc = JsonDocument.Parse(json);
            var samples = doc.RootElement.GetProperty("samples").EnumerateArray().ToList();
            Assert.Equal(new[] { 22, 11 }, samples.Select(s => s.GetProperty("sql_server_cpu").GetInt32()).ToArray());
            /* The tool buckets to the minute (its own downsample), so the emitted stamp is B's LOCAL stamp at
               minute grain — the server's frame, not the twin. */
            var localB = utcB.AddMinutes(SampleOffsetAtTheInstant);
            Assert.Equal(
                new DateTime(localB.Year, localB.Month, localB.Day, localB.Hour, localB.Minute, 0),
                DateTime.Parse(samples[0].GetProperty("sample_time").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind));
        }
        finally
        {
            ServerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    /// <summary>
    /// #3744: the overview read that feeds the CPU alert gate carries BOTH stamps of the newest row. A pre-rung row
    /// (NULL twin) surfaces its local stamp and a null <c>CpuSampleTimeUtc</c> — the host then hands the gate the
    /// local stamp, today's behaviour (fixture 5); a post-rung row surfaces both, and the twin is what the gate
    /// gets. Newest by the LOCAL stamp, which every row has.
    /// </summary>
    [Fact]
    public async Task TheOverviewRead_CarriesBothStampsOfTheNewestCpuRow()
    {
        var now = Truncate(DateTime.UtcNow);

        /* Newest row pre-rung: local stamp only. */
        var utcPre = now.AddMinutes(-2);
        await SeedCpuAsync(_serverId, "TimeHonestySrv", utcPre.AddMinutes(CollectedOffset), null, 41);

        var summary = await _dataService.GetServerSummaryAsync(_serverId, "TimeHonestySrv");
        Assert.NotNull(summary);
        Assert.Equal(41, summary!.CpuPercent);
        Assert.Equal(utcPre.AddMinutes(CollectedOffset), summary.CpuSampleTime);
        Assert.Null(summary.CpuSampleTimeUtc);

        /* A newer post-rung row: both stamps, and the twin is the honest instant. */
        var utcPost = now.AddMinutes(-1);
        await SeedCpuAsync(_serverId, "TimeHonestySrv", utcPost.AddMinutes(CollectedOffset), utcPost, 42);

        summary = await _dataService.GetServerSummaryAsync(_serverId, "TimeHonestySrv");
        Assert.NotNull(summary);
        Assert.Equal(42, summary!.CpuPercent);
        Assert.Equal(utcPost.AddMinutes(CollectedOffset), summary.CpuSampleTime);
        Assert.Equal(utcPost, summary.CpuSampleTimeUtc);

        /* The fold the snapshot applies (MainWindow.AlertEngine, pinned as source above): twin where present. */
        Assert.Equal(utcPost, summary.CpuSampleTimeUtc ?? summary.CpuSampleTime);
    }

    /// <summary>The picker branch (server-local from/to) converts its bounds back to UTC for the twin exactly
    /// as <c>GetTimeRange</c> does, so a post-rung row is selected by its instant there too.</summary>
    [Fact]
    public async Task TheCpuWindow_PickerRange_SelectsAPostRungRowByItsInstant()
    {
        var now = Truncate(DateTime.UtcNow);
        await SeedServerPropertiesAsync(_serverId, "TimeHonestySrv", CollectedOffset, "Eastern Standard Time");

        var utcB = now.AddMinutes(-30);
        await SeedCpuAsync(_serverId, "TimeHonestySrv", utcB.AddMinutes(SampleOffsetAtTheInstant), utcB, 22);

        /* A server-time range of [now - 1 h, now] expressed in the COLLECTED offset's frame. */
        var fromLocal = now.AddHours(-1).AddMinutes(CollectedOffset);
        var toLocal = now.AddMinutes(CollectedOffset);

        var rows = await _dataService.GetCpuUtilizationAsync(_serverId, fromDate: fromLocal, toDate: toLocal, utcOffsetMinutes: CollectedOffset);
        var row = Assert.Single(rows);
        Assert.Equal(22, row.SqlServerCpu);
        Assert.Equal(utcB.AddMinutes(SampleOffsetAtTheInstant), row.SampleTime);
    }

    /// <summary>The properties read carries the pair, NULL zone included, and <c>get_server_properties</c>
    /// publishes both with a note that says what a NULL zone means — on a 2022+ engine and on an older one.</summary>
    [Fact]
    public async Task GetServerProperties_PublishesTheClockPair_AndSaysWhatANullZoneMeans()
    {
        await SeedServerPropertiesAsync(_serverId, "TimeHonestySrv", CollectedOffset, "Eastern Standard Time");
        await SeedServerPropertiesAsync(_pre2022Id, "OlderEngineSrv", CollectedOffset, null);

        var modern = await _dataService.GetLatestServerPropertiesAsync(_serverId);
        Assert.NotNull(modern);
        Assert.Equal(CollectedOffset, modern!.UtcOffsetMinutes);
        Assert.Equal("Eastern Standard Time", modern.TimeZoneId);

        var older = await _dataService.GetLatestServerPropertiesAsync(_pre2022Id);
        Assert.NotNull(older);
        Assert.Equal(CollectedOffset, older!.UtcOffsetMinutes);
        Assert.Null(older.TimeZoneId);

        using (var doc = JsonDocument.Parse(await McpServerInfoTools.GetServerProperties(_dataService, _serverManager, "TimeHonestySrv")))
        {
            Assert.Equal(CollectedOffset, doc.RootElement.GetProperty("utc_offset_minutes").GetInt32());
            Assert.Equal("Eastern Standard Time", doc.RootElement.GetProperty("time_zone_id").GetString());
            Assert.Contains("engine's own zone", doc.RootElement.GetProperty("time_zone_note").GetString(), StringComparison.Ordinal);
        }

        using (var doc = JsonDocument.Parse(await McpServerInfoTools.GetServerProperties(_dataService, _serverManager, "OlderEngineSrv")))
        {
            Assert.Equal(CollectedOffset, doc.RootElement.GetProperty("utc_offset_minutes").GetInt32());
            Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("time_zone_id").ValueKind);
            Assert.Contains("pre-2022 engine", doc.RootElement.GetProperty("time_zone_note").GetString(), StringComparison.Ordinal);
            Assert.Contains("only the offset in force at captured_at is known", doc.RootElement.GetProperty("time_zone_note").GetString(), StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The upgrade itself, on a real DuckDB file: a database initialized at the current schema, both columns
    /// dropped and its version stamped back to 62 — the shape every existing Lite database has the morning of
    /// the upgrade, its <c>v_</c> views present over the narrower tables — re-initialized. Both columns come
    /// back nullable at the tail, the pre-rung rows read NULL through the views, a post-rung row lands, and the
    /// version reads 63.
    /// </summary>
    [Fact]
    public async Task AV62Database_ClimbsToV63_AndItsOldRowsReadNull()
    {
        var dbDir = Path.Combine(_tempDir, "upgrade");
        Directory.CreateDirectory(dbDir);
        var dbPath = Path.Combine(dbDir, "lite-v62.duckdb");

        var initializer = new DuckDbInitializer(dbPath);
        await initializer.InitializeAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();

            foreach (var (table, column, view) in new[]
            {
                ("cpu_utilization_stats", "sample_time_utc", "v_cpu_utilization_stats"),
                ("server_properties", "time_zone_id", "v_server_properties"),
            })
            {
                /* DuckDB refuses to DROP a column a view or index depends on, so the dependents go first for the
                   fixture's sake; the v62 passthrough is then put BACK over the narrower table, because that is
                   the state an existing database is in when the ALTER runs — the ADD COLUMN must succeed with the
                   view present, and Lite's CreateArchiveViewsAsync re-expands it afterwards. */
                var dependents = new List<string>();
                using (var views = conn.CreateCommand())
                {
                    views.CommandText = $"SELECT view_name FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%{table}%'";
                    using var reader = await views.ExecuteReaderAsync();
                    while (await reader.ReadAsync()) dependents.Add(reader.GetString(0));
                }

                Assert.Contains(view, dependents);
                foreach (var dependent in dependents) await ExecAsync(conn, $"DROP VIEW IF EXISTS {dependent}");

                var indexes = new List<string>();
                using (var idx = conn.CreateCommand())
                {
                    idx.CommandText = $"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{table}'";
                    using var reader = await idx.ExecuteReaderAsync();
                    while (await reader.ReadAsync()) indexes.Add(reader.GetString(0));
                }

                foreach (var index in indexes) await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");
                await ExecAsync(conn, $"ALTER TABLE {table} DROP COLUMN {column}");
                await ExecAsync(conn, $"CREATE VIEW {view} AS SELECT * FROM {table}");
            }

            await ExecAsync(conn, "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES (-1, TIMESTAMP '2026-09-18 12:00:00', 1, 'pre-v63', TIMESTAMP '2026-09-18 08:00:00', 40, 5)");
            await ExecAsync(conn, "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes) VALUES (-2, TIMESTAMP '2026-09-18 12:00:00', 1, 'pre-v63', 'Developer Edition', '15.0.4322.2', 'RTM', 3, 8, 1, 16384, -240)");
            await ExecAsync(conn, "DELETE FROM schema_version");
            await ExecAsync(conn, "INSERT INTO schema_version (version) VALUES (62)");
        }

        var upgraded = new DuckDbInitializer(dbPath);
        await upgraded.InitializeAsync();
        await upgraded.CreateArchiveViewsAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();
            /* The climb lands on the CURRENT top, not on 63: a v62 database re-initialized today runs every block
               above it in one InitializeAsync, and since v64 (#3796 / Darling V137, the Query Store capture modes)
               landed on top of this rung that is 64 today and whatever the newest rung is tomorrow. The v63
               HALF of the climb — both columns present, nullable, trailing, the old rows NULL — is what this
               test owns, and it is asserted below; the literal top belongs to the newest Lite rung's own
               test (QsCaptureModeRungTests.AV63Database_ClimbsToV64_AndItsOldRowsReadNull). The
               PerfmonCounterTypeTests v61->v62 climb has read CurrentSchemaVersion for the same reason. */
            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, Convert.ToInt64(await ScalarAsync(conn, "SELECT MAX(version) FROM schema_version")));
            Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 63);

            foreach (var (table, column, type, before, view) in new[]
            {
                ("cpu_utilization_stats", "sample_time_utc", "TIMESTAMP", "other_process_cpu_utilization", "v_cpu_utilization_stats"),
                ("server_properties", "time_zone_id", "VARCHAR", "utc_offset_minutes", "v_server_properties"),
            })
            {
                Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(conn, $"SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name = '{table}'")));
                Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(conn, $"SELECT COUNT(*) FROM duckdb_views() WHERE view_name = '{view}'")));

                using var columns = conn.CreateCommand();
                columns.CommandText = $"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position";
                using var reader = await columns.ExecuteReaderAsync();
                var rows = new List<(string Name, string Type, string Nullable)>();
                while (await reader.ReadAsync())
                {
                    rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
                }

                Assert.Equal((column, type, "YES"), rows[^1]);
                Assert.Equal(before, rows[^2].Name);
            }

            /* The pre-rung rows read NULL through the re-expanded views; post-rung rows land with their values. */
            await ExecAsync(conn, "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time_utc) VALUES (-3, TIMESTAMP '2026-09-18 12:01:00', 1, 'pre-v63', TIMESTAMP '2026-09-18 08:01:00', 41, 5, TIMESTAMP '2026-09-18 12:01:00')");
            await ExecAsync(conn, "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id) VALUES (-4, TIMESTAMP '2026-09-18 12:01:00', 1, 'pre-v63', 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, -240, 'Eastern Standard Time')");

            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT sample_time_utc FROM v_cpu_utilization_stats WHERE collection_id = -1"));
            Assert.Equal(new DateTime(2026, 9, 18, 12, 1, 0), (DateTime)(await ScalarAsync(conn, "SELECT sample_time_utc FROM v_cpu_utilization_stats WHERE collection_id = -3"))!);
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT time_zone_id FROM v_server_properties WHERE collection_id = -2"));
            Assert.Equal("Eastern Standard Time", await ScalarAsync(conn, "SELECT time_zone_id FROM v_server_properties WHERE collection_id = -4"));
        }
    }

    /* ---- helpers ------------------------------------------------------------------------------------- */

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task SeedServerPropertiesAsync(int serverId, string serverName, int? utcOffsetMinutes, string? timeZoneId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, $4, 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $5, $6)";
        foreach (var v in new object[] { _nextId--, DateTime.UtcNow, serverId, serverName, (object?)utcOffsetMinutes ?? DBNull.Value, (object?)timeZoneId ?? DBNull.Value })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedCpuAsync(int serverId, string serverName, DateTime sampleTimeServerLocal, DateTime? sampleTimeUtc, int sqlCpu)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time_utc)
            VALUES ($1, $2, $3, $4, $5, $6, 0, $7)";
        foreach (var v in new object[] { _nextId--, DateTime.UtcNow, serverId, serverName, sampleTimeServerLocal, sqlCpu, (object?)sampleTimeUtc ?? DBNull.Value })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }
}
