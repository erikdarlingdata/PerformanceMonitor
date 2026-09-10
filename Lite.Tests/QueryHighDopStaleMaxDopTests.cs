/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB pin for #2999, the Lite twin of Darling's <c>QueryHighDopStaleMaxDopLiveTests</c>:
/// QUERY_HIGH_DOP must not fire from a <c>max_dop</c> reading the server's current configuration makes
/// impossible.
///
/// <para><c>v_query_stats.max_dop</c> is <c>sys.dm_exec_query_stats</c>' lifetime max for the plan's time
/// in cache — the same semantics <c>get_top_queries_by_cpu</c>'s <c>parallel_only</c> description has
/// always warned about. A plan compiled before <c>max degree of parallelism</c> was lowered keeps
/// reporting its old, higher DOP until it is evicted or recompiled, so on a server now configured to
/// MAXDOP 1 a cached max_dop of 16 is not a current problem; it is a high-water mark that predates the
/// configuration change.</para>
///
/// <para><b>Why it is worse than a noisy finding.</b> Lowering MAXDOP is the ordinary remediation FOR
/// this finding. Uncrossed, the finding keeps firing from plans cached before the change, and the advice
/// it drives is to do what has already been done — a finding whose own remediation does not make it
/// stop.</para>
///
/// <para><b>The carve-outs are asserted, not assumed.</b> A current MAXDOP of 0 (unlimited) and an
/// absent <c>v_server_config</c> row both make no configuration claim, so both must leave the count
/// ALONE. Getting either backwards would silently stop Lite raising the finding at all, which is worse
/// than the over-reporting it replaces — so the two must-still-fire cases carry equal weight here with
/// the must-not-fire one, and the boundary case (current MAXDOP exactly equal to the reading) pins that
/// the test is <c>&lt;=</c> and not <c>&lt;</c>.</para>
///
/// <para><b>Real DuckDB rather than a string pin.</b> The thing under test is the ENGINE's answer: that
/// <c>LEFT JOIN current_maxdop AS m ON true</c> against an EMPTY one-row CTE really does deliver NULL to
/// the count's NULL arm rather than eliminating the row, and that the newest <c>capture_time</c> is the
/// one that wins. A text assertion cannot see either.
/// <c>Lite.Tests.QueryHighDopStaleMaxDopParityTests</c> holds the text half, across both SKUs.</para>
/// </summary>
public sealed class QueryHighDopStaleMaxDopTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 29990;
    private const string ServerName = "SynthDopSrv";
    private const string Db = "SynthDopDb";

    /* The plan's lifetime high-water mark. Every case seeds this one reading and varies only what the
       server's CURRENT configuration says, so nothing but the cross-check can move the answer. */
    private const long CachedMaxDop = 16;

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)),
        DateTimeKind.Unspecified);

    private static readonly DateTime WindowStart = WindowEnd.AddHours(-4);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryHighDopStaleMaxDopTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };

    [Fact]
    public async Task QueryHighDop_DoesNotFire_WhenMaxDopExceedsCurrentServerConfig()
    {
        /* -- RED without the cross-check: the plan's lifetime max_dop of 16 predates the server being
              reconfigured to MAXDOP 1, which makes DOP > 1 impossible right now. -- */
        await ClearAsync();
        await SeedHighDopQueryStatsAsync();
        await SeedServerConfigMaxDopAsync(valueInUse: 1, capturedMinutesBeforeWindowEnd: 5);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        Assert.DoesNotContain(facts, f => f.Key == "QUERY_HIGH_DOP");

        /* And the read's OTHER facts survive: the cross-check must narrow one count, not eliminate the
           row. QUERY_SPILLS comes out of the same SELECT. */
        var spills = Assert.Single(facts, f => f.Key == "QUERY_SPILLS");
        Assert.Equal(500.0, spills.Value);
    }

    [Fact]
    public async Task QueryHighDop_StillFires_WhenCurrentMaxDopIsUnlimited()
    {
        /* MAXDOP 0 makes no reading impossible, so the finding fires exactly as it did before the
           cross-check existed. Reading this as "0 is lower than 16, therefore stale" is the inversion
           that would silently retire the finding on every default-configured server in the fleet. */
        await ClearAsync();
        await SeedHighDopQueryStatsAsync();
        await SeedServerConfigMaxDopAsync(valueInUse: 0, capturedMinutesBeforeWindowEnd: 5);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        var fact = Assert.Single(facts, f => f.Key == "QUERY_HIGH_DOP");
        Assert.Equal(1.0, fact.Metadata["high_dop_query_count"]);
    }

    [Fact]
    public async Task QueryHighDop_StillFires_WhenNoServerConfigHasBeenCollected()
    {
        /* No v_server_config row at all — the state every server passes through before its first config
           capture. An unknown current MAXDOP corroborates nothing, so the count is unchanged rather than
           manufacturing confidence either way: the same "omit rather than invent" rule the CPU-
           attribution ratio already follows. This is also the case that fails if the NULL-safe LEFT JOIN
           is ever narrowed to an inner one. */
        await ClearAsync();
        await SeedHighDopQueryStatsAsync();

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        var fact = Assert.Single(facts, f => f.Key == "QUERY_HIGH_DOP");
        Assert.Equal(1.0, fact.Metadata["high_dop_query_count"]);
    }

    [Fact]
    public async Task QueryHighDop_StillFires_WhenCurrentMaxDopExactlyPermitsTheReading()
    {
        /* The boundary. A cached DOP of 16 on a server configured to MAXDOP 16 is perfectly possible
           today, so it is evidence and not staleness. This is what makes the predicate <= rather than <,
           and an off-by-one here would drop every query running at exactly the configured limit — which
           is most of them on a server that has been tuned. */
        await ClearAsync();
        await SeedHighDopQueryStatsAsync();
        await SeedServerConfigMaxDopAsync(valueInUse: CachedMaxDop, capturedMinutesBeforeWindowEnd: 5);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        var fact = Assert.Single(facts, f => f.Key == "QUERY_HIGH_DOP");
        Assert.Equal(1.0, fact.Metadata["high_dop_query_count"]);
    }

    [Fact]
    public async Task TheCrossCheck_ReadsTheNewestConfigCapture_NotTheOldest()
    {
        /* server_config accumulates a row per capture, so "the server's current MAXDOP" is the LATEST
           one. Seeded out of order on purpose: the stale MAXDOP 1 capture is written second and dated
           first, so a read that took whatever the engine handed back, or the oldest row, would suppress
           a finding that is genuinely current. */
        await ClearAsync();
        await SeedHighDopQueryStatsAsync();
        await SeedServerConfigMaxDopAsync(valueInUse: CachedMaxDop, capturedMinutesBeforeWindowEnd: 5);
        await SeedServerConfigMaxDopAsync(valueInUse: 1, capturedMinutesBeforeWindowEnd: 200);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        var fact = Assert.Single(facts, f => f.Key == "QUERY_HIGH_DOP");
        Assert.Equal(1.0, fact.Metadata["high_dop_query_count"]);
    }

    [Fact]
    public async Task TheCrossCheck_NarrowsOnlyTheImpossibleRows()
    {
        /* Several plans against one configuration, because a fixture of one row per case can never show
           that the cross-check is row-wise rather than a switch on the whole read. At MAXDOP 12: 10 and
           12 are possible and counted, 16 and 32 are not, and 4 never cleared the DOP > 8 floor. */
        await ClearAsync();
        await SeedServerConfigMaxDopAsync(valueInUse: 12, capturedMinutesBeforeWindowEnd: 5);

        foreach (var maxDop in new long[] { 4, 10, 12, 16, 32 })
        {
            await SeedHighDopQueryStatsAsync(maxDop: maxDop, spills: 0);
        }

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        var fact = Assert.Single(facts, f => f.Key == "QUERY_HIGH_DOP");
        Assert.Equal(2.0, fact.Metadata["high_dop_query_count"]);

        /* The windowed totals must still cover EVERY row, including the two the count excluded: the
           cross-check narrows one aggregate, it does not filter the read. */
        Assert.Equal(50.0, fact.Metadata["total_executions"]);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ClearAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        foreach (var table in new[] { "query_stats", "server_config" })
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {table} WHERE server_id = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds one cached plan inside the window. Every value other than <c>max_dop</c> is held constant
    /// across the cases so the cross-check is the only thing that can move the answer.
    /// </summary>
    private async Task SeedHighDopQueryStatsAsync(long maxDop = CachedMaxDop, long spills = 500)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     max_dop, min_dop, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_spills,
     query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, 10, 100000, 200000, $9, $10)";
        var id = _nextId++;
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddMinutes(-30) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"0xDOPHASH{id:D4}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"0xDOPPLAN{id:D4}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = maxDop });
        cmd.Parameters.Add(new DuckDBParameter { Value = spills });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.SynthDopTable" });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedServerConfigMaxDopAsync(long valueInUse, int capturedMinutesBeforeWindowEnd)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name,
     value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, 'max degree of parallelism', $5, $6, true, false)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddMinutes(-capturedMinutesBeforeWindowEnd) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = valueInUse });
        cmd.Parameters.Add(new DuckDBParameter { Value = valueInUse });
        await cmd.ExecuteNonQueryAsync();
    }
}
