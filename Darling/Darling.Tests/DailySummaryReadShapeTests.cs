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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3905: the daily summary's read shape. On the largest production store the web server page's two daily
/// reads were 26 of the Overview tab's 28 seconds, and the issue blamed two things: raw CTEs over the whole
/// window, and a per-call rollup-coverage probe. Measured, the probe half held and the CTE half did not. The
/// month read's cost was the routed <c>queries</c> CTE: its not-carried probes (#3653 A6) planned as joins over
/// the server's entire history, rescanned once per day, and its <c>COUNT(DISTINCT)</c> sorted every rollup row
/// of the month. These pins hold the fixed shapes. Results are identical by construction, and the live half proves
/// that against the old text as an oracle, row for row, on every tier.
/// </summary>
public sealed class DailySummaryReadShapeSqlTests
{
    private static readonly (RetentionTier Tier, string Relation)[] EveryForm =
    {
        (RetentionTier.Raw, TimescaleSupport.QueryStatsHourlyView),
        (RetentionTier.Hourly, TimescaleSupport.QueryStatsHourlyView),
        (RetentionTier.Hourly, TimescaleSupport.QueryStatsIntervalHourlyView),
        (RetentionTier.Daily, TimescaleSupport.QueryStatsHourlyView),
    };

    /// <summary>
    /// No tier counts with a DISTINCT aggregate: PostgreSQL evaluates one by sorting every input row of its
    /// group, where the distinct pairs below can be hashed (1.10 s against 0.24 s for a busy server's month of
    /// hourly rows on the rig). Every member of the CTE counts the same way, so every tier counts the same thing:
    /// one member on raw, the rollup half and the raw tail on a routed tier.
    /// </summary>
    [Fact]
    public void EveryTier_CountsDistinctPairs_NeverADistinctAggregate()
    {
        foreach (var (tier, relation) in EveryForm)
        {
            var sql = DailySummarySql.RangeSqlFor(tier, relation);
            Assert.DoesNotContain("COUNT(DISTINCT", sql, StringComparison.OrdinalIgnoreCase);

            var counted = tier == RetentionTier.Raw ? 1 : 2;
            Assert.Equal(counted, sql.Split("SELECT x.d, COUNT(x.query_hash) AS c").Length - 1);
            Assert.Equal(counted, sql.Split("GROUP BY x.d").Length - 1);
            Assert.Contains("SELECT DISTINCT date_trunc('day', collection_time) AS d, query_hash", sql, StringComparison.Ordinal);
            if (tier != RetentionTier.Raw)
            {
                Assert.Contains("SELECT DISTINCT date_trunc('day', bucket) AS d, query_hash", sql, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>
    /// The seam, as source: the MCP reader routes on the cached gate the compose panels read, and no longer runs
    /// either probe itself. A per-call probe is what put a >= 1.7 s coverage read in front of every daily read,
    /// twice per server-page load. The live half counts the probes.
    /// </summary>
    [Fact]
    public void TheDailyReader_RoutesOnTheCachedGate_NotAPerCallProbe()
    {
        var reader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingHealthReader.cs");
        Assert.Contains("await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("TimescaleSupport.DetectRollupsAsync(", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("TimescaleSupport.DetectRollupCoverageAsync(", reader, StringComparison.Ordinal);
    }

    /// <summary>
    /// The web server page's Overview tab draws today's tile and the month grid from ONE fetch of
    /// get_daily_summary_range: the tile is the row whose date is the read's own <c>to_date</c> (the anchor
    /// day), so the page derives no date of its own. It used to fetch get_daily_summary beside the range, which
    /// ran the day's aggregate twice per load and could print two different query counts for today on one
    /// page.
    /// </summary>
    [Fact]
    public void TheOverviewTab_DrawsTodaysTileFromTheRangeRead_OneFetchForTwoPanels()
    {
        var page = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

        var overviewAt = page.IndexOf("id: \"overview\",", StringComparison.Ordinal);
        var nextTabAt = page.IndexOf("id: \"", overviewAt + 1, StringComparison.Ordinal);
        Assert.True(overviewAt > 0 && nextTabAt > overviewAt, "the Overview tab's block must be found before it can be read");
        var overview = page[overviewAt..nextTabAt];
        Assert.Contains("...dailySummaryPanels(server),", overview, StringComparison.Ordinal);
        Assert.DoesNotContain("\"get_daily_summary\"", overview, StringComparison.Ordinal);

        var composite = page[page.IndexOf("export function dailySummaryPanels(server)", StringComparison.Ordinal)..];
        composite = composite[..composite.IndexOf("\n}", StringComparison.Ordinal)];
        Assert.Equal(1, composite.Split("readTool(").Length - 1);
        Assert.Contains("readTool(\"get_daily_summary_range\", { server, days_back: 30 })", composite, StringComparison.Ordinal);
        Assert.Contains("days.find((day) => day.summary_date === res.data.to_date)", composite, StringComparison.Ordinal);
        Assert.Contains("VIZ.stat(today, { stats: DAILY_STATS })", composite, StringComparison.Ordinal);
        Assert.Contains("return [tile.panel, calendar.panel];", composite, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3905: <see cref="ComposeStoreAvailability"/> runs one coverage probe per data source at a time. The web
/// server page fires its daily reads together, and on the largest production store the probe is >= 1.7 s, so a
/// stale cache refreshed by every concurrent caller was that cost times the callers. Driven through the probe
/// seam with a gated fake, the pattern <see cref="FleetCollectionHealthMemoTests"/> set for the fleet memo, so
/// callers can be parked on the probe before it answers, which is the only way to prove they shared it.
/// </summary>
public sealed class ComposeStoreAvailabilitySingleFlightTests
{
    private static readonly (RollupAvailability, RollupCoverage) Answer = (RollupAvailability.All, RollupCoverage.Unknown);

    private sealed class GatedProbe
    {
        private int _calls;
        public TaskCompletionSource<(RollupAvailability, RollupCoverage)> Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public List<CancellationToken> TokensSeen { get; } = new();
        public int Calls => Volatile.Read(ref _calls);

        public Task<(RollupAvailability Rollups, RollupCoverage Coverage)> Run(NpgsqlDataSource dataSource, CancellationToken token)
        {
            Interlocked.Increment(ref _calls);
            lock (TokensSeen)
            {
                TokensSeen.Add(token);
            }

            return Gate.Task;
        }
    }

    /// <summary>A data source that is never opened: the cache keys on the instance, and nothing here reaches a
    /// store.</summary>
    private static NpgsqlDataSource UnopenedDataSource() => NpgsqlDataSource.Create("Host=unused.invalid;Database=none");

    [Fact]
    public async Task ConcurrentCallers_ShareOneProbe_AndTheAnswerIsThenCached()
    {
        await using var dataSource = UnopenedDataSource();
        var probe = new GatedProbe();

        var callers = Enumerable.Range(0, 5)
            .Select(_ => ComposeStoreAvailability.GetRollupsAsync(dataSource, probe.Run, CancellationToken.None).AsTask())
            .ToArray();

        Assert.All(callers, c => Assert.False(c.IsCompleted));
        Assert.Equal(1, probe.Calls);
        Assert.Equal(1, ComposeStoreAvailability.ProbesStartedFor(dataSource));

        probe.Gate.SetResult(Answer);
        var results = await Task.WhenAll(callers);
        Assert.All(results, r => Assert.Equal(RollupAvailability.All, r.Rollups));

        /* Inside ReprobeInterval the answer is served from memory: no second probe. */
        var cached = await ComposeStoreAvailability.GetRollupsAsync(dataSource, probe.Run, CancellationToken.None);
        Assert.Equal(RollupAvailability.All, cached.Rollups);
        Assert.Equal(1, probe.Calls);
        Assert.Equal(1, ComposeStoreAvailability.ProbesStartedFor(dataSource));

        /* The shared probe ran on no caller's token. */
        Assert.Equal(new[] { CancellationToken.None }, probe.TokensSeen);
    }

    /// <summary>A caller that gives up releases itself and nobody else: the probe keeps running for the
    /// others, and its answer is cached for the next caller.</summary>
    [Fact]
    public async Task ACallersCancellation_ReleasesOnlyThatCaller()
    {
        await using var dataSource = UnopenedDataSource();
        var probe = new GatedProbe();
        using var leaving = new CancellationTokenSource();

        var leaver = ComposeStoreAvailability.GetRollupsAsync(dataSource, probe.Run, leaving.Token).AsTask();
        var stayer = ComposeStoreAvailability.GetRollupsAsync(dataSource, probe.Run, CancellationToken.None).AsTask();
        Assert.Equal(1, probe.Calls);

        await leaving.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leaver);
        Assert.False(stayer.IsCompleted);

        probe.Gate.SetResult(Answer);
        Assert.Equal(RollupAvailability.All, (await stayer).Rollups);
        Assert.Equal(RollupAvailability.All, (await ComposeStoreAvailability.GetRollupsAsync(dataSource, probe.Run, CancellationToken.None)).Rollups);
        Assert.Equal(1, probe.Calls);
    }

    /// <summary>A failed probe answers every waiter with the never-wrong fallback (route to raw, no coverage
    /// evidence) and is cached like any answer, as it was before the probe was shared: the re-probe interval
    /// retries it, and a burst of callers does not become a burst of failing probes.</summary>
    [Fact]
    public async Task AFailedProbe_AnswersRawForEveryWaiter_AndIsCached()
    {
        await using var dataSource = UnopenedDataSource();
        var probe = new GatedProbe();

        var a = ComposeStoreAvailability.GetRollupsAsync(dataSource, probe.Run, CancellationToken.None).AsTask();
        var b = ComposeStoreAvailability.GetRollupsAsync(dataSource, probe.Run, CancellationToken.None).AsTask();
        probe.Gate.SetException(new NpgsqlException("57014: canceling statement due to statement timeout"));

        foreach (var answer in await Task.WhenAll(a, b))
        {
            Assert.Equal(RollupAvailability.None, answer.Rollups);
            Assert.Same(RollupCoverage.Unknown, answer.Coverage);
        }

        Assert.Equal(RollupAvailability.None, (await ComposeStoreAvailability.GetRollupsAsync(dataSource, probe.Run, CancellationToken.None)).Rollups);
        Assert.Equal(1, probe.Calls);
    }
}

/// <summary>
/// #3905, against a store: the rewritten statement answers exactly what the old one did on every tier, the
/// not-carried probes run once per day instead of as joins over the server's history, and the daily reads probe
/// coverage once however many race.
///
/// <para><b>The oracle is the old text.</b> <see cref="OldQueriesCte"/> is the <c>queries</c> CTE as it stood
/// before #3905, for each tier, spliced into the current statement in place of the new one, so the two
/// statements differ in exactly the lines this issue changed. The seed plants the cases those lines decide: a
/// day the rollup skipped below its ceiling (NULL, named), a day with no rows at all (absent), a NULL
/// <c>query_hash</c> (skipped by both counts), a restart-only day (a hole for the legacy hourly, nothing for the
/// interval-honest successor), and the days past each tier's ceiling (the raw tail).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DailySummaryReadShapeLiveTests
{
    private const int ServerId = -390501;
    private const string ServerName = "read-shape-3905";

    /// <summary>The pre-#3905 <c>queries</c> CTE, raw and routed, verbatim but for its comments. Placeholders:
    /// the routed relation (the daily tier ignores <paramref name="hourlyRelation"/>, as
    /// <see cref="DailySummarySql.RangeSqlFor(RetentionTier, string)"/> does), its registered source, the
    /// source's time column and the source filter.</summary>
    private static string OldQueriesCte(RetentionTier tier, string hourlyRelation)
    {
        var relation = tier == RetentionTier.Daily ? TimescaleSupport.QueryStatsDailyView : hourlyRelation;
        if (tier == RetentionTier.Raw)
        {
            return """
                queries AS (
                    SELECT date_trunc('day', collection_time) AS d, COUNT(DISTINCT query_hash) AS c
                    FROM v_query_stats
                    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
                    GROUP BY 1
                ),
                """;
        }

        var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == relation);
        var filter = TimescaleSupport.MaterializationHoleSourceFilterFor(target.CreateSql);
        var sourceFilter = filter.Length == 0 ? string.Empty : " AND " + filter;
        return $"""
            queries_ceiling AS (
                SELECT date_trunc('day', max(bucket)) AS last_day
                FROM collect.{relation}
                WHERE server_id = $1
            ),
            queries AS (
                SELECT date_trunc('day', bucket) AS d, COUNT(DISTINCT query_hash) AS c
                FROM collect.{relation}
                WHERE server_id = $1 AND bucket >= $2 AND bucket < $3
                GROUP BY 1
                UNION ALL
                SELECT date_trunc('day', collection_time) AS d, COUNT(DISTINCT query_hash) AS c
                FROM v_query_stats
                WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
                  AND collection_time >= COALESCE((SELECT last_day + INTERVAL '1 day' FROM queries_ceiling), $2)
                GROUP BY 1
                UNION ALL
                SELECT b.d, NULL::bigint AS c
                FROM generate_series(date_trunc('day', $2::timestamp), date_trunc('day', $3::timestamp), INTERVAL '1 day') AS b(d)
                WHERE b.d < $3
                  AND b.d < COALESCE((SELECT last_day + INTERVAL '1 day' FROM queries_ceiling), $2)
                  AND NOT EXISTS (
                    SELECT 1 FROM collect.{relation} AS r
                    WHERE r.server_id = $1 AND r.bucket >= b.d AND r.bucket < b.d + INTERVAL '1 day')
                  AND EXISTS (
                    SELECT 1 FROM collect.{target.Source} AS s
                    WHERE s.server_id = $1 AND s.{target.SourceTimeColumn} >= b.d AND s.{target.SourceTimeColumn} < b.d + INTERVAL '1 day'{sourceFilter})
            ),
            """;
    }

    /// <summary>The oracle statement: the current one with its <c>queries</c> region (from the first CTE this
    /// issue touched to the <c>deadlocks</c> CTE that follows it) replaced by <see cref="OldQueriesCte"/>.</summary>
    private static string OracleSql(RetentionTier tier, string relation)
    {
        var current = DailySummarySql.RangeSqlFor(tier, relation);
        var start = current.IndexOf(tier == RetentionTier.Raw ? "queries AS (" : "queries_ceiling AS (", StringComparison.Ordinal);
        var end = current.IndexOf("deadlocks AS (", StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "the queries region must be found before it can be swapped");
        return current[..start] + OldQueriesCte(tier, relation) + "\n" + current[end..];
    }

    [Fact]
    public async Task TheRewrittenStatement_AnswersExactlyWhatTheOldOneDid_OnEveryTier_AgainstDevPostgres()
    {
        await using var seed = await SeedAsync("the read-shape oracle test");
        var (connection, days) = (seed.Connection, seed.Days);
        var ct = TestContext.Current.CancellationToken;

        var from = days[0].AddDays(-3);
        var to = days[^1].AddDays(1);
        var forms = new (RetentionTier Tier, string Relation)[]
        {
            (RetentionTier.Raw, TimescaleSupport.QueryStatsHourlyView),
            (RetentionTier.Hourly, TimescaleSupport.QueryStatsHourlyView),
            (RetentionTier.Hourly, TimescaleSupport.QueryStatsIntervalHourlyView),
            (RetentionTier.Daily, TimescaleSupport.QueryStatsHourlyView),
        };

        var answers = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var (tier, relation) in forms)
        {
            var current = await ReadAllAsync(connection, DailySummarySql.RangeSqlFor(tier, relation), from, to, ct);
            var oracle = await ReadAllAsync(connection, OracleSql(tier, relation), from, to, ct);
            Assert.NotEmpty(current);
            Assert.Equal(oracle, current);
            answers[tier + "/" + relation] = current;
        }

        /* Not vacuous: the forms reach the cases the changed lines decide. The legacy hourly names its skipped
           day (D2); the NULL hash is skipped (D3 counts the 4 hashes, not the NULL); the successor's ceiling is
           D3, so D4 onward is its raw tail, restart row included. On the daily tier D2 is absent rather than
           NULL: its source, the legacy hourly, skipped the day too, so nothing witnesses it (the hole scan's
           rule), and D8 comes from the raw tail past the daily's ceiling. */
        string Day(int n) => days[n].ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var legacy = answers[RetentionTier.Hourly + "/" + TimescaleSupport.QueryStatsHourlyView];
        Assert.Contains(legacy, row => row.StartsWith(Day(2) + "|", StringComparison.Ordinal) && row.Split('|')[3] == "NULL");
        Assert.Contains(legacy, row => row.StartsWith(Day(3) + "|", StringComparison.Ordinal) && row.Split('|')[3] == "4");
        Assert.DoesNotContain(legacy, row => row.StartsWith(Day(1) + "|", StringComparison.Ordinal));
        var successor = answers[RetentionTier.Hourly + "/" + TimescaleSupport.QueryStatsIntervalHourlyView];
        Assert.Contains(successor, row => row.StartsWith(Day(4) + "|", StringComparison.Ordinal) && row.Split('|')[3] == "1");
        var daily = answers[RetentionTier.Daily + "/" + TimescaleSupport.QueryStatsHourlyView];
        Assert.DoesNotContain(daily, row => row.StartsWith(Day(2) + "|", StringComparison.Ordinal));
        Assert.Contains(daily, row => row.StartsWith(Day(8) + "|", StringComparison.Ordinal) && row.Split('|')[3] == "1");
    }

    /// <summary>
    /// The plan, not the text: in the routed <c>queries</c> CTE nothing is materialized and nothing is a
    /// semi-join or anti-join, which is how the old probes ran (the server's whole history materialized and
    /// rescanned per day). Each probe is a SubPlan executed per day: the rollup probe once per candidate day at
    /// or below the ceiling, the source probe only on the days the rollup probe found empty. The window is a
    /// calendar month ending on the seed's last day, so most of it predates the seed and exercises both
    /// probes.
    /// </summary>
    [Fact]
    public async Task TheNotCarriedProbes_RunOncePerDay_AndTheSourceOnlyWhereTheRollupIsEmpty_AgainstDevPostgres()
    {
        await using var seed = await SeedAsync("the read-shape plan test");
        var (connection, days) = (seed.Connection, seed.Days);
        var ct = TestContext.Current.CancellationToken;

        var to = days[^1].AddDays(1);
        var from = to.AddDays(-30);
        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, TimescaleSupport.QueryStatsHourlyView);

        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection);
        AddWindow(explain, from, to);
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var plan = JsonDocument.Parse(json);

        var queries = Walk(plan.RootElement[0].GetProperty("Plan"))
            .Single(n => n.TryGetProperty("Subplan Name", out var name) && name.GetString() == "CTE queries");
        var inQueries = Walk(queries).ToArray();
        Assert.DoesNotContain(inQueries, n => NodeType(n) == "Materialize");
        Assert.DoesNotContain(inQueries, n => NodeType(n) == "Nested Loop"
            && n.TryGetProperty("Join Type", out var join) && join.GetString() is "Semi" or "Anti");

        /* The legacy hourly's ceiling is D5 (the seed materializes D0, D3, D4 and D5); every window day at or
           below it is a candidate, and the ones the rollup holds no row for are probed at the source. */
        var candidates = Enumerable.Range(0, (days[5] - from).Days + 1).Count();
        var rollupEmpty = candidates - 4;

        var candidateDays = inQueries.Single(n => NodeType(n) == "Function Scan"
            && n.TryGetProperty("Function Name", out var fn) && fn.GetString() == "generate_series");
        Assert.Equal(candidates, LoopsOfTheOnlySubPlanUnder(candidateDays));

        var notCarried = inQueries.Single(n => NodeType(n) == "Subquery Scan" && n.GetProperty("Alias").GetString()!.StartsWith('b'));
        Assert.Equal(rollupEmpty, LoopsOfTheOnlySubPlanUnder(notCarried));
    }

    /// <summary>
    /// The seam, counted: the single-day and range tools raced together (the web server page's old pattern, and
    /// any agent's) and then called again cost ONE coverage probe on this data source. Before #3905 the reader
    /// probed on every call and never touched the gate, so the count below was zero.
    /// </summary>
    [Fact]
    public async Task EveryDailyRead_ProbesCoverageOnce_HoweverManyRace_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live coverage-probe seam test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await RegisterServerAsync(connection, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        Assert.Equal(0, ComposeStoreAvailability.ProbesStartedFor(postgres));

        var raced = Enumerable.Range(0, 3)
            .SelectMany(_ => new[]
            {
                DarlingMcpHealthTools.GetDailySummary(postgres, ServerName),
                DarlingMcpHealthTools.GetDailySummaryRange(postgres, ServerName, 30),
            })
            .ToArray();
        var payloads = (await Task.WhenAll(raced)).ToList();
        payloads.Add(await DarlingMcpHealthTools.GetDailySummaryRange(postgres, ServerName, 7));
        payloads.Add(await DarlingMcpHealthTools.GetDailySummary(postgres, ServerName));

        foreach (var payload in payloads)
        {
            using var doc = JsonDocument.Parse(payload);
            Assert.False(doc.RootElement.TryGetProperty("status", out var status) && status.GetString() is "error" or "invalid", payload);
        }

        Assert.Equal(1, ComposeStoreAvailability.ProbesStartedFor(postgres));
    }

    /* ───────────────────────────── seed ───────────────────────────── */

    /// <summary>
    /// Nine whole UTC days, D0 thirteen days back, all past RawMaxAge and below every refresh policy's window,
    /// so a background policy run cannot materialize what the seed leaves unmaterialized. D0: 3 hashes. D1:
    /// nothing. D2: 5 hashes, skipped by the hourly and the daily (the hole). D3: 4 hashes and a NULL-hash row.
    /// D4: one restart row (interval 0). D5: 6. D6: 2. D7: 3. D8: 1. Refreshed: the legacy hourly over D0-D1 and
    /// D3-D5 (ceiling D5); the successor over D0 and D3 (ceiling D3; it rejects D4's restart row); the daily over
    /// D0 and D3 (ceiling D3).
    /// </summary>
    private sealed class Seeded(ScratchPostgres scratch, NpgsqlConnection connection, DateTime[] days) : IAsyncDisposable
    {
        public NpgsqlConnection Connection { get; } = connection;

        public DateTime[] Days { get; } = days;

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await scratch.DisposeAsync();
        }
    }

    private static async Task<Seeded> SeedAsync(string what)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            $"Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run {what}.");
        var ct = TestContext.Current.CancellationToken;

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (!await TimescaleSupport.TryEnableAsync(connection, null, ct))
        {
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            Assert.Skip($"{what} needs TimescaleDB: a rollup tier exists only as a continuous aggregate.");
        }

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await RegisterServerAsync(connection, ct);

        var d0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-13), DateTimeKind.Unspecified);
        var days = Enumerable.Range(0, 9).Select(n => d0.AddDays(n)).ToArray();
        Assert.True(DateTime.UtcNow - days[^1].AddDays(1) > TimescaleSupport.DailyRefreshStartSpan, "the seed must sit below every refresh policy's window");

        foreach (var (day, hashes) in new[] { (0, 3), (2, 5), (3, 4), (5, 6), (6, 2), (7, 3), (8, 1) })
        {
            await PlantDayAsync(connection, days[day], hashes, ct);
        }

        await PlantRowAsync(connection, days[3].AddHours(12), queryHash: null, intervalSeconds: 1200, ct);
        await PlantRowAsync(connection, days[4].AddHours(6), queryHash: "0xRESTART", intervalSeconds: 0, ct);

        await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, days[0], days[2], ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, days[3], days[6], ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, days[0], days[1], ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, days[3], days[5], ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, days[0], days[1], ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, days[3], days[4], ct);

        return new Seeded(scratch, connection, days);
    }

    private static async Task PlantDayAsync(NpgsqlConnection connection, DateTime day, int hashes, CancellationToken ct)
    {
        foreach (var hour in new[] { 1, 9, 17 })
        {
            for (var hash = 0; hash < hashes; hash++)
            {
                await PlantRowAsync(connection, day.AddHours(hour), $"0xHASH{hash:D4}", 1200, ct);
            }
        }
    }

    private static int s_collectionId;

    private static async Task PlantRowAsync(NpgsqlConnection connection, DateTime at, string? queryHash, int intervalSeconds, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'ShapeDb', $5, $6, 1000, 1000, 10, $7)", connection);
        insert.Parameters.AddWithValue((long)Interlocked.Increment(ref s_collectionId));
        insert.Parameters.AddWithValue(at);
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = queryHash, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        insert.Parameters.AddWithValue("0xHANDLE" + (queryHash ?? "NULL"));
        insert.Parameters.AddWithValue(intervalSeconds);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /* ───────────────────────────── reads ───────────────────────────── */

    private static void AddWindow(NpgsqlCommand command, DateTime from, DateTime to)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = from });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = to });
    }

    /// <summary>Every column of every row, as text, NULL spelled out: the comparison is the whole answer, not
    /// the columns this issue touched.</summary>
    private static async Task<List<string>> ReadAllAsync(NpgsqlConnection connection, string sql, DateTime from, DateTime to, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var read = new NpgsqlCommand(sql, connection);
        AddWindow(read, from, to);
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                cells[i] = reader.IsDBNull(i)
                    ? "NULL"
                    : reader.GetValue(i) is DateTime at
                        ? at.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                        : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)!;
            }

            rows.Add(string.Join("|", cells));
        }

        return rows;
    }

    private static IEnumerable<JsonElement> Walk(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                foreach (var descendant in Walk(child))
                {
                    yield return descendant;
                }
            }
        }
    }

    private static string? NodeType(JsonElement node) => node.GetProperty("Node Type").GetString();

    /// <summary>How many times the one SubPlan hanging off <paramref name="node"/> ran: the probe's
    /// per-row execution count, which is what tells a per-day probe from a join.</summary>
    private static int LoopsOfTheOnlySubPlanUnder(JsonElement node)
    {
        var subPlans = node.GetProperty("Plans").EnumerateArray()
            .Where(child => child.GetProperty("Parent Relationship").GetString() == "SubPlan")
            .ToArray();
        Assert.Single(subPlans);
        return subPlans[0].GetProperty("Actual Loops").GetInt32();
    }
}
