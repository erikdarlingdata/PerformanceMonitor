/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The vacuum family's exit criterion (#3542 lane 4), against a real store: a planted three-hour-plus backlog
/// series → a <c>PG_AUTOVACUUM_BACKLOG</c> fact naming the worst table with the ratio to ITS OWN threshold and
/// the slope over the run; a wraparound series past <c>autovacuum_freeze_max_age</c> and never lower → a
/// <c>PG_WRAPAROUND_TREND</c> fact graded on the alert's bars with the time-to-wall arithmetic; a chronic xmin
/// holder → a <c>PG_XMIN_HOLD</c> fact on the alert's identity arm; and, through the REAL <c>analyze_server</c>
/// tool, the three chaining into ONE story, <c>PG_WRAPAROUND_TREND → PG_AUTOVACUUM_BACKLOG → PG_XMIN_HOLD</c>.
///
/// <para>Gated on <c>DARLING_TEST_PG</c>; the planting shape is <see cref="PgTargetFactCollectorTests"/>' (the
/// plumbing e2e), with this family's three source tables added. Rows are exactly what the collectors write:
/// <c>pg_autovacuum_stats</c> hourly with the per-table threshold already computed, <c>pg_wraparound_stats</c>
/// every five minutes as a level, <c>pg_xmin_horizon</c> every minute with <c>is_winner</c> stamped at
/// collection. Cleanup runs through <see cref="LiveStoreCleanup"/> (the #1902 ratchet).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetVacuumLiveTests
{
    private const string ServerName = "darling-pg-target-vacuum-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task APlantedBacklogWraparoundAndHold_ProduceTheThreeFacts_AndOneChainedStoryThroughAnalyzeServer()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the vacuum-family e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 18, ct);

            /* Whole-minute bounds, window ending a minute ago (the plumbing e2e's reasoning). */
            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            /* The gate and the coverage witness: 25 h of span, one row a minute across the window. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* ── pg_autovacuum_stats, hourly, three tables:
               hot        — clear at T-4h (900 < 1,050), then past its line for four samples and RISING
                            (2,000 → 5,250) while autovacuum_count moved 10 → 12: running and losing.
               appendonly — the insert arm: 30,000 / 40,000 / 50,000 inserts against a 21,000 insert line for
                            the last three samples, zero dead tuples (invisible to the dead-tuple rule).
               clearish   — past its line at T-4h and T-3h, CLEAR at T-2h, past again for two: the run is two,
                            under the gate, so it must not become the fact and must not count. */
            var hotDead = new long[] { 900, 2_000, 3_000, 4_000, 5_250 };
            var hotRuns = new long[] { 10, 10, 11, 12, 12 };
            var appendInserts = new long[] { 1_000, 5_000, 30_000, 40_000, 50_000 };
            var clearishDead = new long[] { 2_000, 2_000, 100, 2_000, 3_000 };
            for (var h = 0; h < 5; h++)
            {
                var at = windowStart.AddHours(h);
                await PlantAutovacuumAsync(connection, at, "public", "hot", live: 10_000, dead: hotDead[h], vacuumThreshold: 1_050,
                    inserts: 0, insertThreshold: -1, disabled: false, lastAutovacuum: at.AddMinutes(-20), autovacuumCount: hotRuns[h], ct);
                await PlantAutovacuumAsync(connection, at, "public", "appendonly", live: 100_000, dead: 0, vacuumThreshold: 20_050,
                    inserts: appendInserts[h], insertThreshold: 21_000, disabled: false, lastAutovacuum: null, autovacuumCount: 0, ct);
                await PlantAutovacuumAsync(connection, at, "public", "clearish", live: 10_000, dead: clearishDead[h], vacuumThreshold: 1_050,
                    inserts: 0, insertThreshold: -1, disabled: false, lastAutovacuum: null, autovacuumCount: 0, ct);
            }

            /* ── pg_wraparound_stats, every five minutes:
               appdb — XID age 200,000,000 at T-4h climbing 5,000,000/h to 220,000,000: AT the setting and never
                       lower in the window (latest == peak → not keeping up) — the alert's relative Warning arm.
               other — the healthy sawtooth: 190,000,000 for two hours then reset to 50,000,000 (latest < peak). */
            for (var step = 0; step <= 48; step++)
            {
                var at = windowStart.AddMinutes(step * 5);
                var appAge = 200_000_000L + (long)(step * 5 / 60.0 * 5_000_000);
                await PlantWraparoundAsync(connection, at, "appdb", appAge, ct);
                await PlantWraparoundAsync(connection, at, "other", at < windowStart.AddHours(2) ? 190_000_000L : 50_000_000L, ct);
            }

            /* ── pg_xmin_horizon, every minute over the last 41 minutes: session 4242 wins 31 (60,000,000 back,
               above the 50,000,000 bar), a replication slot wins 10 in the middle (70,000,000). The HORIZON is
               above the bar in every one of the 41 captures — one held run of 41, floor 60,000,000, peak
               70,000,000 (#3691 lane 40: persistence is the horizon's, not a holder's) — and the session is the
               modal holder at 31/41 = 0.756 ≥ 0.5, so the card is attributed to it; the latest winner is the session. */
            for (var minute = 40; minute >= 0; minute--)
            {
                var at = windowEnd.AddMinutes(-minute);
                var slotWins = minute is >= 1 and <= 10;
                await PlantXminAsync(connection, at, "session", "4242", "idle in transaction", 60_000_000, isWinner: !slotWins, ct);
                await PlantXminAsync(connection, at, "replication_slot", "slot_a", null, slotWins ? 70_000_000 : 1_000, isWinner: slotWins, ct);
            }

            /* ── the collector alone: the three facts, with the numbers the exit criterion names. */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);
            Assert.False(context.Coverage!.IsPartial);

            var backlog = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AutovacuumBacklog);
            Assert.Equal(PgTargetSources.VacuumSource, backlog.Source);
            Assert.Equal("appdb", backlog.DatabaseName);
            Assert.Equal("public.hot", backlog.ObjectName);
            Assert.Equal(5.0, backlog.Metadata[PgTargetScorer.BacklogRatioKey], precision: 6);
            Assert.Equal(5.0, backlog.Value, precision: 6);
            Assert.Equal(0, backlog.Metadata[PgTargetScorer.BacklogArmIsInsertKey]);
            Assert.Equal(4, backlog.Metadata[PgTargetScorer.BacklogTrailingSamplesKey]);
            Assert.Equal(5, backlog.Metadata[PgTargetScorer.BacklogSamplesInWindowKey]);
            Assert.Equal(3.0, backlog.Metadata[PgTargetScorer.BacklogHoursKey], precision: 6);
            Assert.Equal(1, backlog.Metadata[PgTargetScorer.BacklogSlopeComputableKey]);
            Assert.Equal((5_250 - 2_000) / 3.0, backlog.Metadata[PgTargetScorer.BacklogSlopePerHourKey], precision: 6);
            Assert.Equal(1, backlog.Metadata[PgTargetScorer.BacklogRunsComputableKey]);
            Assert.Equal(2, backlog.Metadata[PgTargetScorer.BacklogAutovacuumRunsKey]);
            Assert.Equal(1_050, backlog.Metadata[PgTargetScorer.BacklogVacuumThresholdKey]);
            Assert.Equal(5_250, backlog.Metadata[PgTargetScorer.BacklogDeadTuplesKey]);
            Assert.Equal(0, backlog.Metadata[PgTargetScorer.BacklogTableAutovacuumDisabledKey]);
            /* hot and appendonly met the gate; clearish (run of two) did not. */
            Assert.Equal(2, backlog.Metadata[PgTargetScorer.BacklogTablesKey]);
            Assert.InRange(backlog.Metadata[PgTargetScorer.BacklogHoursSinceLastAutovacuumKey], 0.3, 0.4);

            /* #3691 lane 43 (M3): the read returns three rows now, so the two tables that met the gate both ride
               on Ranked in the read's own ORDER BY — and [0] IS the subject, same name and same value as the
               fact's, which is the invariant FactRankedTests pins. appendonly is ranked on its INSERT arm (its
               dead-tuple ratio is zero), which is why each row re-derives its own arm rather than inheriting the
               subject's: a shared arm would have put appendonly's dead tuples under a slope its line never
               governed. Figures are the un-prefixed backlog metadata names, so a reader who knows the fact's
               metadata knows these. */
            FactRankedTests.AssertInvariant(backlog);
            Assert.Equal(2, backlog.Ranked.Count);
            Assert.Equal("public.hot", backlog.Ranked[0].ObjectName);
            Assert.Equal("appdb", backlog.Ranked[0].DatabaseName);
            Assert.Equal(5.0, backlog.Ranked[0].Value, precision: 6);
            Assert.Equal(5.0, backlog.Ranked[0].Figures![PgTargetScorer.BacklogRatioKey], precision: 6);
            Assert.Equal(3.0, backlog.Ranked[0].Figures[PgTargetScorer.BacklogHoursKey], precision: 6);
            Assert.Equal((5_250 - 2_000) / 3.0, backlog.Ranked[0].Figures[PgTargetScorer.BacklogSlopePerHourKey], precision: 6);
            Assert.Equal("public.appendonly", backlog.Ranked[1].ObjectName);
            Assert.Equal(50_000 / 21_000.0, backlog.Ranked[1].Value, precision: 6);
            Assert.Equal(50_000 / 21_000.0, backlog.Ranked[1].Figures![PgTargetScorer.BacklogRatioKey], precision: 6);
            Assert.Equal(2.0, backlog.Ranked[1].Figures[PgTargetScorer.BacklogHoursKey], precision: 6);
            Assert.Equal((50_000 - 30_000) / 2.0, backlog.Ranked[1].Figures[PgTargetScorer.BacklogSlopePerHourKey], precision: 6);

            var wraparound = Assert.Single(facts, f => f.Key == PgTargetFactKeys.WraparoundTrend);
            Assert.Equal("appdb", wraparound.DatabaseName);
            Assert.Equal(220_000_000, wraparound.Value);
            Assert.Equal(0, wraparound.Metadata[PgTargetScorer.WraparoundCounterIsMultiXactKey]);
            Assert.Equal(1, wraparound.Metadata[PgTargetScorer.WraparoundArmKey]);
            Assert.Equal(0, wraparound.Metadata[PgTargetScorer.WraparoundXidKeepingUpKey]);
            Assert.Equal(200_000_000, wraparound.Metadata[PgTargetScorer.WraparoundFreezeMaxAgeKey]);
            Assert.Equal(5_000_000, wraparound.Metadata[PgTargetScorer.WraparoundSlopePerHourKey], precision: 3);
            Assert.Equal(1, wraparound.Metadata[PgTargetScorer.WraparoundTimeToWallComputableKey]);
            Assert.Equal((2_147_483_648L - 220_000_000L) / 5_000_000.0, wraparound.Metadata[PgTargetScorer.WraparoundHoursToWallKey], precision: 3);
            Assert.Equal(2, wraparound.Metadata[PgTargetScorer.WraparoundDatabasesKey]);
            Assert.Equal(1, wraparound.Metadata[PgTargetScorer.WraparoundDatabasesGradedKey]);
            Assert.Equal(49, wraparound.Metadata[PgTargetScorer.WraparoundSamplesKey]);

            var hold = Assert.Single(facts, f => f.Key == PgTargetFactKeys.XminHold);
            Assert.Equal("session:4242", hold.ObjectName);
            Assert.Equal(60_000_000, hold.Value);
            Assert.Equal(PgTargetAdvice.HolderSourceCode("session"), hold.Metadata[PgTargetScorer.XminHolderSourceKey]);
            Assert.Equal(41, hold.Metadata[PgTargetScorer.XminObservationsTotalKey]);
            Assert.Equal(41, hold.Metadata[PgTargetScorer.XminHeldCapturesKey]);
            Assert.Equal(1.0, hold.Metadata[PgTargetScorer.XminHeldFractionKey], precision: 6);
            Assert.Equal(60_000_000, hold.Metadata[PgTargetScorer.XminHorizonFloorAgeKey]);
            Assert.Equal(70_000_000, hold.Metadata[PgTargetScorer.XminHorizonRunPeakAgeKey]);
            Assert.Equal(2, hold.Metadata[PgTargetScorer.XminDistinctHoldersKey]);
            Assert.Equal(31.0 / 41, hold.Metadata[PgTargetScorer.XminModalHolderShareKey], precision: 6);
            Assert.Equal(1, hold.Metadata[PgTargetScorer.XminHolderAttributedKey]);
            Assert.Equal(70_000_000, hold.Metadata[PgTargetScorer.XminPeakWinningAgeKey]);
            Assert.Equal(200_000_000, hold.Metadata[PgTargetScorer.XminFreezeMaxAgeKey]);
            Assert.Equal(0, hold.Metadata[PgTargetScorer.XminMinutesSinceLastHolderKey], precision: 3);

            /* Scored: all three above the story threshold — the wraparound at the alert's Warning (0.5 ramp
               start, 220M of a 200M→400M ramp = 0.55), the backlog at 5× (0.72), the hold at 60M on a 50M→200M
               ramp (0.53); every amplifier co-fire matched. */
            new FactScorer().ScoreAll(facts);
            Assert.Equal(0.55, wraparound.BaseSeverity, precision: 6);
            Assert.InRange(backlog.BaseSeverity, 0.72, 0.73);
            Assert.InRange(hold.BaseSeverity, 0.53, 0.54);
            /* Lineage: the backlog's persistence gate and critical multiple are fleet-measured (#3691, 2026-09-19)
               and its concerning line is the engine's, so the fact says 1; the wraparound and hold grade on
               engine-defined bars only and carry no flag. */
            Assert.Equal(1, backlog.Metadata["threshold_lineage"]);
            Assert.False(wraparound.Metadata.ContainsKey("threshold_lineage"));
            Assert.False(hold.Metadata.ContainsKey("threshold_lineage"));
            Assert.Contains(backlog.AmplifierResults, a => a.Matched && a.Description.Contains("running and losing", StringComparison.Ordinal));
            Assert.Contains(wraparound.AmplifierResults, a => a.Matched && a.Description.Contains("PG_XMIN_HOLD co-fired", StringComparison.Ordinal));

            /* ── THE EXIT CRITERION: the real analyze_server, ONE story for the three. The mesh roots at the
               highest amplified severity: backlog 0.72 × (1 + 0.3 rising-while-running + 0.3 hold) = 1.16 leads
               the wraparound 0.55 × 1.8 = 0.99 and the hold 0.53 × 1.8 = 0.96, so the walk is
               backlog → wraparound → hold; the unit tests pin the wraparound-led order. */
            var service = new DarlingAnalysisService(postgres);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var chain = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.AutovacuumBacklog);
                Assert.Equal(
                    $"{PgTargetFactKeys.AutovacuumBacklog} → {PgTargetFactKeys.WraparoundTrend} → {PgTargetFactKeys.XminHold}",
                    chain.GetProperty("story_path").GetString());
                Assert.Equal(3, chain.GetProperty("fact_count").GetInt32());
                Assert.Equal(PgTargetSources.VacuumSource, chain.GetProperty("category").GetString());
                Assert.Equal(5.0, chain.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 6);
                Assert.Equal(PgTargetFactKeys.XminHold, chain.GetProperty("leaf_fact").GetProperty("key").GetString());

                /* Consumed by the chain: neither the wraparound nor the hold roots a second story. */
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() is PgTargetFactKeys.WraparoundTrend or PgTargetFactKeys.XminHold);

                var advice = chain.GetProperty("advice");
                Assert.Contains("public.hot in appdb carries 5,250 dead tuples, 5× its own autovacuum trigger line, for 4 consecutive samples", advice.GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.Contains("rose at 1,083 per hour", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                /* #3691 lane 43 (M3): the clause the retired one-row limit could not write. Before it the card
                   said "1 other table is persistently past their line too" and the operator had to call
                   get_pg_autovacuum to learn which; the population sentence still says that, and now the
                   persistence sentence names the table with its own ratio, span and slope. */
                Assert.Contains(
                    "; and one more: public.appendonly (2.4× for 2 hours, 10,000/h and rising). ",
                    advice.GetProperty("investigation").GetString(),
                    StringComparison.Ordinal);
                Assert.Contains("autovacuum ran on the table 2 times", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("PG_XMIN_HOLD co-fired", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("Resolve PG_XMIN_HOLD first", advice.GetProperty("remediation").GetString(), StringComparison.Ordinal);
                Assert.Contains("Never switch autovacuum off", advice.GetProperty("remediation").GetString(), StringComparison.Ordinal);

                /* ── #3691 lane 43: the payload half, through the REAL tool. The backlog root ranked two
                   tables, so root_fact carries `ranked` — the machine-readable twin of the advice clause
                   asserted above, with each entry's own figures under the family's own metadata names. */
                var ranked = chain.GetProperty("root_fact").GetProperty("ranked").EnumerateArray().ToList();
                Assert.Equal(2, ranked.Count);
                Assert.Equal("public.hot", ranked[0].GetProperty("object_name").GetString());
                Assert.Equal("appdb", ranked[0].GetProperty("database_name").GetString());
                Assert.Equal(5.0, ranked[0].GetProperty("value").GetDouble(), precision: 6);
                Assert.Equal(5.0, ranked[0].GetProperty("figures").GetProperty(PgTargetScorer.BacklogRatioKey).GetDouble(), precision: 6);
                Assert.Equal("public.appendonly", ranked[1].GetProperty("object_name").GetString());

                /* BYTE-IDENTITY, the other arm: the wraparound hop and the xmin-hold LEAF are non-migrated
                   families whose facts rank nothing, and their payload objects carry no `ranked` key at all —
                   not a null. The whole finding holds exactly ONE, the root's, which is what "attached only
                   when owed" means in bytes rather than in prose. */
                Assert.False(chain.GetProperty("leaf_fact").TryGetProperty("ranked", out _));
                Assert.Equal(1, CountOf(chain.GetRawText(), "\"ranked\""));

                /* The next_tools are PostgreSQL reads, never a SQL Server one. */
                var tools = chain.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains("get_pg_autovacuum_health", tools);
                Assert.Contains("get_pg_wraparound_risk", tools);
                Assert.Contains("get_pg_xmin_horizon", tools);
                Assert.DoesNotContain(tools, t => t.StartsWith("get_wait", StringComparison.Ordinal));
            }

            /* The facts read shows the family under its source, with the lineage flag on the unmeasured bar. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.VacuumSource);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var root = doc.RootElement;
                /* total_facts is the pass's whole set (the registry metadata fact + these three + the memory family's stock
                   honesty arm, PG_HOST_MEMORY_PRESSURE unavailable, lane 32); shown is the source-filtered count. */
                Assert.Equal(5, root.GetProperty("total_facts").GetInt32());
                Assert.Equal(3, root.GetProperty("shown").GetInt32());
                var keys = root.GetProperty("facts").EnumerateArray().Select(f => f.GetProperty("key").GetString()).ToList();
                Assert.Contains(PgTargetFactKeys.AutovacuumBacklog, keys);
                Assert.Contains(PgTargetFactKeys.WraparoundTrend, keys);
                Assert.Contains(PgTargetFactKeys.XminHold, keys);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #3691 step 22 (design §3.1), the exit criterion: one table with <c>autovacuum_enabled = off</c> four times
    /// past its own line for six hours, one disabled table that is quiet, one ENABLED table twice past its line
    /// → exactly one <c>CONFIG_PG_AUTOVACUUM_DISABLED</c> fact naming the first (the quiet one is not a finding;
    /// the enabled one is the backlog fact's business), and through the REAL <c>analyze_server</c> ONE story,
    /// <c>PG_AUTOVACUUM_BACKLOG → CONFIG_PG_AUTOVACUUM_DISABLED</c> — the backlog leads (its reloption
    /// amplifier lifts it past the card's flat 0.9) and the same-table edge carries the walk to the named cause.
    /// </summary>
    [Fact]
    public async Task ADisabledTablePastItsLine_IsItsOwnCard_AndJoinsTheBacklogsStoryThroughAnalyzeServer()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the disabled-table e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-6);

            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 6 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* ── pg_autovacuum_stats, hourly, seven samples spanning six hours:
               frozen — autovacuum_enabled = off, 4,200 dead against a 1,050 line (4×) at every sample, never vacuumed.
               quiet  — autovacuum_enabled = off, 100 dead against 1,050: disabled and under its line → not a finding.
               hot    — enabled, 2,100 dead against 1,050 (2×): a backlog, but not this card's. */
            for (var h = 0; h <= 6; h++)
            {
                var at = windowStart.AddHours(h);
                await PlantAutovacuumAsync(connection, at, "public", "frozen", live: 10_000, dead: 4_200, vacuumThreshold: 1_050,
                    inserts: 0, insertThreshold: -1, disabled: true, lastAutovacuum: null, autovacuumCount: 0, ct);
                await PlantAutovacuumAsync(connection, at, "public", "quiet", live: 10_000, dead: 100, vacuumThreshold: 1_050,
                    inserts: 0, insertThreshold: -1, disabled: true, lastAutovacuum: null, autovacuumCount: 0, ct);
                await PlantAutovacuumAsync(connection, at, "public", "hot", live: 10_000, dead: 2_100, vacuumThreshold: 1_050,
                    inserts: 0, insertThreshold: -1, disabled: false, lastAutovacuum: at.AddMinutes(-20), autovacuumCount: 10, ct);
            }

            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);
            Assert.False(context.Coverage!.IsPartial);

            /* Exactly one disabled card, naming frozen; quiet is absent everywhere. */
            var disabled = Assert.Single(facts, f => f.Key == PgTargetFactKeys.ConfigAutovacuumDisabled);
            Assert.Equal(PgTargetSources.VacuumSource, disabled.Source);
            Assert.Equal("appdb", disabled.DatabaseName);
            Assert.Equal("public.frozen", disabled.ObjectName);
            Assert.Equal(4.0, disabled.Value, precision: 6);
            Assert.Equal(4.0, disabled.Metadata[PgTargetScorer.BacklogRatioKey], precision: 6);
            Assert.Equal(0, disabled.Metadata[PgTargetScorer.BacklogArmIsInsertKey]);
            Assert.Equal(7, disabled.Metadata[PgTargetScorer.BacklogTrailingSamplesKey]);
            Assert.Equal(7, disabled.Metadata[PgTargetScorer.BacklogSamplesInWindowKey]);
            Assert.Equal(6.0, disabled.Metadata[PgTargetScorer.BacklogHoursKey], precision: 6);
            Assert.Equal(4_200, disabled.Metadata[PgTargetScorer.BacklogDeadTuplesKey]);
            Assert.Equal(1_050, disabled.Metadata[PgTargetScorer.BacklogVacuumThresholdKey]);
            Assert.Equal(10_000, disabled.Metadata[PgTargetScorer.BacklogLiveTuplesKey]);
            Assert.Equal(1, disabled.Metadata[PgTargetScorer.BacklogTableAutovacuumDisabledKey]);
            Assert.Equal(1, disabled.Metadata[PgTargetScorer.AutovacuumDisabledTablesKey]);
            Assert.Equal(0, disabled.Metadata[PgTargetScorer.AutovacuumDisabledServerOffKey]);
            Assert.False(disabled.Metadata.ContainsKey(PgTargetScorer.BacklogHoursSinceLastAutovacuumKey));
            /* #3691 lane 43: one disabled table met the gate, so the read returned ONE row — Ranked holds the
               subject alone (the [0] == ObjectName/Value invariant) and nothing else, which is the shape the
               retired disabled_rank_2_ratio key used to prove by its absence. Below two entries the advice
               clause is empty and the payload omits `ranked` entirely. */
            FactRankedTests.AssertInvariant(disabled);
            var rankedDisabled = Assert.Single(disabled.Ranked);
            Assert.Equal("public.frozen", rankedDisabled.ObjectName);
            Assert.Equal(4.0, rankedDisabled.Value, precision: 6);
            Assert.Equal(4.0, rankedDisabled.Figures![PgTargetScorer.BacklogRatioKey], precision: 6);
            Assert.Equal(6.0, rankedDisabled.Figures[PgTargetScorer.BacklogHoursKey], precision: 6);
            Assert.DoesNotContain(facts, f => f.ObjectName == "public.quiet");

            /* The backlog fact ranks disabled tables first, so it names frozen too, and counts hot beside it. */
            var backlog = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AutovacuumBacklog);
            Assert.Equal("public.frozen", backlog.ObjectName);
            Assert.Equal(1, backlog.Metadata[PgTargetScorer.BacklogTableAutovacuumDisabledKey]);
            Assert.Equal(2, backlog.Metadata[PgTargetScorer.BacklogTablesKey]);

            /* #3691 lane 43 (M3): the disabled table leads the rank (the read's ORDER BY puts
               autovacuum_disabled first) and hot rides second, so the backlog card names it. hot's dead tuples
               are flat across its run, which is a ZERO slope and says so — an absent slope figure means the run
               had no span to divide by, and the two must not read alike. */
            FactRankedTests.AssertInvariant(backlog);
            Assert.Equal(2, backlog.Ranked.Count);
            Assert.Equal("public.frozen", backlog.Ranked[0].ObjectName);
            Assert.Equal(4.0, backlog.Ranked[0].Value, precision: 6);
            Assert.Equal("public.hot", backlog.Ranked[1].ObjectName);
            Assert.Equal(2.0, backlog.Ranked[1].Value, precision: 6);
            Assert.Equal(6.0, backlog.Ranked[1].Figures![PgTargetScorer.BacklogHoursKey], precision: 6);
            Assert.Equal(0.0, backlog.Ranked[1].Figures[PgTargetScorer.BacklogSlopePerHourKey], precision: 6);

            /* Scored: the card at its flat band with lineage 1; the backlog at 4× (0.67) lifted by the reloption
               boost past it, so the backlog leads. */
            new FactScorer().ScoreAll(facts);
            Assert.Equal(PgTargetScorer.AutovacuumDisabledBaseSeverity, disabled.BaseSeverity);
            Assert.Equal(disabled.BaseSeverity, disabled.Severity);
            Assert.Equal(1, disabled.Metadata["threshold_lineage"]);
            Assert.InRange(backlog.BaseSeverity, 0.66, 0.67);
            Assert.Contains(backlog.AmplifierResults, a => a.Matched && a.Description.Contains("autovacuum_enabled is OFF", StringComparison.Ordinal));
            Assert.True(backlog.Severity > disabled.Severity);

            var cardAdvice = FactAdvice.Compose(PgTargetFactKeys.ConfigAutovacuumDisabled, facts.ToFactLookup())!;
            Assert.Equal("public.frozen in appdb has autovacuum_enabled = off and sits at 4× its own autovacuum trigger line for 6 hours", cardAdvice.Headline);
            Assert.Contains("never run on this table", cardAdvice.Investigation, StringComparison.Ordinal);
            Assert.Contains("ALTER TABLE public.frozen SET (autovacuum_enabled = true);", cardAdvice.Remediation, StringComparison.Ordinal);

            /* ── THE EXIT CRITERION: the real analyze_server, one story, backlog → disabled card, no second root. */
            var service = new DarlingAnalysisService(postgres);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 6);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var chain = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.AutovacuumBacklog);
                Assert.Equal(
                    $"{PgTargetFactKeys.AutovacuumBacklog} → {PgTargetFactKeys.ConfigAutovacuumDisabled}",
                    chain.GetProperty("story_path").GetString());
                Assert.Equal(2, chain.GetProperty("fact_count").GetInt32());
                Assert.Equal(PgTargetFactKeys.ConfigAutovacuumDisabled, chain.GetProperty("leaf_fact").GetProperty("key").GetString());
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConfigAutovacuumDisabled);

                var advice = chain.GetProperty("advice");
                /* The tool anchors its six hours at NOW, so the first planted sample (a minute before now − 6 h) falls
                   outside it: six samples in the tool's window, seven in the collector context above. */
                Assert.Equal("public.frozen in appdb carries 4,200 dead tuples, 4× its own autovacuum trigger line, for 6 consecutive samples", advice.GetProperty("headline").GetString());
                Assert.Contains("ALTER TABLE public.frozen SET (autovacuum_enabled = true);", advice.GetProperty("remediation").GetString(), StringComparison.Ordinal);

                /* #3691 lane 43: the backlog root ranked frozen and hot, so root_fact names them; the
                   DISABLED leaf ranked only its own subject (one disabled table met the gate), so its payload
                   carries no `ranked` at all — the two-or-more rule, proved on one response. */
                Assert.Equal(
                    new[] { "public.frozen", "public.hot" },
                    chain.GetProperty("root_fact").GetProperty("ranked").EnumerateArray()
                        .Select(o => o.GetProperty("object_name").GetString()!).ToArray());
                Assert.False(chain.GetProperty("leaf_fact").TryGetProperty("ranked", out _));
                Assert.Equal(1, CountOf(chain.GetRawText(), "\"ranked\""));

                var tools = chain.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains("get_pg_autovacuum_health", tools);
                Assert.Contains("get_pg_table_bloat", tools);
                Assert.All(tools, t => Assert.StartsWith("get_pg_", t, StringComparison.Ordinal));
            }

            /* The facts read: the registry fact plus these two under pg_vacuum, plus the memory family's stock honesty arm
               (PG_HOST_MEMORY_PRESSURE unavailable, lane 32 — a stock target has no host-memory source) — the quiet table
               is nowhere. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 6, PgTargetSources.VacuumSource);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var root = doc.RootElement;
                Assert.Equal(4, root.GetProperty("total_facts").GetInt32());
                Assert.Equal(2, root.GetProperty("shown").GetInt32());
                var shown = root.GetProperty("facts").EnumerateArray().ToList();
                var card = Assert.Single(shown, f => f.GetProperty("key").GetString() == PgTargetFactKeys.ConfigAutovacuumDisabled);
                Assert.Equal(4.0, card.GetProperty("value").GetDouble(), precision: 6);
                Assert.Equal(PgTargetScorer.AutovacuumDisabledBaseSeverity, card.GetProperty("base_severity").GetDouble(), precision: 4);
                Assert.Equal(1, card.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
                Assert.Equal(1, card.GetProperty("metadata").GetProperty(PgTargetScorer.AutovacuumDisabledTablesKey).GetDouble());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task PlantAutovacuumAsync(
        NpgsqlConnection connection, DateTime at, string schema, string table, long live, long dead, long vacuumThreshold,
        long inserts, long insertThreshold, bool disabled, DateTime? lastAutovacuum, long autovacuumCount, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_autovacuum_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name,
     live_tuples, dead_tuples, mods_since_analyze, inserts_since_vacuum, vacuum_threshold, insert_vacuum_threshold,
     analyze_threshold, autovacuum_disabled, total_bytes, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
     vacuum_count, autovacuum_count, analyze_count, autoanalyze_count)
VALUES ($1, $2, $3, $4, 'appdb', $5, $6, $7, $8, 0, $9, $10, $11, 0, $12, 8192000, NULL, $13, NULL, NULL, 0, $14, 0, 0)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(live);
        command.Parameters.AddWithValue(dead);
        command.Parameters.AddWithValue(inserts);
        command.Parameters.AddWithValue(vacuumThreshold);
        command.Parameters.AddWithValue(insertThreshold);
        command.Parameters.AddWithValue(disabled);
        command.Parameters.Add(new NpgsqlParameter { Value = lastAutovacuum.HasValue ? lastAutovacuum.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        command.Parameters.AddWithValue(autovacuumCount);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWraparoundAsync(NpgsqlConnection connection, DateTime at, string database, long xidAge, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_wraparound_stats
    (collection_id, collection_time, server_id, server_name, database_name, frozen_xid_age, min_multixid_age,
     autovacuum_freeze_max_age, autovacuum_multixact_freeze_max_age, pct_toward_emergency_vacuum, pct_toward_wraparound,
     pct_toward_multixact_emergency, pct_toward_multixact_wraparound, xids_remaining, multixids_remaining, allows_connections)
VALUES ($1, $2, $3, $4, $5, $6, 1000, 200000000, 400000000, 0, 0, 0, 0, $7, 2147482648, TRUE)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(xidAge);
        command.Parameters.AddWithValue(2_147_483_648L - xidAge);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantXminAsync(
        NpgsqlConnection connection, DateTime at, string source, string holder, string? detail, long xminAge, bool isWinner, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_xmin_horizon (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(source);
        command.Parameters.AddWithValue(xminAge);
        command.Parameters.AddWithValue(holder);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)detail ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.AddWithValue(isWinner);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Occurrences of <paramref name="needle"/> in <paramref name="text"/> — for the #3691 lane 43
    /// byte-identity arm, which is a claim about how many times a KEY appears in a response and cannot be made
    /// with an Assert.Contains.</summary>
    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_autovacuum_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_wraparound_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_xmin_horizon WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
