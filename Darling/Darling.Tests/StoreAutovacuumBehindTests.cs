/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/* #7368: the Store Autovacuum Behind self-alert. The edge is pinned through the same evaluator seams as its siblings. */
public sealed class StoreAutovacuumBehindTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static StoreAutovacuumReading Behind(
        int tables = 3, string? worst = "public.big_hourly", double ratio = 23.7, int inUse = 3, int max = 3,
        double? age = 2700, string? table = "public.big_hourly", int version = 170000) =>
        new(tables, worst, ratio, inUse, max, age, table, version);

    private static StoreAutovacuumReading Caught() => new(0, null, 0, 1, 3, 12, "public.t", 170000);

    [Fact]
    public async Task OneTrueRead_DoesNotFire_TheSecondFires_AndTheFirstFalseReadResolves()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        h.Now = h.Now.AddHours(1);
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.AutovacuumBehindMetric, fired.MetricName);
        Assert.Equal(DarlingSelfAlertEvaluator.StoreServerLabel, fired.ServerName);
        Assert.Equal("autovacuum", fired.ServerKey);
        Assert.Empty(h.History.Records);

        h.Now = h.Now.AddHours(1);
        await e.ApplyAutovacuumBehindAsync(Caught(), true, Ct);
        var resolution = Assert.Single(h.History.Records);
        Assert.Equal(DarlingSelfAlertEvaluator.AutovacuumBehindResolvedMetric, resolution.MetricName);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ATrueReadThenAFalseOne_StartsTheCountOver()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        await e.ApplyAutovacuumBehindAsync(Caught(), true, Ct);
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task AFailedRead_ChangesNothing_NeitherCountedNorResolving()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        /* Between two true reads: a null reading and a read against a store that refuses the connection. The count survives both. */
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        await e.ApplyAutovacuumBehindAsync(null, true, Ct);
        await using (var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Timeout=2;Pooling=false"))
        {
            await e.EvaluateAutovacuumBehindAsync(dead, true, Ct);
        }

        Assert.Empty(h.Deliverer.Outcomes);
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* A standing alert is not resolved by a failed read either. */
        await e.ApplyAutovacuumBehindAsync(null, true, Ct);
        await using (var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Timeout=2;Pooling=false"))
        {
            await e.EvaluateAutovacuumBehindAsync(dead, true, Ct);
        }

        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task AStandingCondition_ReFiresOnlyAfterTheCooldown_AndOnlyWhenItWorsens()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Worse, but inside the cooldown: nothing. */
        await e.ApplyAutovacuumBehindAsync(Behind(tables: 4), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* The read runs hourly, so the next one is an hour on. */
        h.Now = h.Now.AddHours(1);
        await e.ApplyAutovacuumBehindAsync(Behind(tables: 4), true, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AStandingConditionThatDoesNotWorsen_SendsNothingOnTheNextHourlyReads()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        h.Now = h.Now.AddHours(1);
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* A day of hourly reads at the same count, and a ratio that creeps up under the step (23.7 to 35): nothing more is sent. */
        for (var hour = 0; hour < 24; hour++)
        {
            h.Now = h.Now.AddHours(1);
            await e.ApplyAutovacuumBehindAsync(Behind(ratio: 23.7 + hour * 0.45), true, Ct);
        }

        Assert.Single(h.Deliverer.Outcomes);
    }

    [Theory]
    [InlineData(4, 23.7, true)]    // one more table over the line
    [InlineData(3, 35.6, true)]    // the worst ratio at 1.5 times the reported one (35.55)
    [InlineData(3, 35.0, false)]   // just under the step
    [InlineData(2, 23.7, false)]   // fewer tables
    public async Task AStandingCondition_ReFiresWhenTheCountOrTheWorstRatioStepsUp(int tables, double ratio, bool reFires)
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddHours(1);
        await e.ApplyAutovacuumBehindAsync(Behind(tables: tables, ratio: ratio), true, Ct);

        Assert.Equal(reFires ? 2 : 1, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AfterTheResolve_ANewEntryFiresAgainOnTheSecondRead_EvenAtALowerLevel()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyAutovacuumBehindAsync(Behind(tables: 6, ratio: 90), true, Ct);
        await e.ApplyAutovacuumBehindAsync(Behind(tables: 6, ratio: 90), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddHours(1);
        await e.ApplyAutovacuumBehindAsync(Caught(), true, Ct);

        h.Now = h.Now.AddHours(1);
        await e.ApplyAutovacuumBehindAsync(Behind(tables: 1, ratio: 4), true, Ct);
        h.Now = h.Now.AddHours(1);
        var before = h.Deliverer.Outcomes.Count;
        await e.ApplyAutovacuumBehindAsync(Behind(tables: 1, ratio: 4), true, Ct);

        Assert.Equal(before + 1, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public void AWorstTableWithNoRatio_IsNamedByItsThresholdOfZero_NeverAtZeroTimes()
    {
        var reading = Behind(ratio: 0) with { WorstRatio = null };

        var detail = DarlingSelfAlertEvaluator.BuildAutovacuumDetail(reading, true);
        var shortMessage = DarlingSelfAlertEvaluator.BuildAutovacuumShortMessage(reading);

        Assert.Contains("public.big_hourly whose threshold is 0", detail, StringComparison.Ordinal);
        Assert.Contains("worst public.big_hourly whose analyze threshold is 0", shortMessage, StringComparison.Ordinal);
        Assert.DoesNotContain("0 times", detail, StringComparison.Ordinal);
        Assert.DoesNotContain("0 times", shortMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void WhenAutovacuumIsOff_TheTextSaysSoAndAdvisesTurningItOn_NotRaisingWorkers()
    {
        var off = Behind() with { AutovacuumOn = false };

        var managed = DarlingSelfAlertEvaluator.BuildAutovacuumDetail(off, true);
        var byo = DarlingSelfAlertEvaluator.BuildAutovacuumDetail(off, false);
        var shortMessage = DarlingSelfAlertEvaluator.BuildAutovacuumShortMessage(off);

        Assert.Contains("Autovacuum is off on the store", managed, StringComparison.Ordinal);
        Assert.Contains("set autovacuum = on", managed, StringComparison.Ordinal);
        Assert.Contains("ALTER SYSTEM", managed, StringComparison.Ordinal);
        Assert.DoesNotContain("Raise autovacuum_vacuum_cost_limit", managed, StringComparison.Ordinal);
        Assert.Contains("Change it on the store's PostgreSQL.", byo, StringComparison.Ordinal);
        Assert.Contains("autovacuum is off and behind", shortMessage, StringComparison.Ordinal);
        Assert.Contains("Raise autovacuum_vacuum_cost_limit",
            DarlingSelfAlertEvaluator.BuildAutovacuumDetail(Behind(), true), StringComparison.Ordinal);
        Assert.DoesNotContain(';', managed + byo + shortMessage);
    }

    [Fact]
    public async Task MasterAlertsOff_CountsNothingAndFiresNothing()
    {
        var h = new DarlingSelfAlertTests.Harness();
        h.Settings.AlertsEnabled = false;
        var e = h.Build();

        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        h.Settings.AlertsEnabled = true;
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task TheText_NamesTheCountWorstTableRatioWorkersAndLongestWorker()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);
        await e.ApplyAutovacuumBehindAsync(Behind(), true, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("3 tables", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("public.big_hourly at 23.7 times its threshold", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("3 of 3 autovacuum workers", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("The longest has been on public.big_hourly for 45 min", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("Raise autovacuum_vacuum_cost_limit and autovacuum_max_workers", fired.DetailText, StringComparison.Ordinal);
        Assert.Equal("3", fired.CurrentValue);
    }

    [Theory]
    [InlineData(170000, false)]
    [InlineData(180000, true)]
    public void TheRestartAdvice_FollowsTheServerVersion(int versionNum, bool reloadOnly)
    {
        var text = DarlingSelfAlertEvaluator.BuildAutovacuumDetail(Behind(version: versionNum), true);

        Assert.Equal(reloadOnly, text.Contains("autovacuum_worker_slots", StringComparison.Ordinal));
        Assert.Equal(!reloadOnly, text.Contains("takes a restart", StringComparison.Ordinal));
    }

    [Fact]
    public void TheWhereToChangeIt_DiffersForTheManagedStoreAndABringYourOwnStore()
    {
        var managed = DarlingSelfAlertEvaluator.BuildAutovacuumDetail(Behind(), managedStore: true);
        var byo = DarlingSelfAlertEvaluator.BuildAutovacuumDetail(Behind(), managedStore: false);

        Assert.Contains("ALTER SYSTEM", managed, StringComparison.Ordinal);
        Assert.Contains("below the include line in postgresql.conf", managed, StringComparison.Ordinal);
        Assert.Contains("Change them on the store's PostgreSQL.", byo, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER SYSTEM", byo, StringComparison.Ordinal);
    }

    [Fact]
    public void ARoleThatCannotSeeOtherSessions_StillGetsTheCountAndLeavesTheTableOut()
    {
        var noQueryText = DarlingSelfAlertEvaluator.BuildAutovacuumDetail(Behind(table: null), true);
        var nothingVisible = DarlingSelfAlertEvaluator.BuildAutovacuumDetail(Behind(age: null, table: null), false);

        Assert.Contains("3 of 3 autovacuum workers", noQueryText, StringComparison.Ordinal);
        Assert.Contains("The longest has been running for 45 min", noQueryText, StringComparison.Ordinal);
        Assert.Contains("not shown", noQueryText, StringComparison.Ordinal);
        Assert.Contains("3 of 3 autovacuum workers", nothingVisible, StringComparison.Ordinal);
        Assert.Contains("not shown", nothingVisible, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePublicText_HasNoEmDashAndNoSemicolon()
    {
        foreach (var managed in new[] { true, false })
        {
            foreach (var version in new[] { 170000, 180000 })
            {
                var text = DarlingSelfAlertEvaluator.BuildAutovacuumDetail(Behind(version: version), managed)
                    + DarlingSelfAlertEvaluator.BuildAutovacuumShortMessage(Behind());
                Assert.DoesNotContain('—', text);
                Assert.DoesNotContain(';', text);
            }
        }
    }

    [Theory]
    [InlineData("autovacuum: VACUUM ANALYZE public.t", "public.t")]
    [InlineData("autovacuum: ANALYZE _timescaledb_internal._hyper_5_90_chunk", "_timescaledb_internal._hyper_5_90_chunk")]
    [InlineData("autovacuum: VACUUM collect.q (to prevent wraparound)", "collect.q")]
    [InlineData("<insufficient privilege>", null)]
    [InlineData("<command string not enabled>", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void TheWorkersTable_IsReadFromItsQueryText_AndAbsentWhenTheTextIsHidden(string? query, string? expected) =>
        Assert.Equal(expected, StoreAutovacuumReader.ParseWorkerTable(query));

    [Theory]
    [InlineData(40, "40 s")]
    [InlineData(2700, "45 min")]
    [InlineData(7500, "2 h 5 min")]
    [InlineData(7200, "2 h")]
    public void TheWorkersAge_ReadsInTheLargestUnit(double seconds, string expected) =>
        Assert.Equal(expected, StoreAutovacuumReader.FormatAge(seconds));
}

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. */
public sealed class StoreAutovacuumReadLiveTests
{
    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the autovacuum read live test.");
        return connectionString!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<double> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToDouble(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task TheRead_ReportsOnlyABigRegularTablePastThreeTimesItsOwnThreshold()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct, plainPostgres: true);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);

            /* The tables that must be reported fall behind for real: autovacuum is on and they are over their own threshold. A table
               autovacuum is not told to skip cannot be held back by a reloption, and the cluster's settings are not this test's to
               change, so a second session holds SHARE UPDATE EXCLUSIVE on them (after the manual ANALYZE, before the changes). That is
               the lock autovacuum's ANALYZE needs, and it skips a table it cannot lock, so n_mod_since_analyze stays put. */

            /* big_t: over 1M rows and its OWN small scale factor, so a modest number of changes passes 3x of
               a threshold of 100 + 0.0001 * reltuples (about 220). The server's default threshold (50 + 0.1 * reltuples, about
               110,000) would put the same changes far under the line, so a read that used the settings would not report it. */
            await ExecAsync(connection,
                "CREATE TABLE big_t (id int, v int) WITH (autovacuum_analyze_threshold = 100, autovacuum_analyze_scale_factor = 0.0001)", ct);
            /* zero_t: a threshold of 0 (no ratio, it is NULL), so any change is past the line. It must not become the worst table. */
            await ExecAsync(connection,
                "CREATE TABLE zero_t (id int, v int) WITH (autovacuum_analyze_threshold = 0, autovacuum_analyze_scale_factor = 0)", ct);
            /* default_t: the same size and the same changes, no reloptions: the server settings give a threshold near 110,000. */
            await ExecAsync(connection, "CREATE TABLE default_t (id int, v int)", ct);
            /* small_t: far past 3x of its own threshold, but under 1M rows. */
            await ExecAsync(connection,
                "CREATE TABLE small_t (id int, v int) WITH (autovacuum_analyze_threshold = 10, autovacuum_analyze_scale_factor = 0.0001)", ct);
            /* A partitioned parent over 1M rows with changes: autovacuum never analyzes it, and it must never be reported. */
            await ExecAsync(connection, "CREATE TABLE part_parent (id int, v int) PARTITION BY RANGE (id)", ct);
            await ExecAsync(connection, "CREATE TABLE part_child PARTITION OF part_parent FOR VALUES FROM (0) TO (2000000)", ct);
            /* off_false_t and off_off_t: autovacuum is told to skip them, with more changes and the same small threshold as big_t, so
               they would be the worst table if they counted. They must never be reported: no alert advice can make autovacuum look. */
            await ExecAsync(connection,
                "CREATE TABLE off_false_t (id int, v int) WITH (autovacuum_enabled = false, autovacuum_analyze_threshold = 100, autovacuum_analyze_scale_factor = 0.0001)", ct);
            await ExecAsync(connection,
                "CREATE TABLE off_off_t (id int, v int) WITH (autovacuum_enabled = off, autovacuum_analyze_threshold = 100, autovacuum_analyze_scale_factor = 0.0001)", ct);

            foreach (var table in new[] { "big_t", "zero_t", "default_t", "part_parent", "off_false_t", "off_off_t" })
            {
                await ExecAsync(connection, $"INSERT INTO {table} SELECT g, 0 FROM generate_series(1, 1100000) AS g", ct);
            }

            await ExecAsync(connection, "INSERT INTO small_t SELECT g, 0 FROM generate_series(1, 100000) AS g", ct);
            await ExecAsync(connection,
                "ANALYZE big_t; ANALYZE zero_t; ANALYZE default_t; ANALYZE small_t; ANALYZE part_parent; ANALYZE off_false_t; ANALYZE off_off_t", ct);

            await using var holder = new NpgsqlConnection(scratch.ConnectionString);
            await holder.OpenAsync(ct);
            await ExecAsync(holder,
                "BEGIN; LOCK TABLE big_t, zero_t, default_t, small_t, part_child, off_false_t, off_off_t IN SHARE UPDATE EXCLUSIVE MODE", ct);

            /* Changes after the ANALYZE: 11,000 on each (1% of the rows), 33,000 on the two autovacuum-off tables. */
            foreach (var table in new[] { "big_t", "zero_t", "default_t", "small_t", "part_parent" })
            {
                await ExecAsync(connection, $"UPDATE {table} SET v = 1 WHERE id <= 11000", ct);
            }

            foreach (var table in new[] { "off_false_t", "off_off_t" })
            {
                await ExecAsync(connection, $"UPDATE {table} SET v = 1 WHERE id <= 33000", ct);
            }

            /* The counters reach the shared stats on a transaction end after a flush; force it so this read sees them. */
            await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
            await ExecAsync(connection, "SELECT 1", ct);

            var reltuples = await ScalarAsync(connection, "SELECT reltuples FROM pg_class WHERE relname = 'big_t'", ct);
            var changes = await ScalarAsync(connection, "SELECT n_mod_since_analyze FROM pg_stat_user_tables WHERE relname = 'big_t'", ct);
            Assert.True(changes >= 11000, $"the counters were not flushed (n_mod_since_analyze = {changes})");

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var reading = await StoreAutovacuumReader.ReadAsync(source, ct);

            /* big_t and zero_t count. The two autovacuum-off tables, default_t, small_t and the partitioned parent never do. */
            Assert.Equal(2, reading.TablesOverLine);
            /* zero_t has no ratio (NULL) and big_t does: the table with a ratio is the worst, never the NULL one. */
            Assert.Equal("public.big_t", reading.WorstTable);
            var threshold = 100 + 0.0001 * reltuples;
            Assert.NotNull(reading.WorstRatio);
            Assert.Equal(changes / threshold, reading.WorstRatio.Value, 3);
            Assert.True(reading.WorstRatio > 3);
            var autovacuumOn = await ScalarAsync(connection, "SELECT (current_setting('autovacuum') = 'on')::int", ct);
            Assert.Equal(autovacuumOn == 1, reading.AutovacuumOn);
            Assert.True(reading.MaxWorkers >= 1);
            Assert.True(reading.ServerVersionNum >= 140000);

            await ExecAsync(holder, "ROLLBACK", ct);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
