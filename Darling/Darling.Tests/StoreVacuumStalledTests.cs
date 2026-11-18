/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableHealthReader;

namespace Darling.Tests;

/* #8950: the Store Vacuum Stalled self-alert. The edge is pinned through the same evaluator seams as Store Autovacuum Behind, and the
   reading is built from rows with an explicit read time, so nothing depends on the clock. */
public sealed class StoreVacuumStalledTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly DateTime AsOf = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime LastCancel = new(2026, 7, 1, 10, 42, 0, DateTimeKind.Utc);

    private static StoreVacuumStalledReading Stalled(int behind = 2, int cancelled = 1, double hours = 31.5, long cancels = 5) =>
        new(behind + cancelled, behind, cancelled,
            behind > 0 ? "collect.wait_stats" : null, behind > 0 ? hours : null, false,
            cancelled > 0 ? "collect.query_stats" : null, cancelled > 0 ? cancels : 0, cancelled > 0 ? LastCancel : null);

    private static StoreVacuumStalledReading Clear() => new(0, 0, 0);

    private static StoreVacuumObjectRow Row(
        string table, long cancels = 0, DateTime? lastCancel = null, int past = 0, int pastNever = 0, DateTime? oldestVacuum = null,
        DateTime? statsSince = null) =>
        new("collect", table, KindTable, cancels, lastCancel, past, pastNever, oldestVacuum, statsSince);

    [Fact]
    public async Task OneStalledRead_DoesNotFire_TheSecondFires_AndTheFirstClearReadResolves()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        h.Now = h.Now.AddHours(1);
        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.VacuumStalledMetric, fired.MetricName);
        Assert.Equal("Store Vacuum Stalled", fired.MetricName);
        Assert.Equal(DarlingSelfAlertEvaluator.StoreServerLabel, fired.ServerName);
        Assert.Equal("vacuum-stalled", fired.ServerKey);
        Assert.Equal("3", fired.CurrentValue);
        Assert.Equal("0", fired.ThresholdValue);
        Assert.Equal(3, fired.NumericCurrentValue);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Empty(h.History.Records);

        h.Now = h.Now.AddHours(1);
        await e.ApplyVacuumStalledAsync(Clear(), true, Ct);
        var resolution = Assert.Single(h.History.Records);
        Assert.Equal(DarlingSelfAlertEvaluator.VacuumStalledResolvedMetric, resolution.MetricName);
        Assert.Single(h.Deliverer.Outcomes);

        /* A second clear read writes no second resolution. */
        h.Now = h.Now.AddHours(1);
        await e.ApplyVacuumStalledAsync(Clear(), true, Ct);
        Assert.Single(h.History.Records);
    }

    [Fact]
    public async Task AStalledReadThenAClearOne_StartsTheCountOver()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        await e.ApplyVacuumStalledAsync(Clear(), true, Ct);
        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task AFailedRead_ChangesNothing_IsCounted_AndNeitherCountsNorResolves()
    {
        var failures = new AlertReadFailureCounter();
        var h = new DarlingSelfAlertTests.Harness { ReadFailures = failures };
        var e = h.Build();

        /* Between two stalled reads: a null reading and a read against a store that refuses the connection. The count survives both. */
        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        await e.ApplyVacuumStalledAsync(null, true, Ct);
        await using (var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Timeout=2;Pooling=false"))
        {
            await e.EvaluateVacuumStalledAsync(dead, true, Ct);
        }

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Equal(1, failures.ReadInstance().ReadFailures);
        Assert.Equal("store vacuum stalled self-alert", failures.ReadInstance().LastFailureRead);
        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* A standing alert is not resolved by a failed read either. */
        await e.ApplyVacuumStalledAsync(null, true, Ct);
        await using (var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Timeout=2;Pooling=false"))
        {
            await e.EvaluateVacuumStalledAsync(dead, true, Ct);
        }

        Assert.Empty(h.History.Records);
        Assert.Equal(2, failures.ReadInstance().ReadFailures);
    }

    [Fact]
    public async Task MasterAlertsOff_ReadsNothing_CountsNothing_AndFiresNothing()
    {
        var failures = new AlertReadFailureCounter();
        var h = new DarlingSelfAlertTests.Harness { ReadFailures = failures };
        h.Settings.AlertsEnabled = false;
        var e = h.Build();

        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        await using (var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Timeout=2;Pooling=false"))
        {
            await e.EvaluateVacuumStalledAsync(dead, true, Ct);
        }

        Assert.Equal(0, failures.ReadInstance().ReadFailures);
        h.Settings.AlertsEnabled = true;
        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AWorseReadingInsideTheCooldown_SendsNothing_AndAnHourOnItReFires()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        await e.ApplyVacuumStalledAsync(Stalled(behind: 3), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* The read runs hourly, so the next one is an hour on. */
        h.Now = h.Now.AddHours(1);
        await e.ApplyVacuumStalledAsync(Stalled(behind: 3), true, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Theory]
    [InlineData(3, 1, true)]    // one more table behind
    [InlineData(2, 2, true)]    // one more table cancelled repeatedly
    [InlineData(2, 1, false)]   // the same tables
    [InlineData(1, 1, false)]   // fewer behind
    [InlineData(3, 0, true)]    // one more behind and one fewer cancelled: each kind is compared on its own
    public async Task AStandingCondition_ReFiresOnlyWhenMoreTablesAreBehindOrCancelled(int behind, int cancelled, bool reFires)
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddHours(1);
        await e.ApplyVacuumStalledAsync(Stalled(behind: behind, cancelled: cancelled), true, Ct);

        Assert.Equal(reFires ? 2 : 1, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AStandingConditionWhoseHoursAndCancelsGrow_SendsNothingOnADayOfHourlyReads()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        h.Now = h.Now.AddHours(1);
        await e.ApplyVacuumStalledAsync(Stalled(), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        for (var hour = 1; hour <= 24; hour++)
        {
            h.Now = h.Now.AddHours(1);
            await e.ApplyVacuumStalledAsync(Stalled(hours: 31.5 + hour, cancels: 5 + hour), true, Ct);
        }

        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AfterTheResolve_ANewStallFiresAgainOnTheSecondRead_EvenWithFewerTables()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyVacuumStalledAsync(Stalled(behind: 5, cancelled: 4), true, Ct);
        await e.ApplyVacuumStalledAsync(Stalled(behind: 5, cancelled: 4), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddHours(1);
        await e.ApplyVacuumStalledAsync(Clear(), true, Ct);

        h.Now = h.Now.AddHours(1);
        await e.ApplyVacuumStalledAsync(Stalled(behind: 1, cancelled: 0), true, Ct);
        Assert.Single(h.Deliverer.Outcomes);
        h.Now = h.Now.AddHours(1);
        await e.ApplyVacuumStalledAsync(Stalled(behind: 1, cancelled: 0), true, Ct);

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public void TheShortMessage_NamesTheCountsAndTheWorstOfEach()
    {
        Assert.Equal(
            "autovacuum is stalled on 3 tables, 2 behind (worst collect.wait_stats, last vacuumed 31 h ago), "
            + "1 cancelled repeatedly (worst collect.query_stats, 5 times in 24 hours)",
            DarlingSelfAlertEvaluator.BuildVacuumStalledShortMessage(Stalled()));
        Assert.Equal(
            "autovacuum is stalled on 1 table, 1 behind (worst collect.big, with no vacuum on record)",
            DarlingSelfAlertEvaluator.BuildVacuumStalledShortMessage(new StoreVacuumStalledReading(1, 1, 0, "collect.big", 50, true)));
        Assert.Equal(
            "autovacuum is stalled on 1 table, 1 cancelled repeatedly (worst collect.q, 2 times in 24 hours)",
            DarlingSelfAlertEvaluator.BuildVacuumStalledShortMessage(new StoreVacuumStalledReading(1, 0, 1, WorstCancelledTable: "collect.q", WorstCancels: 2)));
    }

    [Fact]
    public void TheDetail_ForBothKinds_OnTheManagedStore_IsTheWholeText()
    {
        Assert.Equal(
            "Autovacuum is stalled on 3 tables in the store on two hourly reads in a row. "
            + "2 tables have dead or inserted rows past the point where autovacuum starts and no vacuum in 24 hours. "
            + "The worst is collect.wait_stats, last vacuumed 31 h ago. "
            + "1 table had autovacuum cancelled 2 or more times in 24 hours. "
            + "The worst is collect.query_stats, cancelled 5 times, last at 2026-07-01 10:42 UTC. "
            + "A cancelled autovacuum stops before it finishes the table, and the next one starts the table again. "
            + "PostgreSQL cancels one when another session waits on a lock that conflicts with it, such as ALTER TABLE, TRUNCATE, DROP, LOCK TABLE "
            + "or a TimescaleDB job that compresses or drops chunks. The store log has the lines around each cancel. "
            + "Store Health in the Viewer and on the web page, and the get_store_table_health tool, list these tables first, "
            + "with the store's autovacuum settings and write budget. "
            + "Move the job that takes the lock away from the times autovacuum works on the table, or run VACUUM on the table after the job. "
            + "When every autovacuum worker is busy, raise autovacuum_vacuum_cost_limit and autovacuum_max_workers, or run VACUUM on the table at a quiet hour. "
            + "autovacuum_vacuum_cost_limit takes a reload, and autovacuum_max_workers takes a restart before PostgreSQL 18. "
            + "On the managed store, set them with ALTER SYSTEM or add lines below the include line in postgresql.conf. "
            + "The service does not manage these settings, so there is nothing for them in darling.json, and darling-managed.conf is rewritten on every start.",
            DarlingSelfAlertEvaluator.BuildVacuumStalledDetail(Stalled(), managedStore: true));
    }

    [Fact]
    public void TheDetail_ForATableBehindAlone_OnABringYourOwnStore_GivesOnlyTheBehindAdvice()
    {
        Assert.Equal(
            "Autovacuum is stalled on 1 table in the store on two hourly reads in a row. "
            + "1 table has dead or inserted rows past the point where autovacuum starts and no vacuum in 24 hours. "
            + "The worst is collect.big, with no vacuum on record. "
            + "Store Health in the Viewer and on the web page, and the get_store_table_health tool, list these tables first, "
            + "with the store's autovacuum settings and write budget. "
            + "When every autovacuum worker is busy, raise autovacuum_vacuum_cost_limit and autovacuum_max_workers, or run VACUUM on the table at a quiet hour. "
            + "autovacuum_vacuum_cost_limit takes a reload, and autovacuum_max_workers takes a restart before PostgreSQL 18. "
            + "Change them on the store's PostgreSQL.",
            DarlingSelfAlertEvaluator.BuildVacuumStalledDetail(new StoreVacuumStalledReading(1, 1, 0, "collect.big", 50, true), managedStore: false));
    }

    [Fact]
    public void TheDetail_ForATableCancelledAlone_GivesOnlyTheLockAdvice_AndLeavesOutAMissingCancelTime()
    {
        var text = DarlingSelfAlertEvaluator.BuildVacuumStalledDetail(
            new StoreVacuumStalledReading(1, 0, 1, WorstCancelledTable: "collect.q", WorstCancels: 2), managedStore: true);

        Assert.Contains("The worst is collect.q, cancelled 2 times. A cancelled autovacuum", text, StringComparison.Ordinal);
        Assert.Contains("Move the job that takes the lock away", text, StringComparison.Ordinal);
        Assert.DoesNotContain("raise autovacuum_vacuum_cost_limit", text, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER SYSTEM", text, StringComparison.Ordinal);
        Assert.DoesNotContain("past the point where autovacuum starts", text, StringComparison.Ordinal);
        Assert.EndsWith("or run VACUUM on the table after the job.", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePublicText_HasNoEmDashAndNoSemicolon()
    {
        var readings = new[]
        {
            Stalled(), Stalled(cancelled: 0), Stalled(behind: 0), new StoreVacuumStalledReading(1, 1, 0, "collect.big", 50, true),
            Stalled(behind: 1, cancelled: 1, hours: 100, cancels: 1),
        };
        foreach (var reading in readings)
        {
            foreach (var managed in new[] { true, false })
            {
                var text = DarlingSelfAlertEvaluator.BuildVacuumStalledDetail(reading, managed)
                    + DarlingSelfAlertEvaluator.BuildVacuumStalledShortMessage(reading);
                Assert.DoesNotContain("\u2014", text, StringComparison.Ordinal);
                Assert.DoesNotContain(";", text, StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void TheReading_CountsEachStalledTableOnce_AndNamesTheWorstOfEachKind()
    {
        var rows = new[]
        {
            /* Behind 30 h and cancelled 3 times: one table in the stalled count, in both of the others. */
            Row("both", cancels: 3, lastCancel: AsOf.AddHours(-2), past: 1, oldestVacuum: AsOf.AddHours(-30)),
            /* Behind the longest: a part never vacuumed, so the time runs from when the statistics began. */
            Row("oldest", past: 2, pastNever: 1, oldestVacuum: AsOf.AddHours(-26), statsSince: AsOf.AddHours(-80)),
            /* Cancelled the most. */
            Row("most_cancelled", cancels: 7, lastCancel: AsOf.AddHours(-1)),
            /* Past its trigger, vacuumed 23 h ago: not behind yet. Cancelled once: not repeatedly. */
            Row("recent", cancels: 1, lastCancel: AsOf.AddHours(-3), past: 1, oldestVacuum: AsOf.AddHours(-23)),
        };

        var reading = StoreVacuumStalledReading.From(rows, AsOf);

        Assert.Equal(3, reading.TablesStalled);
        Assert.Equal(2, reading.TablesBehind);
        Assert.Equal(2, reading.TablesRepeatedlyCancelled);
        Assert.Equal("collect.oldest", reading.WorstBehindTable);
        Assert.Equal(80, reading.WorstBehindHours);
        Assert.True(reading.WorstBehindNeverVacuumed);
        Assert.Equal("collect.most_cancelled", reading.WorstCancelledTable);
        Assert.Equal(7, reading.WorstCancels);
        Assert.Equal(AsOf.AddHours(-1), reading.WorstLastCancel);
        Assert.True(reading.IsStalled);
    }

    [Fact]
    public void TheReading_BreaksATieByTheFirstNameInOrdinalOrder_WhateverTheRowOrder()
    {
        var rows = new[]
        {
            Row("zeta", cancels: 4, lastCancel: AsOf.AddHours(-5), past: 1, oldestVacuum: AsOf.AddHours(-40)),
            Row("alpha", cancels: 4, lastCancel: AsOf.AddHours(-6), past: 1, oldestVacuum: AsOf.AddHours(-40)),
        };

        foreach (var order in new[] { rows, new[] { rows[1], rows[0] } })
        {
            var reading = StoreVacuumStalledReading.From(order, AsOf);
            Assert.Equal("collect.alpha", reading.WorstBehindTable);
            Assert.False(reading.WorstBehindNeverVacuumed);
            Assert.Equal("collect.alpha", reading.WorstCancelledTable);
            Assert.Equal(AsOf.AddHours(-6), reading.WorstLastCancel);
        }
    }

    [Fact]
    public void TheReading_OfRowsNoneStalled_IsClear_AndNamesNoTable()
    {
        var reading = StoreVacuumStalledReading.From(
            [Row("recent", cancels: 1, past: 3, oldestVacuum: AsOf.AddHours(-1)), Row("quiet")], AsOf);

        Assert.False(reading.IsStalled);
        Assert.Equal((0, 0, 0), (reading.TablesStalled, reading.TablesBehind, reading.TablesRepeatedlyCancelled));
        Assert.Null(reading.WorstBehindTable);
        Assert.Null(reading.WorstBehindHours);
        Assert.Null(reading.WorstCancelledTable);
        Assert.False(StoreVacuumStalledReading.From([], AsOf).IsStalled);
    }

    [Theory]
    [InlineData(0, "0 h")]
    [InlineData(31.9, "31 h")]
    [InlineData(71.99, "71 h")]
    [InlineData(72, "3 days")]
    [InlineData(100, "4 days")]
    [InlineData(-2, "0 h")]
    public void TheHours_ReadInHoursBelowThreeDays_ThenInDays(double hours, string expected) =>
        Assert.Equal(expected, StoreVacuumStalledReading.FormatHours(hours));
}

/* #8950: the rules the store table health read applies to its autovacuum progress rows, pinned on rows built in memory. */
public sealed class StoreVacuumBehindRuleTests
{
    private static readonly DateTime AsOf = new(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);

    private static StoreVacuumObjectRow Row(
        int past = 1, int pastNever = 0, DateTime? oldestVacuum = null, DateTime? statsSince = null, long cancels = 0, DateTime? lastCancel = null) =>
        new("collect", "wait_stats", KindHypertable, cancels, lastCancel, past, pastNever, oldestVacuum, statsSince);

    private static StoreTableHealthRow TableRow() =>
        new(
            "collect", "wait_stats", KindHypertable, 2_000_000_000, 1_999_999_900, 60, 40, 1234, 12, 0, 1_500_000_000, 0.8, 1, 1, null,
            500, 100, null, null, null, null, false, null, false, 10, 5, null, 50.0, 1000.0, 0, null, 0, 0, 100, 10, 0, 0, null, 0, 0, 0,
            null, StatsSourceReset, 24, false, 0, 0, 0, null);

    [Fact]
    public void APartPastItsTrigger_IsBehindOnlyOnceItsLastVacuumIsAWindowOld()
    {
        var justInside = BehindAsOf(Row(oldestVacuum: AsOf - BehindWindow + TimeSpan.FromMinutes(1)), AsOf);
        Assert.False(justInside.Behind);
        Assert.Equal(BehindSourceLastVacuum, justInside.Source);

        var atTheWindow = BehindAsOf(Row(oldestVacuum: AsOf - BehindWindow), AsOf);
        Assert.True(atTheWindow.Behind);
        Assert.Equal(24, atTheWindow.Hours);
        Assert.Equal(AsOf - BehindWindow, atTheWindow.Since);
    }

    [Fact]
    public void APartNeverVacuumed_RunsFromWhenTheStatisticsBegan_WhenThatIsOlderThanTheOldestVacuum()
    {
        var behind = BehindAsOf(Row(past: 2, pastNever: 1, oldestVacuum: AsOf.AddHours(-2), statsSince: AsOf.AddHours(-50)), AsOf);
        Assert.True(behind.Behind);
        Assert.Equal(BehindSourceStatsSince, behind.Source);
        Assert.Equal(50, behind.Hours);

        /* Statistics begun two hours ago: a part with no vacuum since is not behind yet. */
        var fresh = BehindAsOf(Row(pastNever: 1, statsSince: AsOf.AddHours(-2)), AsOf);
        Assert.False(fresh.Behind);
        Assert.Equal(BehindSourceStatsSince, fresh.Source);
    }

    [Fact]
    public void NoPartPastATrigger_IsNeverBehind_HoweverOldItsLastVacuum()
    {
        var none = BehindAsOf(Row(past: 0, oldestVacuum: AsOf.AddDays(-30), statsSince: AsOf.AddDays(-60)), AsOf);

        Assert.False(none.Behind);
        Assert.Null(none.Since);
        Assert.Null(none.Source);
        Assert.Null(none.Hours);
    }

    [Fact]
    public void WithVacuum_CopiesTheCancelsAndTheBehindState_AndNoRowLeavesTheTableAsItWas()
    {
        var row = WithVacuum(TableRow(), Row(oldestVacuum: AsOf.AddHours(-30), cancels: 3, lastCancel: AsOf.AddHours(-1)), AsOf);

        Assert.Equal(3, row.AutovacuumCancels);
        Assert.True(row.RepeatedlyCancelled);
        Assert.Equal(AsOf.AddHours(-1), row.LastAutovacuumCancel);
        Assert.Equal(1, row.RelationsPastTrigger);
        Assert.True(row.Behind);
        Assert.Equal(AsOf.AddHours(-30), row.BehindSince);
        Assert.Equal(BehindSourceLastVacuum, row.BehindSinceSource);
        Assert.Equal(30, row.BehindHours);

        var untouched = WithVacuum(TableRow(), null, AsOf);
        Assert.Equal(TableRow(), untouched);
        Assert.False(untouched.Behind);
        Assert.False(untouched.RepeatedlyCancelled);
        Assert.False(WithVacuum(TableRow(), Row(cancels: 1, past: 0), AsOf).RepeatedlyCancelled);
    }

    [Fact]
    public void TheSettings_AtThePostgresDefaults_GiveAWriteBudgetOf39MBps_FromTheirPlainTwins()
    {
        /* PostgreSQL 12 and later: autovacuum_vacuum_cost_limit -1 (so vacuum_cost_limit 200), a 2 ms delay, page dirty cost 20, 8 kB pages. */
        var s = Settings(true, 3, 1, 60, -1, 200, 2, 0, -1, 65536, 20, 8192, 4, 1);

        Assert.Equal(200, s.CostLimit);
        Assert.Equal("vacuum_cost_limit", s.CostLimitSetting);
        Assert.Equal(2, s.CostDelayMs);
        Assert.Equal("autovacuum_vacuum_cost_delay", s.CostDelaySetting);
        Assert.Equal(65536, s.WorkMemKb);
        Assert.Equal("maintenance_work_mem", s.WorkMemSetting);
        Assert.Equal(39.0625, s.WriteBudgetMBps);
        Assert.Equal((4L, 1L), (s.CancelsInWindow, s.CancelsUnresolved));
    }

    [Fact]
    public void TheSettings_UseTheirOwnValuesWhenSet_AndNoDelayMeansNoBudget()
    {
        var own = Settings(true, 6, 0, 30, 2000, 200, 1, 0, 1_048_576, 65536, 20, 8192, 0);
        Assert.Equal(("autovacuum_vacuum_cost_limit", 2000), (own.CostLimitSetting, own.CostLimit));
        Assert.Equal("autovacuum_work_mem", own.WorkMemSetting);
        Assert.Equal(781.25, own.WriteBudgetMBps);

        Assert.Null(Settings(true, 3, 0, 60, -1, 200, 0, 0, -1, 65536, 20, 8192, 0).WriteBudgetMBps);
        /* A delay of -1 falls back to vacuum_cost_delay, whose default 0 is unthrottled. */
        var fallback = Settings(true, 3, 0, 60, -1, 200, -1, 0, -1, 65536, 20, 8192, 0);
        Assert.Equal("vacuum_cost_delay", fallback.CostDelaySetting);
        Assert.Null(fallback.WriteBudgetMBps);
        Assert.Null(Settings(true, 3, 0, 60, -1, 200, 2, 0, -1, 65536, 0, 8192, 0).WriteBudgetMBps);
    }
}
