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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The self-monitor pair "External Alerts Undelivered" / "External Alerts Delivered Again" (#5654): the evaluator's fire,
/// hold and clear, the classifier and triage mapping the history grids rely on, and a live fact that reads the counts from a
/// real store and drives the evaluator from them.
/// </summary>
public sealed class ExternalAlertDeliverySelfAlertTests
{
    private const int Deadline = 30;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DarlingSelfAlertEvaluator.ExternalAlertsReport Report(params (string Source, string Status, int Count)[] rows) =>
        new(rows.Select(r => new ExternalAlertUndeliveredCount(r.Source, r.Status, r.Count)).ToList());

    [Fact]
    public async Task Undelivered_FiresOnceWithTheCountsPerSource_AndNeverRepeatsASubmittedText()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyExternalAlertsAsync(Report(("backup_svc", "expired", 2), ("backup_svc", "failed", 1), ("etl", "interrupted", 4)), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.ExternalAlertsUndeliveredMetric, fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal("externalalerts", fired.ServerKey);
        Assert.Equal(DarlingSelfAlertEvaluator.StoreServerLabel, fired.ServerName);
        Assert.Equal("7", fired.CurrentValue);
        Assert.Contains("7 external alerts were not delivered in the last hour", fired.ShortMessage);
        Assert.Contains("2 expired, 4 interrupted, 1 failed", fired.ShortMessage);
        Assert.Contains("- Source backup_svc: 2 expired, 1 failed", fired.DetailText);
        Assert.Contains("- Source etl: 4 interrupted", fired.DetailText);

        /* Same report on the next tick, inside the refire span: held, not sent again. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyExternalAlertsAsync(Report(("backup_svc", "expired", 2), ("backup_svc", "failed", 1), ("etl", "interrupted", 4)), Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Undelivered_ClearsWithOneDeliveredAgainRow_WhenTheWindowIsEmpty_AndOnlyOnce()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyExternalAlertsAsync(Report(("etl", "expired", 1)), Ct);
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        await e.ApplyExternalAlertsAsync(Report(), Ct);
        Assert.Single(h.Deliverer.Outcomes);
        var resolution = Assert.Single(h.History.Records);
        Assert.Equal(DarlingSelfAlertEvaluator.ExternalAlertsDeliveredAgainMetric, resolution.MetricName);

        /* An empty window with nothing standing writes nothing. */
        await e.ApplyExternalAlertsAsync(Report(), Ct);
        Assert.Single(h.History.Records);
        Assert.Single(h.Deliverer.Outcomes);

        /* A later give-up is a new episode and fires again. */
        h.Now = h.Now.AddMinutes(10);
        await e.ApplyExternalAlertsAsync(Report(("etl", "failed", 1)), Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task Undelivered_NothingToReport_NeverWritesAResolution_AndTheMasterSwitchSilencesIt()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        await e.ApplyExternalAlertsAsync(Report(), Ct);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        h.Settings.AlertsEnabled = false;
        await e.ApplyExternalAlertsAsync(Report(("etl", "expired", 3)), Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Undelivered_ListsAtMostTwentySources_AndTheCountReflectsAll()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var rows = Enumerable.Range(0, 25).Select(i => ("src" + i.ToString("D2", CultureInfo.InvariantCulture), "expired", 1)).ToArray();

        await e.ApplyExternalAlertsAsync(Report(rows), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("25", fired.CurrentValue);
        Assert.Contains("- Source src19:", fired.DetailText);
        Assert.DoesNotContain("- Source src20:", fired.DetailText);
        Assert.Contains("+ 5 more sources.", fired.DetailText);
    }

    [Fact]
    public async Task Undelivered_TheWordsAgreeWithTheCounts_ForOneAlertAndForOneUnlistedSource()
    {
        /* #6700: "1 external alert was not delivered", not "1 external alert(s) were"; the 21st source is "1 more source.". */
        var one = new DarlingSelfAlertTests.Harness();
        await one.Build().ApplyExternalAlertsAsync(Report(("etl", "failed", 1)), Ct);
        var single = Assert.Single(one.Deliverer.Outcomes);
        Assert.StartsWith("1 external alert was not delivered in the last hour", single.ShortMessage);
        Assert.DoesNotContain("(s)", single.DetailText);

        var h = new DarlingSelfAlertTests.Harness();
        var rows = Enumerable.Range(0, 21).Select(i => ("src" + i.ToString("D2", CultureInfo.InvariantCulture), "expired", 1)).ToArray();
        await h.Build().ApplyExternalAlertsAsync(Report(rows), Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("+ 1 more source.", fired.DetailText);
        Assert.DoesNotContain("(s)", fired.DetailText);
    }

    [Fact]
    public void TheTwoNames_ClassifyAsACountAlertAndAResolution_AndTriageReachesTheSameSections()
    {
        Assert.False(AlertMetricClassifier.IsResolution(DarlingSelfAlertEvaluator.ExternalAlertsUndeliveredMetric));
        Assert.True(AlertMetricClassifier.IsWarning(DarlingSelfAlertEvaluator.ExternalAlertsUndeliveredMetric));
        Assert.Equal("7", AlertMetricClassifier.FormatHistoryValue(DarlingSelfAlertEvaluator.ExternalAlertsUndeliveredMetric, 7));
        Assert.False(AlertMetricClassifier.IsStateOnly(DarlingSelfAlertEvaluator.ExternalAlertsUndeliveredMetric));
        Assert.True(AlertMetricClassifier.IsResolution(DarlingSelfAlertEvaluator.ExternalAlertsDeliveredAgainMetric));

        var firing = DarlingTriageEndpoint.SectionsFor(DarlingSelfAlertEvaluator.ExternalAlertsUndeliveredMetric);
        Assert.NotSame(DarlingTriageEndpoint.DefaultSections, firing);
        Assert.Contains(firing, s => s.Read == "get_alert_history");
        Assert.All(firing, s => Assert.True(s.FleetLevel));
        Assert.Same(firing, DarlingTriageEndpoint.SectionsFor(DarlingSelfAlertEvaluator.ExternalAlertsDeliveredAgainMetric));
    }

    /// <summary>
    /// Fire and clear against a real store: rows the drain gave up on inside the hour are counted per source and status, the
    /// rows it did not give up on (delivered, muted, no channel, ignored, still pending or claimed) and the ones older than
    /// the window are not, the evaluator fires from the real counts, and once the window holds none it writes the
    /// "Delivered Again" row.
    /// </summary>
    [Fact]
    public async Task Live_TheCountsFireTheAlert_AndAnEmptyWindowClearsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the external alert delivery self-alert's live fact (it mints its own scratch database).");

        var ct = Ct;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var seed = new NpgsqlConnection(scratch.ConnectionString);
        await seed.OpenAsync(ct);
        await PgMigrations.MigrateAsync(seed, ct);

        var bodySucceeded = false;
        try
        {
            var etl = await AddSourceAsync(seed, "etl", ct);
            var backup = await AddSourceAsync(seed, "backup_svc", ct);
            await SeedAsync(seed, etl, "expired", completedMinutesAgo: 5, ct);
            await SeedAsync(seed, etl, "expired", completedMinutesAgo: 30, ct);
            await SeedAsync(seed, etl, "interrupted", completedMinutesAgo: 10, ct);
            await SeedAsync(seed, backup, "failed", completedMinutesAgo: 59, ct);
            /* Outside the window, and outcomes that are not a give-up. */
            await SeedAsync(seed, etl, "expired", completedMinutesAgo: 61, ct);
            await SeedAsync(seed, etl, "delivered", completedMinutesAgo: 1, ct);
            await SeedAsync(seed, etl, "muted", completedMinutesAgo: 1, ct);
            await SeedAsync(seed, etl, "no_channel", completedMinutesAgo: 1, ct);
            await SeedAsync(seed, etl, "ignored", completedMinutesAgo: 1, ct);
            await SeedAsync(seed, etl, "pending", completedMinutesAgo: null, ct);
            await SeedAsync(seed, etl, "claimed", completedMinutesAgo: null, ct);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var store = new ExternalAlertStore(dataSource, Deadline);

            var counts = await store.CountUndeliveredAsync(DarlingSelfAlertEvaluator.ExternalAlertsWindow, ct);
            Assert.Equal(
                new[] { "backup_svc/failed/1", "etl/expired/2", "etl/interrupted/1" },
                counts.Select(c => $"{c.SourceName}/{c.Status}/{c.Count}").ToArray());

            var h = new DarlingSelfAlertTests.Harness();
            var evaluator = h.Build();
            await evaluator.ApplyExternalAlertsAsync(new DarlingSelfAlertEvaluator.ExternalAlertsReport(counts), ct);
            var fired = Assert.Single(h.Deliverer.Outcomes);
            Assert.Equal(DarlingSelfAlertEvaluator.ExternalAlertsUndeliveredMetric, fired.MetricName);
            Assert.Equal("4", fired.CurrentValue);

            /* The give-ups age out of the hour: nothing left to count, so the alert clears. */
            await using (var age = new NpgsqlCommand(
                "UPDATE config.external_alert_pending SET completed_at = completed_at - interval '2 hours' WHERE completed_at IS NOT NULL", seed)
            { CommandTimeout = Deadline })
            {
                await age.ExecuteNonQueryAsync(ct);
            }

            var after = await store.CountUndeliveredAsync(DarlingSelfAlertEvaluator.ExternalAlertsWindow, ct);
            Assert.Empty(after);
            await evaluator.ApplyExternalAlertsAsync(new DarlingSelfAlertEvaluator.ExternalAlertsReport(after), ct);
            var resolution = Assert.Single(h.History.Records);
            Assert.Equal(DarlingSelfAlertEvaluator.ExternalAlertsDeliveredAgainMetric, resolution.MetricName);
            Assert.Single(h.Deliverer.Outcomes);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<int> AddSourceAsync(NpgsqlConnection connection, string name, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('{name}', 'role_{name}') RETURNING source_id", connection)
        {
            CommandTimeout = Deadline,
        };
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task SeedAsync(NpgsqlConnection connection, int sourceId, string status, int? completedMinutesAgo, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, title, expires_at, status, claimed_at, completed_at)"
            + " VALUES ($1, 'fire', 'k', 'warning', 'seeded', now() + interval '15 minutes', $2,"
            + " CASE WHEN $2 = 'claimed' THEN now() ELSE NULL END,"
            + " CASE WHEN $3::int IS NULL THEN NULL ELSE now() - make_interval(mins => $3::int) END)", connection)
        {
            CommandTimeout = Deadline,
        };
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sourceId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = status });
        command.Parameters.Add(new NpgsqlParameter { Value = completedMinutesAgo is null ? DBNull.Value : completedMinutesAgo.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(ct);
    }
}
