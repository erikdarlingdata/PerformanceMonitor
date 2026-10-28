/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6874: the "Agent Alert Gaps" self-alert and the fleet card's gap chip. The alert is judged from
/// <see cref="AgentAlertStatus.Evaluate"/> over the newest fresh capture. These tests drive the evaluator with a controllable clock and a
/// recording deliverer, and the live ones read a seeded <c>collect.agent_alerts</c> on a throwaway PostgreSQL.
/// </summary>
public sealed class AgentAlertGapsAlertTests
{
    private const int ServerId = 424242;
    private const string Name = "AGENT-GAPS-SRV";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static AgentAlertFact Alert(int id, string name, int severity, int message, bool enabled = true, int operators = 1, int notification = 1) =>
        new(id, name, enabled, "MSSQLSERVER", message, severity, null, null, null, 300, null, null, 0, null, notification, operators, null, null);

    /// <summary>Every severity 19 to 25 and every error 823 to 825, enabled and notifying one operator: no gap.</summary>
    private static List<AgentAlertFact> CompleteSetup()
    {
        var alerts = new List<AgentAlertFact>();
        var id = 1;
        foreach (var severity in AgentAlertStatus.RequiredSeverities)
            alerts.Add(Alert(id++, $"Severity {severity}", severity, 0));
        foreach (var error in AgentAlertStatus.RequiredErrors)
            alerts.Add(Alert(id++, $"Error {error}", 0, error));
        return alerts;
    }

    private static AgentAlertRead Read(IReadOnlyList<AgentAlertFact> alerts, DateTime now) =>
        new(alerts, now.AddMinutes(-5), now.AddMinutes(-5), 1);

    [Fact]
    public async Task TheAlert_IsOffByDefault_AndFiresNothingEvenForAnInstanceWithNoAlertsAtAll()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.ApplyAgentAlertSetupAsync(ServerId, Name, Read(new List<AgentAlertFact>(), h.Now), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        /* The same read fires once the switch is on. */
        h.AgentAlertSetupEnabled = true;
        await h.Build().ApplyAgentAlertSetupAsync(ServerId, Name, Read(new List<AgentAlertFact>(), h.Now), Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AGap_FiresOnceNamingTheGaps_StaysQuiet_AndClearsWithOneHistoryRow()
    {
        var h = new DarlingSelfAlertTests.Harness { AgentAlertSetupEnabled = true };
        var e = h.Build();

        /* Severity 20 has no operator, error 825 and the other severities have no alert. */
        var partial = new List<AgentAlertFact> { Alert(1, "Sev 19", 19, 0), Alert(2, "Sev 20", 20, 0, operators: 0, notification: 0) };
        await e.ApplyAgentAlertSetupAsync(ServerId, Name, Read(partial, h.Now), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Agent Alert Gaps", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Contains("No alert for severity 21, 22, 23, 24, 25.", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("No alert for error 823, 824, 825.", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("(Sev 20)", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("missing_severity_alerts", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("alert_without_operator", fired.ShortMessage, StringComparison.Ordinal);

        /* The gap stands a minute later: one send per occurrence. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyAgentAlertSetupAsync(ServerId, Name, Read(partial, h.Now), Ct);
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        /* A complete setup clears it: one audit row, no send, and a second pass writes nothing more. */
        await e.ApplyAgentAlertSetupAsync(ServerId, Name, Read(CompleteSetup(), h.Now), Ct);
        var cleared = Assert.Single(h.History.Records);
        Assert.Equal("Agent Alert Gaps Cleared", cleared.MetricName);
        Assert.False(cleared.AlertSent);
        Assert.Single(h.Deliverer.Outcomes);
        await e.ApplyAgentAlertSetupAsync(ServerId, Name, Read(CompleteSetup(), h.Now), Ct);
        Assert.Single(h.History.Records);
    }

    [Fact]
    public async Task ACompleteSetup_NeverFires_AndNoFreshCapture_JudgesNothing()
    {
        var h = new DarlingSelfAlertTests.Harness { AgentAlertSetupEnabled = true };
        var e = h.Build();

        await e.ApplyAgentAlertSetupAsync(ServerId, Name, Read(CompleteSetup(), h.Now), Ct);
        await e.ApplyAgentAlertSetupAsync(ServerId, Name, null, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task ALaggingFeed_NeitherFiresNorClearsAStandingAlert()
    {
        var h = new DarlingSelfAlertTests.Harness { AgentAlertSetupEnabled = true };
        var e = h.Build();
        await e.ApplyAgentAlertSetupAsync(ServerId, Name, Read(new List<AgentAlertFact>(), h.Now), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* No fresh capture: the standing state is untouched, so the recovery below still writes its row. */
        await e.ApplyAgentAlertSetupAsync(ServerId, Name, null, Ct);
        Assert.Empty(h.History.Records);
        await e.ApplyAgentAlertSetupAsync(ServerId, Name, Read(CompleteSetup(), h.Now), Ct);
        Assert.Single(h.History.Records);
    }

    [Fact]
    public async Task TheMasterSwitchOff_JudgesNothing()
    {
        var h = new DarlingSelfAlertTests.Harness { AgentAlertSetupEnabled = true };
        h.Settings.AlertsEnabled = false;

        await h.Build().ApplyAgentAlertSetupAsync(ServerId, Name, Read(new List<AgentAlertFact>(), h.Now), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    private static DarlingFleetReader.FleetServerRow Server(int? edition) => new(1, "srv", "srv", edition, "sqlserver", false);

    private static FleetServerCard Card(IReadOnlyList<FleetAgentAlertGap>? gaps) =>
        DarlingFleetReader.BuildCard(
            Server(3), default, default, default, default, default, default, default, default,
            new DateTime(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc).AddSeconds(-30), default, null,
            new DateTime(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc), TimeSpan.FromHours(1), DeadlockRateThresholds.Default,
            agentAlertGaps: gaps);

    [Fact]
    public void TheFleetCard_CarriesTheGaps_AndItsBandIsTheSameWithOrWithoutThem()
    {
        var gaps = new[] { new FleetAgentAlertGap("no_alerts_defined", "No SQL Server Agent alert is defined on this instance.") };

        var with = Card(gaps);
        var without = Card(null);
        var empty = Card(Array.Empty<FleetAgentAlertGap>());

        Assert.Equal("no_alerts_defined", Assert.Single(with.AgentAlertGaps!).Code);
        Assert.Null(without.AgentAlertGaps);
        Assert.Null(empty.AgentAlertGaps);
        Assert.Equal(without.OverallMetricSeverity, with.OverallMetricSeverity);
        Assert.Equal(without.CollectorSeverity, with.CollectorSeverity);
    }

    /* ---------------- live: a seeded store ---------------- */

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, CancellationToken, Task<T>> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live agent alert gap tests.");
        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: a scratch database of its own. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Name, ct);
        await seed(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await body(postgres, ct);
    }

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, AgentAlertFact? a) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.agent_alerts
(collection_id, collection_time, server_id, server_name, alert_id, alert_name, enabled, event_source, message_id, severity,
 delay_between_responses, occurrence_count, has_notification, operator_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, Name, a?.AlertId, a?.Name, a?.Enabled,
            a is null ? null : "MSSQLSERVER", a?.MessageId, a?.Severity, a is null ? null : 300, a is null ? null : 0,
            a?.HasNotification, a?.OperatorCount);

    private static async Task<int> Sweep(DarlingSelfAlertEvaluator e, DarlingSelfAlertTests.Harness h, NpgsqlDataSource postgres, CancellationToken ct)
    {
        await e.EvaluateStoreAlertsAsync(postgres, ServerId, Name, connected: true, ct);
        return h.Deliverer.Outcomes.Count(o => o.MetricName == "Agent Alert Gaps");
    }

    [Fact]
    public async Task ASeededStore_Fires_WhenTheNewestCaptureHasAGap_AndClearsWhenAFreshCompleteCaptureLands()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await InsertAsync(connection, ct, DateTime.UtcNow.AddMinutes(-5), Alert(1, "Sev 19", 19, 0));
        }, async (postgres, ct) =>
        {
            var h = new DarlingSelfAlertTests.Harness { AgentAlertSetupEnabled = true, Now = DateTime.UtcNow };
            var e = h.Build();
            Assert.Equal(1, await Sweep(e, h, postgres, ct));
            var fired = h.Deliverer.Outcomes.Single(o => o.MetricName == "Agent Alert Gaps");
            Assert.Contains("No alert for severity 20, 21, 22, 23, 24, 25.", fired.DetailText, StringComparison.Ordinal);

            /* A newer, complete capture clears it. */
            foreach (var a in CompleteSetup())
                await InsertAsync(await OpenAsync(postgres, ct), ct, DateTime.UtcNow.AddMinutes(-1), a);
            h.Now = DateTime.UtcNow;
            Assert.Equal(1, await Sweep(e, h, postgres, ct));
            Assert.Contains(h.History.Records, r => r.MetricName == "Agent Alert Gaps Cleared");
            return 0;
        });
    }

    private static async Task<NpgsqlConnection> OpenAsync(NpgsqlDataSource postgres, CancellationToken ct) => await postgres.OpenConnectionAsync(ct);

    [Fact]
    public async Task ASeededStore_DoesNotFire_ForAStaleCaptureOrForNoCaptureAtAll_AndNotWhenTheSwitchIsOff()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            /* The marker row (collected, none defined) is a gap, but three hours old is past the stale window. */
            await InsertAsync(connection, ct, DateTime.UtcNow.AddHours(-3), null);
        }, async (postgres, ct) =>
        {
            var stale = new DarlingSelfAlertTests.Harness { AgentAlertSetupEnabled = true, Now = DateTime.UtcNow };
            Assert.Equal(0, await Sweep(stale.Build(), stale, postgres, ct));

            /* A fresh marker row, switch off: nothing. */
            await InsertAsync(await OpenAsync(postgres, ct), ct, DateTime.UtcNow.AddMinutes(-2), null);
            var off = new DarlingSelfAlertTests.Harness { Now = DateTime.UtcNow };
            Assert.Equal(0, await Sweep(off.Build(), off, postgres, ct));

            /* The same store with the switch on fires. */
            var on = new DarlingSelfAlertTests.Harness { AgentAlertSetupEnabled = true, Now = DateTime.UtcNow };
            Assert.Equal(1, await Sweep(on.Build(), on, postgres, ct));
            return 0;
        });
    }

    [Fact]
    public async Task TheFleetRead_ReturnsGapsOnlyForAServerWithAFreshCaptureThatHasThem()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await InsertAsync(connection, ct, DateTime.UtcNow.AddMinutes(-5), null);
        }, async (postgres, ct) =>
        {
            var gaps = await DarlingFleetReader.ReadAgentAlertGapsAsync(postgres, DateTime.UtcNow, ct);
            var gap = Assert.Single(gaps[ServerId], g => g.Code == "no_alerts_defined");
            Assert.Contains("No SQL Server Agent alert", gap.Message, StringComparison.Ordinal);

            /* A newer complete capture leaves the server out. */
            foreach (var a in CompleteSetup())
                await InsertAsync(await OpenAsync(postgres, ct), ct, DateTime.UtcNow.AddMinutes(-1), a);
            Assert.False((await DarlingFleetReader.ReadAgentAlertGapsAsync(postgres, DateTime.UtcNow, ct)).ContainsKey(ServerId));

            /* The same capture read three hours later is past the freshness window: no gaps and no claim. */
            Assert.Empty(await DarlingFleetReader.ReadAgentAlertGapsAsync(postgres, DateTime.UtcNow.AddHours(3), ct));
            return 0;
        });
    }
}
