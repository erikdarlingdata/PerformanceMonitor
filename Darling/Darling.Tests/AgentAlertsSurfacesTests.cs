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
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6874 part 3: the Viewer's Agent Alerts words as functions of the read. They are the words the web page's Configuration tab shows, so
/// the assertions are on the values.
/// </summary>
public sealed class ViewerAgentAlertsTextTests
{
    private static AgentAlertFact Alert(int id, string name, int severity = 0, int message = 0, bool enabled = true, int operators = 1, bool? fired = null) =>
        new(id, name, enabled, "MSSQLSERVER", message, severity, null, null, null, 300, new DateTime(2026, 10, 27, 13, 5, 9), null, 4, null,
            operators > 0 ? 1 : 0, operators, null, fired);

    private static ViewerAgentAlertsRead Read(params AgentAlertFact[] alerts)
    {
        var captured = new DateTime(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc);
        var read = new AgentAlertRead(alerts, captured, captured.AddHours(-3), 2);
        var status = AgentAlertStatus.Evaluate(alerts);
        return new ViewerAgentAlertsRead(status.Word, null, status, read);
    }

    private static List<AgentAlertFact> Complete() =>
        AgentAlertStatus.RequiredSeverities.Select(s => Alert(s, "Sev " + s, severity: s))
            .Concat(AgentAlertStatus.RequiredErrors.Select(e => Alert(e, "Err " + e, message: e))).ToList();

    [Fact]
    public void TheGaps_AreRowsOfSentenceAndWhich()
    {
        var alerts = Complete().Where(a => a.Severity is not (21 or 23)).Select(a => a.Severity == 20 ? a with { OperatorCount = 0, HasNotification = 0 } : a).ToArray();

        var rows = ViewerAgentAlertsText.GapRows(Read(alerts));

        Assert.Equal(
            [
                new ViewerAgentAlertGapRow("No alert for severity 21, 23.", "21, 23"),
                new ViewerAgentAlertGapRow("An enabled alert tells no operator, so nobody hears when it fires.", "Sev 20"),
            ],
            rows);
    }

    [Fact]
    public void AMissingErrorAlert_NamesTheErrorNumbers()
    {
        var rows = ViewerAgentAlertsText.GapRows(Read(Complete().Where(a => a.MessageId is not (824 or 825)).ToArray()));

        Assert.Equal([new ViewerAgentAlertGapRow("No alert for error 824, 825.", "824, 825")], rows);
    }

    [Fact]
    public void TheStatusLine_NamesTheWord_TheCount_AndTheCaptureTime()
    {
        var read = Read(Complete().Where(a => a.Severity != 21).ToArray());
        var captured = new DateTime(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc).ToLocalTime().ToString("g", CultureInfo.CurrentCulture);

        Assert.Equal($"Alert setup: gaps   Alerts defined: 9   Captured: {captured}", ViewerAgentAlertsText.StatusLine(read));
        Assert.StartsWith("Alert setup: ok   Alerts defined: 10", ViewerAgentAlertsText.StatusLine(Read(Complete().ToArray())), StringComparison.Ordinal);
    }

    [Fact]
    public void AnOkStatus_HasNoGapRows_AndSaysSo()
    {
        var read = Read(Complete().ToArray());

        Assert.Empty(ViewerAgentAlertsText.GapRows(read));
        Assert.Equal(
            "No gaps. Every severity from 19 to 25 and every error from 823 to 825 has an enabled alert that tells an operator.",
            ViewerAgentAlertsText.GapsEmptyText(read));
    }

    [Fact]
    public void TheMarkerRow_IsAGapAndAnEmptyAlertList()
    {
        var read = Read();

        Assert.Equal("gaps", read.Status);
        Assert.Equal("No SQL Server Agent alert is defined on this instance.", ViewerAgentAlertsText.GapRows(read)[0].Message);
        Assert.Empty(ViewerAgentAlertsText.AlertRows(read));
        Assert.Equal("No SQL Server Agent alert is defined on this instance.", ViewerAgentAlertsText.AlertsEmptyText(read));
    }

    [Fact]
    public void AnAlertRow_ReadsYesNoDashAndTheServerClock()
    {
        var rows = ViewerAgentAlertsText.AlertRows(Read(
            Alert(7, "Sev 20", severity: 20, operators: 0, fired: true),
            Alert(8, "Err 823", message: 823, enabled: false, fired: false),
            Alert(9, "Sev 19", severity: 19, fired: null)));

        Assert.Equal(new ViewerAgentAlertRow("Sev 20", "yes", "20", "0", "", "0", "yes", "4", "2026-10-27T13:05:09"), rows[0]);
        Assert.Equal(new ViewerAgentAlertRow("Err 823", "no", "0", "823", "", "1", "no", "4", "2026-10-27T13:05:09"), rows[1]);
        Assert.Equal("—", rows[2].FiredInWindow);
    }

    [Fact]
    public void ARead_WithoutACapture_ShowsTheReadsOwnSentence_InPlaceOfTheLists()
    {
        var read = new ViewerAgentAlertsRead(ViewerAgentAlertsRead.NotCollected, "Azure SQL Database does not have this data.", null,
            new AgentAlertRead([], null, null, 0));

        Assert.Equal("Azure SQL Database does not have this data.", ViewerAgentAlertsText.StatusLine(read));
        Assert.Equal("Azure SQL Database does not have this data.", ViewerAgentAlertsText.GapsEmptyText(read));
        Assert.Equal("Azure SQL Database does not have this data.", ViewerAgentAlertsText.AlertsEmptyText(read));
        Assert.Empty(ViewerAgentAlertsText.GapRows(read));
        Assert.Empty(ViewerAgentAlertsText.AlertRows(read));
    }
}

/// <summary>
/// #6874 part 3: the web read (<c>get_agent_alerts</c> through the web dispatch) and the Viewer's data method over one seeded store give
/// the same status words and gaps, and an Azure SQL Database or Managed Instance answers <c>not_collected</c> on both.
/// </summary>
[Collection("live-postgres")]
public sealed class AgentAlertsSurfacesLiveTests
{
    private const string ServerName = "darling-agent-alerts-surfaces";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, string, CancellationToken, Task<T>> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live agent alert surface tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await seed(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await body(postgres, scratch.ConnectionString, ct);
    }

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, int? alertId, string? name, bool? enabled, int severity, int message, int count, int operators, int notification) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.agent_alerts
(collection_id, collection_time, server_id, server_name, alert_id, alert_name, enabled, event_source, message_id, severity,
 delay_between_responses, last_occurrence, occurrence_count, has_notification, operator_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, alertId, name, enabled,
            alertId is null ? null : "MSSQLSERVER", alertId is null ? null : message, alertId is null ? null : severity,
            alertId is null ? null : 300, null, alertId is null ? null : count, alertId is null ? null : notification, alertId is null ? null : operators);

    private static async Task<JsonElement> WebReadAsync(NpgsqlDataSource postgres)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(new List<KeyValuePair<string, string?>> { new("server_name", ServerName) });
        return JsonDocument.Parse(await DarlingWebEndpoints.BuildReadDispatch()["get_agent_alerts"](context, postgres, null!)).RootElement;
    }

    private static DarlingServer Server(int edition = CollectorEngineCapability.UnknownEngineEdition) =>
        new(ServerId, ServerName, ServerName, true, 16, engineKind: MonitoredEngineKind.SqlServer, engineEdition: edition);

    [Fact]
    public async Task ASeededStore_GivesTheSameStatusAndGaps_OnTheWebReadAndInTheViewer()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var first = DateTime.UtcNow.AddHours(-3);
            var second = DateTime.UtcNow.AddMinutes(-10);
            await InsertAsync(connection, ct, first, 1, "Sev 19", true, 19, 0, 7, 1, 1);
            await InsertAsync(connection, ct, first, 2, "Sev 20", true, 20, 0, 2, 0, 0);
            await InsertAsync(connection, ct, second, 1, "Sev 19", true, 19, 0, 7, 1, 1);
            await InsertAsync(connection, ct, second, 2, "Sev 20", true, 20, 0, 5, 0, 0);
        }, async (postgres, cs, ct) =>
        {
            var web = await WebReadAsync(postgres);
            await using var viewer = new ViewerDataService(cs);
            var read = await viewer.ReadAgentAlertsAsync(Server(), DateTime.UtcNow, cancellationToken: ct);

            Assert.Equal("gaps", web.GetProperty("instance_status").GetString());
            Assert.Equal("gaps", read.Status);
            var webGaps = web.GetProperty("gaps").EnumerateArray().Select(g => g.GetProperty("code").GetString()).ToArray();
            Assert.Equal(["missing_severity_alerts", "missing_error_alerts", "alert_without_operator", "fired_unnotified"], webGaps);
            Assert.Equal(webGaps, read.InstanceStatus!.Gaps.Select(g => g.Code).ToArray());
            Assert.Equal(2, read.Read!.Alerts.Count);

            var rows = ViewerAgentAlertsText.GapRows(read);
            Assert.Equal("No alert for severity 21, 22, 23, 24, 25.", rows[0].Message);
            Assert.Equal("Sev 20", rows[2].Which);
            Assert.Equal("Sev 20", rows[3].Which);
            var alert20 = ViewerAgentAlertsText.AlertRows(read).Single(a => a.Name == "Sev 20");
            Assert.Equal(("yes", "5", "0"), (alert20.FiredInWindow, alert20.TimesFired, alert20.Operators));
            return 0;
        });
    }

    [Theory]
    [InlineData(5)]  // Azure SQL Database
    [InlineData(8)]  // Azure SQL Managed Instance
    public async Task NoCapture_OnAzureSqlDatabaseAndManagedInstance_IsNotCollected_OnBothSurfaces(int edition)
    {
        await WithStoreAsync(
            (connection, ct) => DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_engine_edition = $1 WHERE server_id = $2", edition, ServerId),
            async (postgres, cs, ct) =>
            {
                var web = await WebReadAsync(postgres);
                await using var viewer = new ViewerDataService(cs);
                var read = await viewer.ReadAgentAlertsAsync(Server(edition), DateTime.UtcNow, cancellationToken: ct);

                Assert.Equal("not_collected", web.GetProperty("status").GetString());
                Assert.Equal(ViewerAgentAlertsRead.NotCollected, read.Status);
                Assert.Contains("agent_alerts", read.Message, StringComparison.Ordinal);
                Assert.Equal(read.Message, ViewerAgentAlertsText.StatusLine(read));
                return 0;
            });
    }

    [Fact]
    public async Task NoCapture_OnAServerThatCollectsElsewhere_IsAPrecondition_NamingRds_OnBothSurfaces()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT $1::bigint + row_number() OVER (ORDER BY t), $2::int, $3::text, 'wait_stats', t, 'SUCCESS'
FROM generate_series($4::timestamp, $5::timestamp, INTERVAL '5 minutes') AS t",
                CollectionIdGenerator.Next(), ServerId, ServerName,
                DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(-3)), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-2)));
        }, async (postgres, cs, ct) =>
        {
            var web = await WebReadAsync(postgres);
            await using var viewer = new ViewerDataService(cs);
            var read = await viewer.ReadAgentAlertsAsync(Server(), DateTime.UtcNow, cancellationToken: ct);

            Assert.Equal("precondition", web.GetProperty("status").GetString());
            Assert.Equal(ViewerAgentAlertsRead.Precondition, read.Status);
            /* The Viewer words the answer for a reader: the collector's display name, where the tool names agent_alerts. */
            Assert.Contains("collector has never run", read.Message, StringComparison.Ordinal);
            Assert.Contains(CollectorRuntimePrecondition.AgentAlertsPossibleCauses, read.Message, StringComparison.Ordinal);
            return 0;
        });
    }

    [Fact]
    public async Task NoCapture_OnAPlainServerThatHasCollectedNothing_IsUnavailable_OnBothSurfaces()
    {
        await WithStoreAsync((_, _) => Task.CompletedTask, async (postgres, cs, ct) =>
        {
            var web = await WebReadAsync(postgres);
            await using var viewer = new ViewerDataService(cs);
            var read = await viewer.ReadAgentAlertsAsync(Server(), DateTime.UtcNow, cancellationToken: ct);

            Assert.Equal("unavailable", web.GetProperty("status").GetString());
            Assert.Equal(ViewerAgentAlertsRead.Unavailable, read.Status);
            Assert.StartsWith("No SQL Server Agent alert capture in the 24-hour window ending ", read.Message, StringComparison.Ordinal);
            return 0;
        });
    }
}
