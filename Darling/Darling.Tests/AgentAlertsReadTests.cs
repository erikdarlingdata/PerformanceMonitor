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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#6874 part 2: the pure instance status over a capture's alerts, and the fired-in-window rule.</summary>
public sealed class AgentAlertStatusTests
{
    private static AgentAlertFact Alert(int id, int severity = 0, int message = 0, bool enabled = true, int operators = 1, int? notification = 1, bool? fired = null) =>
        new(id, "alert " + id, enabled, "MSSQLSERVER", message, severity, null, null, null, 300, null, null, 0, null,
            notification, operators, null, fired);

    private static IEnumerable<AgentAlertFact> Severities(params int[] values) => values.Select(v => Alert(v, severity: v));

    private static IEnumerable<AgentAlertFact> Errors(params int[] values) => values.Select(v => Alert(v, message: v));

    private static readonly int[] AllSeverities = [19, 20, 21, 22, 23, 24, 25];
    private static readonly int[] AllErrors = [823, 824, 825];

    private static List<AgentAlertFact> Complete() => Severities(AllSeverities).Concat(Errors(AllErrors)).ToList();

    public static TheoryData<string, AgentAlertFact[], string, string[]> Cases() => new()
    {
        { "complete and notified", Complete().ToArray(), "ok", [] },
        { "no alerts at all (the marker row)", [], "gaps", ["no_alerts_defined", "missing_severity_alerts", "missing_error_alerts"] },
        { "severity 24 and 25 missing", Complete().Where(a => a.Severity is not (24 or 25)).ToArray(), "gaps", ["missing_severity_alerts"] },
        { "error 825 missing", Complete().Where(a => a.MessageId != 825).ToArray(), "gaps", ["missing_error_alerts"] },
        { "an enabled alert with no operator", Complete().Select(a => a.Severity == 20 ? a with { OperatorCount = 0 } : a).ToArray(), "gaps", ["alert_without_operator"] },
        { "operators exist but the notification bitmask is zero", Complete().Select(a => a.Severity == 20 ? a with { HasNotification = 0 } : a).ToArray(), "gaps", ["alert_without_operator"] },
        { "a disabled alert is disabled, not missing and not unnotified", Complete().Select(a => a.Severity == 21 ? a with { Enabled = false, OperatorCount = 0 } : a).ToArray(), "gaps", ["alert_disabled"] },
        { "fired in the window and told no one", Complete().Select(a => a.MessageId == 823 ? a with { OperatorCount = 0, FiredInWindow = true } : a).ToArray(), "gaps", ["alert_without_operator", "fired_unnotified"] },
        { "fired in the window and notified", Complete().Select(a => a.MessageId == 823 ? a with { FiredInWindow = true } : a).ToArray(), "ok", [] },
        { "unknown operator count is not a gap", Complete().Select(a => a.Severity == 20 ? a with { OperatorCount = null, HasNotification = null } : a).ToArray(), "ok", [] },
        { "unnotified alert that never fired is only the operator gap", Complete().Select(a => a.Severity == 22 ? a with { OperatorCount = 0, FiredInWindow = false } : a).ToArray(), "gaps", ["alert_without_operator"] },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void TheInstanceStatus_NamesEachGap(string name, AgentAlertFact[] alerts, string word, string[] codes)
    {
        var status = AgentAlertStatus.Evaluate(alerts);

        Assert.True(word == status.Word, name + ": word " + status.Word);
        Assert.Equal(codes, status.Gaps.Select(g => g.Code).ToArray());
    }

    [Fact]
    public void TheMissingGaps_NameTheSeveritiesAndErrors_AndTheAlertGapsNameTheAlerts()
    {
        var alerts = Complete()
            .Where(a => a.Severity != 19 && a.MessageId != 824)
            .Select(a => a.Severity == 20 ? a with { OperatorCount = 0, FiredInWindow = true } : a)
            .ToList();

        var gaps = AgentAlertStatus.Evaluate(alerts).Gaps.ToDictionary(g => g.Code);

        Assert.Equal([19], gaps["missing_severity_alerts"].Missing);
        Assert.Equal([824], gaps["missing_error_alerts"].Missing);
        Assert.Equal(["alert 20"], gaps["alert_without_operator"].Alerts);
        Assert.Equal(["alert 20"], gaps["fired_unnotified"].Alerts);
    }

    [Theory]
    [InlineData(5, 8, true, true)]     // the count rose
    [InlineData(5, 5, true, false)]    // unchanged
    [InlineData(9, 2, true, true)]     // reset in the window, then counted again
    [InlineData(9, 0, true, false)]    // reset in the window, nothing since
    [InlineData(null, 3, true, true)]  // an alert new to the newest capture starts from zero
    [InlineData(5, 8, false, null)]    // one capture holds no change
    public void FiredInTheWindow_ComesFromTheChangeInTheCount(int? oldest, int? newest, bool twoCaptures, bool? expected)
    {
        Assert.Equal(expected, AgentAlertStatus.Fired(oldest, newest, twoCaptures));
    }
}

/// <summary>
/// #6874 part 2: <c>get_agent_alerts</c> over a real store. Two seeded captures give the alerts, the status and which alert fired in the
/// window, the marker row reads as a gap and not as empty data, and Azure SQL Database, Managed Instance and Amazon RDS each get their
/// own no-capture answer through the routes the tool takes.
/// </summary>
[Collection("live-postgres")]
public sealed class AgentAlertsReadLiveTests
{
    private const string ServerName = "darling-agent-alerts";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, CancellationToken, Task<T>> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live agent alert tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await seed(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await body(postgres, ct);
    }

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, int? alertId, string? name, bool? enabled, int severity, int message, int count, int operators, int notification) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.agent_alerts
(collection_id, collection_time, server_id, server_name, alert_id, alert_name, enabled, event_source, message_id, severity,
 delay_between_responses, last_occurrence, occurrence_count, has_notification, operator_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, alertId, name, enabled,
            alertId is null ? null : "MSSQLSERVER", alertId is null ? null : message, alertId is null ? null : severity,
            alertId is null ? null : 300, null, alertId is null ? null : count, alertId is null ? null : notification, alertId is null ? null : operators);

    private static Task<string> Call(NpgsqlDataSource postgres, CancellationToken ct) =>
        DarlingMcpAgentAlertTools.GetAgentAlerts(postgres, ServerName, 24, null, ct);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public async Task TwoCaptures_GiveTheAlerts_TheStatus_AndWhichAlertFiredInTheWindow()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var first = DateTime.UtcNow.AddHours(-3);
            var second = DateTime.UtcNow.AddMinutes(-10);
            /* Severity 19 is notified and quiet. Severity 20 has no operator and its count rose from 2 to 5. Error 823 is disabled. 24 is new. */
            await InsertAsync(connection, ct, first, 1, "Sev 19", true, 19, 0, 7, 1, 1);
            await InsertAsync(connection, ct, first, 2, "Sev 20", true, 20, 0, 2, 0, 0);
            await InsertAsync(connection, ct, first, 3, "Err 823", false, 0, 823, 0, 1, 1);
            await InsertAsync(connection, ct, second, 1, "Sev 19", true, 19, 0, 7, 1, 1);
            await InsertAsync(connection, ct, second, 2, "Sev 20", true, 20, 0, 5, 0, 0);
            await InsertAsync(connection, ct, second, 3, "Err 823", false, 0, 823, 0, 1, 1);
            await InsertAsync(connection, ct, second, 4, "Sev 24", true, 24, 0, 1, 1, 1);
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));

            Assert.Equal(2, root.GetProperty("captures_in_window").GetInt32());
            Assert.Equal(4, root.GetProperty("alert_count").GetInt32());
            Assert.Equal("gaps", root.GetProperty("instance_status").GetString());
            var gaps = root.GetProperty("gaps").EnumerateArray().ToDictionary(g => g.GetProperty("code").GetString()!);
            Assert.Equal([21, 22, 23, 25], gaps["missing_severity_alerts"].GetProperty("missing").EnumerateArray().Select(v => v.GetInt32()).ToArray());
            Assert.Equal([824, 825], gaps["missing_error_alerts"].GetProperty("missing").EnumerateArray().Select(v => v.GetInt32()).ToArray());
            Assert.Equal(["Sev 20"], gaps["alert_without_operator"].GetProperty("alerts").EnumerateArray().Select(v => v.GetString()).ToArray());
            Assert.Equal(["Err 823"], gaps["alert_disabled"].GetProperty("alerts").EnumerateArray().Select(v => v.GetString()).ToArray());
            Assert.Equal(["Sev 20"], gaps["fired_unnotified"].GetProperty("alerts").EnumerateArray().Select(v => v.GetString()).ToArray());
            Assert.False(gaps.ContainsKey("no_alerts_defined"));

            var alerts = root.GetProperty("alerts").EnumerateArray().ToDictionary(a => a.GetProperty("alert_id").GetInt32());
            Assert.False(alerts[1].GetProperty("fired_in_window").GetBoolean());
            Assert.True(alerts[2].GetProperty("fired_in_window").GetBoolean());
            Assert.Equal(5, alerts[2].GetProperty("occurrence_count").GetInt32());
            Assert.True(alerts[4].GetProperty("fired_in_window").GetBoolean());
            Assert.False(alerts[3].GetProperty("enabled").GetBoolean());
            return 0;
        });
    }

    [Fact]
    public async Task OneCapture_SaysFiredInWindowIsUnknown_NotFalse()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await InsertAsync(connection, ct, DateTime.UtcNow.AddMinutes(-5), 1, "Sev 19", true, 19, 0, 7, 1, 1);
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));

            Assert.Equal(1, root.GetProperty("captures_in_window").GetInt32());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("alerts")[0].GetProperty("fired_in_window").ValueKind);
            return 0;
        });
    }

    [Fact]
    public async Task TheMarkerRow_IsAGap_NotEmptyData()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await InsertAsync(connection, ct, DateTime.UtcNow.AddMinutes(-5), null, null, null, 0, 0, 0, 0, 0);
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));

            Assert.False(root.TryGetProperty("status", out _));
            Assert.Equal(0, root.GetProperty("alert_count").GetInt32());
            Assert.Equal("gaps", root.GetProperty("instance_status").GetString());
            Assert.Equal("no_alerts_defined", root.GetProperty("gaps")[0].GetProperty("code").GetString());
            return 0;
        });
    }

    [Fact]
    public async Task NoCapture_OnAPlainSqlServer_ReadsUnavailable()
    {
        await WithStoreAsync((_, _) => Task.CompletedTask, async (postgres, ct) =>
        {
            Assert.Equal("unavailable", Parse(await Call(postgres, ct)).GetProperty("status").GetString());
            return 0;
        });
    }

    [Theory]
    [InlineData(5)]  // Azure SQL Database
    [InlineData(8)]  // Azure SQL Managed Instance
    public async Task NoCapture_OnAzureSqlDatabaseAndManagedInstance_ReadsNotCollected(int edition)
    {
        await WithStoreAsync((connection, ct) =>
            DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_engine_edition = $1 WHERE server_id = $2", edition, ServerId),
        async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));

            Assert.Equal("not_collected", root.GetProperty("status").GetString());
            Assert.Contains("agent_alerts", root.GetProperty("message").GetString(), StringComparison.Ordinal);
            return 0;
        });
    }

    /// <summary>
    /// Amazon RDS reports an ordinary edition, so the engine route cannot see it and the collector never runs there. The tool takes the
    /// gated-off route get_running_jobs takes: the server collects other things, this collector never ran, and the answer names RDS as a
    /// possible cause. That route words the answer as <c>precondition</c>, not <c>not_collected</c>, because the registry cannot prove RDS.
    /// </summary>
    [Fact]
    public async Task NoCapture_OnAServerThatCollectsElsewhere_TakesTheGatedOffRoute_AndNamesRds()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT $1::bigint + row_number() OVER (ORDER BY t), $2::int, $3::text, 'wait_stats', t, 'SUCCESS'
FROM generate_series($4::timestamp, $5::timestamp, INTERVAL '5 minutes') AS t",
                CollectionIdGenerator.Next(), ServerId, ServerName,
                DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(-3)), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-2)));
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));

            Assert.Equal("precondition", root.GetProperty("status").GetString());
            var message = root.GetProperty("message").GetString()!;
            Assert.Contains("agent_alerts collector has never run", message, StringComparison.Ordinal);
            Assert.Contains(CollectorRuntimePrecondition.AgentAlertsPossibleCauses, message, StringComparison.Ordinal);
            return 0;
        });
    }
}
