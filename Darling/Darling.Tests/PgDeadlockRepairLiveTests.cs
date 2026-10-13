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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The deadlock repair pass (<see cref="PgDeadlockRemask.RunRepairAsync"/>) against a real store: each of its four
/// walks rewrites the rows it should (a stored statement the graph does not hold, an incident keyed <c>hk-</c> or
/// <c>at-</c>, a stamped finding, a stamped finding alert), a second run writes nothing, and the marker is written
/// only once the walks covered the rows: an UPDATE that finds no row leaves the walk unclean, and a walk cut by the
/// slice's budget resumes and finishes.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")], like PgDeadlockRemaskTests. The test reaches
   DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it: the
   walks read whole tables, so they must not meet another class's rows. Leave it out; this comment is here so the next
   sweep does not "fix" it. */
public sealed class PgDeadlockRepairLiveTests
{
    private const int ServerId = 4012;
    private const string Weak = "SELECT secretword FROM t";

    private const string Graph =
        "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
        + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n"
        + "Process 3101: UPDATE accounts SET pin = '47Q1' WHERE card = 4111111111111111\n"
        + "Process 3102: UPDATE accounts SET pin = '90X4' WHERE card = 5500005555555559";

    private static readonly DateTime s_occurred = new(2026, 10, 1, 12, 30, 15, 250, DateTimeKind.Unspecified);

    [Fact]
    public async Task EachWalkRewritesItsRows_TheMarkerWaitsForThem_AndASecondRunWritesNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock repair test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var (expectedStatement, normalizedGraph, identity) = PgDeadlockRemask.RemaskReport(s_occurred, 3101, Graph);
        Assert.NotNull(expectedStatement);
        Assert.DoesNotContain("secretword", expectedStatement, StringComparison.Ordinal);
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        /* A report in #4005's form holding a statement the graph does not hold. */
        await ExecuteAsync(connection, @"
INSERT INTO pg_deadlocks (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES (1, $1, $2, 'repair-live', $3, 3101, 2, $4, 'ShareLock', 'transaction 5500', $5, $6)", ct,
            now.AddHours(-3), ServerId, s_occurred, identity, Weak, normalizedGraph);

        /* Alerts: one naming that report by its hash, one keyed hk- and one keyed at-, each holding the weak statement. */
        var keyed = PgLogHashKey.DeadlockAlertKeyTag + new string('b', 40);
        var atKey = PgDeadlockLogParser.ReportIdentity(s_occurred.AddDays(-2), 4242);
        var hashes = new[] { identity, keyed, atKey };
        for (var i = 0; i < hashes.Length; i++)
        {
            var context = AlertContextSerializer.Serialize(new AlertContext { Incidents = [new AlertIncident(hashes[i], [Weak])] });
            await PlantAlertAsync(connection, now.AddMinutes(-30 + i), "involved: " + Weak, context, AlertEngine.DeadlockWatermarkMetric, ct);
        }

        /* A finding an earlier build stamped (no rules version) holding the weak statement, and a finding alert
           stamped the same way whose finding is not in the store. */
        var (legacyDrillDown, legacyStory) = PgDeadlockRemaskTests.LegacyFinding();
        var stamped = PgDeadlockRemask.RemaskFinding(legacyDrillDown, legacyStory)!.Value;
        var weakFinding = WithEarlierStamp(stamped.DrillDownJson, Weak);
        await PgDeadlockRemaskTests.PlantFindingAsync(connection, 1, now.AddDays(-1), weakFinding, stamped.StoryText, ct, storyPathHash: "4012abcd00000000");
        var (metric, legacyContext) = PgDeadlockRemaskTests.LegacyFindingAlert(legacyDrillDown, legacyStory, "4012beef00000000");
        var stampedAlert = PgDeadlockRepairTests.WithExemplarField(legacyContext, "Sql Normalized", "true");
        await PlantAlertAsync(connection, now.AddHours(-2), null, stampedAlert, metric, ct);

        var before = await SnapshotAsync(connection, ct);
        var progress = new PgDeadlockRemask.RemaskProgress();
        for (var tick = 0; tick < 8 && progress.RulesCurrent != true; tick++)
        {
            Assert.Null(await PgDeadlockRemask.ReadRulesMarkerAsync(connection, ct));
            await PgDeadlockRemask.RunRepairAsync(connection, progress, NullLogger.Instance, ct);
        }

        Assert.True(progress.RulesCurrent);
        Assert.Equal(PgDeadlockRemask.RulesVersion.ToString(), await PgDeadlockRemask.ReadRulesMarkerAsync(connection, ct));

        /* The report walk wrote the graph's statement; the alert walk withheld the keyed incidents and rebuilt the named one;
           the finding walk derived the exemplar and stamped the version; the finding alert walk withheld the alert. */
        Assert.Equal(expectedStatement, await ScalarAsync(connection, "SELECT victim_statement FROM pg_deadlocks", ct));
        var alerts = await TextAsync(connection, $"SELECT context_json || '|' || coalesce(detail_text, '') FROM config_alert_log WHERE metric_name = '{AlertEngine.DeadlockWatermarkMetric}' ORDER BY alert_time", ct);
        Assert.Equal(3, alerts.Count);
        Assert.All(alerts, a => Assert.DoesNotContain("secretword", a, StringComparison.Ordinal));
        Assert.Contains(keyed, alerts[1], StringComparison.Ordinal);
        Assert.Contains(PgLogTextRedactor.WithheldStatement, alerts[1], StringComparison.Ordinal);
        Assert.Contains(atKey, alerts[2], StringComparison.Ordinal);
        Assert.Contains(PgLogTextRedactor.WithheldStatement, alerts[2], StringComparison.Ordinal);
        var findings = await TextAsync(connection, "SELECT drill_down_json FROM analysis_findings", ct);
        Assert.DoesNotContain("secretword", Assert.Single(findings), StringComparison.Ordinal);
        Assert.Contains("\"text_rules_version\":" + PgDeadlockRemask.RulesVersion, findings[0], StringComparison.Ordinal);
        var findingAlerts = await TextAsync(connection, $"SELECT context_json FROM config_alert_log WHERE metric_name = '{metric.Replace("'", "''")}'", ct);
        Assert.Contains(PgDeadlockRemask.WithheldBefore4005, Assert.Single(findingAlerts), StringComparison.Ordinal);

        var after = await SnapshotAsync(connection, ct);
        Assert.NotEqual(before, after);

        /* A second run, with the marker taken away so the walks run again, writes no row. */
        await ExecuteAsync(connection, "DELETE FROM collect.collector_state WHERE collector_name = $1", ct, PgDeadlockRemask.StateCollectorName);
        var second = new PgDeadlockRemask.RemaskProgress();
        for (var tick = 0; tick < 4 && second.RulesCurrent != true; tick++)
        {
            await PgDeadlockRemask.RunRepairAsync(connection, second, NullLogger.Instance, ct);
        }

        Assert.True(second.RulesCurrent);
        Assert.Equal(after, await SnapshotAsync(connection, ct));
    }

    /// <summary>An UPDATE that finds no row is a row left, never a clean walk: the marker is not written over it.</summary>
    [Fact]
    public async Task AReportWhoseUpdateFindsNoRow_IsLeft_AndNoMarkerIsWritten()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock repair test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await PlantReportsAsync(connection, 3, ct);

        /* A trigger that skips every UPDATE: the statement matches a row, and nothing is written. */
        await ExecuteAsync(connection, "CREATE FUNCTION skip_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$", ct);
        await ExecuteAsync(connection, "CREATE TRIGGER skip_update BEFORE UPDATE ON pg_deadlocks FOR EACH ROW EXECUTE FUNCTION skip_update()", ct);

        var progress = new PgDeadlockRemask.RemaskProgress();
        for (var tick = 0; tick < 6; tick++)
        {
            await PgDeadlockRemask.RunRepairAsync(connection, progress, NullLogger.Instance, ct);
        }

        Assert.NotEqual(true, progress.RulesCurrent);
        Assert.True(progress.RepairReports.GaveUp);
        Assert.Null(await PgDeadlockRemask.ReadRulesMarkerAsync(connection, ct));
        Assert.Equal(3L, Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM pg_deadlocks WHERE victim_statement = '" + Weak + "'", ct)));
    }

    /// <summary>A walk the slice's budget cut resumes at its cursor on the next tick and finishes: it is neither
    /// unclean nor started over, and the marker is written once every row is covered.</summary>
    [Fact]
    public async Task AWalkCutByTheBudget_ResumesAndFinishes_AndWritesTheMarker()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock repair test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var total = PgDeadlockRemask.MaxReportRowsPerPass + 150;
        await PlantReportsAsync(connection, total, ct);

        /* The budget ends the first tick part-way through its first page. */
        using var cut = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var progress = new PgDeadlockRemask.RemaskProgress();
        var counted = 0;
        progress.RowCounted = (_, outcome) =>
        {
            if (outcome == PgDeadlockRemask.RowOutcome.Rewritten && ++counted == 40)
            {
                cut.Cancel();
            }
        };
        await PgDeadlockRemask.RunRepairAsync(connection, progress, NullLogger.Instance, cut.Token);

        Assert.False(progress.RepairReports.WalkInterrupted);
        Assert.NotEqual(true, progress.RulesCurrent);
        Assert.Null(await PgDeadlockRemask.ReadRulesMarkerAsync(connection, ct));
        Assert.NotNull(progress.RepairReportCursor);

        progress.RowCounted = null;
        for (var tick = 0; tick < 6 && progress.RulesCurrent != true; tick++)
        {
            await PgDeadlockRemask.RunRepairAsync(connection, progress, NullLogger.Instance, ct);
        }

        Assert.True(progress.RulesCurrent);
        Assert.False(progress.RepairReports.GaveUp);
        Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM pg_deadlocks WHERE victim_statement = '" + Weak + "'", ct)));
        Assert.Equal(PgDeadlockRemask.RulesVersion.ToString(), await PgDeadlockRemask.ReadRulesMarkerAsync(connection, ct));
    }

    /// <summary>Reports in #4005's form (a hash that is not over the raw graph), one batch each, holding the weak statement.</summary>
    private static async Task PlantReportsAsync(NpgsqlConnection connection, int count, CancellationToken ct)
    {
        var (_, normalizedGraph, _) = PgDeadlockRemask.RemaskReport(s_occurred, 3101, Graph);
        await ExecuteAsync(connection, @"
INSERT INTO pg_deadlocks (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
SELECT 9000000000 + g, $1::timestamp + g * interval '1 second', $2, 'repair-live', $3::timestamp + g * interval '1 second', 3101, 2, upper(md5(g::text)), 'ShareLock', 'transaction 5500', $4, $5
FROM generate_series(1, $6) AS g", ct, DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified), ServerId, s_occurred, Weak, normalizedGraph, count);
    }

    private static string WithEarlierStamp(string drillDownJson, string statement)
    {
        var drillDown = DrillDownSerializer.Deserialize(drillDownJson)!;
        var node = JsonNode.Parse(((JsonElement)drillDown[PgTargetDrillDownCollector.DeadlockExemplarsSection]).GetRawText())!.AsObject();
        node["exemplars"]!.AsArray()[0]!.AsObject()["victim_statement"] = statement;
        Assert.True(node.Remove(PgTargetDrillDownCollector.DeadlockTextRulesVersionKey));
        drillDown[PgTargetDrillDownCollector.DeadlockExemplarsSection] = JsonSerializer.SerializeToElement(node);
        return DrillDownSerializer.Serialize(drillDown)!;
    }

    private static async Task PlantAlertAsync(
        NpgsqlConnection connection, DateTime alertTime, string? detailText, string contextJson, string metric, CancellationToken ct) =>
        await ExecuteAsync(connection, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1, $2, 'repair-live', $3, 1, 1, true, 'webhook', NULL, false, $4, $5)", ct,
            DateTime.SpecifyKind(alertTime, DateTimeKind.Unspecified), ServerId, metric, (object?)detailText ?? DBNull.Value, contextJson);

    /// <summary>The versions of every row the repair can write: a row written has a new <c>xmin</c>.</summary>
    private static async Task<string> SnapshotAsync(NpgsqlConnection connection, CancellationToken ct) =>
        string.Join(
            ";",
            await TextAsync(connection, @"
SELECT 'd' || xmin::text || victim_statement FROM pg_deadlocks
UNION ALL SELECT 'a' || xmin::text || context_json FROM config_alert_log
UNION ALL SELECT 'f' || xmin::text || drill_down_json FROM analysis_findings
ORDER BY 1", ct));

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task<List<string>> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in parameters)
        {
            var parameter = new NpgsqlParameter { Value = value };
            if (value is DateTime)
            {
                parameter.NpgsqlDbType = NpgsqlDbType.Timestamp;
            }

            command.Parameters.Add(parameter);
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
