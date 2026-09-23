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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4012: the deadlock reports and deadlock alerts stored before #4005 are rewritten in place, once, to #4005's
/// rules, so a direct <c>SELECT</c> finds no literal and no hash over a raw graph in either table.
///
/// <para>#4005 normalized what the collector stores and what every read returns; this pins the rewrite of what was
/// already stored (<see cref="PgDeadlockRemask"/>): only rows still raw change, they read back exactly as every read
/// showed them before (the identity aside, which becomes #4005's), a second pass changes nothing, and rows in
/// #4005's form, SQL Server alerts and alerts fired since are left as they are.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The live test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres, then works entirely inside it: the pass sweeps whole
   tables, so it must not meet another class's rows, and it cannot race the live collection. Leave it out; this
   comment is here so the next sweep does not "fix" it. */
public sealed class PgDeadlockRemaskTests
{
    private const int ServerId = 4012;
    private const string ServerName = "darling-pg-deadlock-remask-4012";

    /* A report as the collector stored it before #4005: the DETAIL block verbatim, tabs stripped, a PIN and a card
       number in each query, and a hash over that raw graph. */
    private const string LegacyGraph =
        "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
        + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n"
        + "Process 3101: UPDATE accounts SET pin = '4721' WHERE card = 4111111111111111\n"
        + "Process 3102: UPDATE accounts SET pin = '9034' WHERE card = 5500005555555559";

    private const string LegacyVictimStatement = "UPDATE accounts SET pin = '4721' WHERE card = 4111111111111111";

    private static readonly string[] s_secrets = ["4721", "9034", "4111111111111111", "5500005555555559", "Leak4012"];

    /* ───────────────────────── pure ───────────────────────── */

    /// <summary>
    /// The page, the lookup and the write all carry the parser's raw-hash test. The write's is what makes the
    /// pass safe to repeat and safe beside the collector: without it a rewrite could land on a row already in
    /// #4005's form.
    /// </summary>
    [Fact]
    public void EverySliceStatementCarriesTheParsersRawHashTest()
    {
        Assert.Contains(PgDeadlockLogParser.RawGraphHashSql("p.deadlock_hash", "p.graph_text"), PgDeadlockRemask.ReportPageSql, StringComparison.Ordinal);
        Assert.Contains(PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "d.graph_text"), PgDeadlockRemask.ReportUpdateSql, StringComparison.Ordinal);
        Assert.Contains(PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "d.graph_text"), PgDeadlockRemask.AlertReportLookupSql, StringComparison.Ordinal);
        Assert.Contains("AND   context_json = $4", PgDeadlockRemask.AlertUpdateSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A raw report becomes what every read already shows for it, under the identity the collector gives the same
    /// report since #4005, so the two sightings group as one. Applying it again changes nothing.
    /// </summary>
    [Fact]
    public void ARawReport_BecomesWhatEveryReadShows_UnderTheCollectorsIdentity()
    {
        var at = new DateTime(2026, 8, 26, 22, 25, 24, 100);

        var (victim, graph, hash) = PgDeadlockRemask.RemaskReport(at, LegacyVictimStatement, LegacyGraph);

        Assert.Equal(PgDeadlockLogParser.NormalizeStatement(LegacyVictimStatement), victim);
        Assert.Equal(PgDeadlockLogParser.NormalizeGraph(LegacyGraph), graph);
        AssertNoSecret(victim);
        AssertNoSecret(graph);

        var captured = Assert.Single(PgDeadlockLogParser.Extract(Report(at, 3101, 3102)));
        Assert.Equal(captured.DeadlockHash, hash);
        Assert.Equal(captured.GraphText, graph);
        Assert.Equal(captured.VictimStatement, victim);
        Assert.NotEqual(PgDeadlockLogParser.HashOf(graph), hash);

        Assert.Equal((victim, graph, hash), PgDeadlockRemask.RemaskReport(at, victim, graph));
    }

    /// <summary>
    /// A deadlock alert fired before #4005, in either delivery mode, takes the incident the live alert builds for
    /// its report once rewritten: its persisted context is byte for byte what that alert persists, and a per-event
    /// row's detail text is what that alert records. Read again it is left as it is.
    /// </summary>
    [Fact]
    public void ALegacyAlert_BecomesWhatTheLiveAlertPersistsForTheRewrittenReport()
    {
        var at = new DateTime(2026, 8, 26, 22, 25, 24, 100);
        var rawHash = PgDeadlockLogParser.HashOf(LegacyGraph);
        var legacy = new AlertContext
        {
            Incidents = [new AlertIncident(rawHash, [AlertContextBuilders.TruncateText(LegacyVictimStatement)])],
            SeverityOverride = AlertSeverityLevel.Warning,
        };
        var rewritten = PgDeadlockRemask.RemaskedIncident(at, 3101, 2, LegacyVictimStatement, LegacyGraph);
        var live = new AlertContext { Incidents = [rewritten], SeverityOverride = AlertSeverityLevel.Warning };

        Assert.Equal(PgDeadlockRemask.RemaskReport(at, LegacyVictimStatement, LegacyGraph).DeadlockHash, rewritten.DedupKey);
        Assert.Equal("UPDATE accounts SET pin = '?' WHERE card = ?", Assert.Single(rewritten.InvolvedObjects));

        PgDeadlockRemask.ResolvedReport Resolve(string key) =>
            key == rawHash
                ? new(PgDeadlockRemask.ReportState.Raw, rewritten)
                : new(PgDeadlockRemask.ReportState.Current, null);

        var summary = PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(legacy), null, Resolve);
        Assert.NotNull(summary);
        Assert.Equal(AlertContextSerializer.Serialize(live), summary.Value.ContextJson);
        Assert.Null(summary.Value.DetailText);

        var legacyPerEvent = Assert.Single(PerEventNotification.Split(legacy, 10)).Context;
        var livePerEvent = Assert.Single(PerEventNotification.Split(live, 10)).Context;
        var perEvent = PgDeadlockRemask.RemaskAlert(
            AlertContextSerializer.Serialize(legacyPerEvent), AlertContextBuilders.ContextToDetailText(legacyPerEvent), Resolve);
        Assert.NotNull(perEvent);
        Assert.Equal(AlertContextSerializer.Serialize(livePerEvent), perEvent.Value.ContextJson);
        Assert.Equal(AlertContextBuilders.ContextToDetailText(livePerEvent), perEvent.Value.DetailText);

        foreach (var text in new[] { summary.Value.ContextJson, perEvent.Value.ContextJson, perEvent.Value.DetailText })
        {
            AssertNoSecret(text);
            Assert.DoesNotContain(rawHash, text, StringComparison.Ordinal);
        }

        Assert.Null(PgDeadlockRemask.RemaskAlert(summary.Value.ContextJson, null, Resolve));
        Assert.Null(PgDeadlockRemask.RemaskAlert(perEvent.Value.ContextJson, perEvent.Value.DetailText, Resolve));
    }

    /// <summary>
    /// An alert whose report retention already dropped keeps its key, which nothing left can test, and has its
    /// statement normalized as it stands; its no-statement text carries no SQL and is kept. An alert built from
    /// normalized text (its report current, or named by a report identity), a SQL Server alert and a context that
    /// does not parse are all left as they are.
    /// </summary>
    [Fact]
    public void OnlyAnAlertCarryingRawText_IsRewritten()
    {
        var goneHash = PgDeadlockLogParser.HashOf("a report retention dropped");
        var raw = new AlertContext
        {
            Incidents =
            [
                new AlertIncident(goneHash, ["UPDATE creds SET pw = 'Leak4012' WHERE id = 7"]),
                new AlertIncident(goneHash[..31] + "0", ["victim pid 3101, 2 participant(s)"]),
            ],
        };
        static PgDeadlockRemask.ResolvedReport Gone(string key) => new(PgDeadlockRemask.ReportState.Missing, null);

        var gone = PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(raw), null, Gone);
        Assert.NotNull(gone);
        Assert.True(AlertContextSerializer.TryDeserialize(gone.Value.ContextJson, out var context));
        Assert.Equal(goneHash, context.Incidents![0].DedupKey);
        Assert.Equal("UPDATE creds SET pw = '?' WHERE id = ?", Assert.Single(context.Incidents[0].InvolvedObjects));
        Assert.Equal("victim pid 3101, 2 participant(s)", Assert.Single(context.Incidents[1].InvolvedObjects));
        AssertNoSecret(gone.Value.ContextJson);
        Assert.Null(PgDeadlockRemask.RemaskAlert(gone.Value.ContextJson, null, Gone));

        static PgDeadlockRemask.ResolvedReport Current(string key) => new(PgDeadlockRemask.ReportState.Current, null);
        Assert.Null(PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(raw), null, Current));

        var normalizedSince = new AlertContext
        {
            Incidents = [new AlertIncident(PgDeadlockLogParser.ReportIdentity(DateTime.UtcNow, 3101), ["UPDATE creds SET pw = 'Leak4012'"])],
        };
        Assert.Null(PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(normalizedSince), null, Gone));

        var sqlServer = new AlertContext
        {
            Incidents = [new AlertIncident(new string('a', 64), ["UPDATE creds SET pw = 'Leak4012'"])],
        };
        Assert.Null(PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(sqlServer), null, Gone));

        Assert.Null(PgDeadlockRemask.RemaskAlert("{ not json", null, Gone));
    }

    /* ───────────────────────── live ───────────────────────── */

    /// <summary>
    /// Pre-#4005 reports (one seen twice, across two compressed chunks, and one with no victim statement), a report
    /// seen once raw and once since #4005, a report stored since #4005, and alerts of every kind. After the pass no
    /// literal and no raw hash is left in either table; every report reads back as it read before, under #4005's
    /// identity, and the one seen by both builds reads as one report; the rows in #4005's form, the alert fired
    /// since and the SQL Server alert are untouched; and a second pass changes nothing.
    /// </summary>
    [Fact]
    public async Task RawReportsAndAlerts_AreRewrittenOnce_ReadAsBefore_AndNothingElseChanges()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock re-mask test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        /* Enabled on the probe's own connection (#1922), and run on a plain-PostgreSQL store too: the pass is the
           same there, only no chunk is compressed. */
        if (await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct) is var timescale && timescale)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified).AddHours(12);
        var atA = now.AddDays(-3);
        var atB = now.AddDays(-3).AddMinutes(30);
        var atD = now.AddDays(-2);
        var atC = now.AddDays(-1);

        /* A: raw, seen twice, a day apart. B: raw, no victim statement. D: raw, then seen again by this build. C: this build's. */
        var graphB = LegacyGraph.Replace("3101", "4101", StringComparison.Ordinal).Replace("3102", "4102", StringComparison.Ordinal);
        var graphD = LegacyGraph.Replace("3101", "5101", StringComparison.Ordinal).Replace("3102", "5102", StringComparison.Ordinal);
        await PlantAsync(connection, atA.AddMinutes(1), atA, 3101, "A", PgDeadlockLogParser.HashOf(LegacyGraph), LegacyVictimStatement, LegacyGraph, ct);
        await PlantAsync(connection, atA.AddDays(1), atA, 3101, "A", PgDeadlockLogParser.HashOf(LegacyGraph), LegacyVictimStatement, LegacyGraph, ct);
        await PlantAsync(connection, atB.AddMinutes(1), atB, 4101, "B", PgDeadlockLogParser.HashOf(graphB), null, graphB, ct);
        await PlantAsync(connection, atD.AddMinutes(1), atD, 5101, "D", PgDeadlockLogParser.HashOf(graphD),
            "UPDATE accounts SET pin = '4721' WHERE card = 4111111111111111", graphD, ct);
        var capturedD = Assert.Single(PgDeadlockLogParser.Extract(Report(atD, 5101, 5102)));
        await PlantAsync(connection, atD.AddMinutes(6), atD, 5101, "D", capturedD.DeadlockHash, capturedD.VictimStatement, capturedD.GraphText, ct);
        var capturedC = Assert.Single(PgDeadlockLogParser.Extract(Report(atC, 6101, 6102)));
        await PlantAsync(connection, atC.AddMinutes(1), atC, 6101, "C", capturedC.DeadlockHash, capturedC.VictimStatement, capturedC.GraphText, ct);

        /* The alerts: the summary and per-event rows the old build wrote for A and B, one fired since for C, one
           whose report is gone, and a SQL Server deadlock alert. */
        var legacyContext = new AlertContext
        {
            Incidents =
            [
                new AlertIncident(PgDeadlockLogParser.HashOf(LegacyGraph), [AlertContextBuilders.TruncateText(LegacyVictimStatement)]),
                new AlertIncident(PgDeadlockLogParser.HashOf(graphB), ["victim pid 4101, 2 participant(s)"]),
            ],
            SeverityOverride = AlertSeverityLevel.Warning,
        };
        await PlantAlertAsync(connection, atA.AddMinutes(2), null, AlertContextSerializer.Serialize(legacyContext), ct);
        var perEventA = PerEventNotification.Split(legacyContext, 10)[0].Context;
        await PlantAlertAsync(connection, atA.AddMinutes(2), AlertContextBuilders.ContextToDetailText(perEventA), AlertContextSerializer.Serialize(perEventA), ct);
        var sinceC = new AlertContext { Incidents = [new AlertIncident(capturedC.DeadlockHash, [capturedC.VictimStatement!])] };
        await PlantAlertAsync(connection, atC.AddMinutes(2), null, AlertContextSerializer.Serialize(sinceC), ct);
        var goneHash = PgDeadlockLogParser.HashOf("a report retention dropped");
        await PlantAlertAsync(connection, atA.AddMinutes(3), null, AlertContextSerializer.Serialize(new AlertContext
        {
            Incidents = [new AlertIncident(goneHash, ["UPDATE creds SET pw = 'Leak4012' WHERE id = 7"])],
        }), ct);
        var sqlServerJson = AlertContextSerializer.Serialize(new AlertContext
        {
            Incidents = [new AlertIncident(new string('b', 64), ["UPDATE creds SET pw = 'Leak4012'"])],
        });
        await PlantAlertAsync(connection, atA.AddMinutes(4), null, sqlServerJson, ct);

        if (timescale)
        {
            await ExecuteAsync(connection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('pg_deadlocks') AS c", ct);
        }

        var readsBefore = await ReadEverythingAsync(postgres, connection, now, ct);
        var newFormatBefore = await DumpAsync(connection, $"SELECT victim_statement, graph_text, deadlock_hash FROM pg_deadlocks WHERE deadlock_hash = '{capturedC.DeadlockHash}'", ct);
        Assert.Equal(5, readsBefore.Summary.Count);

        /* The pass, as the worker runs it: the alerts to their end, then the reports. */
        var (alertsRewritten, reportsRewritten, failed) = await RunPassAsync(connection, ct);
        Assert.Equal(3, alertsRewritten);
        Assert.Equal(4, reportsRewritten);
        Assert.Equal(0, failed);

        /* No literal and no raw hash left in either table, whatever reads them. */
        var deadlockDump = await DumpAsync(connection, "SELECT concat_ws('|', deadlock_hash, victim_statement, graph_text) FROM pg_deadlocks", ct);
        var alertDump = await DumpAsync(connection, $"SELECT concat_ws('|', detail_text, context_json) FROM config_alert_log WHERE context_json <> '{sqlServerJson.Replace("'", "''", StringComparison.Ordinal)}'", ct);
        foreach (var text in new[] { deadlockDump, alertDump })
        {
            AssertNoSecret(text);
            foreach (var raw in new[] { LegacyGraph, graphB, graphD })
            {
                Assert.DoesNotContain(PgDeadlockLogParser.HashOf(raw), text, StringComparison.Ordinal);
            }
        }

        /* Every report reads as it did, the identity aside. A and B take #4005's identity, and the name every read
           gave them before still finds them. D's two sightings are one report now. C is untouched. */
        var readsAfter = await ReadEverythingAsync(postgres, connection, now, ct);
        Assert.Equal(4, readsAfter.Summary.Count);
        foreach (var before in readsBefore.Summary.Where(r => r.OccurredAtUtc != atD))
        {
            var after = Assert.Single(readsAfter.Summary, r => r.OccurredAtUtc == before.OccurredAtUtc);
            Assert.Equal(before with { DeadlockHash = after.DeadlockHash }, after);
            var detailBefore = Assert.Single(readsBefore.Details[before.DeadlockHash]);
            var detailAfter = Assert.Single(readsAfter.Details[after.DeadlockHash]);
            Assert.Equal(before.DeadlockHash, detailBefore.DeadlockHash);
            Assert.Equal(detailBefore with { DeadlockHash = after.DeadlockHash }, detailAfter);
            if (before.OccurredAtUtc == atC)
            {
                Assert.Equal(before.DeadlockHash, after.DeadlockHash);
                continue;
            }

            Assert.Equal(PgDeadlockLogParser.ReportIdentity(before.OccurredAtUtc, before.VictimPid), before.DeadlockHash);
            Assert.False(PgDeadlockLogParser.TryParseReportIdentity(after.DeadlockHash, out _, out _));
            var byOldName = Assert.Single(await DarlingPgDeadlockReader.GetDeadlockDetailAsync(postgres, ServerId, before.DeadlockHash, 5, ct));
            Assert.Equal(detailAfter, byOldName);
        }

        var mergedD = Assert.Single(readsAfter.Summary, r => r.OccurredAtUtc == atD);
        Assert.Equal(capturedD.DeadlockHash, mergedD.DeadlockHash);
        Assert.Equal(2, mergedD.TimesSeen);
        Assert.Equal(capturedD.VictimStatement, mergedD.VictimStatement);

        /* The drill-down's shapes, read by its own SQL and normalized as it normalizes them, are the same: B's is
           a raw report before and a rewritten one after. Its report counts are not compared, because D's two
           sightings are one report now. */
        Assert.Equal(readsBefore.DrillDown, readsAfter.DrillDown);
        Assert.Equal(newFormatBefore, await DumpAsync(connection, $"SELECT victim_statement, graph_text, deadlock_hash FROM pg_deadlocks WHERE deadlock_hash = '{capturedC.DeadlockHash}'", ct));

        /* The alerts carry the rewritten reports' incidents; the one fired since and the SQL Server one are as
           planted. */
        var alertsAfter = await ReadAlertsAsync(connection, ct);
        var expectedA = PgDeadlockRemask.RemaskedIncident(atA, 3101, 2, LegacyVictimStatement, LegacyGraph);
        var expectedB = PgDeadlockRemask.RemaskedIncident(atB, 4101, 2, null, graphB);
        var liveContext = new AlertContext { Incidents = [expectedA, expectedB], SeverityOverride = AlertSeverityLevel.Warning };
        var livePerEvent = PerEventNotification.Split(liveContext, 10)[0].Context;
        Assert.Contains((AlertContextSerializer.Serialize(liveContext), (string?)null), alertsAfter);
        Assert.Contains((AlertContextSerializer.Serialize(livePerEvent), AlertContextBuilders.ContextToDetailText(livePerEvent)), alertsAfter);
        Assert.Contains((AlertContextSerializer.Serialize(sinceC), (string?)null), alertsAfter);
        Assert.Contains((sqlServerJson, (string?)null), alertsAfter);
        Assert.Contains(alertsAfter, a => a.Context.Contains(goneHash, StringComparison.Ordinal) && a.Context.Contains("WHERE id = ?", StringComparison.Ordinal));

        /* A second pass finds nothing to do and changes nothing. */
        var tablesBefore = deadlockDump + "\n" + await DumpAsync(connection, "SELECT concat_ws('|', detail_text, context_json) FROM config_alert_log", ct);
        var second = await RunPassAsync(connection, ct);
        Assert.Equal((0, 0, 0), second);
        Assert.Equal(tablesBefore, await DumpAsync(connection, "SELECT concat_ws('|', deadlock_hash, victim_statement, graph_text) FROM pg_deadlocks", ct)
            + "\n" + await DumpAsync(connection, "SELECT concat_ws('|', detail_text, context_json) FROM config_alert_log", ct));
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private sealed record Reads(
        List<DarlingPgDeadlockReader.PgDeadlockRow> Summary,
        Dictionary<string, List<DarlingPgDeadlockReader.PgDeadlockDetailRow>> Details,
        string DrillDown);

    /* Every product read of the reports: the summary and detail reads (get_pg_deadlocks, get_pg_deadlock_detail,
       the web dispatch, the viewer and the alert all go through them) and the analysis drill-down's own SQL, its
       columns normalized as CollectDeadlockExemplarsAsync normalizes them. */
    private static async Task<Reads> ReadEverythingAsync(NpgsqlDataSource postgres, NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        var summary = await DarlingPgDeadlockReader.GetDeadlocksAsync(postgres, ServerId, now.AddDays(-10), now, 50, ct);
        var details = new Dictionary<string, List<DarlingPgDeadlockReader.PgDeadlockDetailRow>>();
        foreach (var row in summary)
        {
            details[row.DeadlockHash] = await DarlingPgDeadlockReader.GetDeadlockDetailAsync(postgres, ServerId, row.DeadlockHash, 5, ct);
        }

        var drill = new List<string>();
        await using var command = new NpgsqlCommand(PgTargetDrillDownCollector.PgTargetDeadlockExemplarsSql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(now.AddDays(-10));
        command.Parameters.AddWithValue(now);
        command.Parameters.AddWithValue(PgTargetDrillDownCollector.DeadlockExemplarCap);
        command.Parameters.AddWithValue(PgDeadlockLogParser.NormalizeReadCap);
        command.Parameters.AddWithValue(PgDeadlockLogParser.NormalizeReadCap);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            drill.Add(string.Join("|",
                reader.GetValue(0), reader.GetValue(1), reader.GetValue(2),
                PgDeadlockLogParser.NormalizeStatement(reader.IsDBNull(9) ? null : reader.GetString(9)),
                PgDeadlockLogParser.NormalizeGraph(reader.IsDBNull(11) ? null : reader.GetString(11))));
        }

        drill.Sort(StringComparer.Ordinal);
        return new Reads(summary, details, string.Join("\n", drill));
    }

    private static async Task<(int Alerts, int Reports, int Failed)> RunPassAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        int alerts = 0, reports = 0, failed = 0;
        string? alertCursor = null;
        do
        {
            var (next, _, rewritten, raced) = await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, alertCursor, ct);
            Assert.Equal(0, raced);
            alerts += rewritten;
            alertCursor = next;
        }
        while (alertCursor is not null);

        PgDeadlockRemask.ReportCursor? reportCursor = null;
        do
        {
            var (next, _, rewritten, failures) = await PgDeadlockRemask.RemaskStoredReportsAsync(connection, reportCursor, ct);
            reports += rewritten;
            failed += failures;
            reportCursor = next;
        }
        while (reportCursor is not null);

        return (alerts, reports, failed);
    }

    private static async Task<List<(string Context, string? Detail)>> ReadAlertsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var rows = new List<(string, string?)>();
        await using var command = new NpgsqlCommand("SELECT context_json, detail_text FROM config_alert_log WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(ServerId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        return rows;
    }

    private static async Task<string> DumpAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => reader.IsDBNull(i) ? "<null>" : reader.GetValue(i).ToString())));
        }

        rows.Sort(StringComparer.Ordinal);
        return string.Join("\n", rows);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /* A report as a target writes it, with values in both queries and the HINT that proves it whole. */
    private static string Report(DateTime at, int victim, int other)
    {
        var prefix = at.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture) + $" UTC [{victim}] ";
        return prefix + "ERROR:  deadlock detected\n"
            + prefix + $"DETAIL:  Process {victim} waits for ShareLock on transaction 5501; blocked by process {other}.\n"
            + $"\tProcess {other} waits for ShareLock on transaction 5500; blocked by process {victim}.\n"
            + $"\tProcess {victim}: UPDATE accounts SET pin = '4721' WHERE card = 4111111111111111\n"
            + $"\tProcess {other}: UPDATE accounts SET pin = '9034' WHERE card = 5500005555555559\n"
            + prefix + "HINT:  See server log for query details.\n";
    }

    private static void AssertNoSecret(string? text)
    {
        Assert.NotNull(text);
        foreach (var secret in s_secrets)
        {
            Assert.DoesNotContain(secret, text, StringComparison.Ordinal);
        }
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, DateTime collectedAt, DateTime occurredAt, int victimPid, string shape, string hash,
        string? victimStatement, string graphText, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1, $2, $3, $4, $5, $6, 2, $7, 'ShareLock', $10, $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectedAt, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(occurredAt, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(victimPid);
        command.Parameters.AddWithValue(hash);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)victimStatement ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.AddWithValue(graphText);
        /* One lock shape per report, so the drill-down names each report's own latest sighting. */
        command.Parameters.AddWithValue("transaction 5500, transaction 5501, report " + shape);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantAlertAsync(
        NpgsqlConnection connection, DateTime alertTime, string? detailText, string contextJson, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1, $2, $3, $4, 1, 1, true, 'webhook', NULL, false, $5, $6)", connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(alertTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(AlertEngine.DeadlockWatermarkMetric);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)detailText ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.AddWithValue(contextJson);
        await command.ExecuteNonQueryAsync(ct);
    }
}
