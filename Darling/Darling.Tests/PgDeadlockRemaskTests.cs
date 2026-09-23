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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4012: the deadlock reports, deadlock alerts and analysis findings' deadlock exemplars stored before #4005 are
/// rewritten in place, once, to #4005's rules, so a direct <c>SELECT</c> finds no literal and no hash over a raw graph
/// in any of the three tables.
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

    /* A store's log-hash key (#4004), fixed so the expected values are reproducible. */
    private static readonly PgLogHashKey s_key = new(Enumerable.Range(1, PgLogHashKey.KeyLength).Select(i => (byte)i).ToArray());

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
        Assert.Contains(PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "d.graph_text"), PgDeadlockRemask.ReportUpdateOwnBatchSql, StringComparison.Ordinal);
        Assert.Contains(PgDeadlockLogParser.RawGraphHashSql("a.deadlock_hash", "a.graph_text"), PgDeadlockRemask.ReportUpdateSql, StringComparison.Ordinal);
        Assert.Contains(PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "d.graph_text"), PgDeadlockRemask.AlertPageSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4012's review: no write sends a row's raw text back to the store as a parameter, where
    /// <c>log_min_duration_statement</c> would put it in the store's own log. The alert and finding writes check the
    /// row by its version (<c>xmin</c>); the report write by the raw-hash test, which binds the graph it read. And the
    /// findings page looks only into findings rooted where the deadlock section is attached, so it detoasts no other.
    /// </summary>
    [Fact]
    public void NoWriteSendsRawTextBack_AndTheFindingsPageIsFilteredByRoot()
    {
        Assert.Contains("AND   xmin = $4::xid", PgDeadlockRemask.AlertUpdateSql, StringComparison.Ordinal);
        Assert.Contains("AND   xmin = $4::xid", PgDeadlockRemask.FindingUpdateSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$5", PgDeadlockRemask.AlertUpdateSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$5", PgDeadlockRemask.FindingUpdateSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$9", PgDeadlockRemask.ReportUpdateSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", PgDeadlockRemask.FindingAlertUpdateSql, StringComparison.Ordinal);
        foreach (var sql in new[] { PgDeadlockRemask.AlertUpdateSql, PgDeadlockRemask.FindingUpdateSql, PgDeadlockRemask.FindingAlertUpdateSql, PgDeadlockRemask.ReportUpdateSql, PgDeadlockRemask.ReportUpdateOwnBatchSql })
        {
            Assert.DoesNotContain("context_json =", sql.Split("WHERE")[1], StringComparison.Ordinal);
            Assert.DoesNotContain("drill_down_json =", sql.Split("WHERE")[1], StringComparison.Ordinal);
            Assert.DoesNotContain("victim_statement", sql.Split("WHERE")[1], StringComparison.Ordinal);
        }

        /* #4012's review, finding 4: by the chain, where the section is attached, not by the root alone. */
        Assert.Contains($"string_to_array(f.story_path, ' → ') && ARRAY['{PgTargetFactKeys.DeadlockRate}', '{PgTargetFactKeys.AnomalyDeadlockRate}']", PgDeadlockRemask.FindingPageSql, StringComparison.Ordinal);
        Assert.DoesNotContain("root_fact_key", PgDeadlockRemask.FindingPageSql, StringComparison.Ordinal);
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

        var summary = PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(legacy), null, Resolve, s_key);
        Assert.NotNull(summary);
        Assert.Equal(AlertContextSerializer.Serialize(live), summary.Value.ContextJson);
        Assert.Null(summary.Value.DetailText);

        var legacyPerEvent = Assert.Single(PerEventNotification.Split(legacy, 10)).Context;
        var livePerEvent = Assert.Single(PerEventNotification.Split(live, 10)).Context;
        var perEvent = PgDeadlockRemask.RemaskAlert(
            AlertContextSerializer.Serialize(legacyPerEvent), AlertContextBuilders.ContextToDetailText(legacyPerEvent), Resolve, s_key);
        Assert.NotNull(perEvent);
        Assert.Equal(AlertContextSerializer.Serialize(livePerEvent), perEvent.Value.ContextJson);
        Assert.Equal(AlertContextBuilders.ContextToDetailText(livePerEvent), perEvent.Value.DetailText);

        foreach (var text in new[] { summary.Value.ContextJson, perEvent.Value.ContextJson, perEvent.Value.DetailText })
        {
            AssertNoSecret(text);
            Assert.DoesNotContain(rawHash, text, StringComparison.Ordinal);
        }

        Assert.Null(PgDeadlockRemask.RemaskAlert(summary.Value.ContextJson, null, Resolve, s_key));
        Assert.Null(PgDeadlockRemask.RemaskAlert(perEvent.Value.ContextJson, perEvent.Value.DetailText, Resolve, s_key));
    }

    /// <summary>
    /// An alert whose report cannot be found (retention dropped it, or the report stage already rewrote it) has its
    /// statement normalized as it stands, and its key, which may be a raw graph's hash, keyed under the store's secret
    /// (#4012's review): the same key in, the same key out, so two alerts that shared one still do, and not a report
    /// hash's shape, so a later walk leaves it alone. Its no-statement text carries no SQL and is kept. An alert built
    /// from normalized text (its report current, or named by a report identity), a SQL Server alert and a context that
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

        var gone = PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(raw), null, Gone, s_key);
        Assert.NotNull(gone);
        Assert.True(AlertContextSerializer.TryDeserialize(gone.Value.ContextJson, out var context));
        Assert.Equal(s_key.DeadlockAlertKey(goneHash), context.Incidents![0].DedupKey);
        Assert.Equal(s_key.DeadlockAlertKey(goneHash[..31] + "0"), context.Incidents[1].DedupKey);
        Assert.Equal("UPDATE creds SET pw = '?' WHERE id = ?", Assert.Single(context.Incidents[0].InvolvedObjects));
        Assert.Equal("victim pid 3101, 2 participant(s)", Assert.Single(context.Incidents[1].InvolvedObjects));
        AssertNoSecret(gone.Value.ContextJson);
        Assert.DoesNotContain(goneHash, gone.Value.ContextJson, StringComparison.Ordinal);
        Assert.DoesNotContain(goneHash[..31] + "0", gone.Value.ContextJson, StringComparison.Ordinal);
        Assert.Null(PgDeadlockRemask.RemaskAlert(gone.Value.ContextJson, null, Gone, s_key));

        /* Before every report is rewritten (no key passed), an unresolvable key is left as it is: it may be a raw
           report's new identity, which names nothing until that report is rewritten. Its statement is normalized. */
        var early = PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(raw), null, Gone, null);
        Assert.NotNull(early);
        Assert.True(AlertContextSerializer.TryDeserialize(early.Value.ContextJson, out var earlyContext));
        Assert.Equal(goneHash, earlyContext.Incidents![0].DedupKey);
        AssertNoSecret(early.Value.ContextJson);
        Assert.Null(PgDeadlockRemask.RemaskAlert(early.Value.ContextJson, null, Gone, null));

        /* A per-event row renders the key in its detail item and its detail_text: both take the keyed one. */
        var perEvent = PerEventNotification.Split(raw, 10)[0].Context;
        var perEventGone = PgDeadlockRemask.RemaskAlert(
            AlertContextSerializer.Serialize(perEvent), AlertContextBuilders.ContextToDetailText(perEvent), Gone, s_key);
        Assert.NotNull(perEventGone);
        Assert.DoesNotContain(goneHash, perEventGone.Value.ContextJson, StringComparison.Ordinal);
        Assert.DoesNotContain(goneHash, perEventGone.Value.DetailText!, StringComparison.Ordinal);
        Assert.Contains(s_key.DeadlockAlertKey(goneHash), perEventGone.Value.DetailText!, StringComparison.Ordinal);

        /* The keyed value is the store's own: another key gives another value, and it is not the raw-line subkey's. */
        var otherKey = new PgLogHashKey(Enumerable.Repeat((byte)7, PgLogHashKey.KeyLength).ToArray());
        Assert.NotEqual(s_key.DeadlockAlertKey(goneHash), otherKey.DeadlockAlertKey(goneHash));
        Assert.NotEqual(PgLogHashKey.DeadlockAlertKeyTag + s_key.RawLineHash(goneHash), s_key.DeadlockAlertKey(goneHash));
        Assert.False(PgDeadlockRemask.IsReportHash(s_key.DeadlockAlertKey(goneHash)));

        static PgDeadlockRemask.ResolvedReport Current(string key) => new(PgDeadlockRemask.ReportState.Current, null);
        Assert.Null(PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(raw), null, Current, s_key));

        var normalizedSince = new AlertContext
        {
            Incidents = [new AlertIncident(PgDeadlockLogParser.ReportIdentity(DateTime.UtcNow, 3101), ["UPDATE creds SET pw = 'Leak4012'"])],
        };
        Assert.Null(PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(normalizedSince), null, Gone, s_key));

        var sqlServer = new AlertContext
        {
            Incidents = [new AlertIncident(new string('a', 64), ["UPDATE creds SET pw = 'Leak4012'"])],
        };
        Assert.Null(PgDeadlockRemask.RemaskAlert(AlertContextSerializer.Serialize(sqlServer), null, Gone, s_key));

        Assert.Null(PgDeadlockRemask.RemaskAlert("{ not json", null, Gone, s_key));
    }

    /// <summary>
    /// A finding written before #4005 is rewritten to what every read of it already showed
    /// (<see cref="PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars"/>) and stamped, so the read leaves the
    /// rewritten row as it is and a second pass finds nothing. A finding with no deadlock section is not touched.
    /// </summary>
    [Fact]
    public void ALegacyFinding_BecomesWhatEveryReadShows_AndIsStampedSo()
    {
        var (drillDown, story) = LegacyFinding();
        var read = new AnalysisFinding { DrillDown = DrillDownSerializer.Deserialize(drillDown), StoryText = story };
        Assert.True(PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(read));

        var rewritten = PgDeadlockRemask.RemaskFinding(drillDown, story);
        Assert.NotNull(rewritten);
        foreach (var text in new[] { rewritten.Value.DrillDownJson, rewritten.Value.StoryText })
        {
            AssertNoSecret(text);
            Assert.DoesNotContain(PgDeadlockLogParser.HashOf(LegacyGraph), text, StringComparison.Ordinal);
        }

        var persisted = new AnalysisFinding { DrillDown = DrillDownSerializer.Deserialize(rewritten.Value.DrillDownJson), StoryText = rewritten.Value.StoryText };
        Assert.False(PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(persisted));
        Assert.Equal(read.StoryText, persisted.StoryText);
        Assert.Equal(ExemplarsOf(read), ExemplarsOf(persisted));

        Assert.Null(PgDeadlockRemask.RemaskFinding(rewritten.Value.DrillDownJson, rewritten.Value.StoryText));
        Assert.Null(PgDeadlockRemask.RemaskFinding(
            DrillDownSerializer.Serialize(new Dictionary<string, object> { ["top_waits"] = JsonSerializer.SerializeToElement(new { wait = "LCK_M_X" }) })!,
            story));
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

        /* A finding the analysis stored before #4005, its exemplar raw, and one it stored with no deadlock section. */
        var (legacyDrillDown, legacyStory) = LegacyFinding();
        await PlantFindingAsync(connection, 1, now.AddDays(-2), legacyDrillDown, legacyStory, ct);
        var otherDrillDown = DrillDownSerializer.Serialize(new Dictionary<string, object> { ["top_waits"] = JsonSerializer.SerializeToElement(new { wait = "LCK_M_X" }) })!;
        var otherStory = FactAdvice.SerializeForStoryText(new AdviceBlock("Waits", "Investigate the lock waits.", "Fix the order."));
        await PlantFindingAsync(connection, 2, now.AddDays(-2), otherDrillDown, otherStory, ct);
        var legacyRead = new AnalysisFinding { DrillDown = DrillDownSerializer.Deserialize(legacyDrillDown), StoryText = legacyStory };
        PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(legacyRead);

        if (timescale)
        {
            await ExecuteAsync(connection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('pg_deadlocks') AS c", ct);
        }

        var readsBefore = await ReadEverythingAsync(postgres, connection, now, ct);
        var newFormatBefore = await DumpAsync(connection, $"SELECT victim_statement, graph_text, deadlock_hash FROM pg_deadlocks WHERE deadlock_hash = '{capturedC.DeadlockHash}'", ct);
        Assert.Equal(5, readsBefore.Summary.Count);

        /* The pass, as the worker runs it: the alerts to their end, then the reports, then the findings. */
        var (alertsRewritten, findingsRewritten, reportsRewritten, failed) = await RunPassAsync(connection, ct);
        Assert.Equal(4, alertsRewritten);
        Assert.Equal(1, findingsRewritten);
        Assert.Equal(4, reportsRewritten);
        Assert.Equal(0, failed);

        /* No literal and no raw hash left in either table, whatever reads them. */
        var deadlockDump = await DumpAsync(connection, "SELECT concat_ws('|', deadlock_hash, victim_statement, graph_text) FROM pg_deadlocks", ct);
        var alertDump = await DumpAsync(connection, $"SELECT concat_ws('|', detail_text, context_json) FROM config_alert_log WHERE context_json <> '{sqlServerJson.Replace("'", "''", StringComparison.Ordinal)}'", ct);
        var findingDump = await DumpAsync(connection, "SELECT concat_ws('|', drill_down_json, story_text) FROM analysis_findings", ct);
        foreach (var text in new[] { deadlockDump, alertDump, findingDump })
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
        Assert.Contains(alertsAfter, a => a.Context.Contains(s_key.DeadlockAlertKey(goneHash), StringComparison.Ordinal) && a.Context.Contains("WHERE id = ?", StringComparison.Ordinal));
        Assert.DoesNotContain(goneHash, alertDump, StringComparison.Ordinal);

        /* The finding reads as every read showed it before; the one with no deadlock section is as planted. */
        var findingsAfter = await ReadFindingsAsync(connection, ct);
        var rewrittenFinding = new AnalysisFinding { DrillDown = DrillDownSerializer.Deserialize(findingsAfter[1].DrillDown), StoryText = findingsAfter[1].Story };
        Assert.False(PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(rewrittenFinding));
        Assert.Equal(legacyRead.StoryText, rewrittenFinding.StoryText);
        Assert.Equal(ExemplarsOf(legacyRead), ExemplarsOf(rewrittenFinding));
        Assert.Equal((otherDrillDown, otherStory), findingsAfter[2]);

        /* A second pass finds nothing to do and changes nothing. */
        var tablesBefore = await DumpTablesAsync(connection, ct);
        var second = await RunPassAsync(connection, ct);
        Assert.Equal((0, 0, 0, 0), second);
        Assert.Equal(tablesBefore, await DumpTablesAsync(connection, ct));
    }

    /// <summary>
    /// #4012's review, finding 5: what the stored text cannot say, the rewrite cannot match. The collector before
    /// #4005 took every tab out of the DETAIL, so a query's own tab is gone (`LIMIT&lt;tab&gt;50` stored as `LIMIT50`,
    /// one identifier that keeps its digits, as every read already shows it); and it kept no proof a report was whole,
    /// so after a middle query that does not read to its end the rewrite withholds the rest (fail closed) where the
    /// live read, which saw the HINT, keeps them. Both keep the old and new sightings apart rather than merge them;
    /// neither leaks a literal the reads do not already show. A report with neither reads exactly as the live one.
    /// </summary>
    [Fact]
    public void WhatTheStoredTextCannotSay_TheRewriteCannotMatch_AndStaysFailClosed()
    {
        var at = new DateTime(2026, 8, 26, 22, 25, 24, 100);

        /* A tab inside the victim's query: the live collector keeps it, the old one stored the query without it. */
        var tabbed = Report(at, 3101, 3102).Replace("WHERE card = 4111111111111111", "LIMIT\t50", StringComparison.Ordinal);
        var live = Assert.Single(PgDeadlockLogParser.Extract(tabbed));
        var storedTabless = LegacyGraph.Replace("WHERE card = 4111111111111111", "LIMIT50", StringComparison.Ordinal);
        var (_, graph, hash) = PgDeadlockRemask.RemaskReport(at, null, storedTabless);
        Assert.Contains("LIMIT50", graph, StringComparison.Ordinal);
        Assert.Contains("LIMIT ?", live.GraphText, StringComparison.Ordinal);
        Assert.NotEqual(live.DeadlockHash, hash);
        Assert.Equal(PgDeadlockLogParser.NormalizeGraph(storedTabless), graph);

        /* A middle query cut inside a literal (track_activity_query_size): the live read saw the HINT and keeps the
           query after it; the stored graph carries no such proof, so the rewrite withholds it. */
        var cut = "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
            + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n"
            + "Process 3101: UPDATE accounts SET pin = '47\n"
            + "Process 3102: UPDATE accounts SET pin = '9034' WHERE card = 5500005555555559";
        var (_, cutGraph, cutHash) = PgDeadlockRemask.RemaskReport(at, null, cut);
        AssertNoSecret(cutGraph);
        Assert.Equal(PgDeadlockLogParser.NormalizeGraph(cut, complete: false), cutGraph);
        Assert.NotEqual(PgDeadlockLogParser.NormalizeGraph(cut, complete: true)!.TrimEnd('\n'), cutGraph);
        Assert.NotEqual(PgDeadlockLogParser.IdentityOf(at, PgDeadlockLogParser.NormalizeGraph(cut, complete: true)!.TrimEnd('\n')), cutHash);

        /* Neither: the rewrite is the live collector's report, identity included. */
        var whole = Assert.Single(PgDeadlockLogParser.Extract(Report(at, 3101, 3102)));
        Assert.Equal(whole.DeadlockHash, PgDeadlockRemask.RemaskReport(at, LegacyVictimStatement, LegacyGraph).DeadlockHash);
    }

    /// <summary>
    /// #4012's review, the High: the stages run alerts, then reports, then findings, page after page, and each is
    /// done only after a whole walk that rewrote nothing. An alert is rewritten while its report is still raw, so it
    /// takes the report's new identity; an alert whose report is gone, on more than one page of alerts, takes a keyed
    /// key; more than one page of reports is walked in one tick; a raw alert sitting behind the cursor of a walk that
    /// rewrote rows (where a dismissal moves one) is caught by the next walk; and a restart, fresh state, walks every
    /// table once more and changes nothing.
    /// </summary>
    [Fact]
    public async Task RunAsync_WalksEveryPage_InOrder_AndIsDoneOnlyAfterACleanWalk()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock re-mask test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenMigratedAsync(scratch.ConnectionString, ct);
        var now = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified).AddHours(12);

        /* More than a page of raw reports, each its own report, with a literal in each query. */
        var reportRows = PgDeadlockRemask.MaxReportRowsPerPass + 150;
        await PlantRawReportsAsync(connection, reportRows, now.AddDays(-3), ct);

        /* An alert naming the first raw report, and more than a page of alerts whose report is gone. */
        var first = await ReadFirstRawReportAsync(connection, ct);
        var named = new AlertContext { Incidents = [new AlertIncident(first.Hash, [first.Victim!])] };
        await PlantAlertAsync(connection, now.AddDays(-3), null, AlertContextSerializer.Serialize(named), ct);
        var goneHash = PgDeadlockLogParser.HashOf("a report retention dropped");
        var goneJson = AlertContextSerializer.Serialize(new AlertContext
        {
            Incidents = [new AlertIncident(goneHash, ["UPDATE creds SET pw = 'Leak4012' WHERE id = 7"])],
        });
        await PlantAlertsAsync(connection, PgDeadlockRemask.MaxAlertRowsPerPass + 20, goneJson, now.AddDays(-2), ct);

        var progress = new PgDeadlockRemask.RemaskProgress();
        await PgDeadlockRemask.RunAsync(connection, progress, s_key, NullLoggerFor(), ct);

        Assert.True(progress.Done);
        Assert.False(progress.Alerts.GaveUp || progress.Reports.GaveUp || progress.Findings.GaveUp);
        Assert.Equal(2, progress.Alerts.Walks);
        Assert.Equal(2, progress.Reports.Walks);
        Assert.Equal(2, progress.AlertKeys.Walks);
        Assert.Equal(1, progress.Findings.Walks);
        Assert.Equal(1, progress.FindingAlerts.Walks);
        Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM pg_deadlocks WHERE {PgDeadlockLogParser.RawGraphHashSql("deadlock_hash", "graph_text")}", ct));
        Assert.Equal((long)reportRows, await ScalarAsync(connection, "SELECT count(*) FROM pg_deadlocks", ct));

        /* Alerts first: the named alert took the report's new identity, not a keyed key. */
        var expected = PgDeadlockRemask.RemaskedIncident(first.OccurredAt, first.VictimPid, 2, first.Victim, first.Graph);
        var alertDump = await DumpAsync(connection, "SELECT concat_ws('|', detail_text, context_json) FROM config_alert_log", ct);
        var namedAfter = await ReadAlertsAsync(connection, ct);
        var namedIncident = Assert.Single(
            namedAfter.Select(a => AlertContextSerializer.TryDeserialize(a.Context, out var c) ? c.Incidents!.Single() : null),
            i => i is not null && !i.DedupKey.StartsWith(PgLogHashKey.DeadlockAlertKeyTag, StringComparison.Ordinal));
        Assert.Equal(expected.DedupKey, namedIncident!.DedupKey);
        Assert.Equal(expected.InvolvedObjects, namedIncident.InvolvedObjects);
        Assert.Equal((long)(PgDeadlockRemask.MaxAlertRowsPerPass + 20), await ScalarAsync(connection,
            $"SELECT count(*) FROM config_alert_log WHERE strpos(context_json, '{s_key.DeadlockAlertKey(goneHash)}') > 0", ct));
        AssertNoSecret(alertDump);
        Assert.DoesNotContain(goneHash, alertDump, StringComparison.Ordinal);
        Assert.DoesNotContain(first.Hash, alertDump, StringComparison.Ordinal);
        AssertNoSecret(await DumpAsync(connection, "SELECT concat_ws('|', victim_statement, graph_text) FROM pg_deadlocks", ct));

        /* A raw alert behind the cursor of a walk that has rewritten rows: planted, then the walk's state set to just
           past it, as a dismissal that moved it there would leave it. The walk ends, and the next one catches it. */
        await PlantAlertAsync(connection, now.AddDays(-1), null, goneJson, ct);
        var behind = new PgDeadlockRemask.RemaskProgress();
        behind.Alerts.WalkRewritten = 1;
        behind.AlertCursor = await ScalarTextAsync(connection, "SELECT max(ctid)::text FROM config_alert_log", ct);
        await PgDeadlockRemask.RunAsync(connection, behind, s_key, NullLoggerFor(), ct);
        Assert.True(behind.Alerts.Done);
        Assert.Equal(3, behind.Alerts.Walks);
        Assert.DoesNotContain(goneHash, await DumpAsync(connection, "SELECT context_json FROM config_alert_log", ct), StringComparison.Ordinal);

        /* A restart: fresh state walks every table once, rewrites nothing, and changes nothing. */
        var tablesBefore = await DumpTablesAsync(connection, ct);
        var restarted = new PgDeadlockRemask.RemaskProgress();
        await PgDeadlockRemask.RunAsync(connection, restarted, s_key, NullLoggerFor(), ct);
        Assert.True(restarted.Done);
        Assert.Equal(1, restarted.Alerts.Walks);
        Assert.Equal(1, restarted.Reports.Walks);
        Assert.Equal(1, restarted.AlertKeys.Walks);
        Assert.Equal(1, restarted.Findings.Walks);
        Assert.Equal(1, restarted.FindingAlerts.Walks);
        Assert.Equal(tablesBefore, await DumpTablesAsync(connection, ct));
    }

    /// <summary>
    /// #4012's review: a stage that fails is logged, backs off (none, 1, then 3 ticks), and after
    /// <see cref="PgDeadlockRemask.MaxConsecutiveStageFailures"/> failures in a row stops for the process; meanwhile
    /// the stages after it run, so the reports are rewritten on the first tick although the alert stage fails on
    /// every one.
    /// </summary>
    [Fact]
    public async Task AFailingStage_BacksOff_IsCapped_AndDoesNotStarveTheOthers()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock re-mask test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenMigratedAsync(scratch.ConnectionString, ct);
        await PlantRawReportsAsync(connection, 30, DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified).AddDays(-2), ct);

        /* The alert stage's table is gone, so its page fails with 42P01 on every tick. */
        await ExecuteAsync(connection, "ALTER TABLE config_alert_log RENAME TO config_alert_log_away", ct);

        var logger = new CountingLogger();
        var progress = new PgDeadlockRemask.RemaskProgress();
        await PgDeadlockRemask.RunAsync(connection, progress, s_key, logger, ct);
        Assert.Equal(1, progress.Alerts.ConsecutiveFailures);
        Assert.False(progress.Alerts.Done);
        Assert.True(progress.Reports.Done);
        Assert.True(progress.Findings.Done);
        Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM pg_deadlocks WHERE {PgDeadlockLogParser.RawGraphHashSql("deadlock_hash", "graph_text")}", ct));

        /* Failures 2, 3 and 4 come after 0, 1 and 3 skipped ticks; the fourth stops the stage. */
        var failuresByTick = new List<int>();
        for (var tick = 0; tick < 8; tick++)
        {
            await PgDeadlockRemask.RunAsync(connection, progress, s_key, logger, ct);
            failuresByTick.Add(progress.Alerts.ConsecutiveFailures);
        }

        Assert.Equal([2, 2, 3, 3, 3, 3, 4, 4], failuresByTick);
        Assert.True(progress.Alerts.Done);
        Assert.True(progress.Alerts.GaveUp);
        Assert.True(progress.AlertKeys.GaveUp);
        Assert.True(progress.FindingAlerts.GaveUp);
        Assert.Equal(3 * PgDeadlockRemask.MaxConsecutiveStageFailures, logger.Warnings);

        /* #4012's review, finding 5: a stage that gave up is not given up for the life of the process. Within the day
           it stays given up; a day after, it is tried again from its start, and with its table back it finishes. */
        await ExecuteAsync(connection, "ALTER TABLE config_alert_log_away RENAME TO config_alert_log", ct);
        await PgDeadlockRemask.RunAsync(connection, progress, s_key, logger, ct);
        Assert.True(progress.Alerts.GaveUp);
        foreach (var stage in progress.Stages.Where(s => s.GaveUp))
        {
            stage.GaveUpUtc = DateTime.UtcNow - PgDeadlockRemask.RetryGivenUpAfter - TimeSpan.FromMinutes(1);
        }

        await PgDeadlockRemask.RunAsync(connection, progress, s_key, logger, ct);
        Assert.True(progress.Done);
        Assert.All(progress.Stages, stage => Assert.False(stage.GaveUp, stage.Name));
    }

    /// <summary>
    /// #4012's review: a cancel mid-page (the tick's budget) keeps the cursor at the last row the page finished, so
    /// the next tick resumes after it rather than reading the page again; and the driver ends quietly on its cancel,
    /// leaving each cursor where it stood.
    /// </summary>
    [Fact]
    public async Task ABudgetCancel_KeepsTheCursorAtTheLastFinishedRow()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock re-mask test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenMigratedAsync(scratch.ConnectionString, ct);
        await PlantRawReportsAsync(connection, 40, DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified).AddDays(-2), ct);

        using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var finished = new List<PgDeadlockRemask.ReportCursor>();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => PgDeadlockRemask.RemaskStoredReportsAsync(connection, null, (row, _) =>
        {
            /* Each planted report is its own batch, so every row closes one. */
            finished.Add(Assert.NotNull(row));
            if (finished.Count == 10)
            {
                budget.Cancel();
            }
        }, budget.Token));

        Assert.Equal(10, finished.Count);
        Assert.Equal(30L, await ScalarAsync(connection, $"SELECT count(*) FROM pg_deadlocks WHERE {PgDeadlockLogParser.RawGraphHashSql("deadlock_hash", "graph_text")}", ct));

        /* The driver, handed that cursor, resumes after the tenth row: the ten are not read again. */
        var progress = new PgDeadlockRemask.RemaskProgress { ReportCursor = finished[^1] };
        progress.Alerts.Done = true;
        var (_, examined, rewritten, _) = await PgDeadlockRemask.RemaskStoredReportsAsync(connection, progress.ReportCursor, null, ct);
        Assert.Equal(30, rewritten);

        /* The cursor is the last finished batch, with no hash in it (#4035), so not one of the ten is read again. */
        Assert.Equal(30, examined);

        /* Canceled before it starts, the driver returns quietly and moves no cursor. */
        using var spent = new CancellationTokenSource();
        await spent.CancelAsync();
        await PgDeadlockRemask.RunAsync(connection, progress, s_key, NullLoggerFor(), spent.Token);
        Assert.Equal(finished[^1], progress.ReportCursor);
        Assert.False(progress.Reports.Done);
    }

    /// <summary>
    /// #4012's review, finding 6 (#4035): no statement sends a raw report's hash back to the store as a parameter,
    /// where <c>log_min_duration_statement</c> would write it into the store's own log. The report walk's cursor is
    /// its batch, the report write takes the hash from the row the page read, and the alert page resolves its
    /// incidents' reports in SQL from its own <c>context_json</c>.
    /// </summary>
    [Fact]
    public void NoStatementBindsARawReportHash()
    {
        Assert.DoesNotContain("deadlock_hash) >", PgDeadlockRemask.ReportPageSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", PgDeadlockRemask.ReportPageSql, StringComparison.Ordinal);
        foreach (var sql in new[] { PgDeadlockRemask.ReportUpdateSql, PgDeadlockRemask.ReportUpdateOwnBatchSql })
        {
            Assert.DoesNotMatch(@"deadlock_hash\s*=\s*\$", sql[sql.IndexOf("WHERE", StringComparison.Ordinal)..]);
            Assert.Contains("d.deadlock_hash = (", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("$8", sql, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("unnest(", PgDeadlockRemask.AlertPageSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$3", PgDeadlockRemask.AlertPageSql, StringComparison.Ordinal);
        Assert.Contains("regexp_matches(p.context_json", PgDeadlockRemask.AlertPageSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4012's review, finding 1: the worker runs the re-mask whether or not it has a log-hash key, and hands the key
    /// over as it stands; <see cref="PgDeadlockRemask.RunAsync"/> decides what waits for one.
    /// </summary>
    [Fact]
    public void TheWorkerRunsTheRemaskWithOrWithoutAKey()
    {
        var worker = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");
        Assert.Contains("await PgDeadlockRemask.RunAsync(connection, _pgDeadlockRemask, _pgDeadlockRemaskKey, _logger, remaskBudget.Token);", worker, StringComparison.Ordinal);
        Assert.DoesNotContain("_pgDeadlockRemaskKey is not", worker, StringComparison.Ordinal);
        Assert.DoesNotContain("_pgDeadlockRemaskKey is null", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4012's review, finding 1: without a log-hash key (an untrusted key directory or ACL, an unreadable key file,
    /// DPAPI after the service moved machines) the reports, alerts, findings and finding alerts are re-masked all the
    /// same; only the alert-key stage waits, says so once, and is never counted done. Given a key later, it runs.
    /// </summary>
    [Fact]
    public async Task WithoutAKey_EveryStageButTheAlertKeysRuns()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock re-mask test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenMigratedAsync(scratch.ConnectionString, ct);
        var now = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified).AddHours(12);

        await PlantRawReportsAsync(connection, 20, now.AddDays(-3), ct);
        var first = await ReadFirstRawReportAsync(connection, ct);
        await PlantAlertAsync(connection, now.AddDays(-3), null, AlertContextSerializer.Serialize(
            new AlertContext { Incidents = [new AlertIncident(first.Hash, [first.Victim!])] }), ct);
        var goneHash = PgDeadlockLogParser.HashOf("a report retention dropped");
        await PlantAlertAsync(connection, now.AddDays(-2), null, AlertContextSerializer.Serialize(new AlertContext
        {
            Incidents = [new AlertIncident(goneHash, ["UPDATE creds SET pw = 'Leak4012' WHERE id = 7"])],
        }), ct);
        var (legacyDrillDown, legacyStory) = LegacyFinding();
        var analysedAt = now.AddDays(-1);
        await PlantFindingAsync(connection, 1, analysedAt, legacyDrillDown, legacyStory, ct, storyPathHash: "4012abcd00000000");
        var (findingMetric, findingContext) = LegacyFindingAlert(legacyDrillDown, legacyStory, "4012abcd00000000");
        await PlantAlertAsync(connection, analysedAt.AddMinutes(2), null, findingContext, ct, findingMetric);

        var logger = new CountingLogger();
        var progress = new PgDeadlockRemask.RemaskProgress();
        await PgDeadlockRemask.RunAsync(connection, progress, null, logger, ct);
        await PgDeadlockRemask.RunAsync(connection, progress, null, logger, ct);

        Assert.True(progress.Alerts.Done && progress.Reports.Done && progress.Findings.Done && progress.FindingAlerts.Done);
        Assert.False(progress.Alerts.GaveUp || progress.Reports.GaveUp || progress.Findings.GaveUp || progress.FindingAlerts.GaveUp);
        Assert.False(progress.AlertKeys.Done);
        Assert.False(progress.AlertKeys.GaveUp);
        Assert.Equal(0, progress.AlertKeys.Walks);
        Assert.False(progress.Done);
        Assert.Equal(1, logger.Warnings);

        Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM pg_deadlocks WHERE {PgDeadlockLogParser.RawGraphHashSql("deadlock_hash", "graph_text")}", ct));
        var tables = await DumpTablesAsync(connection, ct);
        AssertNoSecret(tables);
        Assert.DoesNotContain(first.Hash, tables, StringComparison.Ordinal);
        Assert.DoesNotContain(PgDeadlockLogParser.HashOf(LegacyGraph), tables, StringComparison.Ordinal);

        /* The alert whose report is gone has its statement normalized, and keeps its key until there is one to key
           it with. */
        Assert.Contains(goneHash, tables, StringComparison.Ordinal);
        await PgDeadlockRemask.RunAsync(connection, progress, s_key, logger, ct);
        Assert.True(progress.Done);
        Assert.Equal(1, logger.Warnings);
        Assert.DoesNotContain(goneHash, await DumpTablesAsync(connection, ct), StringComparison.Ordinal);
    }

    /// <summary>
    /// #4012's review, finding 2: a page cut short loses no row's outcome. A tick canceled mid-page (as the row it was
    /// writing committed) after rewriting three reports and leaving two, then a tick that completes the walk over
    /// rows that are all current, must not take that walk for a clean one: the stage walks again, rewrites the report
    /// it left, and is done only then.
    /// </summary>
    [Fact]
    public async Task ACancelMidPage_ThenACompletedWalk_NeverLeavesTheStageDoneOverARawRow()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock re-mask test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenMigratedAsync(scratch.ConnectionString, ct);
        var from = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified).AddDays(-2);

        /* Five raw reports; the fourth's batch also holds a raw twin (the same time and victim, another graph), which
           the rewrite cannot tell from it and so leaves, both; then three reports already current. */
        await PlantRawReportsAsync(connection, 5, from, ct);
        await ExecuteAsync(connection, @"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
SELECT collection_id + 100, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count,
       upper(left(encode(sha256(convert_to(graph_text || ' twin', 'UTF8')), 'hex'), 32)), lock_modes, resources, victim_statement, graph_text || ' twin'
FROM pg_deadlocks
WHERE victim_pid = 10004", ct);
        for (var i = 0; i < 3; i++)
        {
            var at = from.AddMinutes(10 + i);
            var current = Assert.Single(PgDeadlockLogParser.Extract(Report(at, 7101 + i, 7201 + i)));
            await PlantAsync(connection, at.AddMinutes(1), at, 7101 + i, "C" + i, current.DeadlockHash, current.VictimStatement, current.GraphText, ct);
        }

        /* The fifth report as the row being written when the cancel lands, its commit landing with it. */
        var fifth = await DumpAsync(connection, "SELECT occurred_at FROM pg_deadlocks WHERE victim_pid = 10005", ct);
        Assert.NotEmpty(fifth);
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var progress = new PgDeadlockRemask.RemaskProgress();
        var counted = 0;
        progress.RowCounted = (stage, _) =>
        {
            if (!ReferenceEquals(stage, progress.Reports) || ++counted != 5)
            {
                return;
            }

            using var other = new NpgsqlConnection(scratch.ConnectionString);
            other.Open();
            using var read = new NpgsqlCommand("SELECT occurred_at, victim_statement, graph_text FROM pg_deadlocks WHERE victim_pid = 10005", other);
            DateTime occurredAt;
            string? victimStatement;
            string graphText;
            using (var reader = read.ExecuteReader())
            {
                Assert.True(reader.Read());
                occurredAt = reader.GetDateTime(0);
                victimStatement = reader.IsDBNull(1) ? null : reader.GetString(1);
                graphText = reader.GetString(2);
            }

            var (victim, graph, hash) = PgDeadlockRemask.RemaskReport(occurredAt, victimStatement, graphText);
            using var write = new NpgsqlCommand("UPDATE pg_deadlocks SET victim_statement = $1, graph_text = $2, deadlock_hash = $3 WHERE victim_pid = 10005", other);
            write.Parameters.Add(new NpgsqlParameter { Value = (object?)victim ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            write.Parameters.AddWithValue(graph);
            write.Parameters.AddWithValue(hash);
            Assert.Equal(1, write.ExecuteNonQuery());
            budget.Cancel();
        };

        await PgDeadlockRemask.RunAsync(connection, progress, s_key, NullLoggerFor(), budget.Token);
        Assert.False(progress.Reports.Done);
        Assert.Equal(5, counted);
        Assert.Equal(2L, await ScalarAsync(connection, $"SELECT count(*) FROM pg_deadlocks WHERE {PgDeadlockLogParser.RawGraphHashSql("deadlock_hash", "graph_text")}", ct));

        /* The twin goes, so the report left can be rewritten; the next tick completes the walk over current rows. */
        await ExecuteAsync(connection, "DELETE FROM pg_deadlocks WHERE graph_text LIKE '% twin'", ct);
        progress.RowCounted = null;
        await PgDeadlockRemask.RunAsync(connection, progress, s_key, NullLoggerFor(), ct);

        Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM pg_deadlocks WHERE {PgDeadlockLogParser.RawGraphHashSql("deadlock_hash", "graph_text")}", ct));
        Assert.True(progress.Reports.Done);
        Assert.False(progress.Reports.GaveUp);
        Assert.True(progress.Reports.Walks >= 2);
    }

    /// <summary>
    /// #4012's review, finding 3: a finding sent as an alert before #4005 flattened its deadlock exemplars into the
    /// alert's context, a 300-character JSON prefix with the raw <c>deadlock_hash</c> and a note naming the raw victim
    /// fingerprint, and the prose it was frozen with names it too. The finding-alert stage rewrites them from the
    /// finding's normalized section while the finding is stored, and withholds them once it is gone; either way no
    /// raw hash and no literal is left, and a second pass changes nothing.
    /// </summary>
    [Fact]
    public async Task AFindingAlert_IsRewrittenFromItsFinding_OrWithheld()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock re-mask test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenMigratedAsync(scratch.ConnectionString, ct);
        var now = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified).AddHours(12);

        var (legacyDrillDown, legacyStory) = LegacyFinding();
        await PlantFindingAsync(connection, 1, now.AddDays(-1), legacyDrillDown, legacyStory, ct, storyPathHash: "4012abcd00000000");
        var (kept, keptContext) = LegacyFindingAlert(legacyDrillDown, legacyStory, "4012abcd00000000");
        await PlantAlertAsync(connection, now.AddDays(-1).AddMinutes(3), null, keptContext, ct, kept);
        var (gone, goneContext) = LegacyFindingAlert(legacyDrillDown, legacyStory, "4012dead00000000");
        await PlantAlertAsync(connection, now.AddDays(-40), null, goneContext, ct, gone);
        Assert.Contains(PgDeadlockLogParser.HashOf(LegacyGraph), keptContext, StringComparison.Ordinal);
        Assert.Contains("4721", keptContext, StringComparison.Ordinal);

        var progress = new PgDeadlockRemask.RemaskProgress();
        await PgDeadlockRemask.RunAsync(connection, progress, s_key, NullLoggerFor(), ct);
        Assert.True(progress.Done);
        Assert.Equal(2, progress.FindingAlerts.Walks);

        var alerts = await ReadAlertsAsync(connection, ct);
        foreach (var (context, _) in alerts)
        {
            AssertNoSecret(context);
            Assert.DoesNotContain(PgDeadlockLogParser.HashOf(LegacyGraph), context, StringComparison.Ordinal);
        }

        /* The finding is stored: the section reads as the live alert flattens the finding's normalized one. */
        var fingerprint = PgTargetDrillDownCollector.Fingerprint(PgDeadlockLogParser.NormalizeStatement(LegacyVictimStatement))!;
        var rewritten = Assert.Single(alerts, a => a.Context.Contains("{\"Label\":\"Sql Normalized\",\"Value\":\"true\"}", StringComparison.Ordinal)).Context;
        Assert.Contains(JsonSerializer.Serialize("with the victim `" + fingerprint + "`")[1..^1], rewritten, StringComparison.Ordinal);
        Assert.DoesNotContain(PgDeadlockRemask.WithheldBefore4005, rewritten, StringComparison.Ordinal);

        /* The finding is gone: withheld. */
        var withheld = Assert.Single(alerts, a => a.Context.Contains(PgDeadlockRemask.WithheldBefore4005, StringComparison.Ordinal)).Context;
        Assert.DoesNotContain("Sql Normalized", withheld, StringComparison.Ordinal);
        Assert.DoesNotContain("with the victim `", withheld, StringComparison.Ordinal);

        var tablesBefore = await DumpTablesAsync(connection, ct);
        var again = new PgDeadlockRemask.RemaskProgress();
        await PgDeadlockRemask.RunAsync(connection, again, s_key, NullLoggerFor(), ct);
        Assert.Equal(1, again.FindingAlerts.Walks);
        Assert.Equal(tablesBefore, await DumpTablesAsync(connection, ct));
    }

    /// <summary>
    /// #4012's review, finding 4: the exemplar section is attached when a deadlock fact is anywhere on the chain, not
    /// only at its root, so a finding rooted elsewhere with the deadlock fact behind it is rewritten too.
    /// </summary>
    [Fact]
    public async Task AFindingWhoseDeadlockFactIsOffTheRoot_IsRewrittenToo()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock re-mask test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenMigratedAsync(scratch.ConnectionString, ct);
        var now = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified).AddHours(12);

        var (legacyDrillDown, legacyStory) = LegacyFinding();
        await PlantFindingAsync(connection, 1, now.AddDays(-1), legacyDrillDown, legacyStory, ct,
            storyPath: "PG_LOCK_WAIT_RATE → " + PgTargetFactKeys.AnomalyDeadlockRate);

        var progress = new PgDeadlockRemask.RemaskProgress();
        await PgDeadlockRemask.RunAsync(connection, progress, s_key, NullLoggerFor(), ct);

        var (drillDown, story) = (await ReadFindingsAsync(connection, ct))[1];
        AssertNoSecret(drillDown);
        AssertNoSecret(story);
        Assert.Contains("\"sql_normalized\":true", drillDown, StringComparison.Ordinal);
    }

    /* ───────────────────────── helpers ───────────────────────── */

    /* A finding alert as the analysis sent it before #4005 for the legacy finding: the live context builder over
       that finding, so the flattened section, its note and the frozen prose carry what they carried then. */
    private static (string Metric, string ContextJson) LegacyFindingAlert(string drillDown, string story, string storyPathHash)
    {
        var finding = new AnalysisFinding
        {
            ServerId = ServerId,
            ServerName = ServerName,
            Category = "deadlocks",
            StoryPath = PgTargetFactKeys.DeadlockRate,
            StoryPathHash = storyPathHash,
            RootFactKey = PgTargetFactKeys.DeadlockRate,
            Severity = 0.9,
            Confidence = 1,
            FactCount = 1,
            StoryText = story,
            DrillDown = DrillDownSerializer.Deserialize(drillDown),
        };
        return (FindingMessageFormatter.MetricName(finding), AlertContextSerializer.Serialize(FindingMessageFormatter.BuildContext(finding, 0.5)));
    }

    private static string ReadSource(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relativePath)))
        {
            dir = System.IO.Directory.GetParent(dir)?.FullName;
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relativePath));
    }

    private static Microsoft.Extensions.Logging.ILogger NullLoggerFor() => Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

    /* Counts the warnings the driver logs, one per failure. */
    private sealed class CountingLogger : Microsoft.Extensions.Logging.ILogger
    {
        public int Warnings { get; private set; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == Microsoft.Extensions.Logging.LogLevel.Warning)
            {
                Warnings++;
            }
        }
    }

    private static async Task<NpgsqlConnection> OpenMigratedAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(connectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
        }

        return connection;
    }

    /* n raw reports as the collector before #4005 stored them: each its own report (its own timestamp and pids), a
       PIN in each query, the hash over the raw graph, spread a minute apart. */
    private static async Task PlantRawReportsAsync(NpgsqlConnection connection, int n, DateTime from, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
SELECT $1 + g, $2 + g * interval '1 minute', $3, $4, $2 + g * interval '1 minute' - interval '5 seconds', 10000 + g, 2,
       upper(left(encode(sha256(convert_to(t.graph, 'UTF8')), 'hex'), 32)), 'ShareLock', 'transaction 5500, transaction 5501',
       'UPDATE accounts SET pin = ''4721'' WHERE id = ' || g, t.graph
FROM generate_series(1, $5) AS g
CROSS JOIN LATERAL (SELECT
    'Process ' || (10000 + g) || ' waits for ShareLock on transaction 5501; blocked by process ' || (20000 + g) || '.' || chr(10)
    || 'Process ' || (20000 + g) || ' waits for ShareLock on transaction 5500; blocked by process ' || (10000 + g) || '.' || chr(10)
    || 'Process ' || (10000 + g) || ': UPDATE accounts SET pin = ''4721'' WHERE id = ' || g || chr(10)
    || 'Process ' || (20000 + g) || ': UPDATE accounts SET pin = ''9034'' WHERE id = ' || g AS graph) AS t", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(n);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(string Hash, DateTime OccurredAt, int VictimPid, string? Victim, string Graph)> ReadFirstRawReportAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT deadlock_hash, occurred_at, victim_pid, victim_statement, graph_text FROM pg_deadlocks ORDER BY collection_time LIMIT 1", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetString(0), reader.GetDateTime(1), reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetString(3), reader.GetString(4));
    }

    private static async Task PlantAlertsAsync(NpgsqlConnection connection, int n, string contextJson, DateTime at, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
SELECT $1 + g * interval '1 second', $2, $3, $4, 1, 1, true, 'webhook', NULL, false, NULL, $5
FROM generate_series(1, $6) AS g", connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(AlertEngine.DeadlockWatermarkMetric);
        command.Parameters.AddWithValue(contextJson);
        command.Parameters.AddWithValue(n);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<string> ScalarTextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

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

    private static async Task<(int Alerts, int Findings, int Reports, int Failed)> RunPassAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        int alerts = 0, findings = 0, reports = 0, failed = 0;
        string? alertCursor = null;
        do
        {
            var (next, _, rewritten, raced) = await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, alertCursor, null, null, ct);
            Assert.Equal(0, raced);
            alerts += rewritten;
            alertCursor = next;
        }
        while (alertCursor is not null);

        PgDeadlockRemask.ReportCursor? reportCursor = null;
        do
        {
            var (next, _, rewritten, failures) = await PgDeadlockRemask.RemaskStoredReportsAsync(connection, reportCursor, null, ct);
            reports += rewritten;
            failed += failures;
            reportCursor = next;
        }
        while (reportCursor is not null);

        /* Every report rewritten: the alerts once more, keying the ones whose report cannot be found. */
        do
        {
            var (next, _, rewritten, raced) = await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, alertCursor, s_key, null, ct);
            Assert.Equal(0, raced);
            alerts += rewritten;
            alertCursor = next;
        }
        while (alertCursor is not null);

        string? findingCursor = null;
        do
        {
            var (next, _, rewritten, raced) = await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, findingCursor, null, ct);
            Assert.Equal(0, raced);
            findings += rewritten;
            findingCursor = next;
        }
        while (findingCursor is not null);

        return (alerts, findings, reports, failed);
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

    private static async Task<string> DumpTablesAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DumpAsync(connection, "SELECT concat_ws('|', deadlock_hash, victim_statement, graph_text) FROM pg_deadlocks", ct)
        + "\n" + await DumpAsync(connection, "SELECT concat_ws('|', detail_text, context_json) FROM config_alert_log", ct)
        + "\n" + await DumpAsync(connection, "SELECT concat_ws('|', finding_id, drill_down_json, story_text) FROM analysis_findings", ct);

    private static async Task<Dictionary<long, (string DrillDown, string Story)>> ReadFindingsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var rows = new Dictionary<long, (string, string)>();
        await using var command = new NpgsqlCommand("SELECT finding_id, drill_down_json, story_text FROM analysis_findings", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows[reader.GetInt64(0)] = (reader.GetString(1), reader.GetString(2));
        }

        return rows;
    }

    /* A finding as the analysis stored it before #4005: the exemplar's statement, fingerprint and graph raw, the
       fingerprint in its prose, and a hash over the raw graph (PgTargetDeadlockDrillDownTests' shape). */
    private static (string DrillDown, string Story) LegacyFinding()
    {
        var lastSeen = new DateTime(2026, 9, 1, 12, 30, 15, 250);
        var sentence = $"Exemplars: 1 report captured. The most frequent 2-participant shape involves ShareLock on transaction with the victim `{LegacyVictimStatement}`, seen 1 time.";
        var section = JsonSerializer.SerializeToElement(new
        {
            engine_counted = 1,
            exemplars = new[]
            {
                new
                {
                    rank = 1,
                    last_seen = lastSeen.ToString("o", CultureInfo.InvariantCulture),
                    deadlock_hash = PgDeadlockLogParser.HashOf(LegacyGraph),
                    victim_pid = 3101,
                    victim_statement_fingerprint = LegacyVictimStatement,
                    victim_statement = LegacyVictimStatement,
                    graph_text = LegacyGraph,
                },
            },
            note = sentence,
        });
        return (
            DrillDownSerializer.Serialize(new Dictionary<string, object> { [PgTargetDrillDownCollector.DeadlockExemplarsSection] = section })!,
            FactAdvice.SerializeForStoryText(new AdviceBlock("Deadlocks", "Investigate. " + sentence, "Fix the order.")));
    }

    private static string ExemplarsOf(AnalysisFinding finding)
    {
        var section = (JsonElement)finding.DrillDown![PgTargetDrillDownCollector.DeadlockExemplarsSection];
        return section.GetProperty("exemplars").GetRawText() + "\n" + section.GetProperty("note").GetString();
    }

    private static async Task PlantFindingAsync(
        NpgsqlConnection connection, long findingId, DateTime analysisTime, string drillDown, string story, CancellationToken ct,
        string storyPath = PgTargetFactKeys.DeadlockRate, string storyPathHash = "h")
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, severity, confidence, category, story_path, story_path_hash, story_text, root_fact_key, fact_count, drill_down_json)
VALUES ($1, $2, $3, $4, 0.6, 1, 'deadlocks', $7, $8, $5, split_part($7, ' → ', 1), 1, $6)", connection);
        command.Parameters.AddWithValue(findingId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(analysisTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(story);
        command.Parameters.AddWithValue(drillDown);
        command.Parameters.AddWithValue(storyPath);
        command.Parameters.AddWithValue(storyPathHash);
        await command.ExecuteNonQueryAsync(ct);
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
        NpgsqlConnection connection, DateTime alertTime, string? detailText, string contextJson, CancellationToken ct, string? metricName = null)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1, $2, $3, $4, 1, 1, true, 'webhook', NULL, false, $5, $6)", connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(alertTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(metricName ?? AlertEngine.DeadlockWatermarkMetric);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)detailText ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.AddWithValue(contextJson);
        await command.ExecuteNonQueryAsync(ct);
    }
}
