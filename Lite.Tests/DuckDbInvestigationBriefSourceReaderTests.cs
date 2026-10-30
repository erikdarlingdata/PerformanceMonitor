/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Investigations;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <see cref="DuckDbInvestigationBriefSourceReader"/> on a throwaway DuckDB (#5820), the Lite twin of Darling's
/// <c>PgInvestigationBriefSourceReaderLiveTests</c>: one server with an alert history row, a finding, a mute rule, an
/// approved note, a server profile and an earlier report, and every source comes back. A request with nothing else on the
/// store reads as empty sections, never a throw, and an unknown request reads as null. The brief composed from the planted
/// store masks the earlier report's and the alert's literals under both masked levels. Also the family-call table (every
/// name is a tool Lite serves) and the composer's registration.
/// </summary>
public sealed class DuckDbInvestigationBriefSourceReaderTests : IDisposable
{
    private const int ServerId = 7_563_201;
    private const string ServerName = "LcBriefReaderSrv";
    private const string LiteralCanary = "zq-lit-5820";
    private const string NumericCanary = "86753091";
    private const string MaskStatement = "SELECT zq_census_col FROM dbo.t WHERE c = N'" + LiteralCanary + "' AND n = " + NumericCanary;

    private static readonly DateTime Now = Micros(DateTime.UtcNow);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;

    public DuckDbInvestigationBriefSourceReaderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DuckDbInvestigationBriefSourceReaderTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _initializer.InitializeAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static DateTime Micros(DateTime t) => new(t.Ticks - (t.Ticks % 10), DateTimeKind.Unspecified);

    private async Task ExecAsync(string sql, params object?[] parameters)
    {
        using var writeLock = _initializer.AcquireWriteLock();
        using var connection = _initializer.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var parameter in parameters)
            command.Parameters.Add(new DuckDBParameter { Value = parameter ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private Task InsertRequestAsync(Guid id, DateTime alertAt, string state = "open", string? storyPathHash = null) =>
        ExecAsync(@"
INSERT INTO investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash,
     first_alert_at, last_alert_at, window_until, state, opened_at, expires_at)
VALUES ($1, 1, $2, $3, 'blocking', 'Blocking', $4, $5, $5, $6, $7, $5, $8)",
            id, ServerId, ServerName, storyPathHash, Micros(alertAt), Micros(alertAt.AddMinutes(30)), state, Micros(alertAt.AddHours(4)));

    private Task InsertLinkAsync(long linkId, Guid requestId, DateTime alertAt, double? value = null, double? threshold = null,
        bool? muted = null, string? detail = null, string? contextJson = null) =>
        ExecAsync(@"
INSERT INTO investigation_request_alerts
    (request_alert_id, request_id, alert_time, server_id, metric_name, dedup_key,
     current_value, threshold_value, muted, detail_text, context_json)
VALUES ($1, $2, $3, $4, 'Blocking', $5, $6, $7, $8, $9, $10)",
            linkId, requestId, Micros(alertAt), ServerId, ServerId + ":Blocking", value, threshold, muted, detail, contextJson);

    private static string Context() => AlertContextSerializer.Serialize(new AlertContext
    {
        Details =
        {
            new AlertDetailItem { Heading = "Blocking", Fields = { ("Blocked Query", MaskStatement) } },
        },
    });

    /// <summary>
    /// #6565, the twin of Darling's: the brief of a request names the report of another request on the same server and family that was
    /// reported in the last 24 hours and whose story has the same root fact (key and database), under another story hash (the hash is a pure hash of the story's path), first and
    /// marked same_root_cause. Another root query, another database and a report 25 hours old are not marked.
    /// </summary>
    [Fact]
    public async Task SameRootFactUnderAnotherStoryHash_ReportedWithin24Hours_IsNamedFirstAndMarked_OthersAreNot()
    {
        var reportId = 100L;
        async Task<Guid> ReportedAsync(string hash, DateTime at, string rootKey, string database)
        {
            var id = Guid.NewGuid();
            await InsertRequestAsync(id, at, state: "reported", storyPathHash: hash);
            await ExecAsync(@"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name, severity, confidence, category, story_path, story_path_hash, story_text, root_fact_key, fact_count)
VALUES ($1, $2, $3, $4, $5, 0.8, 0.6, 'queries', $6, $7, 'story', $8, 2)",
                reportId, Micros(at), ServerId, ServerName, database, "path-" + hash, hash, rootKey);
            await ExecAsync(@"
INSERT INTO investigation_reports
    (report_id, request_id, report_format, agent_id, claim_id, submitted_at, verdict, root_cause, root_cause_found, summary,
     evidence, server_id, metric_name)
VALUES ($1, $2, 1, 'agent-a', $3, $4, 'real', 'a regressed plan', true, NULL, '[{""tool"":""get_query_store_top""}]', $5, 'Blocking')",
                reportId, id, Guid.NewGuid(), Micros(at), ServerId);
            reportId++;
            return id;
        }

        var first = await ReportedAsync("hashA0001", Now.AddMinutes(-45), "BAD_ACTOR_q1", "Sales");
        var otherQuery = await ReportedAsync("hashC0003", Now.AddMinutes(-50), "BAD_ACTOR_q2", "Sales");
        var otherDatabase = await ReportedAsync("hashD0004", Now.AddMinutes(-40), "BAD_ACTOR_q1", "Hr");
        var tooOld = await ReportedAsync("hashE0005", Now.AddHours(-25), "BAD_ACTOR_q1", "Sales");

        var second = Guid.NewGuid();
        await InsertRequestAsync(second, Now, storyPathHash: "hashB0002");
        await ExecAsync(@"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name, severity, confidence, category, story_path, story_path_hash, story_text, root_fact_key, fact_count)
VALUES (99, $1, $2, $3, 'Sales', 0.8, 0.6, 'queries', 'path-hashB0002', 'hashB0002', 'story', 'BAD_ACTOR_q1', 2)", Micros(Now), ServerId, ServerName);

        var sources = await new DuckDbInvestigationBriefSourceReader(_initializer, () => Now).ReadAsync(second, TestContext.Current.CancellationToken);

        var marked = Assert.Single(sources!.EarlierReports, r => r.SameRootCause);
        Assert.Equal(first, marked.RequestId);
        Assert.Equal(first, sources.EarlierReports[0].RequestId);
        Assert.Equal(new[] { otherQuery, otherDatabase, tooOld }.OrderBy(g => g), sources.EarlierReports.Where(r => !r.SameRootCause).Select(r => r.RequestId).OrderBy(g => g));

        var brief = InvestigationBriefComposer.Compose(sources, LiteInvestigationBriefTools.Tools, McpDataDetail.Full, Now);
        Assert.Contains("\"same_root_cause\":true", brief, StringComparison.Ordinal);
        Assert.Contains("is marked same_root_cause", brief, StringComparison.Ordinal);
    }

    /// <summary>
    /// #6582: the root fact is the newest finding for the request's story hash at or before the request opened. The brief composes at claim
    /// time, so a later analysis run of the same story (another database, another root) must not re-point the match: the earlier report
    /// on the root the request opened with stays marked, and a report on the later run's root is not.
    /// </summary>
    [Fact]
    public async Task TheRootFact_IsAnchoredToTheRequestsOpenTime_ALaterRunDoesNotRepointIt()
    {
        var findingId = 300L;
        async Task FindingAsync(string hash, string rootKey, DateTime at) =>
            await ExecAsync(@"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name, severity, confidence, category, story_path, story_path_hash, story_text, root_fact_key, fact_count)
VALUES ($1, $2, $3, $4, 'Sales', 0.8, 0.6, 'queries', $5, $6, 'story', $7, 2)",
                findingId++, Micros(at), ServerId, ServerName, "path-" + hash, hash, rootKey);

        async Task<Guid> ReportedAsync(string hash, string rootKey, DateTime at)
        {
            var id = Guid.NewGuid();
            await InsertRequestAsync(id, at, state: "reported", storyPathHash: hash);
            await FindingAsync(hash, rootKey, at);
            await ExecAsync(@"
INSERT INTO investigation_reports
    (report_id, request_id, report_format, agent_id, claim_id, submitted_at, verdict, root_cause, root_cause_found, summary,
     evidence, server_id, metric_name)
VALUES ($1, $2, 1, 'agent-a', $3, $4, 'real', 'a regressed plan', true, NULL, '[{""tool"":""get_query_store_top""}]', $5, 'Blocking')",
                findingId++, id, Guid.NewGuid(), Micros(at), ServerId);
            return id;
        }

        var onTheOpeningRoot = await ReportedAsync("hashA0001", "BAD_ACTOR_q1", Now.AddMinutes(-45));
        var onTheLaterRoot = await ReportedAsync("hashC0003", "BAD_ACTOR_q9", Now.AddMinutes(-40));

        var second = Guid.NewGuid();
        await InsertRequestAsync(second, Now, storyPathHash: "hashB0002");
        await FindingAsync("hashB0002", "BAD_ACTOR_q1", Now.AddMinutes(-1));
        await FindingAsync("hashB0002", "BAD_ACTOR_q9", Now.AddMinutes(10));

        var sources = await new DuckDbInvestigationBriefSourceReader(_initializer, () => Now.AddMinutes(20))
            .ReadAsync(second, TestContext.Current.CancellationToken);

        var marked = Assert.Single(sources!.EarlierReports, r => r.SameRootCause);
        Assert.Equal(onTheOpeningRoot, marked.RequestId);
        Assert.Contains(sources.EarlierReports, r => r.RequestId == onTheLaterRoot && !r.SameRootCause);
    }

    /// <summary>
    /// #6565, the twin of Darling's: PLAN_REGRESSION is a server-wide root fact (its key names no query), so key and database alone would
    /// mark every plan regression report on the database. Two stories are the same root cause only when their findings share a force target
    /// (database and query id). The request on queries 101 and 102 marks the report on query 101; a report on queries 201 and 202, one whose
    /// finding has no targets and one with an empty target list are named but not marked. A request whose own finding has no targets marks nothing.
    /// </summary>
    [Fact]
    public async Task PlanRegression_IsMarkedOnlyWhenTheFindingsShareAForceTarget()
    {
        static string Action(params long[] queryIds) => AlertContextSerializer.SerializeAction(new PerformanceMonitor.Analysis.RemediationAction(
            "PLAN_REGRESSION", "force", queryIds.Select(q => new PerformanceMonitor.Analysis.ForcePlanTarget("Sales", q, q + 1000)).ToList()))!;

        var findingId = 200L;
        async Task FindingAsync(string hash, string? action, DateTime at) =>
            await ExecAsync(@"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name, severity, confidence, category, story_path, story_path_hash, story_text, root_fact_key, fact_count, remediation_action_json)
VALUES ($1, $2, $3, $4, 'Sales', 0.8, 0.6, 'queries', $5, $6, 'story', 'PLAN_REGRESSION', 2, $7)",
                findingId++, Micros(at), ServerId, ServerName, "path-" + hash, hash, action);

        var reportId = 300L;
        async Task<Guid> ReportedAsync(string hash, string? action, DateTime at)
        {
            var id = Guid.NewGuid();
            await InsertRequestAsync(id, at, state: "reported", storyPathHash: hash);
            await FindingAsync(hash, action, at);
            await ExecAsync(@"
INSERT INTO investigation_reports
    (report_id, request_id, report_format, agent_id, claim_id, submitted_at, verdict, root_cause, root_cause_found, summary,
     evidence, server_id, metric_name)
VALUES ($1, $2, 1, 'agent-a', $3, $4, 'real', 'a regressed plan', true, NULL, '[{""tool"":""get_query_store_top""}]', $5, 'Blocking')",
                reportId++, id, Guid.NewGuid(), Micros(at), ServerId);
            return id;
        }

        var shared = await ReportedAsync("hashA0001", Action(101), Now.AddMinutes(-45));
        var otherQueries = await ReportedAsync("hashC0003", Action(201, 202), Now.AddMinutes(-50));
        var noTargets = await ReportedAsync("hashD0004", null, Now.AddMinutes(-40));
        var emptyTargets = await ReportedAsync("hashE0005", Action(), Now.AddMinutes(-35));
        var targetless = await ReportedAsync("hashG0007", null, Now.AddMinutes(-30));

        var second = Guid.NewGuid();
        await InsertRequestAsync(second, Now, storyPathHash: "hashB0002");
        await FindingAsync("hashB0002", Action(101, 102), Now);

        var reader = new DuckDbInvestigationBriefSourceReader(_initializer, () => Now);

        var none = await reader.ReadAsync(targetless, TestContext.Current.CancellationToken);
        Assert.DoesNotContain(none!.EarlierReports, r => r.SameRootCause);
        Assert.Contains(none.EarlierReports, r => r.RequestId == shared);

        var sources = await reader.ReadAsync(second, TestContext.Current.CancellationToken);
        var marked = Assert.Single(sources!.EarlierReports, r => r.SameRootCause);
        Assert.Equal(shared, marked.RequestId);
        Assert.Equal(shared, sources.EarlierReports[0].RequestId);
        Assert.Equal(new[] { otherQueries, noTargets, emptyTargets, targetless }.OrderBy(g => g), sources.EarlierReports.Where(r => !r.SameRootCause).Select(r => r.RequestId).OrderBy(g => g));
    }

    [Fact]
    public async Task EverySource_ComesBackFromAPlantedStore_AndTheComposedBriefMasksItsLiterals()
    {
        /* An earlier request of the same server and family, reported, its root cause naming the literals. */
        var earlierId = Guid.NewGuid();
        await InsertRequestAsync(earlierId, Now.AddHours(-2), state: "reported");
        await ExecAsync(@"
INSERT INTO investigation_reports
    (report_id, request_id, report_format, agent_id, claim_id, submitted_at, verdict, root_cause, root_cause_found, summary,
     evidence, server_id, metric_name)
VALUES (1, $1, 1, 'agent-a', $2, $3, 'real', $4, true, $5, '[{""tool"":""get_blocked_process_reports""}]', $6, 'Blocking')",
            earlierId, Guid.NewGuid(), Micros(Now.AddHours(-1)), "blocking on " + MaskStatement, "summary " + LiteralCanary, ServerId);

        /* The request the brief is for, ten minutes ago, with its alert history row (and a link that carries no facts). */
        var requestId = Guid.NewGuid();
        var alertAt = Now.AddMinutes(-10);
        await InsertRequestAsync(requestId, alertAt, storyPathHash: "LCHASH01");
        await InsertLinkAsync(1, requestId, alertAt);
        await ExecAsync(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, 'Blocking', 45, 30, TRUE, 'email', NULL, FALSE, FALSE, $4, $5)",
            Micros(alertAt.AddSeconds(2)), ServerId, ServerName, "Blocked for 45 s: " + MaskStatement, Context());

        await ExecAsync(@"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name, time_range_start, time_range_end, severity, confidence,
     category, story_path, story_path_hash, story_text, root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value,
     fact_count, incident_id, remediation_action_json, drill_down_json)
VALUES (1, $1, $2, $3, 'LcDb', $4, $1, 0.8, 0.6, 'blocking', 'BLOCKING → LOCK_WAIT', 'LCHASH01', 'Lock waits behind one session.',
        'BLOCKING', 45, 'LOCK_WAIT', 3, 2, 'LC_INCIDENT_1', NULL, NULL)",
            Micros(Now.AddMinutes(-5)), ServerId, ServerName, Micros(Now.AddHours(-4)));

        await new DuckDbMuteRuleStore(_initializer).InsertAsync(new MuteRule
        {
            Id = "lc-mute-1",
            ServerName = ServerName,
            QueryTextPattern = "%" + LiteralCanary + "%",
            Reason = "nightly load",
            CreatedAtUtc = Now.AddMinutes(-30),
        });

        await ExecAsync(@"
INSERT INTO investigation_notes (note_id, server_id, note_text, author, state, created_at, expires_at)
VALUES (1, $1, 'the nightly load blocks for a minute', 'ops', 'approved', $2, $3),
       (2, $1, 'a pending note the brief must not carry', 'ops', 'pending', $2, $3),
       (3, NULL, 'a fleet note every brief carries', 'ops', 'approved', $2, $3),
       (4, $1 + 1, 'another server''s note the brief must not carry', 'ops', 'approved', $2, $3),
       (5, $1, 'an expired note the brief must not carry', 'ops', 'approved', $2, $4)",
            ServerId, Micros(Now.AddDays(-2)), Micros(Now.AddDays(30)), Micros(Now.AddMinutes(-1)));

        await ExecAsync(@"
INSERT INTO agent_environment_profiles (profile_id, scope_server_id, profile_text, modified_at)
VALUES (1, $1, 'production order entry', $2)", ServerId, Micros(Now));

        var reader = new DuckDbInvestigationBriefSourceReader(_initializer, () => Now);
        var sources = await reader.ReadAsync(requestId, TestContext.Current.CancellationToken);

        Assert.NotNull(sources);
        Assert.Equal(ServerName, sources!.Request.ServerName);
        Assert.Equal("blocking", sources.Request.Family);
        Assert.False(sources.Request.IsPostgres);

        /* The history row two seconds off the link still answers for what the link lacks, and the stamp is the link's. */
        var alert = Assert.Single(sources.Alerts);
        Assert.Equal("Blocking", alert.MetricName);
        Assert.Equal(DateTimeKind.Utc, alert.AlertTimeUtc.Kind);
        Assert.Equal(alertAt, DateTime.SpecifyKind(alert.AlertTimeUtc, DateTimeKind.Unspecified));
        Assert.Equal(45, alert.CurrentValue);
        Assert.Equal(30, alert.ThresholdValue);
        Assert.Contains(LiteralCanary, alert.DetailText, StringComparison.Ordinal);
        Assert.Contains(alert.Context, c => c.Fields.Any(f => f.Key == "Blocked Query"));

        var finding = Assert.Single(sources.Findings);
        Assert.Equal("LCHASH01", finding.StoryPathHash);

        var report = Assert.Single(sources.EarlierReports);
        Assert.Equal(earlierId, report.RequestId);
        Assert.Equal("agent-a", report.AgentId);
        Assert.Contains(LiteralCanary, report.RootCause, StringComparison.Ordinal);

        var mute = Assert.Single(sources.RecentChanges, c => c.Kind == "mute_rule");
        Assert.Equal("%" + LiteralCanary + "%", mute.Pattern);
        Assert.Contains("nightly load", mute.Text, StringComparison.Ordinal);
        Assert.Equal(Now.AddMinutes(-30), DateTime.SpecifyKind(mute.AtUtc, DateTimeKind.Utc));

        /* The server's note, then the fleet's (server_id NULL); another server's, a pending and an expired note stay out. */
        Assert.Equal(
            new[] { "the nightly load blocks for a minute", "a fleet note every brief carries" },
            sources.Notes.Select(n => n.Text).ToArray());
        var profile = Assert.Single(sources.Profiles);
        Assert.Equal("server", profile.Scope);

        /* The brief composed from this store: under full the literals are there (the control); under maskLiterals and
           metadataOnly no section, the earlier report's included, still carries one. */
        string? full = await new InvestigationBriefComposer(reader, new McpDataDetailSetting(McpDataDetail.Full), LiteInvestigationBriefTools.Tools, () => Now)
            .ComposeAsync(requestId, TestContext.Current.CancellationToken);
        Assert.Contains(LiteralCanary, full, StringComparison.Ordinal);
        Assert.Contains(NumericCanary, full, StringComparison.Ordinal);
        foreach (var mode in new[] { McpDataDetail.MaskLiterals, McpDataDetail.MetadataOnly })
        {
            string? brief = await new InvestigationBriefComposer(reader, new McpDataDetailSetting(mode), LiteInvestigationBriefTools.Tools, () => Now)
                .ComposeAsync(requestId, TestContext.Current.CancellationToken);
            Assert.NotNull(brief);
            Assert.DoesNotContain(LiteralCanary, brief, StringComparison.Ordinal);
            Assert.DoesNotContain(NumericCanary, brief, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task ARequestAloneReadsAsEmptySections_AndAnUnknownRequestAsNull()
    {
        var requestId = Guid.NewGuid();
        await InsertRequestAsync(requestId, Now.AddMinutes(-10));
        await InsertLinkAsync(1, requestId, Now.AddMinutes(-10));

        var reader = new DuckDbInvestigationBriefSourceReader(_initializer, () => Now);
        var sources = await reader.ReadAsync(requestId, TestContext.Current.CancellationToken);

        Assert.NotNull(sources);
        /* The request's own alert link with no history row behind it: the alert, with nothing read from the log. */
        var alert = Assert.Single(sources!.Alerts);
        Assert.Null(alert.DetailText);
        Assert.Empty(alert.Context);
        Assert.Empty(sources.Findings);
        Assert.Empty(sources.EarlierReports);
        Assert.Empty(sources.RecentChanges);
        Assert.Empty(sources.Notes);
        Assert.Empty(sources.Profiles);

        Assert.Null(await reader.ReadAsync(Guid.NewGuid(), TestContext.Current.CancellationToken));
        Assert.NotNull(await new InvestigationBriefComposer(reader, new McpDataDetailSetting(McpDataDetail.MetadataOnly), LiteInvestigationBriefTools.Tools, () => Now)
            .ComposeAsync(requestId, TestContext.Current.CancellationToken));
    }

    /// <summary>#5821: an alert that opens or joins before its page is sent writes its link rows before its history row, so
    /// a claim in that gap must read the link's own facts, and a history row that lands later must not replace them.</summary>
    [Fact]
    public async Task ALinksOwnFacts_AreReadBeforeAnyHistoryRowExists_AndWinOverOneThatLandsLater()
    {
        var requestId = Guid.NewGuid();
        var alertAt = Now.AddMinutes(-10);
        await InsertRequestAsync(requestId, alertAt);
        await InsertLinkAsync(1, requestId, alertAt, value: 45, threshold: 30, muted: true, detail: "link detail", contextJson: Context());

        var reader = new DuckDbInvestigationBriefSourceReader(_initializer, () => Now);
        var alert = Assert.Single((await reader.ReadAsync(requestId, TestContext.Current.CancellationToken))!.Alerts);
        Assert.Equal(45, alert.CurrentValue);
        Assert.Equal(30, alert.ThresholdValue);
        Assert.True(alert.Muted);
        Assert.Equal("link detail", alert.DetailText);
        Assert.NotEmpty(alert.Context);
        Assert.Null(alert.SendError);

        await ExecAsync(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error,
     muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, 'Blocking', 99, 98, FALSE, 'email', 'smtp refused', FALSE, FALSE, 'history detail', NULL)",
            Micros(alertAt), ServerId, ServerName);

        alert = Assert.Single((await reader.ReadAsync(requestId, TestContext.Current.CancellationToken))!.Alerts);
        Assert.Equal(45, alert.CurrentValue);
        Assert.Equal("link detail", alert.DetailText);
        Assert.True(alert.Muted);
        Assert.Equal("smtp refused", alert.SendError);
    }

    /// <summary>Four distinct alerts or more: the first, then the newest three, the first leading and the rest newest first.</summary>
    [Fact]
    public async Task ManyAlerts_ReadAsTheFirstThenTheNewestThree()
    {
        var requestId = Guid.NewGuid();
        var start = Now.AddMinutes(-60);
        await InsertRequestAsync(requestId, start);
        for (var i = 0; i < 6; i++)
            await InsertLinkAsync(i + 1, requestId, start.AddMinutes(i * 5), value: i);

        var reader = new DuckDbInvestigationBriefSourceReader(_initializer, () => Now);
        var alerts = (await reader.ReadAsync(requestId, TestContext.Current.CancellationToken))!.Alerts;

        Assert.Equal(new double?[] { 0, 5, 4, 3 }, alerts.Select(a => a.CurrentValue).ToArray());
    }

    /// <summary>Every name the table can put in a brief is a tool Lite serves: a call naming a tool the surface dropped would
    /// send an agent to an unknown-tool error. Blocking is Lite's own name, and Availability Group health is not served.</summary>
    [Fact]
    public void EveryNameInTheFamilyTable_IsAServedLiteTool_AndTheNameableListHoldsThemAll()
    {
        var served = typeof(McpAnalysisTools).Assembly.GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
            .Where(n => n is not null)
            .ToHashSet(StringComparer.Ordinal);

        var tools = LiteInvestigationBriefTools.Tools;
        Assert.Empty(tools.PostgresFamilyCalls);
        foreach (var family in InvestigationFamily.All)
            Assert.True(tools.SqlServerFamilyCalls.ContainsKey(family), family);
        foreach (var name in tools.SqlServerFamilyCalls.Values.SelectMany(c => c))
        {
            Assert.Contains(name, served);
            Assert.Contains(name, tools.NameableTools);
        }

        foreach (var name in tools.NameableTools)
            Assert.Contains(name, served);

        Assert.Contains("get_blocked_process_reports", tools.SqlServerFamilyCalls[InvestigationFamily.Blocking]);
        Assert.DoesNotContain("get_blocking", tools.NameableTools);
        Assert.DoesNotContain("get_ag_health", tools.NameableTools);
    }

    private Task InsertFireAsync(DateTime at, bool muted = false, string metric = "Blocking") =>
        ExecAsync(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 45, 30, $5, 'email', NULL, $6, FALSE, NULL, NULL)",
            Micros(at), ServerId, ServerName, metric, !muted, muted);

    /// <summary>
    /// #6261 (Darling's <c>TheStreak_*</c> live facts, same planted shape): days 1, 2 (muted only) and 3 fired, day 4 did not,
    /// day 5 did, so the streak is 3 and the brief says so beside the #6000 counts, with the note that muted fires count; and a
    /// 23:50 anchor counts its day-1 fire at 00:10 of its own date, which a count by calendar date would lose.
    /// </summary>
    [Fact]
    public async Task TheStreak_RunsBackFromTheDayBefore_CountsAMutedOnlyDay_StopsAtAGap_AndCrossesMidnight()
    {
        var anchor = Now.Date.AddDays(-1).AddHours(2).AddMinutes(10);
        var requestId = Guid.NewGuid();
        await InsertRequestAsync(requestId, anchor);
        await InsertLinkAsync(1, requestId, anchor);
        await InsertFireAsync(anchor.AddDays(-1).AddMinutes(3));
        await InsertFireAsync(anchor.AddDays(-2), muted: true);
        await InsertFireAsync(anchor.AddDays(-3).AddMinutes(-12));
        await InsertFireAsync(anchor.AddDays(-5));
        await InsertFireAsync(anchor.AddDays(-4).AddHours(6)); // day 4, at another time of day

        var reader = new DuckDbInvestigationBriefSourceReader(_initializer, () => Now);
        var sources = await reader.ReadAsync(requestId, TestContext.Current.CancellationToken);
        var recurrence = sources!.Recurrence!.Single();
        Assert.Equal(3, recurrence.StreakDays);
        Assert.False(recurrence.StreakAtLeastDaysWindow);

        var node = System.Text.Json.Nodes.JsonNode.Parse(InvestigationBriefComposer.Compose(
            sources, LiteInvestigationBriefTools.Tools, McpDataDetail.Full, Now))!["recurrence"]!.AsArray().Single()!;
        Assert.Equal(3, node["streak_days"]!.GetValue<int>());
        Assert.False(node["streak_at_least_days_window"]!.GetValue<bool>());
        Assert.Contains("muted fires count", node["streak_note"]!.GetValue<string>(), StringComparison.Ordinal);

        /* A 23:50 anchor: the day-1 fire is at 00:10 of the anchor's own date, day 2 at 00:10 the date before. */
        var late = Now.Date.AddDays(-3).AddHours(23).AddMinutes(50);
        var lateId = Guid.NewGuid();
        await InsertRequestAsync(lateId, late);
        await InsertLinkAsync(2, lateId, late);
        await InsertFireAsync(late.Date.AddMinutes(10));
        await InsertFireAsync(late.Date.AddDays(-1).AddMinutes(10));
        var lateRecurrence = (await reader.ReadAsync(lateId, TestContext.Current.CancellationToken))!.Recurrence!.Single();
        Assert.Equal(2, lateRecurrence.StreakDays);
    }

    /// <summary>
    /// #6261: a 00:10 anchor counts a 23:50 fire two calendar dates back as day 1 (24 hours 20 minutes back) and the one before it
    /// as day 2, so the streak is 2; a count by calendar date would number them 2 and 3 and find no run.
    /// </summary>
    [Fact]
    public async Task TheStreak_OfA_0010Anchor_CountsA_2350FireTwoDatesBackAsDayOne()
    {
        var anchor = Now.Date.AddDays(-1).AddMinutes(10);
        var requestId = Guid.NewGuid();
        await InsertRequestAsync(requestId, anchor);
        await InsertLinkAsync(1, requestId, anchor);
        await InsertFireAsync(anchor.AddDays(-1).AddMinutes(-20));
        await InsertFireAsync(anchor.AddDays(-2).AddMinutes(-20));

        var reader = new DuckDbInvestigationBriefSourceReader(_initializer, () => Now);
        var recurrence = (await reader.ReadAsync(requestId, TestContext.Current.CancellationToken))!.Recurrence!.Single();
        Assert.Equal(2, recurrence.StreakDays);
    }

    /// <summary>
    /// #6261: one calendar date holds two day numbers. For a 23:50 anchor, 23:50 on the date before is day 1 and 00:10 on that same
    /// date is day 2 (47 hours 40 minutes back); a date difference would see one day and a streak of 1.
    /// </summary>
    [Fact]
    public async Task TheStreak_OfA_2350Anchor_CanHoldTwoDayNumbersOnOneCalendarDate()
    {
        var anchor = Now.Date.AddDays(-3).AddHours(23).AddMinutes(50);
        var requestId = Guid.NewGuid();
        await InsertRequestAsync(requestId, anchor);
        await InsertLinkAsync(1, requestId, anchor);
        await InsertFireAsync(anchor.AddDays(-1));
        await InsertFireAsync(anchor.Date.AddDays(-1).AddMinutes(10));

        var reader = new DuckDbInvestigationBriefSourceReader(_initializer, () => Now);
        var recurrence = (await reader.ReadAsync(requestId, TestContext.Current.CancellationToken))!.Recurrence!.Single();
        Assert.Equal(2, recurrence.StreakDays);
    }

    /// <summary>A fire on every one of the Days days says so as a floor, and a fire older than the look-back adds nothing.</summary>
    [Fact]
    public async Task TheStreak_NeverPassesTheLookBack_AndSaysItIsAtLeast()
    {
        var anchor = Now.Date.AddDays(-1).AddHours(2).AddMinutes(10);
        var requestId = Guid.NewGuid();
        await InsertRequestAsync(requestId, anchor);
        await InsertLinkAsync(1, requestId, anchor);
        for (var day = 1; day <= InvestigationAlertRecurrence.Days + 2; day++)
            await InsertFireAsync(anchor.AddDays(-day));

        var reader = new DuckDbInvestigationBriefSourceReader(_initializer, () => Now);
        var recurrence = (await reader.ReadAsync(requestId, TestContext.Current.CancellationToken))!.Recurrence!.Single();
        Assert.Equal(InvestigationAlertRecurrence.Days, recurrence.StreakDays);
        Assert.True(recurrence.StreakAtLeastDaysWindow);
    }

    /// <summary>The registration hands out a composer over the Lite reader, masking at the registered level, and at full when
    /// the host registered no setting (so the slice runs whether or not the settings registration has landed).</summary>
    [Fact]
    public async Task TheRegistration_ResolvesTheComposer_WithAndWithoutADataDetailSetting()
    {
        var requestId = Guid.NewGuid();
        await InsertRequestAsync(requestId, Now.AddMinutes(-10));
        await InsertLinkAsync(1, requestId, Now.AddMinutes(-10), value: 1, threshold: 2, detail: "Blocked: " + MaskStatement, contextJson: Context());

        var withoutSetting = new ServiceCollection();
        LiteInvestigationServices.AddInvestigationBrief(withoutSetting, _initializer);
        using var providerWithout = withoutSetting.BuildServiceProvider();
        Assert.IsType<DuckDbInvestigationBriefSourceReader>(providerWithout.GetRequiredService<IInvestigationBriefSourceReader>());
        var composer = providerWithout.GetRequiredService<InvestigationBriefComposer>();
        Assert.Equal(McpDataDetail.Full, composer.Mode);
        Assert.Contains(LiteralCanary, await composer.ComposeAsync(requestId, TestContext.Current.CancellationToken), StringComparison.Ordinal);

        var withSetting = new ServiceCollection();
        withSetting.AddSingleton(new McpDataDetailSetting(McpDataDetail.MetadataOnly));
        LiteInvestigationServices.AddInvestigationBrief(withSetting, _initializer);
        using var providerWith = withSetting.BuildServiceProvider();
        var masked = providerWith.GetRequiredService<InvestigationBriefComposer>();
        Assert.Equal(McpDataDetail.MetadataOnly, masked.Mode);
        Assert.DoesNotContain(LiteralCanary, await masked.ComposeAsync(requestId, TestContext.Current.CancellationToken), StringComparison.Ordinal);
    }
}
