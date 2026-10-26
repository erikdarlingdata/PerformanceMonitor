/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// <see cref="PgInvestigationBriefSourceReader"/> on a live store (#5632 Phase A): its SQL had only run against fakes. One
/// server with an alert history row, a finding, a mute rule, a service start, an approved note, a server profile and an
/// earlier report: every source comes back. A request with nothing else on the store reads as empty sections, never a
/// throw, and an unknown request reads as null. The brief composed from the planted store masks the earlier report's and
/// the alert's literals under both masked levels (R3).
/// </summary>
public sealed class PgInvestigationBriefSourceReaderLiveTests
{
    private const int ServerId = 7_563_201;
    private const string ServerName = "LcBriefReaderSrv";

    private static readonly DateTime Now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static InvestigationAlert Alert(DateTime at) =>
        new(1, ServerId, ServerName, "blocking", "Blocking", at, new[] { ServerId + ":Blocking" }, null,
            20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    [Fact]
    public async Task EverySource_ComesBackFromAPlantedStore_AndTheComposedBriefMasksItsLiterals()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var store = new InvestigationRequestStore(dataSource, 30, () => Now.AddHours(-2));

            /* An earlier request of the same server and family, claimed and reported, its root cause naming the literals. */
            var earlierId = (await store.OpenOrJoinAsync(Alert(Now.AddHours(-2)), ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(earlierId, "agent-a", ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                earlierId, claim.ClaimId!.Value, "agent-a", 1, "real",
                "blocking on " + StatementFilterCensus.MaskStatement, true, "summary " + StatementFilterCensus.LiteralCanary,
                "[{\"tool\":\"get_blocking\"}]"), ct));

            /* The request the brief is for, ten minutes ago, with its alert history row. */
            var current = new InvestigationRequestStore(dataSource, 30, () => Now.AddMinutes(-10));
            var alertAt = Now.AddMinutes(-10);
            var requestId = (await current.OpenOrJoinAsync(Alert(alertAt), ct)).RequestId!.Value;
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, 'Blocking', 45, 30, TRUE, 'email', NULL, FALSE, FALSE, $4, $5)",
                DarlingMcpTestData.Naive(alertAt), ServerId, ServerName,
                "Blocked for 45 s: " + StatementFilterCensus.MaskStatement,
                AlertContextSerializer.Serialize(new AlertContext
                {
                    Details =
                    {
                        new AlertDetailItem { Heading = "Blocking", Fields = { ("Blocked Query", StatementFilterCensus.MaskStatement) } },
                    },
                }));

            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
     incident_id, remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, $4, 'LcDb', $5, $6, 0.8, 0.6, 'blocking',
        'BLOCKING → LOCK_WAIT', 'LCHASH01', $7, 'BLOCKING', 45, 'LOCK_WAIT', 3, 2, 'LC_INCIDENT_1', NULL, NULL)",
                CollectionIdGenerator.Next(), Now.AddMinutes(-5), ServerId, ServerName, Now.AddHours(-4), Now.AddMinutes(-5),
                "Lock waits behind one session.");

            await new PgMuteRuleStore(dataSource).InsertAsync(new MuteRule
            {
                Id = "lc-mute-1",
                ServerName = ServerName,
                QueryTextPattern = "%" + StatementFilterCensus.LiteralCanary + "%",
                Reason = "nightly load",
            });

            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config.service_version (service_host, product_version, process_started_at, updated_at)
VALUES ('lc-host', '9.9.9', $1, now())", Now.AddHours(-1));

            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config.investigation_notes (server_id, note_text, author, state, expires_at)
VALUES ($1, 'the nightly load blocks for a minute', 'ops', 'approved', $2),
       ($1, 'a pending note the brief must not carry', 'ops', 'pending', $2),
       (NULL, 'a fleet note every brief carries', 'ops', 'approved', $2),
       ($1 + 1, 'another server''s note the brief must not carry', 'ops', 'approved', $2)", ServerId, Now.AddDays(30));

            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config.agent_environment_profiles (scope_server_id, profile_text) VALUES ($1, 'production order entry')",
                ServerId);

            var reader = new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now);
            var sources = await reader.ReadAsync(requestId, ct);

            Assert.NotNull(sources);
            Assert.Equal(ServerName, sources!.Request.ServerName);
            Assert.Equal("blocking", sources.Request.Family);

            var alert = Assert.Single(sources.Alerts);
            Assert.Equal("Blocking", alert.MetricName);
            Assert.Equal(45, alert.CurrentValue);
            Assert.Contains(StatementFilterCensus.LiteralCanary, alert.DetailText, StringComparison.Ordinal);
            Assert.Contains(alert.Context, c => c.Fields.Any(f => f.Key == "Blocked Query"));

            var finding = Assert.Single(sources.Findings);
            Assert.Equal("LCHASH01", finding.StoryPathHash);

            var report = Assert.Single(sources.EarlierReports);
            Assert.Equal(earlierId, report.RequestId);
            Assert.Equal("agent-a", report.AgentId);
            Assert.Contains(StatementFilterCensus.LiteralCanary, report.RootCause, StringComparison.Ordinal);

            var mute = Assert.Single(sources.RecentChanges, c => c.Kind == "mute_rule");
            Assert.Equal("%" + StatementFilterCensus.LiteralCanary + "%", mute.Pattern);
            var start = Assert.Single(sources.RecentChanges, c => c.Kind == "service_version");
            Assert.Contains("9.9.9", start.Text, StringComparison.Ordinal);
            Assert.Equal(Now.AddHours(-1), DateTime.SpecifyKind(start.AtUtc, DateTimeKind.Utc));

            /* #5632 review L3: the server's note, then the fleet's (server_id NULL); another server's and a pending note
               stay out. */
            Assert.Equal(
                new[] { "the nightly load blocks for a minute", "a fleet note every brief carries" },
                sources.Notes.Select(n => n.Text).ToArray());
            var profile = Assert.Single(sources.Profiles);
            Assert.Equal("server", profile.Scope);

            /* R3: the brief composed from this store. Under full the literals are there (the control); under maskLiterals and
               metadataOnly no section, the earlier report's included, still carries one. */
            string? full = await new InvestigationBriefComposer(reader, new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools, () => Now)
                .ComposeAsync(requestId, ct);
            Assert.Contains(StatementFilterCensus.LiteralCanary, full, StringComparison.Ordinal);
            Assert.Contains(StatementFilterCensus.NumericCanary, full, StringComparison.Ordinal);
            foreach (var mode in new[] { McpDataDetail.MaskLiterals, McpDataDetail.MetadataOnly })
            {
                string? brief = await new InvestigationBriefComposer(reader, new McpDataDetailSetting(mode), DarlingInvestigationBriefTools.Tools, () => Now)
                    .ComposeAsync(requestId, ct);
                Assert.NotNull(brief);
                Assert.DoesNotContain(StatementFilterCensus.LiteralCanary, brief, StringComparison.Ordinal);
                Assert.DoesNotContain(StatementFilterCensus.NumericCanary, brief, StringComparison.Ordinal);
            }
        });
    }

    [Fact]
    public async Task ARequestAloneReadsAsEmptySections_AndAnUnknownRequestAsNull()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var store = new InvestigationRequestStore(dataSource, 30, () => Now.AddMinutes(-10));
            var requestId = (await store.OpenOrJoinAsync(Alert(Now.AddMinutes(-10)), ct)).RequestId!.Value;

            var reader = new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now);
            var sources = await reader.ReadAsync(requestId, ct);

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

            Assert.Null(await reader.ReadAsync(Guid.NewGuid(), ct));
            Assert.NotNull(await new InvestigationBriefComposer(reader, new McpDataDetailSetting(McpDataDetail.MetadataOnly), DarlingInvestigationBriefTools.Tools, () => Now)
                .ComposeAsync(requestId, ct));
        });
    }

    /// <summary>
    /// #5914: <c>prior_links</c> are the links of other requests on the same server and family recorded in the last 30
    /// days, newest first; a link 31 days old, one on another family and one on another server are left out. The brief
    /// carries them, and <c>request_url</c> (the opening alert's triage page) when a public base URL is set; with neither
    /// set, neither key is in the brief.
    /// </summary>
    [Fact]
    public async Task PriorLinks_AreTheSameServerAndFamilys_LastThirtyDays_AndTheBriefCarriesThemWithTheRequestUrl()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId + 1, ServerName + "B", ct);
            async Task ReportedAsync(DateTime at, InvestigationAlert alert, string url)
            {
                var store = new InvestigationRequestStore(dataSource, 30, () => at);
                var id = (await store.OpenOrJoinAsync(alert, ct)).RequestId!.Value;
                var claim = await store.ClaimAsync(id, "agent-a", ct);
                Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                    id, claim.ClaimId!.Value, "agent-a", 1, "real", "a chain", true, null, "[{\"tool\":\"get_blocking\"}]",
                    Links: new[] { new InvestigationReportLink("jira", url, "OPS-1") }), ct));
            }

            await ReportedAsync(Now.AddDays(-31), Alert(Now.AddDays(-31)), "https://jira.example.invalid/browse/OLD");
            await ReportedAsync(Now.AddDays(-3), Alert(Now.AddDays(-3)), "https://jira.example.invalid/browse/KEEP-3D");
            await ReportedAsync(Now.AddHours(-2), Alert(Now.AddHours(-2)), "https://jira.example.invalid/browse/KEEP-2H");
            await ReportedAsync(Now.AddHours(-1), new InvestigationAlert(1, ServerId, ServerName, "jobs", "Job failed", Now.AddHours(-1),
                new[] { ServerId + ":Job" }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), "https://jira.example.invalid/browse/FAMILY");
            await ReportedAsync(Now.AddHours(-1), new InvestigationAlert(1, ServerId + 1, ServerName + "B", "blocking", "Blocking", Now.AddHours(-1),
                new[] { (ServerId + 1) + ":Blocking" }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), "https://jira.example.invalid/browse/SERVER");

            var current = new InvestigationRequestStore(dataSource, 30, () => Now.AddMinutes(-10));
            var requestId = (await current.OpenOrJoinAsync(Alert(Now.AddMinutes(-10)), ct)).RequestId!.Value;

            var sources = await new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now, publicBaseUrl: "https://monitor.example.invalid")
                .ReadAsync(requestId, ct);
            Assert.Equal(new[] { "https://jira.example.invalid/browse/KEEP-2H", "https://jira.example.invalid/browse/KEEP-3D" },
                sources!.PriorLinks!.Select(l => l.Url));
            Assert.StartsWith("https://monitor.example.invalid/#/triage", sources.RequestUrl, StringComparison.Ordinal);

            var brief = JsonNode.Parse(InvestigationBriefComposer.Compose(sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now))!;
            Assert.Equal(2, brief["prior_links"]!.AsArray().Count);
            Assert.Equal("OPS-1", brief["prior_links"]![0]!["key"]!.GetValue<string>());
            Assert.Equal(sources.RequestUrl, brief["request_url"]!.GetValue<string>());

            /* No base URL and no prior link (another server's request): neither key. */
            var lone = new InvestigationRequestStore(dataSource, 30, () => Now.AddMinutes(-5));
            var loneId = (await lone.OpenOrJoinAsync(new InvestigationAlert(1, ServerId + 1, ServerName + "B", "jobs", "Job failed", Now.AddMinutes(-5),
                new[] { (ServerId + 1) + ":Job" }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), ct)).RequestId!.Value;
            var loneSources = await new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now).ReadAsync(loneId, ct);
            Assert.Null(loneSources!.PriorLinks);
            Assert.Null(loneSources.RequestUrl);
            var loneBrief = InvestigationBriefComposer.Compose(loneSources, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now);
            Assert.DoesNotContain("prior_links", loneBrief, StringComparison.Ordinal);
            Assert.DoesNotContain("request_url", loneBrief, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// #6334: the brief lists the operator's known-pattern entries for each linked metric AT THE REQUEST'S FIRST ALERT, not at the
    /// metric's own alert time. The request links Blocking at T and Deadlocks at T+5 minutes. An entry for Deadlocks at T matches
    /// (it is judged at the first alert's time), one for Deadlocks at T+5 does not (that is the metric's own time), one for another
    /// server does not, and a scope-free entry matches both metrics. A store without the table gives no part and no error.
    /// </summary>
    [Fact]
    public async Task KnownPatterns_AreMatchedPerLinkedMetric_AtTheFirstAlertsTime_AndAStoreWithoutTheTableGivesNone()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId + 1, ServerName + "B", ct);
            var first = Now.AddMinutes(-20);
            var second = first.AddMinutes(5);
            var requestStore = new InvestigationRequestStore(dataSource, 30, () => second);
            var requestId = (await requestStore.OpenOrJoinAsync(Alert(first), ct)).RequestId!.Value;
            var joined = await requestStore.OpenOrJoinAsync(new InvestigationAlert(1, ServerId, ServerName, "blocking", "Deadlocks", second,
                new[] { ServerId + ":Blocking" }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), ct);
            Assert.Equal(requestId, joined.RequestId!.Value);

            var firstTime = new TimeOnly(first.Hour, first.Minute);
            var secondTime = new TimeOnly(second.Hour, second.Minute);
            await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
            await DarlingMcpTestData.ExecAsync(connection, ct, DarlingManagedRoles.BuildKnownPatternFunctionsSql("config"));
            await DarlingMcpTestData.ExecAsync(connection, ct, $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({ServerId}, 'kp-a', 'kp-a', TRUE), ({ServerId + 1}, 'kp-b', 'kp-b', TRUE) ON CONFLICT DO NOTHING");
            var patterns = new PgKnownPatternStore(dataSource);
            var blockingAtFirst = await patterns.InsertAsync(new KnownPatternInput(ServerId, "Blocking", firstTime, null, 2, "blocking at T", null), "erik", ct);
            var deadlocksAtFirst = await patterns.InsertAsync(new KnownPatternInput(ServerId, "Deadlocks", firstTime, null, 2, "deadlocks at T", null), "erik", ct);
            var deadlocksAtOwnTime = await patterns.InsertAsync(new KnownPatternInput(ServerId, "Deadlocks", secondTime, null, 2, "deadlocks at T+5", null), "erik", ct);
            var otherServer = await patterns.InsertAsync(new KnownPatternInput(ServerId + 1, "Blocking", firstTime, null, 2, "another server", null), "erik", ct);
            var anything = await patterns.InsertAsync(new KnownPatternInput(ServerId, null, null, null, null, "anything on this server", null), null, ct);

            var reader = new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now);
            var sources = await reader.ReadAsync(requestId, ct);
            var got = sources!.KnownPatterns!.Select(m => (m.MetricName, m.EntryId)).OrderBy(m => m.MetricName, StringComparer.Ordinal).ThenBy(m => m.EntryId).ToArray();
            Assert.Equal(new[] { ("Blocking", blockingAtFirst), ("Blocking", anything), ("Deadlocks", deadlocksAtFirst), ("Deadlocks", anything) }, got);
            Assert.DoesNotContain(sources.KnownPatterns!, m => m.EntryId == deadlocksAtOwnTime || m.EntryId == otherServer);
            Assert.Equal("erik", sources.KnownPatterns!.Single(m => m.EntryId == blockingAtFirst).CreatedBy);
            Assert.Null(sources.KnownPatterns!.First(m => m.EntryId == anything).CreatedBy);

            var brief = JsonNode.Parse(InvestigationBriefComposer.Compose(sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now))!;
            Assert.Equal(4, brief["known_patterns"]!.AsArray().Count);
            Assert.Contains("written by a person or an MCP client with write access", brief["known_patterns_note"]!.GetValue<string>(), StringComparison.Ordinal);

            /* A store that has not taken V214 has no table: no part, no error. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "DROP TABLE config.config_known_patterns CASCADE");
            var without = await reader.ReadAsync(requestId, ct);
            Assert.NotNull(without);
            Assert.Null(without!.KnownPatterns);
            Assert.DoesNotContain("known_patterns", InvestigationBriefComposer.Compose(without, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now), StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// #6565: one root cause can open a second request when the analysis chain differs (so the story hash differs) or the 30-minute join window
    /// has passed. The brief of the second
    /// request names the first one's report and verdict, marked same_root_cause, when it was reported in the last 24 hours and its story has
    /// the same root fact (key and database) on the same server and family: two stories 45 minutes apart on one root query, different
    /// hashes (the hash is a pure hash of the story's path). A report on another root query, the same root query in another database, a same-root report 25 hours old, and a request with
    /// no finding behind its hash are not marked.
    /// </summary>
    [Fact]
    public async Task SameRootFactUnderAnotherStoryHash_ReportedWithin24Hours_IsNamedFirstAndMarked_OthersAreNot()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            async Task FindingAsync(string hash, string rootKey, string database, DateTime at) =>
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO analysis_findings (finding_id, analysis_time, server_id, server_name, database_name, severity, confidence, category, story_path, story_path_hash, story_text, root_fact_key, fact_count) " +
                    "VALUES ($1, $2, $3, $4, $5, 0.8, 0.8, 'queries', $6, $7, 'story', $8, 2)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, database, "path-" + hash, hash, rootKey);

            InvestigationAlert StoryAlert(string hash, DateTime at) =>
                new(1, ServerId, ServerName, "blocking", "Analysis: queries [" + hash + "]", at, new[] { ServerId + ":" + hash }, hash,
                    20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

            async Task<Guid> OpenAsync(string hash, DateTime at) =>
                (await new InvestigationRequestStore(dataSource, 30, () => at).OpenOrJoinAsync(StoryAlert(hash, at), ct)).RequestId!.Value;

            async Task<Guid> ReportedAsync(string hash, DateTime at, string verdict)
            {
                var store = new InvestigationRequestStore(dataSource, 30, () => at);
                var id = (await store.OpenOrJoinAsync(StoryAlert(hash, at), ct)).RequestId!.Value;
                var claim = await store.ClaimAsync(id, "agent-a", ct);
                Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                    id, claim.ClaimId!.Value, "agent-a", 1, verdict, "a regressed plan", true, null, "[{\"tool\":\"get_query_store_top\"}]"), ct));
                return id;
            }

            await FindingAsync("hashA0001", "BAD_ACTOR_q1", "Sales", Now.AddMinutes(-45));
            await FindingAsync("hashB0002", "BAD_ACTOR_q1", "Sales", Now);
            await FindingAsync("hashC0003", "BAD_ACTOR_q2", "Sales", Now.AddMinutes(-50));
            await FindingAsync("hashD0004", "BAD_ACTOR_q1", "Hr", Now.AddMinutes(-40));
            await FindingAsync("hashE0005", "BAD_ACTOR_q1", "Sales", Now.AddHours(-25));

            var first = await ReportedAsync("hashA0001", Now.AddMinutes(-45), "real");
            var otherQuery = await ReportedAsync("hashC0003", Now.AddMinutes(-50), "false_positive");
            var otherDatabase = await ReportedAsync("hashD0004", Now.AddMinutes(-40), "real");
            var tooOld = await ReportedAsync("hashE0005", Now.AddHours(-25), "real");
            var second = await OpenAsync("hashB0002", Now);

            var reader = new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now);
            var sources = await reader.ReadAsync(second, ct);

            var marked = Assert.Single(sources!.EarlierReports, r => r.SameRootCause);
            Assert.Equal(first, marked.RequestId);
            Assert.Equal("real", marked.Verdict);
            Assert.Equal(first, sources.EarlierReports[0].RequestId);
            Assert.Equal(new[] { otherQuery, otherDatabase, tooOld }.OrderBy(g => g), sources.EarlierReports.Where(r => !r.SameRootCause).Select(r => r.RequestId).OrderBy(g => g));

            var brief = JsonNode.Parse(InvestigationBriefComposer.Compose(sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now))!;
            var reports = brief["earlier_reports"]!.AsArray();
            Assert.Equal(first.ToString(), reports[0]!["request_id"]!.GetValue<string>());
            Assert.Equal("real", reports[0]!["verdict"]!.GetValue<string>());
            Assert.True(reports[0]!["same_root_cause"]!.GetValue<bool>());
            Assert.Single(reports, r => r!["same_root_cause"] is not null);
            Assert.Contains(brief["read_rules"]!.AsArray(), r => r!.GetValue<string>() == InvestigationBriefComposer.SameRootCauseRule);

            /* A request whose story has no finding behind its hash (another family, so it opens its own request) names nothing, and its brief carries neither the mark nor the rule. */
            var loneId = (await new InvestigationRequestStore(dataSource, 30, () => Now.AddMinutes(-1)).OpenOrJoinAsync(
                new InvestigationAlert(1, ServerId, ServerName, "jobs", "Analysis: jobs [hashF0006]", Now.AddMinutes(-1), new[] { ServerId + ":hashF0006" }, "hashF0006",
                    20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), ct)).RequestId!.Value;
            var lone = await reader.ReadAsync(loneId, ct);
            Assert.DoesNotContain(lone!.EarlierReports, r => r.SameRootCause);
            Assert.DoesNotContain("same_root_cause", InvestigationBriefComposer.Compose(lone, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now), StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// #6582: the root fact is the newest finding for the request's story hash at or before the request opened. The brief composes at claim
    /// time, so a later analysis run of the same story (another root) must not re-point the match: the earlier report on the root the
    /// request opened with stays marked, and a report on the later run's root is not.
    /// </summary>
    [Fact]
    public async Task TheRootFact_IsAnchoredToTheRequestsOpenTime_ALaterRunDoesNotRepointIt()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            async Task FindingAsync(string hash, string rootKey, DateTime at) =>
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO analysis_findings (finding_id, analysis_time, server_id, server_name, database_name, severity, confidence, category, story_path, story_path_hash, story_text, root_fact_key, fact_count) " +
                    "VALUES ($1, $2, $3, $4, 'Sales', 0.8, 0.8, 'queries', $5, $6, 'story', $7, 2)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, "path-" + hash, hash, rootKey);

            InvestigationAlert StoryAlert(string hash, DateTime at) =>
                new(1, ServerId, ServerName, "blocking", "Analysis: queries [" + hash + "]", at, new[] { ServerId + ":" + hash }, hash,
                    20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

            async Task<Guid> ReportedAsync(string hash, string rootKey, DateTime at)
            {
                await FindingAsync(hash, rootKey, at);
                var store = new InvestigationRequestStore(dataSource, 30, () => at);
                var id = (await store.OpenOrJoinAsync(StoryAlert(hash, at), ct)).RequestId!.Value;
                var claim = await store.ClaimAsync(id, "agent-a", ct);
                Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                    id, claim.ClaimId!.Value, "agent-a", 1, "real", "a regressed plan", true, null, "[{\"tool\":\"get_query_store_top\"}]"), ct));
                return id;
            }

            var onTheOpeningRoot = await ReportedAsync("hashA0001", "BAD_ACTOR_q1", Now.AddMinutes(-45));
            var onTheLaterRoot = await ReportedAsync("hashC0003", "BAD_ACTOR_q9", Now.AddMinutes(-40));

            await FindingAsync("hashB0002", "BAD_ACTOR_q1", Now.AddMinutes(-1));
            var second = (await new InvestigationRequestStore(dataSource, 30, () => Now).OpenOrJoinAsync(StoryAlert("hashB0002", Now), ct)).RequestId!.Value;
            await FindingAsync("hashB0002", "BAD_ACTOR_q9", Now.AddMinutes(10));

            var sources = await new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now.AddMinutes(20)).ReadAsync(second, ct);

            var marked = Assert.Single(sources!.EarlierReports, r => r.SameRootCause);
            Assert.Equal(onTheOpeningRoot, marked.RequestId);
            Assert.Contains(sources.EarlierReports, r => r.RequestId == onTheLaterRoot && !r.SameRootCause);
        });
    }

    /// <summary>
    /// #6565: PLAN_REGRESSION is a server-wide root fact (its key names no query), so key and database alone would mark every plan
    /// regression report on the database. Two stories on one database are the same root cause only when their findings share a force
    /// target (database and query id). The request's brief marks the report whose finding shares query 101; a report on queries 201 and 202,
    /// one whose finding has no targets and one with an empty target list are named but not marked. A request whose own finding has no
    /// targets marks nothing, the query-101 report included.
    /// </summary>
    [Fact]
    public async Task PlanRegression_IsMarkedOnlyWhenTheFindingsShareAForceTarget()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            static string Action(params long[] queryIds) => AlertContextSerializer.SerializeAction(new PerformanceMonitor.Analysis.RemediationAction(
                "PLAN_REGRESSION", "force", queryIds.Select(q => new PerformanceMonitor.Analysis.ForcePlanTarget("Sales", q, q + 1000)).ToList()))!;

            async Task FindingAsync(string hash, string? action, DateTime at) =>
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO analysis_findings (finding_id, analysis_time, server_id, server_name, database_name, severity, confidence, category, story_path, story_path_hash, story_text, root_fact_key, fact_count, remediation_action_json) " +
                    "VALUES ($1, $2, $3, $4, 'Sales', 0.8, 0.8, 'queries', $5, $6, 'story', 'PLAN_REGRESSION', 2, $7)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, "path-" + hash, hash, action);

            InvestigationAlert StoryAlert(string hash, DateTime at) =>
                new(1, ServerId, ServerName, "blocking", "Analysis: queries [" + hash + "]", at, new[] { ServerId + ":" + hash }, hash,
                    20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

            async Task<Guid> ReportedAsync(string hash, DateTime at)
            {
                var store = new InvestigationRequestStore(dataSource, 30, () => at);
                var id = (await store.OpenOrJoinAsync(StoryAlert(hash, at), ct)).RequestId!.Value;
                var claim = await store.ClaimAsync(id, "agent-a", ct);
                Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                    id, claim.ClaimId!.Value, "agent-a", 1, "real", "a regressed plan", true, null, "[{\"tool\":\"get_query_store_top\"}]"), ct));
                return id;
            }

            await FindingAsync("hashA0001", Action(101), Now.AddMinutes(-45));
            await FindingAsync("hashC0003", Action(201, 202), Now.AddMinutes(-50));
            await FindingAsync("hashD0004", null, Now.AddMinutes(-40));
            await FindingAsync("hashE0005", Action(), Now.AddMinutes(-35));
            await FindingAsync("hashG0007", null, Now.AddMinutes(-30));
            await FindingAsync("hashB0002", Action(101, 102), Now);

            var shared = await ReportedAsync("hashA0001", Now.AddMinutes(-45));
            var otherQueries = await ReportedAsync("hashC0003", Now.AddMinutes(-50));
            var noTargets = await ReportedAsync("hashD0004", Now.AddMinutes(-40));
            var emptyTargets = await ReportedAsync("hashE0005", Now.AddMinutes(-35));
            var targetless = await ReportedAsync("hashG0007", Now.AddMinutes(-30));

            var reader = new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now);

            /* A request whose own finding has no force targets marks nothing (it was reported, so it is read by id). */
            var none = await reader.ReadAsync(targetless, ct);
            Assert.DoesNotContain(none!.EarlierReports, r => r.SameRootCause);
            Assert.Contains(none.EarlierReports, r => r.RequestId == shared);

            var second = (await new InvestigationRequestStore(dataSource, 30, () => Now).OpenOrJoinAsync(StoryAlert("hashB0002", Now), ct)).RequestId!.Value;
            var sources = await reader.ReadAsync(second, ct);

            var marked = Assert.Single(sources!.EarlierReports, r => r.SameRootCause);
            Assert.Equal(shared, marked.RequestId);
            Assert.Equal(shared, sources.EarlierReports[0].RequestId);
            Assert.Equal(new[] { otherQueries, noTargets, emptyTargets, targetless }.OrderBy(g => g), sources.EarlierReports.Where(r => !r.SameRootCause).Select(r => r.RequestId).OrderBy(g => g));
        });
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation brief reader live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(dataSource, connection, ct);
    }
}
