/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
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
