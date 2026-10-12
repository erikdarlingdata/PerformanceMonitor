/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.TestHost;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, L8: the read-time census for the derived reads. The persisted finding's drill-down JSON (rows 24 and 25:
/// <c>get_analysis_findings</c>, and the stored findings <c>analyze_server</c> answers from), an alert history row's detail
/// and context (row 26) and a mute rule's query-text pattern (row 27) are planted with the canary statement, read RAW through
/// the tool method (which must still hold the canary: the control), and then sent through the host's registered output
/// filter, which must withhold it and keep the plain statement beside it. Rows 28 to 30 (<c>get_analysis_facts</c>,
/// <c>compare_analysis</c>, <c>get_sweep_reports</c>) carry numbers, fact keys and object names, never statement text, and a source
/// scan pins that.
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>), never the shared
/// <c>live-postgres</c> collection. In the <c>DarlingOutputFormatEnv</c> collection because the host filter list changes with
/// <c>DARLING_OUTPUT_FORMAT</c>.</para>
/// </summary>
[Collection("DarlingOutputFormatEnv")]
public sealed class StatementFilterAnalysisReadsLiveTests : IDisposable
{
    private const int ServerId = -453_301;
    private const string ServerName = "SsfAnalysisReadsSrv";

    public StatementFilterAnalysisReadsLiveTests() =>
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    public void Dispose() => Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    private static async Task AssertFilteredAsync(TestServer server, string label, string raw)
    {
        try
        {
            StatementFilterCensus.AssertRawHoldsTheCanary(raw);
            var (filtered, isError) = await StatementFilterCensus.FilterThroughHostAsync(server, raw);
            Assert.False(isError);
            StatementFilterCensus.AssertFilteredWithholdsTheCanary(raw, filtered);
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException || !ex.Message.StartsWith(label, StringComparison.Ordinal))
        {
            throw new Xunit.Sdk.XunitException(label + ": " + ex.Message);
        }
    }

    [Fact]
    public async Task PersistedFindingsAlertHistoryAndMuteRules_WithholdTheCanaryStatement_ThroughTheRegisteredFilter()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live analysis-reads filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

        /* Row 25 (and the stored findings behind row 24): a persisted finding whose drill-down JSON carries the statements. */
        var drillDown = new System.Text.Json.Nodes.JsonObject
        {
            ["top_queries"] = new System.Text.Json.Nodes.JsonArray(
                new System.Text.Json.Nodes.JsonObject { ["query_text"] = StatementScrubCanary.CanaryStatement, ["executions"] = 5 },
                new System.Text.Json.Nodes.JsonObject { ["query_text"] = StatementScrubCanary.PlainStatement, ["executions"] = 7 }),
        }.ToJsonString();
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
     incident_id, remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, NULL, $20)",
            CollectionIdGenerator.Next(), now, ServerId, ServerName, "SsfDb",
            now.AddHours(-4), now, 0.72, 0.55, "waits",
            "HIGH_CPU → TOP_QUERY", "SSFHASH1", "story", "HIGH_CPU", 92.5, "TOP_QUERY", 4.1, 2, "SSF_INCIDENT_1", drillDown);

        /* Row 26: an alert history row, detail text and context both naming the statements. */
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, 'High CPU', 1, 1, FALSE, 'none', NULL, FALSE, FALSE, $4, $5)",
            DarlingMcpTestData.Naive(now), ServerId, ServerName,
            "Top statement: " + StatementScrubCanary.CanaryStatement,
            "{\"queries\":[{\"query_text\":\"" + StatementScrubCanary.CanaryStatement + "\"}]}");

        /* Row 27: a mute rule whose pattern is the statement text. */
        await new PgMuteRuleStore(postgres).InsertAsync(new MuteRule
        {
            Id = "ssf-mute-1",
            ServerName = ServerName,
            QueryTextPattern = StatementScrubCanary.CanaryStatement,
            Reason = "census " + StatementScrubCanary.PlainStatement,
        });

        using var server = await StatementFilterCensus.BuildHostAsync();

        var findings = await DarlingMcpTools.GetAnalysisFindings(
            new DarlingAnalysisService(postgres), postgres, ServerName, hours_back: 24, include_drilldown: true, as_of: null, cancellationToken: ct);
        await AssertFilteredAsync(server, "get_analysis_findings", findings);

        var history = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 24, 50, null, false, ct);
        await AssertFilteredAsync(server, "get_alert_history", history);

        var rules = await DarlingMcpAlertTools.GetMuteRules(postgres, true, ct);
        await AssertFilteredAsync(server, "get_mute_rules", rules);
    }

    /// <summary>Rows 28 to 30: the three tools answer from fact keys, numbers, object names and sweep check results. A
    /// statement-text column or a drill-down read appearing in one of them fails here until it is judged.</summary>
    [Theory]
    [InlineData("DarlingMcpTools.cs", "GetAnalysisFacts")]
    [InlineData("DarlingMcpTools.cs", "CompareAnalysis")]
    [InlineData("DarlingMcpFleetSweepTools.cs", "GetSweepReports")]
    public void TheFactAndSweepReadsCarryNoStatementTextColumn(string file, string method)
    {
        var path = System.IO.Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", file);
        var source = StatementColumnCensusTests.CodeOnly(System.IO.File.ReadAllText(path));

        var start = source.IndexOf("Task<string> " + method + "(", StringComparison.Ordinal);
        Assert.True(start >= 0, method + " is gone from " + file + "; update this census");
        var next = source.IndexOf("[McpServerTool", start, StringComparison.Ordinal);
        var body = source.Substring(start, (next < 0 ? source.Length : next) - start);

        var hit = Regex.Match(body, @"query_text|sql_text|statement_text|batch_text|text_data|QueryText|StatementText|query_plan|drill_?down",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        Assert.False(hit.Success, method + " now mentions '" + hit.Value + "': judge what it reads, then add a case to this census");
    }

    private static string RepoRoot()
    {
        var dir = new System.IO.DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("repository root not found");
    }
}
