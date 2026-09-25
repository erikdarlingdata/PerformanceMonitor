/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198: get_analysis_findings' own response-budget pin. A busy production store's default call (24h,
/// default arguments) measured 66,838 bytes, over the shared 32 KB <see cref="McpResponseBudget.DefaultBytes"/>.
/// Unlike get_deadlock_detail's one super-wide field, this tool's bytes are spread across MANY findings, each
/// repeating confidence_basis (near-fixed methodology prose), advice (headline/investigation/remediation,
/// frozen into story_text at analysis time) and, on remediable findings, the FULL copy-paste
/// <c>remediation_command</c> T-SQL. Plants 30 distinct chains (so none collapse together) with
/// realistic-length prose and a mix of remediation shapes (server-config, missing-index, none) to reproduce
/// that shape, and asserts the default call (limit 18) stays under budget with the three fields previewed,
/// while <c>limit: 30, full_text: true</c> returns every chain with all three untruncated. New file (not a
/// shared seeding helper) because #4198 ran a dozen lanes against this store tonight.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpAnalysisFindingsBudgetLiveTests
{
    private const string ServerName = "darling-mcp-analysis-findings-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private readonly ITestOutputHelper _output;

    public DarlingMcpAnalysisFindingsBudgetLiveTests(ITestOutputHelper output) => _output = output;

    private const string Investigation =
        "Checked wait_stats, query_stats and the current plan cache for corroborating evidence: the fired " +
        "symptom alone is a weak signal, but this chain matched additional amplifier checks (blocking chains, " +
        "memory grant pressure and recent plan changes on the same object) within the same analysis window, " +
        "which is what raises the confidence score above the lone-symptom floor. Compare against the server's " +
        "own baseline before treating the raw number as universal.";

    private const string Remediation =
        "Start with the least invasive change and re-measure before going further: confirm the finding is " +
        "still current, check for an obvious root cause (a deployment, an index change, a parameter-sensitive " +
        "plan), and only then apply the suggested fix below. Re-run analyze_server after the change to confirm " +
        "the story cleared rather than assuming it did.";

    [Fact]
    public async Task GetAnalysisFindings_Default_StaysUnderResponseBudget_WithThirtyDistinctChains()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live analysis-findings budget test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var baseTime = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-1);

            for (var i = 0; i < 30; i++)
            {
                var t = baseTime.AddMinutes(i);
                string? remediationJson = (i % 3) switch
                {
                    0 => AlertContextSerializer.SerializeAction(new RemediationAction(
                        FactKey: "DB_CONFIG",
                        Action: "set",
                        Targets: [],
                        ServerConfigTargets: [new ServerConfigTarget(ServerConfigSetting.Maxdop, 0, 8)])),
                    1 => AlertContextSerializer.SerializeAction(new RemediationAction(
                        FactKey: "MISSING_INDEX",
                        Action: "set",
                        Targets: [],
                        MissingIndexTargets: [new MissingIndexTarget(
                            "dbo.Posts",
                            83.2,
                            $"CREATE NONCLUSTERED INDEX [ix_Posts_OwnerUserId_{i}] ON [dbo].[Posts] ([OwnerUserId], [PostTypeId]) " +
                            "INCLUDE ([Score], [ViewCount], [CreationDate], [LastActivityDate], [Title]) WITH (ONLINE = ON, SORT_IN_TEMPDB = ON);")])),
                    _ => null,
                };

                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
     incident_id, remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, NULL)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "StackOverflow",
                    t.AddHours(-4), t, 0.72, 0.55, "waits",
                    $"HIGH_CPU → PLAN_REGRESSION_{i}", $"AF4198_HASH_{i}",
                    FactAdvice.SerializeForStoryText(new AdviceBlock($"High CPU with a regressed plan, chain {i}", Investigation, Remediation)),
                    "HIGH_CPU", 92.5, "PLAN_REGRESSION", 4.1, 2,
                    $"AF4198_INCIDENT_{i}", remediationJson);
            }

            var analysisService = new DarlingAnalysisService(postgres);

            var defaultJson = await DarlingMcpTools.GetAnalysisFindings(analysisService, postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(defaultJson, ServerName, "findings");
            JsonAssert.Contains("\"finding_count\":18", defaultJson);
            JsonAssert.Contains("\"total_finding_count\":30", defaultJson);
            JsonAssert.Contains("\"findings_truncated\":true", defaultJson);

            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);

            using var parsed = JsonDocument.Parse(defaultJson);
            var findings = parsed.RootElement.GetProperty("findings");
            long remediationCommandBytes = 0;
            long adviceBytes = 0;
            foreach (var f in findings.EnumerateArray())
            {
                if (f.TryGetProperty("remediation_command", out var rc) && rc.ValueKind == JsonValueKind.String)
                    remediationCommandBytes += Encoding.UTF8.GetByteCount(rc.GetString()!);
                if (f.TryGetProperty("advice", out var adv) && adv.ValueKind == JsonValueKind.Object)
                {
                    adviceBytes += Encoding.UTF8.GetByteCount(adv.GetProperty("investigation").GetString() ?? "");
                    adviceBytes += Encoding.UTF8.GetByteCount(adv.GetProperty("remediation").GetString() ?? "");
                }
            }

            _output.WriteLine($"get_analysis_findings default call: {defaultBytes:N0} bytes (budget {McpResponseBudget.DefaultBytes:N0}) over 30 planted chains, 18 returned.");
            _output.WriteLine($"  remediation_command bytes across returned findings: {remediationCommandBytes:N0}");
            _output.WriteLine($"  advice.investigation+remediation bytes across returned findings: {adviceBytes:N0}");

            Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
                $"get_analysis_findings' default call is {defaultBytes:N0} bytes over 30 planted chains, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            Assert.All(findings.EnumerateArray(), f =>
            {
                Assert.True(f.GetProperty("confidence_basis_truncated").GetBoolean());
                Assert.True(f.GetProperty("confidence_basis").GetString()!.Length < 200);
            });

            /* remediation_command is NEVER previewed, even at default — a destructive change's two-sided
               risk disclosure cut in half is worse than not shown (#4198's own review catch on this lane).
               The default page (limit 18) still carries at least one whole CREATE INDEX command. */
            Assert.Contains(findings.EnumerateArray(), f =>
                f.TryGetProperty("remediation_command", out var rc) && rc.ValueKind == JsonValueKind.String
                && rc.GetString()!.Contains("CREATE NONCLUSTERED INDEX", StringComparison.Ordinal));

            /* full_text opts every finding's confidence_basis and advice back to the untruncated text — and
               a bigger limit opts back into all 30 chains. */
            var fullJson = await DarlingMcpTools.GetAnalysisFindings(analysisService, postgres, ServerName, limit: 30, full_text: true);
            JsonAssert.Contains("\"finding_count\":30", fullJson);
            JsonAssert.Contains("\"findings_truncated\":false", fullJson);
            using var fullParsed = JsonDocument.Parse(fullJson);
            Assert.All(fullParsed.RootElement.GetProperty("findings").EnumerateArray(), f =>
            {
                Assert.False(f.GetProperty("confidence_basis_truncated").GetBoolean());
                Assert.False(f.GetProperty("advice_truncated").GetBoolean());
                Assert.Equal(Investigation, f.GetProperty("advice").GetProperty("investigation").GetString());
            });


            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
