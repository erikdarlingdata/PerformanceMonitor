/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Investigations;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5914 (#5632 Phase B slice L) lands its shared link work with Lite's bytes held still: the brief Lite composes for a
/// fixed input, at every <c>mcp.dataDetail</c> level, is pinned by SHA-256 here, written before any production change of the slice. Darling's
/// <c>prior_links</c> and <c>request_url</c> are absent from a Lite brief because Lite's reader supplies neither. A
/// deliberate change to Lite's brief updates these pins in the same PR, with the reason.
/// </summary>
public sealed class LiteInvestigationGoldenTests
{
    private static readonly DateTime At = new(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);

    private static InvestigationBriefSources Sources() => new(
        new InvestigationBriefRequest(Guid.Parse("11111111-2222-3333-4444-555555555555"), 7, "SQL01\\PROD", false, "blocking",
            "claimed", At.AddHours(4), At.AddMinutes(30), 2, "blocking_seconds", "abc123"),
        new[]
        {
            new InvestigationBriefAlert(At.AddMinutes(-20), "blocking_seconds", 95.5, 60, false,
                "Blocking on SalesDb: SELECT * FROM dbo.Orders WHERE id = 42", null,
                new[] { new InvestigationBriefContextItem("Head blocker", new[] { new KeyValuePair<string, string>("Login", "corp\\alice") }) }),
        },
        new[]
        {
            new InvestigationBriefFinding("abc123", "blocking", "Blocking chain on SalesDb", "A scan WHERE id = 42 held the lock.",
                "blocking_seconds", 95.5, 0.8, 0.9, At.AddMinutes(-15), 3,
                new InvestigationBriefRemediation("force_plan", "Check the plan first.",
                    new[] { new InvestigationBriefRemediationTarget("SalesDb", 11, 22, false, new[] { "plan_missing" }, null) }),
                new[] { new InvestigationNextCall("get_blocking", new Dictionary<string, string> { ["hours_back"] = "4" }, "See the chain.") }),
        },
        new[] { new InvestigationEarlierReport(Guid.Parse("99999999-8888-7777-6666-555555555555"), At.AddDays(-2), "agent-a", "real",
            "A report job WHERE id = 42 blocked.", true, "Moved the job.") },
        new[] { new InvestigationRecentChange(At.AddDays(-1), "mute_rule", "Muted blocking for 2 hours", "SELECT%Orders%") },
        new[] { new InvestigationBriefNote("corp\\bob", "corp\\carol", At.AddDays(30), "The nightly ETL blocks at 02:00.") },
        new[] { new InvestigationProfileText("server", "OLTP box, 32 cores, a nightly ETL at 02:00.") });

    private static string Sha(string text) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));

    /// <summary>
    /// #6462: the three hashes below moved with <c>report_contract.required</c> (it now lists summary and drops root_cause_found, and
    /// <c>service_sets</c> names the one the service sets); the keyed pin further down moved with it. Re-pinned in the same PR.
    /// </summary>
    [Theory]
    [InlineData(McpDataDetail.Full, "2A1A81C9E4BC0FC37E717758570F670539ADECA5D6BC2987A9B95689CAD9BBC2")]
    [InlineData(McpDataDetail.MaskLiterals, "D68F95DC98BA4706D8A4C630475B241904C9201B1F4B96BEBC93E16D2FC41F37")]
    [InlineData(McpDataDetail.MetadataOnly, "9C7E884F127787EFA70D37CF08869E3227358D1979F49CF3C97E18D97C9A2944")]
    public void TheLiteBrief_ForAFixedInput_IsByteForByteUnchanged(McpDataDetail mode, string expectedSha)
    {
        /* No key: the brief numbers logins per answer (login#1), as before #6066, so this pin did not move. */
        var brief = InvestigationBriefComposer.Compose(Sources(), LiteInvestigationBriefTools.Tools, mode, At);

        Assert.True(expectedSha == Sha(brief), $"The Lite brief at {mode} moved; its SHA-256 is {Sha(brief)}, length {brief.Length}:\n{brief}");
        Assert.DoesNotContain("prior_links", brief, StringComparison.Ordinal);
        Assert.DoesNotContain("request_url", brief, StringComparison.Ordinal);
    }

    /// <summary>
    /// #6066: the same brief at <c>metadataOnly</c> with the fixed test key (the 32 bytes 0x00 to 0x1f, the same key Darling's
    /// pseudonym tests use). Re-pinned in the same PR as the change, because every login in it is now <c>login#</c> plus 8 hex instead of
    /// <c>login#1</c>. #6462: moved again with <c>report_contract.required</c> (see the pin above).
    /// </summary>
    [Fact]
    public void TheLiteBrief_AtMetadataOnly_WithTheFixedKey_IsByteForByteUnchanged()
    {
        var brief = InvestigationBriefComposer.Compose(Sources(), LiteInvestigationBriefTools.Tools, McpDataDetail.MetadataOnly, At, FixedKey());

        Assert.True("65DC09863AF1BD6A16722B928B3CF90E79C934B09B6349CFA426E5C1FDFBFF9F" == Sha(brief), $"The keyed Lite brief moved; its SHA-256 is {Sha(brief)}, length {brief.Length}:\n{brief}");
        Assert.DoesNotContain("corp", brief, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("alice", brief, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The brief's <c>report_contract.required</c> is exactly the required parameters of Lite's submit tool (found by reflection, not
    /// by reading source text). It once listed root_cause_found, which the tool does not take, and left out summary. The brief's
    /// golden bytes above moved for that, and the pins were updated in the same PR.
    /// </summary>
    [Fact]
    public void ReportContractRequired_IsExactlyTheSubmitToolsRequiredParameters()
    {
        var brief = System.Text.Json.Nodes.JsonNode.Parse(InvestigationBriefComposer.Compose(Sources(), LiteInvestigationBriefTools.Tools, McpDataDetail.Full, At))!;
        var required = brief["report_contract"]!["required"]!.AsArray().Select(n => (string)n!).OrderBy(n => n, StringComparer.Ordinal).ToList();
        var method = typeof(McpInvestigationTools).GetMethod(nameof(McpInvestigationTools.SubmitInvestigationReport))!;
        var fromTool = method.GetParameters()
            .Where(p => !p.IsOptional && p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => p.Name!).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.Equal(fromTool, required);
    }

    /// <summary>An ANOMALY_WRITE_LATENCY story names get_file_io_trend, then get_file_io_stats, ahead of the family calls.</summary>
    [Fact]
    public void AnAnomalyWriteLatencyStory_NamesTheFileIoReadsFirst()
    {
        var s = Sources();
        var finding = s.Findings[0] with { RootFactKey = "ANOMALY_WRITE_LATENCY", NextTools = Array.Empty<InvestigationNextCall>() };
        var withFinding = new InvestigationBriefSources(s.Request, s.Alerts, new[] { finding }, s.EarlierReports, s.RecentChanges, s.Notes, s.Profiles);
        var brief = System.Text.Json.Nodes.JsonNode.Parse(InvestigationBriefComposer.Compose(withFinding, LiteInvestigationBriefTools.Tools, McpDataDetail.Full, At))!;
        var calls = brief["next_calls"]!.AsArray().Select(c => (string)c!["tool"]!).ToList();
        Assert.Equal("get_file_io_trend", calls[0]);
        Assert.Equal("get_file_io_stats", calls[1]);
        Assert.Contains("get_blocked_process_reports", calls);
        Assert.DoesNotContain("get_job_history", calls);
    }

    private static PseudonymKey FixedKey()
    {
        var bytes = new byte[32];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)i;
        return new PseudonymKey(bytes);
    }
}
