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
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Investigations;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5914 (#5632 Phase B slice L) lands its shared link work with Lite's bytes held still: the brief Lite composes for a
/// fixed input, at every <c>mcp.dataDetail</c> level, and Lite's Level 1 tool texts (each investigation tool's description
/// and parameter descriptions) are pinned by SHA-256 here, written before any production change of the slice. Darling's
/// <c>prior_links</c> and <c>request_url</c> are absent from a Lite brief because Lite's reader supplies neither. A
/// deliberate change to Lite's brief or tool text updates these pins in the same PR, with the reason.
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

    [Theory]
    [InlineData(McpDataDetail.Full, "103E37D369912AE9161B2691C10F7F51BFEBF5FA72E60CE18B4A50BE17ADAD67")]
    [InlineData(McpDataDetail.MaskLiterals, "33838ECB78F26BE7280938D128B0812E8265E2383B0C607EE81C79C34F59E49B")]
    [InlineData(McpDataDetail.MetadataOnly, "5038BF910A6FD6915F5CA1DF43B1EDFBE782BB89F6B8E2FFAD7C6E2907C0049A")]
    public void TheLiteBrief_ForAFixedInput_IsByteForByteUnchanged(McpDataDetail mode, string expectedSha)
    {
        var brief = InvestigationBriefComposer.Compose(Sources(), LiteInvestigationBriefTools.Tools, mode, At);

        Assert.True(expectedSha == Sha(brief), $"The Lite brief at {mode} moved; its SHA-256 is {Sha(brief)}, length {brief.Length}:\n{brief}");
        Assert.DoesNotContain("prior_links", brief, StringComparison.Ordinal);
        Assert.DoesNotContain("request_url", brief, StringComparison.Ordinal);
    }

    [Fact]
    public void LitesLevel1ToolTexts_AreUnchanged()
    {
        var text = new StringBuilder();
        foreach (var method in typeof(McpInvestigationTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
                     .Where(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name is not null)
                     .OrderBy(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name, StringComparer.Ordinal))
        {
            text.Append(method.GetCustomAttribute<McpServerToolAttribute>()!.Name).Append('\n')
                .Append(method.GetCustomAttribute<DescriptionAttribute>()?.Description).Append('\n');
            foreach (var parameter in method.GetParameters())
                text.Append(parameter.Name).Append(": ").Append(parameter.GetCustomAttribute<DescriptionAttribute>()?.Description).Append('\n');
        }

        text.Append(InvestigationFormats.UnsupportedReportFormatMessage(2, "Lite"));
        var all = text.ToString();
        Assert.True(Sha(all) == "061953075B45E959B8FE504B1E03D07FC2E122E02C7A839713FC7E542B4448BA", $"Lite's Level 1 tool texts moved; their SHA-256 is {Sha(all)}:\n{all}");
    }
}
