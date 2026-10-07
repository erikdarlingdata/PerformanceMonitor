/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, L8: the diagnostics bundle runs every string it writes through the statement filter, because the sections it
/// builds (the slow-read log's arguments, the collection health texts, the service log lines) are raw tool and store
/// output that no MCP host filter has swept. A statement the filter names reads as the placeholder, a plan or report
/// document goes through the XML judge, and a JSON document held in a string is walked. Text it keeps is unchanged.
/// </summary>
public sealed class DiagnosticsBundleStatementFilterTests
{
    private static string AssembleOne(string section, JsonNode node)
    {
        var (text, leaks, _) = DiagnosticsBundle.Assemble(
            new[] { new BundleSection(section, node, false) }, new BundleAliaser(), DiagnosticsBundle.BuildManifest(1, "fleet", 0));
        Assert.Empty(leaks);
        Assert.NotNull(text);
        return text!;
    }

    private static void AssertNoSecret(string text)
    {
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ASlowReadArgumentHoldingAStatement_IsWithheld_AndAPlainOneIsKept()
    {
        var node = new JsonObject
        {
            ["status"] = "ok",
            ["reads"] = new JsonArray(new JsonObject
            {
                ["tool"] = "analyze_plan_xml",
                ["arguments"] = new JsonObject
                {
                    ["statement"] = StatementScrubCanary.CanaryStatement,
                    ["other"] = StatementScrubCanary.PlainStatement,
                },
            }),
        };

        var text = AssembleOne("slow_reads", node);

        AssertNoSecret(text);
        Assert.Contains(SensitiveStatements.PlaceholderText, text, StringComparison.Ordinal);
        Assert.Contains("canary_plain_ssf", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AHealthErrorQuotingAStatement_IsWithheld()
    {
        var node = new JsonObject
        {
            ["status"] = "ok",
            ["health_by_server"] = new JsonArray(new JsonObject
            {
                ["last_error"] = "Msg 15118: " + StatementScrubCanary.CanaryStatement + " failed",
            }),
        };

        AssertNoSecret(AssembleOne("collection", node));
    }

    [Fact]
    public void APlanHeldInAString_IsJudgedAsAPlan_ItsOtherStatementsKept()
    {
        var node = new JsonObject { ["status"] = "ok", ["plan"] = StatementScrubCanary.CanaryPlan() };

        var text = AssembleOne("store_log", node);

        AssertNoSecret(text);
        Assert.Contains(SensitiveStatements.PlaceholderText, text, StringComparison.Ordinal);
        Assert.Contains("canary_plain_ssf", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AJsonDocumentHeldInAString_IsWalked()
    {
        var inner = new JsonObject { ["sql"] = StatementScrubCanary.CanaryStatement, ["ok"] = "kept-value-ssf" }.ToJsonString();
        var node = new JsonObject { ["status"] = "ok", ["payload"] = inner };

        var text = AssembleOne("store_log", node);

        AssertNoSecret(text);
        Assert.Contains("kept-value-ssf", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ALogLineThatOnlyLooksLikeJson_IsKeptWhole()
    {
        var node = new JsonObject { ["status"] = "ok", ["line"] = "[12:00:01] collector tick finished in 40 ms" };

        Assert.Contains("collector tick finished in 40 ms", AssembleOne("store_log", node), StringComparison.Ordinal);
    }
}
