/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the Darling performance monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6640: what an agent reads. The card's investigation text names the queries, and <c>get_analysis_facts</c> carries
/// <c>object_name</c> for the two query facts and for no other fact.
/// </summary>
public sealed class FactsNameQueriesAdviceTests
{
    private static Fact Spills() => new()
    {
        Source = "queries", Key = "QUERY_SPILLS", Value = 1100, Severity = 1,
        ObjectName = "Db/0xSB",
        Metadata = new Dictionary<string, double> { ["total_spills"] = 1100, ["spilling_query_count"] = 4 },
        Ranked = [new("Db/0xSB", "Db", 500), new("Db/0xSA", "Db", 300)],
    };

    [Fact]
    public void QuerySpillsAdvice_NamesTheSpillingQueries()
    {
        var advice = FactAdvice.Compose("QUERY_SPILLS", new Dictionary<string, Fact> { ["QUERY_SPILLS"] = Spills() })!;
        Assert.Contains("Most spilled: Db/0xSB (500 pages (3.91 MB)), Db/0xSA (300 pages (2.34 MB)).", advice.Investigation);
    }

    [Fact]
    public void ParameterSensitivityAdvice_NamesThePlans()
    {
        var fact = new Fact
        {
            Source = "queries", Key = "PARAMETER_SENSITIVITY", Value = 200, Severity = 1,
            ObjectName = "Db/0xQB/0xPB",
            Metadata = new Dictionary<string, double> { ["worst_ratio"] = 200, ["offender_count"] = 2 },
            Ranked = [new("Db/0xQB/0xPB", "Db", 200), new("Db/0xQA/0xPA", "Db", 50)],
        };
        var advice = FactAdvice.Compose("PARAMETER_SENSITIVITY", new Dictionary<string, Fact> { ["PARAMETER_SENSITIVITY"] = fact })!;
        Assert.Contains("Db/0xQB/0xPB (200×), Db/0xQA/0xPA (50×).", advice.Investigation);
    }

    [Fact]
    public void FactPayload_AttachSubjectThenAttach_KeepsRankedLast_AndTheSubjectOnTheWire()
    {
        /* The tools' wiring: Attach(AttachSubject(card, f), f.Ranked). A swapped nesting would drop one block. */
        var fact = Spills();
        var wired = FactRanked.Attach(FactRanked.AttachSubject(new { key = "QUERY_SPILLS", value = 1100 }, fact, McpHelpers.JsonOptions), fact.Ranked, McpHelpers.JsonOptions);
        var json = (JsonObject)wired;
        Assert.Equal("Db/0xSB", (string?)json["object_name"]);
        Assert.Equal(2, json["ranked"]!.AsArray().Count);
        Assert.Equal("ranked", json.Last().Key);
    }

    [Fact]
    public void FactPayload_CarriesObjectName_ForTheTwoQueryFactsOnly()
    {
        var card = new { key = "X", value = 1 };

        var named = (JsonObject)FactRanked.AttachSubject(card, Spills(), McpHelpers.JsonOptions);
        Assert.Equal("Db/0xSB", (string?)named["object_name"]);

        var single = new Fact { Key = "QUERY_SPILLS", ObjectName = "Db/0xSB" };
        Assert.Equal("Db/0xSB", (string?)((JsonObject)FactRanked.AttachSubject(card, single, McpHelpers.JsonOptions))["object_name"]);

        /* Byte-identity: another fact, or a query fact with no name, comes back as the same reference. */
        Assert.Same(card, FactRanked.AttachSubject(card, new Fact { Key = "RUNNING_JOBS", ObjectName = "job" }, McpHelpers.JsonOptions));
        Assert.Same(card, FactRanked.AttachSubject(card, new Fact { Key = "QUERY_SPILLS" }, McpHelpers.JsonOptions));
        Assert.Equal(JsonSerializer.Serialize(card, McpHelpers.JsonOptions),
            JsonSerializer.Serialize(FactRanked.AttachSubject(card, new Fact { Key = "BAD_ACTOR", ObjectName = "q" }, McpHelpers.JsonOptions), McpHelpers.JsonOptions));
    }
}
