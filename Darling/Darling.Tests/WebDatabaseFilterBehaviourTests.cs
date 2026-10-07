/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// What the web viewer does with the database filter (#5244, #5245), from the shipped <c>util.js</c>,
/// <c>database-filter-reads.js</c> and <c>pages/server-tabs.js</c> run under Node (<c>web-database-filter-harness.mjs</c>) against a
/// recording fetch: which reads carry the chosen databases and how they are written on the wire, and what the scope chips say.
/// The shipped FILTERED list is empty until the pages that wire a read to the filter land, so the scenarios that need a filtered
/// read run on the real module with a few reads moved into it; the plain census runs the module as shipped. Node is skipped
/// when it is not installed, the way <see cref="WebServerTrendsBehaviourTests"/> does; <see cref="WebDatabaseFilterReadsTests"/>
/// holds the rosters without Node.
/// </summary>
public sealed class WebDatabaseFilterBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-database-filter-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the database-filter harness did not finish in 60 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the database-filter harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Strings(JsonElement node, string name) =>
        node.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    /// <summary>The one request a case sent.</summary>
    private static string One(JsonElement node, string name) => Assert.Single(Strings(node, name));

    /// <summary>The six names a database can have that a careless list would mangle (#5245): a comma, a closing bracket, a leading
    /// space, a quote, a percent with a plus, and markup. Each must reach the wire as ONE key holding exactly that name.</summary>
    private static readonly string[] Awkward = { "A,B", "x]", " SalesDb", "O'Brien", "50%+off", "<img src=x onerror=alert(1)>" };

    /// <summary>An array is written as repeated keys, one encodeURIComponent per name, and as nothing for an empty array; the six
    /// awkward names survive as six single keys. Before this an array was ONE comma-joined value, so "A,B" and the pair A and B
    /// were the same query.</summary>
    [Fact]
    public void BuildQuery_WritesAnArrayAsRepeatedKeys_AndTheSixAwkwardNamesSurviveAsSixSingleKeys()
    {
        var r = Run("query");
        Assert.Equal("?server=SRV1", r.GetProperty("empty").GetString());
        Assert.Equal("?database_name=A%2CB", r.GetProperty("comma").GetString());
        Assert.Equal("?server=SRV1&database_name=SalesDb&database_name=Orders&hours=4", r.GetProperty("pair").GetString());

        var parts = Strings(r, "sixParts");
        Assert.Equal(Awkward.Length, parts.Length);
        Assert.Equal(Awkward, Strings(r, "sixDecoded"));
        Assert.Equal(Awkward, Strings(r, "sixViaUrl"));
        Assert.Equal(
            new[] { "database_name=A%2CB", "database_name=x%5D", "database_name=%20SalesDb", "database_name=O'Brien", "database_name=50%25%2Boff", "database_name=%3Cimg%20src%3Dx%20onerror%3Dalert(1)%3E" },
            parts);
        Assert.DoesNotContain(parts, p => p.Contains(',', System.StringComparison.Ordinal));
        Assert.Equal("?database_name=A%2CB&database_name=x%5D&database_name=%20SalesDb&database_name=O'Brien&database_name=50%25%2Boff&database_name=%3Cimg%20src%3Dx%20onerror%3Dalert(1)%3E", r.GetProperty("six").GetString());

        /* Nothing else about the builder changed: a scalar is skipped when null, undefined or empty, and kept otherwise (0 and false too). */
        Assert.Equal("?a=x%20y&d=0&f=false", r.GetProperty("scalar").GetString());
        Assert.Equal("", r.GetProperty("none").GetString());
    }

    /// <summary>readTool adds the chosen databases only to a FILTERED read of the active server on a server page, and an explicit
    /// name wins. Identity, unfiltered and server-wide reads, another server's reads, any other page, an emptied filter and a filter
    /// with no server are left alone, the caller's params are never changed, and the narrowed retry of a kept-history read and a
    /// custom range both carry the keys too.</summary>
    [Fact]
    public void ReadTool_InjectsTheChosenDatabases_OnlyIntoFilteredReadsOfTheActiveServer_AndAnExplicitNameWins()
    {
        var r = Run("injection");
        const string Read = "/api/read/get_top_queries_by_cpu?server=SRV1&hours=4";
        const string Both = "&database_name=SalesDb&database_name=Orders";

        Assert.Equal(Read + Both, One(r, "filtered"));
        /* A param the panel left unset, or empty, names no database: the filter applies. */
        Assert.Equal(Read + Both, One(r, "unsetName"));
        Assert.Equal(Read + Both, One(r, "emptyName"));
        /* An explicit name, or list, wins: the request carries exactly that and no chosen database. */
        Assert.Equal(Read + "&database_name=Other", One(r, "explicit"));
        Assert.Equal(Read + "&database_name=X&database_name=Y", One(r, "explicitArray"));

        Assert.Equal("/api/read/get_top_queries_by_cpu?server=SRV2&hours=4", One(r, "otherServer"));
        Assert.Equal("/api/read/get_top_queries_by_cpu?hours=4", One(r, "noServer"));
        Assert.Equal("/api/read/get_database_sizes?server=SRV1&hours=4", One(r, "unfilteredRead"));
        Assert.Equal("/api/read/get_cpu_utilization?server=SRV1&hours=4", One(r, "serverWideRead"));
        Assert.Equal("/api/read/get_pg_top_queries?server=SRV1&hours=4", One(r, "pgRead"));

        /* An identity read keeps its own single database_name, and is never given one. */
        Assert.Equal("/api/read/get_query_trend?server=SRV1&query_hash=0x1&database_name=Sales", One(r, "identityRead"));
        Assert.Equal("/api/read/get_query_trend?server=SRV1&query_hash=0x1", One(r, "identityNoName"));
        Assert.Equal("/api/read/get_plan_xml?server=SRV1&query_hash=0x1&database_name=Sales", One(r, "planRead"));

        Assert.Equal(new[] { "server", "hours" }, Strings(r, "callerParamsAfter"));
        Assert.Equal(new[] { "SalesDb", "Orders" }, Strings(r, "filterNames"));

        Assert.Equal(Read + "&as_of=2026-01-02T10%3A30%3A00.000Z" + Both, One(r, "withRange"));
        Assert.Equal(new[] { "/api/read/get_top_queries_by_cpu?server=SRV1&hours=720" + Both, "/api/read/get_top_queries_by_cpu?server=SRV1&hours=168" + Both }, Strings(r, "keptHistory"));

        foreach (var untouched in new[] { "offPage", "cleared", "emptied", "filterForOtherServer" })
        {
            Assert.Equal(Read, One(r, untouched));
        }

        Assert.Equal("/api/read/get_top_queries_by_cpu?hours=4", One(r, "noFilterServer"));
    }

    /// <summary>The chip says, per panel, whether the filter applies, and only while a filter is active on the server page:
    /// the single name or "N databases" for a filtered read, "All databases" in the warning look for an unfiltered one (with the
    /// deadlock sentence on a deadlock read), "Server-wide" muted, "Graphs: all; process rows: N databases" for the Deadlock
    /// Graphs panel, and nothing for an identity read. A panel's own override wins. A name is only ever text.</summary>
    [Fact]
    public void DbScopeChip_ShowsFilteredUnfilteredServerWideAndProcessRows_AndNothingForIdentityOrAnEmptyFilter()
    {
        var r = Run("chips");
        var one = r.GetProperty("one");
        const string Unfiltered = "This panel's read cannot take the database filter, so it shows every database.";
        const string DeadlockSentence = " The databases are inside each deadlock graph.";
        const string ServerTitle = "This data has no database, so the filter does not apply.";
        const string ProcessTitle = "Each graph is whole; process rows outside the chosen databases are hidden.";

        void Chip(JsonElement chip, string state, string text, string title, string look)
        {
            Assert.Equal(state, chip.GetProperty("state").GetString());
            Assert.Equal(text, chip.GetProperty("text").GetString());
            Assert.Equal(title, chip.GetProperty("title").GetString());
            Assert.Equal("badge db-scope db-scope-" + state + look, chip.GetProperty("className").GetString());
            /* The label is text, never markup: the chip holds no child element. */
            Assert.Equal(0, chip.GetProperty("childElements").GetInt32());
        }

        Chip(one.GetProperty("filtered"), "filtered", "SalesDb", "SalesDb", "");
        Chip(one.GetProperty("unfiltered"), "unfiltered", "All databases", Unfiltered, " band-Warning");
        Chip(one.GetProperty("deadlock"), "unfiltered", "All databases", Unfiltered + DeadlockSentence, " band-Warning");
        Chip(one.GetProperty("server"), "server", "Server-wide", ServerTitle, " engine");
        Chip(one.GetProperty("processRows"), "process-rows", "Graphs: all; process rows: 1 database", ProcessTitle, "");

        /* An identity read, a read no class names and no read at all show no chip. */
        foreach (var none in new[] { "identity", "plan", "unknown", "noRead" })
        {
            Assert.Equal(JsonValueKind.Null, one.GetProperty(none).ValueKind);
        }

        /* A panel's override wins over its read's class; an override the chip does not know is ignored. */
        Chip(one.GetProperty("overrideServer"), "server", "Server-wide", ServerTitle, " engine");
        Chip(one.GetProperty("overrideUnfiltered"), "unfiltered", "All databases", Unfiltered, " band-Warning");
        Chip(one.GetProperty("overrideDeadlock"), "unfiltered", "All databases", Unfiltered + DeadlockSentence, " band-Warning");
        Chip(one.GetProperty("unknownOverride"), "unfiltered", "All databases", Unfiltered, " band-Warning");

        /* Several databases: "N databases", with the names one per line in the title (a name may hold a comma). */
        var three = r.GetProperty("three");
        Chip(three.GetProperty("filtered"), "filtered", "3 databases", "SalesDb\nOrders\n<img src=x onerror=alert(1)>", "");
        Chip(three.GetProperty("processRows"), "process-rows", "Graphs: all; process rows: 3 databases", ProcessTitle, "");

        /* The six awkward names, and markup alone, are text: the label and every line of the title are the names as given. */
        Chip(r.GetProperty("markup"), "filtered", "<img src=x onerror=alert(1)>", "<img src=x onerror=alert(1)>", "");
        Chip(r.GetProperty("awkward"), "filtered", "6 databases", string.Join("\n", Awkward), "");

        /* No filter, an emptied filter and any page but a server page show no chip, whatever the read or override. */
        foreach (var name in new[] { "filtered", "unfiltered", "server", "processRows" })
        {
            Assert.Equal(JsonValueKind.Null, r.GetProperty("none").GetProperty(name).ValueKind);
        }

        Assert.Equal(JsonValueKind.Null, r.GetProperty("emptied").ValueKind);
        Assert.Equal(JsonValueKind.Null, r.GetProperty("offPage").ValueKind);
        Assert.Equal(JsonValueKind.Null, r.GetProperty("viewsPage").ValueKind);

        var state = r.GetProperty("state");
        Assert.Equal("filtered", state.GetProperty("filtered").GetString());
        Assert.Equal("unfiltered", state.GetProperty("unfiltered").GetString());
        Assert.Equal("server", state.GetProperty("server").GetString());
        Assert.Equal(JsonValueKind.Null, state.GetProperty("identity").ValueKind);
        Assert.Equal("process-rows", state.GetProperty("processRows").GetString());
    }

    /// <summary>
    /// The runtime census, on the module as shipped: every SQL Server tab is built against a stub fetch with a filter active, and
    /// every <c>/api/read/&lt;tool&gt;</c> it records is in exactly one class. Every FILTERED request carries the chosen databases
    /// (none are filtered yet, so this half has nothing to check until the first read is wired), and no other request carries
    /// them. A read a tab adds without classifying it fails here by name, even when it is built from a name no source scan finds.
    /// </summary>
    [Fact]
    public void TheRuntimeCensus_PutsEveryReadTheTabsMake_InExactlyOneClass_AndInjectsOnlyFilteredReads()
    {
        var r = Run("census");
        Assert.True(r.GetProperty("tabs").GetInt32() >= 10, "the SQL Server tab registry is smaller than expected, so the census read too little");
        Assert.True(Strings(r, "tools").Length > 40, "the tabs made too few distinct reads for the census to mean anything");
        Assert.Empty(Strings(r, "unclassified"));
        Assert.Empty(Strings(r, "inSeveralClasses"));
        Assert.Empty(Strings(r, "filteredMissingKeys"));
        Assert.Empty(Strings(r, "filteredPartialKeys"));
        Assert.Empty(Strings(r, "strayKeys"));
        Assert.Equal(r.GetProperty("filteredRequests").GetInt32(), r.GetProperty("filteredWithKeys").GetInt32() + Strings(r, "filteredExplicit").Length);
    }

    /// <summary>
    /// The same census with every database-scoped read but the three deadlock reads moved into FILTERED, which is where the
    /// plan ends: each of those reads the tabs make carries all the chosen databases, in order, and nothing else does. It shows the
    /// injection reaches the real tab code (no panel reads around readTool) before any page wires a read, and it fails if a
    /// filtered read is sent with fewer or no keys.
    /// </summary>
    [Fact]
    public void TheRuntimeCensus_WithEveryDatabaseScopedReadFiltered_GivesEachOneAllTheKeys_AndNothingElseAny()
    {
        var r = Run("censusSeeded");
        Assert.Empty(Strings(r, "unclassified"));
        Assert.Empty(Strings(r, "inSeveralClasses"));

        var filtered = Strings(r, "filteredReads");
        Assert.True(filtered.Length >= 25, $"only {filtered.Length} filtered reads were made, so the census read too little: {string.Join(", ", filtered)}");
        foreach (var expected in new[] { "get_top_queries_by_cpu", "get_active_queries", "get_blocking", "get_database_sizes", "get_query_store_top" })
        {
            Assert.Contains(expected, filtered);
        }

        Assert.DoesNotContain(filtered, read => read.StartsWith("get_deadlock", System.StringComparison.Ordinal) || read == "get_deadlocks");
        Assert.True(r.GetProperty("filteredRequests").GetInt32() > 0);
        Assert.Equal(r.GetProperty("filteredRequests").GetInt32(), r.GetProperty("filteredWithKeys").GetInt32() + Strings(r, "filteredExplicit").Length);
        Assert.Empty(Strings(r, "filteredMissingKeys"));
        Assert.Empty(Strings(r, "filteredPartialKeys"));
        Assert.Empty(Strings(r, "strayKeys"));
        var scopes = r.GetProperty("scopes");
        Assert.Equal(3, scopes.GetProperty("UNFILTERED").GetInt32());
        /* Six since #5300: the Query Store History panel's read is one query's own rows, keyed by its database and query_id. */
        /* Nine since the Blocking and Deadlocks plan reads (#5236) and the repro script (#5233) joined the plan viewer: each names one stored row. */
        Assert.Equal(9, scopes.GetProperty("IDENTITY").GetInt32());
    }
}
