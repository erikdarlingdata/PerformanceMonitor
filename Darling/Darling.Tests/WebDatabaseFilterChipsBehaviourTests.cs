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
/// The database-scope chips on the web viewer's panels (#5244, #5245), from the shipped <c>panels.js</c>, <c>util.js</c> and
/// <c>pages/server-tabs.js</c> run under Node (<c>web-database-filter-chips-harness.mjs</c>): every SQL Server tab is built with
/// a filter active and the h3 of each panel is read back. A few reads are moved into FILTERED for the run, because the shipped
/// list is empty until the pages that wire a read to the filter land. Node is skipped when it is not installed, the way
/// <see cref="WebDatabaseFilterBehaviourTests"/> does.
/// </summary>
public sealed class WebDatabaseFilterChipsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-database-filter-chips-harness.mjs"));
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
                Assert.Fail("the database-filter chips harness did not finish in 60 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the database-filter chips harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    /// <summary>The six names a database can have that a careless chip would mangle (#5245): a comma, a closing bracket, a leading
    /// space, a quote, a percent with a plus, and markup.</summary>
    private static readonly string[] Awkward = { "A,B", "x]", " SalesDb", "O'Brien", "50%+off", "<img src=x onerror=alert(1)>" };

    private const string UnfilteredTitle = "This panel's read cannot take the database filter, so it shows every database.";
    private const string ServerTitle = "This data has no database, so the filter does not apply.";

    /// <summary>The one heading of that title on a tab.</summary>
    private static JsonElement Heading(JsonElement r, string tab, string title) =>
        r.GetProperty("tabs").GetProperty(tab).EnumerateArray().Single(h => h.GetProperty("title").GetString() == title);

    private static void AssertChip(JsonElement heading, string state, string text, string? title = null)
    {
        Assert.Equal(1, heading.GetProperty("chips").GetInt32());
        Assert.Equal(state, heading.GetProperty("state").GetString());
        Assert.Equal(text, heading.GetProperty("text").GetString());
        if (title != null) Assert.Equal(title, heading.GetProperty("chipTitle").GetString());
    }

    /// <summary>On a tab that mixes the states (Queries has filtered, unfiltered and server-wide panels; Blocking is nearly all
    /// unfiltered with server-wide panels beside them), each panel states its own: a filtered read names the databases, a
    /// database-scoped read that cannot take the filter says "All databases", and a read with no database says "Server-wide". The
    /// fanout overrides win over the shared read's class, and every panel of every tab carries exactly one chip.</summary>
    [Fact]
    public void EveryPanelOnAMixedTab_StatesItsOwnDatabaseScope_AndTheFanoutOverridesWin()
    {
        var r = Run("mixed");

        /* Queries: filtered (seeded), unfiltered, and server-wide, on one tab. */
        AssertChip(Heading(r, "queries", "Top Queries by CPU"), "filtered", "2 databases", "SalesDb\nOrders");
        AssertChip(Heading(r, "queries", "Active Queries"), "filtered", "2 databases");
        /* #5245 PR1: all six top reads take the filter now, so the other two of them name the databases as well. */
        AssertChip(Heading(r, "queries", "Query Store Regressions"), "filtered", "2 databases", "SalesDb\nOrders");
        AssertChip(Heading(r, "queries", "Top Procedures by CPU"), "filtered", "2 databases", "SalesDb\nOrders");

        /* The Query Store fanout: Clutter is database-scoped and says so; the overhead table and the memory clerk are instance-wide
           and override the shared read's class. */
        AssertChip(Heading(r, "queries", "Query Store Clutter"), "unfiltered", "All databases");
        AssertChip(Heading(r, "queries", "Query Store Overhead (per server)"), "server", "Server-wide", ServerTitle);
        AssertChip(Heading(r, "queries", "Query Store Memory Clerk"), "server", "Server-wide", ServerTitle);

        /* Blocking: the Waiting Tasks SERIES (get_current_waits_trend) is server-wide, the table of the same name is a
           database-scoped read; Deadlock Severity is unfiltered, and Deadlock Graphs keeps every graph whole while its process rows follow the filter (#5244). */
        var blocking = r.GetProperty("tabs").GetProperty("blocking").EnumerateArray().Where(h => h.GetProperty("title").GetString() == "Waiting Tasks").ToArray();
        AssertChip(Assert.Single(blocking), "server", "Server-wide", ServerTitle);
        AssertChip(Heading(r, "waits", "Waiting Tasks"), "unfiltered", "All databases", UnfilteredTitle);
        AssertChip(Heading(r, "blocking", "Blocked Sessions"), "unfiltered", "All databases");
        AssertChip(Heading(r, "blocking", "Blocking Severity"), "unfiltered", "All databases");
        AssertChip(Heading(r, "blocking", "Deadlock Severity"), "unfiltered", "All databases", UnfilteredTitle);
        AssertChip(Heading(r, "blocking", "Deadlock Graphs"), "process-rows", "Graphs: all; process rows: 2 databases", "Each graph is whole; process rows outside the chosen databases are hidden.");
        AssertChip(Heading(r, "blocking", "Lock Waits"), "server", "Server-wide");

        /* A composite built by panelShell passes its main read. */
        AssertChip(Heading(r, "waits", "Wait Stats"), "server", "Server-wide");
        AssertChip(Heading(r, "io", "File I/O Latency"), "unfiltered", "All databases");
        AssertChip(Heading(r, "memory", "Memory Pressure Events"), "server", "Server-wide");
        AssertChip(Heading(r, "overview", "Daily Summary"), "server", "Server-wide");
        AssertChip(Heading(r, "config", "Database Scoped Configuration"), "unfiltered", "All databases");
        AssertChip(Heading(r, "recommendations", "Recommendations"), "server", "Server-wide");

        /* No panel on any SQL Server tab is left without a chip, and none draws two. */
        var headings = r.GetProperty("tabs").EnumerateObject().SelectMany(t => t.Value.EnumerateArray()).ToArray();
        Assert.True(headings.Length > 90, "the tabs drew only " + headings.Length + " panels");
        Assert.Empty(headings.Where(h => h.GetProperty("chips").GetInt32() != 1).Select(h => h.GetProperty("title").GetString()));
    }

    /// <summary>With no filter, an emptied one, or off the server page, no panel draws a chip. A read that names a database as the
    /// identity of a row (the query drill and the seven plan-viewer reads) never draws one either, and neither does a panel with no
    /// read of its own.</summary>
    [Fact]
    public void NoChipDrawsWithoutAnActiveFilter_OrForAnIdentityRead()
    {
        var none = Run("none");
        Assert.True(none.GetProperty("noFilterPanels").GetInt32() > 90);
        Assert.Equal(0, none.GetProperty("noFilter").GetInt32());
        Assert.Equal(0, none.GetProperty("emptied").GetInt32());
        Assert.Equal(0, Assert.Single(none.GetProperty("offPage").EnumerateArray()).GetProperty("chips").GetInt32());

        var r = Run("renderPanel");
        AssertChip(r.GetProperty("filtered"), "filtered", "SalesDb", "SalesDb");
        AssertChip(r.GetProperty("unfiltered"), "unfiltered", "All databases", UnfilteredTitle);
        AssertChip(r.GetProperty("server"), "server", "Server-wide", ServerTitle);
        Assert.Equal(0, r.GetProperty("identity").GetProperty("chips").GetInt32());
        Assert.Equal(0, r.GetProperty("plan").GetProperty("chips").GetInt32());
        Assert.Equal(0, r.GetProperty("noRead").GetProperty("chips").GetInt32());

        /* An override wins over the read's class, in renderPanel too. */
        AssertChip(r.GetProperty("overrideServer"), "server", "Server-wide");
        AssertChip(r.GetProperty("overrideUnfiltered"), "unfiltered", "All databases");
        AssertChip(r.GetProperty("overrideProcessRows"), "process-rows", "Graphs: all; process rows: 1 database",
            "Each graph is whole; process rows outside the chosen databases are hidden.");
    }

    /// <summary>A chip title lists the chosen names, one per line, and a name only ever reaches the page as text: the six awkward
    /// names each stand alone as the chip's label and title, never as markup (no element is created for the markup name), and
    /// the six together are "6 databases" with all six on their own lines.</summary>
    [Fact]
    public void AChipTitleListsTheNames_AndTheSixAwkwardNamesAreOnlyEverText()
    {
        var r = Run("awkward");
        var names = r.GetProperty("names").EnumerateArray().ToArray();
        Assert.Equal(Awkward.Length, names.Length);
        for (var i = 0; i < Awkward.Length; i++)
        {
            Assert.Equal(Awkward[i], names[i].GetProperty("name").GetString());
            Assert.Equal("filtered", names[i].GetProperty("state").GetString());
            Assert.Equal(Awkward[i], names[i].GetProperty("text").GetString());
            Assert.Equal(Awkward[i], names[i].GetProperty("chipTitle").GetString());
            Assert.Equal(0, names[i].GetProperty("chipChildren").GetInt32());
            Assert.Equal(0, names[i].GetProperty("imgs").GetInt32());
        }

        var all = r.GetProperty("all");
        Assert.Equal("filtered", all.GetProperty("state").GetString());
        Assert.Equal("6 databases", all.GetProperty("text").GetString());
        Assert.Equal(string.Join("\n", Awkward), all.GetProperty("chipTitle").GetString());
        Assert.Equal(Awkward, all.GetProperty("lines").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal(0, all.GetProperty("imgs").GetInt32());
    }
}
