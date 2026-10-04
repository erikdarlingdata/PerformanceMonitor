/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the FinOps Storage Growth tab: it reads get_finops with view storage_growth at three levels, and each table shows the keys its row function emits.</summary>
public sealed class FinOpsTabStorageGrowthPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "storage-growth.js")
            .ReplaceLineEndings("\n");

    private static string Css() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "app.css")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.StorageGrowth.cs")
            .ReplaceLineEndings("\n");

    /// <summary>The keys of one COLUMNS array, in order.</summary>
    private static System.Collections.Generic.List<string> Keys(string constName)
    {
        var tab = Tab();
        var start = tab.IndexOf("const " + constName + " = [", System.StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = tab.IndexOf("\n];", start, System.StringComparison.Ordinal);
        Assert.True(end > start);
        return Regex.Matches(tab.Substring(start, end - start), "\\bkey: \"([a-z_0-9]+)\"").Select(m => m.Groups[1].Value).ToList();
    }

    /// <summary>One expression-bodied row function's object initializer: from the first "new" after the method name to the given close.</summary>
    private static string RowSlice(string method, string close)
    {
        var source = ToolSource();
        var at = source.IndexOf(" " + method + "(", System.StringComparison.Ordinal);
        Assert.True(at >= 0);
        var start = source.IndexOf("new\n", at, System.StringComparison.Ordinal);
        Assert.True(start > at);
        var end = source.IndexOf(close, start, System.StringComparison.Ordinal);
        Assert.True(end > start);
        return source.Substring(start, end - start);
    }

    /// <summary>Every shown key is emitted by the row; the shown count equals the emitted count less the listed deliberate omissions.</summary>
    private static void AssertKeysMatchRow(string constName, string method, string close, int emittedCount, params string[] omitted)
    {
        var keys = Keys(constName);
        var row = RowSlice(method, close);
        foreach (var key in keys)
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + " = ", row);
        foreach (var key in omitted)
        {
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + "( = |,\r?$)", row);
            Assert.DoesNotContain(key, keys);
        }
        var emitted = Regex.Matches(row, "(?m)^\\s+[a-z_0-9]+( = |,$)").Count;
        Assert.Equal(emittedCount, emitted);
        Assert.Equal(emitted - omitted.Length, keys.Count);
    }

    [Fact]
    public void TheTabReadsGetFinOpsWithTheStorageGrowthViewAtEachLevel()
    {
        var tab = Tab();
        // Pins the databases-level read: view and the 24-hour value the view requires (any other hours is refused).
        Assert.Contains("const params = { server, view: \"storage_growth\", hours: HOURS };", tab);
        Assert.Matches("(?m)^const HOURS = 24;$", tab);
        // Pins the objects-level read: the database plus the desktop's top 20, the most the view allows.
        Assert.Contains("if (state.level === \"objects\") Object.assign(params, { database_name: state.database, limit: OBJECT_LIMIT });", tab);
        Assert.Matches("(?m)^const OBJECT_LIMIT = 20;$", tab);
        // Pins the indexes-level read: no limit, because the view refuses any limit at this level.
        Assert.Contains("if (state.level === \"indexes\") Object.assign(params, { database_name: state.database, object_name: state.object });", tab);
        // Pins the one call site that sends them.
        Assert.Contains("readTool(\"get_finops\", params, ctx && ctx.signal)", tab);
        Assert.Single(Regex.Matches(tab, "readTool\\("));
    }

    [Fact]
    public void EveryDatabaseColumnKeyIsEmittedByTheDatabaseRow() =>
        // has_sibling_row and has_log_service_file are left out on purpose: the desktop grid shows neither, and the note says what they mean.
        AssertKeysMatchRow("DATABASE_COLUMNS", "StorageGrowthDatabaseRow", "\n    };", 11, "has_sibling_row", "has_log_service_file");

    [Fact]
    public void EveryObjectColumnKeyIsEmittedByTheObjectRow() =>
        // object_name and cells are left out on purpose: object_name is schema.table (already two columns), and cells feed the heatmap.
        AssertKeysMatchRow("OBJECT_COLUMNS", "StorageGrowthObjectRows", "\n            });", 11, "object_name", "cells");

    [Fact]
    public void EveryIndexColumnKeyIsEmittedByTheIndexRow() =>
        // database_name, schema_name, table_name and index_id are left out on purpose: the drill path already names the first three, and the desktop grid has no id column.
        AssertKeysMatchRow("INDEX_COLUMNS", "StorageGrowthIndexRow", "\n    };", 15, "database_name", "schema_name", "table_name", "index_id");

    [Fact]
    public void TheColumnsAreInTheDesktopGridOrder()
    {
        // Databases grid: FinOpsTab.xaml FinOpsStorageGrowthDataGrid (~:541-558), Note last.
        Assert.Equal("database_name,current_size_mb,size_7d_ago_mb,size_30d_ago_mb,growth_7d_mb,growth_30d_mb,daily_growth_rate_mb,growth_pct_30d,note", string.Join(",", Keys("DATABASE_COLUMNS")));
        // Objects grid: FinOpsTab.xaml FinOpsObjectGrowthDetailGrid (~:571-583).
        Assert.Equal("schema_name,table_name,reserved_mb,used_mb,total_rows,index_count,growth_mb,growth_pct,daily_growth_rate_mb", string.Join(",", Keys("OBJECT_COLUMNS")));
        // Indexes grid: FinOpsTab.xaml FinOpsObjectIndexDetailGrid (~:590-601).
        Assert.Equal("index_name,classification,index_type_desc,reserved_mb,total_rows,user_seeks,user_scans,user_lookups,total_reads,user_updates,last_user_access_server_local", string.Join(",", Keys("INDEX_COLUMNS")));
    }

    [Fact]
    public void TheServerLocalLastAccessIsPlainTextNeverTheTimeFormatter()
    {
        var tab = Tab();
        Assert.Contains("{ key: \"last_user_access_server_local\", label: \"Last access (server time)\" },", tab);
        Assert.DoesNotContain("last_user_access_server_local\", label: \"Last access (server time)\", format", tab);
    }

    [Fact]
    public void TheDayAxisIsLabelledUtcAndShowsTheFirstTenCharacters()
    {
        var tab = Tab();
        Assert.Contains("el(\"caption\", { text: \"Day (UTC)\" })", tab);
        Assert.Contains("String(d).slice(0, 10)", tab);
    }

    [Fact]
    public void TheNoticesAreExact()
    {
        var tab = Tab();
        Assert.Contains("let text = n === 1 ? \"1 database\" : n + \" databases\";", tab);
        Assert.Contains("text += s.truncated ? \"; the top \" + n + \" of \" + (s.database_count ?? \"more\") + \" by 30-day growth.\" : \".\";", tab);
        Assert.Contains("let text = \"Top \" + (s.rows || []).length + \" objects by growth over the last \" + (s.window_days ?? \"?\") + \" days\";", tab);
        Assert.Contains("text += s.truncated ? \"; more exist.\" : \".\";", tab);
        Assert.Contains("let text = n === 1 ? \"1 index\" : n + \" indexes\";", tab);
        Assert.Contains("text += s.truncated ? \"; the first \" + n + \" of \" + (s.index_count ?? \"more\") + \".\" : \".\";", tab);
        // The indexes level's one snapshot stamp rides the notice line, worded by the shared helpers; the browser computes nothing.
        Assert.Contains("if (typeof s.captured_at === \"string\" && s.captured_at) text += \" Collected \" + relTime(s.captured_at) + \" (\" + localTime(s.captured_at) + \").\";", tab);
        Assert.Contains("Shade: the service's size band for each cell (log scale over this table); blank means no sample or zero.", tab);
    }

    [Fact]
    public void TheHeatmapClassComesOnlyFromTheServiceBandAndTheBrowserComputesNothing()
    {
        var tab = Tab();
        // The class is built from the band alone; nothing else in the tab mentions the class family.
        Assert.Contains("const known = Number.isInteger(shade) && shade >= 0 && shade <= 7;", tab);
        Assert.Contains("class: known ? \"num heat-band-\" + shade : \"num\",", tab);
        Assert.Single(Regex.Matches(tab, "heat-band"));
        Assert.DoesNotContain("Math.", tab);
        Assert.Contains("title: r.schema_name + \".\" + r.table_name + \" | \" + String(d).slice(0, 10) + \" | \" + applyFormat(\"num1\", mb) + \" MB reserved\",", tab);
    }

    [Fact]
    public void TheCssHasExactlyEightBandRulesZeroThroughSeven()
    {
        var found = Regex.Matches(Css(), "(?m)^\\.heat-band-([0-9]+) \\{").Select(m => m.Groups[1].Value).ToList();
        Assert.Equal("0,1,2,3,4,5,6,7", string.Join(",", found));
        Assert.Equal(8, Regex.Matches(Css(), "heat-band-").Count);
    }

    [Fact]
    public void TheDrillBackAndBreadcrumbArePresent()
    {
        var tab = Tab();
        Assert.Contains("const state = drillFor(server);", tab);
        Assert.Contains("drillColumn(\"Show objects\", openObjects)", tab);
        Assert.Contains("drillColumn(\"Show indexes\", openIndexes)", tab);
        Assert.Contains("el(\"button\", { type: \"button\", class: \"btn\", text: \"Back\", onClick: back })", tab);
        Assert.Contains("parts.join(\" › \")", tab);
        Assert.Contains("const parts = [\"Storage Growth\"];", tab);
        // A refusal or error on a drill keeps Back: the error strip goes through the same chrome.
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"error\"\\) return chrome\\(\\[readErrorStrip\\(res\\.message\\)\\]\\);$", tab);
    }

    [Fact]
    public void TheDrillLivesAtModuleScopeSoTheRepaintKeepsIt()
    {
        var tab = Tab();
        Assert.Matches("(?m)^const drills = new Map\\(\\);$", tab);
        Assert.Contains("drills.get(server)", tab);
        var build = tab.Substring(tab.IndexOf("build(server, ctx)", System.StringComparison.Ordinal));
        Assert.DoesNotContain("level: \"databases\"", build);
        Assert.DoesNotContain("new Map(", build);
    }

    [Fact]
    public void ARestoredIndexesReadForAGoneObjectDropsBackToObjects()
    {
        var tab = Tab();
        Assert.Contains("const OBJECT_GONE = \"is not among the\";", tab);
        // The tab's drop-back depends on this refusal wording in the C# message.
        Assert.Contains("is not among the", ToolSource());
        Assert.Contains("if (res.kind === \"error\" && state.level === \"indexes\" && String(res.message).includes(OBJECT_GONE)) {\n          state.level = \"objects\";", tab.Replace("\r\n", "\n"));
    }

    [Fact]
    public void LastAccessShowsMinutesAndTheDarkTextSitsOnTheStrongBands()
    {
        Assert.Contains("String(v).slice(0, 16).replace(\"T\", \" \")", Tab());
        var css = Css();
        foreach (var b in new[] { 5, 6, 7 }) Assert.Matches("(?m)^\\.heat-band-" + b + " \\{ color: var\\(--bg\\);", css);
        foreach (var b in new[] { 0, 1, 2, 3, 4 }) Assert.DoesNotMatch("(?m)^\\.heat-band-" + b + " \\{ color:", css);
    }

    [Fact]
    public void EachSectionReadsItsOwnStatusAndMapsTheThreeStates()
    {
        var tab = Tab();
        Assert.Contains("if (s.status === \"ok\") return [noticeStrip(notice(s)), VIZ.table(", tab);
        Assert.Contains("if (s.status === \"empty\") return [emptyStrip(emptyText)];", tab);
        Assert.Contains("return [noticeStrip(s.message ?? \"This section was not collected.\")];", tab);
    }

    [Fact]
    public void TheReadStatesAreHandled()
    {
        var tab = Tab();
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"aborted\" \\|\\| res\\.kind === \"auth\"\\) return;$", tab);
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"empty\"\\) return chrome\\(\\[emptyStrip\\(res\\.message\\)\\]\\);$", tab);
        Assert.Contains("Could not render this tab: ", tab);
    }

    [Fact]
    public void TheTabImportsOnlyWhatItUses()
    {
        var tab = Tab();
        var imports = Regex.Matches(tab, "from \"([^\"]+)\";").Select(m => m.Groups[1].Value).ToList();
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../util.js" }));
        var names = Regex.Match(tab, "import \\{([^}]*)\\} from \"../../util.js\"").Groups[1].Value.Split(',').Select(n => n.Trim());
        var body = tab.Substring(tab.IndexOf("const HOURS", System.StringComparison.Ordinal));
        Assert.All(names, n => Assert.Contains(n + "(", body));
    }

    [Fact]
    public void TheStubTextIsGone()
    {
        Assert.DoesNotContain("Not on the web yet", Tab());
    }
}
