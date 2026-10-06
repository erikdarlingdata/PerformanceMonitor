/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the FinOps Index Analysis tab: it reads get_finops with view index_analysis and shows only keys that read emits.</summary>
public sealed class FinOpsTabIndexAnalysisPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "index-analysis.js")
            .ReplaceLineEndings("\n");

    // The Database box's draft, caret, focus and commit handling lives in the shared module (#5231).
    private static string Box() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "database-box.js")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.IndexAnalysis.cs")
            .ReplaceLineEndings("\n");

    private static string ToolsMain() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.cs")
            .ReplaceLineEndings("\n");

    private static string Slice(string startMarker, string endMarker)
    {
        var source = ToolSource();
        var start = source.IndexOf(startMarker, System.StringComparison.Ordinal);
        Assert.True(start >= 0, startMarker);
        var end = source.IndexOf(endMarker, start, System.StringComparison.Ordinal);
        Assert.True(end > start, endMarker);
        return source.Substring(start, end - start);
    }

    private static string DatabaseRowSlice() =>
        Slice("        return new\n        {\n            database_name = r.DatabaseName,", "\n        };");

    private static string RecommendationSlice() => Slice(" IndexAnalysisRecommendationRow(", "\n        };");

    private static string EnvelopeSlice() => Slice("return JsonSerializer.Serialize(new", "}, McpHelpers.JsonOptions);");

    /// <summary>The keys of one column array, in order.</summary>
    private static System.Collections.Generic.List<string> Keys(string constName)
    {
        var tab = Tab();
        var start = tab.IndexOf("const " + constName + " = [", System.StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = tab.IndexOf("\n];", start, System.StringComparison.Ordinal);
        Assert.True(end > start);
        return Regex.Matches(tab.Substring(start, end - start), "\\bkey: \"([a-z_0-9]+)\"").Select(m => m.Groups[1].Value).ToList();
    }

    private static System.Collections.Generic.List<string> DerivedKeys()
    {
        var m = Regex.Match(Tab(), "(?m)^const DERIVED_KEYS = \\[(.*)\\];$");
        Assert.True(m.Success);
        return Regex.Matches(m.Groups[1].Value, "\"([a-z_0-9]+)\"").Select(x => x.Groups[1].Value).ToList();
    }

    private static string KeyPattern(string key) => "(?m)^\\s+" + Regex.Escape(key) + "( = |,$)";

    [Fact]
    public void TheTabReadsIndexAnalysisWithTheToolMaxLimit()
    {
        var tab = Tab();
        Assert.Contains("const params = { server, view: \"index_analysis\", limit: LIMIT };", tab);
        Assert.Contains("readTool(\"get_finops\", params, ctx && ctx.signal)", tab);
        var limit = Regex.Match(tab, "(?m)^const LIMIT = (\\d+);$");
        Assert.True(limit.Success);
        // The most the view takes for this view (#5238): the 500 of MaxIndexAnalysisRecommendations, which MaxLimitFor hands to index_analysis,
        // not the 50 every other top-N view stops at.
        var max = Regex.Match(ToolSource(), "internal const int MaxIndexAnalysisRecommendations = (\\d+);");
        Assert.True(max.Success);
        Assert.Equal(500, int.Parse(max.Groups[1].Value));
        Assert.Equal(int.Parse(max.Groups[1].Value), int.Parse(limit.Groups[1].Value));
        Assert.Contains("IndexAnalysisView => MaxIndexAnalysisRecommendations,", ToolsMain());
        Assert.Contains("var maxLimit = MaxLimitFor(normalized);", ToolsMain());
        // The first, unfiltered read sends neither optional parameter: they are added to params only from a choice.
        var call = tab.Substring(tab.IndexOf("const params = ", System.StringComparison.Ordinal), 200);
        call = call.Substring(0, call.IndexOf(';'));
        Assert.DoesNotContain("hours:", call);
        Assert.DoesNotContain("database_name:", call);
        Assert.DoesNotContain("full_text:", call);
        Assert.DoesNotContain("hours:", tab);
        Assert.DoesNotContain("full_text:", tab);
    }

    [Fact]
    public void TheSuggestionListComesOnlyFromAnUnfilteredRead()
    {
        var tab = Tab();
        Assert.Matches(@"if\s*\(\s*!choice\.db\s*\)\s*\{\s*choice\.names\s*=\s*\(data\.databases\s*\|\|\s*\[\]\)", tab);
        Assert.Single(Regex.Matches(tab, @"choice\.names\s*="));
    }

    [Fact]
    public void TheDatabaseFilterReReadsWithDatabaseName()
    {
        var tab = Tab();
        Assert.Contains("if (choice.db) params.database_name = choice.db;", tab);
        Assert.DoesNotContain("database_name: choice", tab);
        Assert.Contains("databaseBox(choice, { onCommit: () => reread() })", tab);
        Assert.Contains("choice.names = (data.databases || []).map((d) => d.database_name)", tab);
        var box = Box();
        Assert.Contains("type: \"text\", list: listId", box);
        Assert.Contains("el(\"datalist\", { id: listId })", box);
        Assert.Contains("el(\"span\", { text: \"Database\" })", box);
        Assert.Contains("input.addEventListener(\"change\"", box);
        // One name, exactly as typed (no trim, #5244 M3), in the stored spelling when it matches a suggestion ignoring case (#5244 L8c).
        Assert.Contains("choice.db = storedSpelling(input.value, known());", box);
        Assert.DoesNotContain(".trim();", box.Replace("text.trim() === \"\"", "").Replace("String(typed).trim() === \"\"", ""));
    }

    [Fact]
    public void TheFullTextToggleReReadsWithFullText()
    {
        var tab = Tab();
        Assert.Contains("if (choice.full) params.full_text = true;", tab);
        Assert.Contains("type: \"checkbox\"", tab);
        Assert.Contains("Full script and definition text", tab);
        Assert.Contains("fullBox.addEventListener(\"change\"", tab);
        Assert.Contains("choice.full = fullBox.checked;", tab);
    }

    [Fact]
    public void TheFilterAndToggleSurviveAPoll()
    {
        var tab = Tab();
        Assert.Matches("(?m)^const choices = new Map\\(\\);$", tab);
        var build = tab.Substring(tab.IndexOf("  build(server, ctx) {", System.StringComparison.Ordinal));
        Assert.StartsWith("  build(server, ctx) {\n    const choice = choiceFor(server);", build);
        Assert.DoesNotContain("const choices", build);
        Assert.Contains("choices.get(server)", tab);
    }

    [Fact]
    public void ALateReadDoesNotOverwriteANewerOne()
    {
        var tab = Tab();
        Assert.Contains("const mine = ++generation;", tab);
        Assert.Matches("(?m)^\\s+const res = await readTool\\(.*\\n\\s+if \\(mine !== generation\\) return;$", tab);
    }

    [Fact]
    public void TheControlsAreNotRemountedByARead()
    {
        var tab = Tab();
        Assert.Contains("return el(\"div\", {}, [controls, content]);", tab);
        Assert.DoesNotContain("mount(controls", tab);
        Assert.DoesNotContain("mount(body", tab);
        Assert.Contains("mount(content, loadingStrip());", tab);
    }

    [Fact]
    public void TheCollectedColumnIsLast_AndTheNoticeNamesEachDatabasesSnapshot()
    {
        var tab = Tab();
        Assert.Contains("{ key: \"captured_at\", label: \"Collected\", format: \"time\" },\n];", tab);
        Assert.Contains("Analyzed from each database's newest collected snapshot.", tab);
        Assert.DoesNotContain("latest collected snapshot", tab);
        /* The overall row is built with no captured_at, so its cell stays blank. */
        Assert.DoesNotContain("captured_at", DatabaseRowSliceOverall());
    }

    private static string DatabaseRowSliceOverall() =>
        Slice("if (!withWorkload)", "        return new");

    [Fact]
    public void AFilteredNoticeNamesTheDatabase()
    {
        Assert.Contains("\" Database \" + db + \".\"", Tab());
    }

    [Fact]
    public void EveryRollupColumnKeyIsEmittedByTheDatabaseRow()
    {
        var derived = DerivedKeys();
        var keys = Keys("ROLLUP_COLUMNS");
        var row = DatabaseRowSlice();
        foreach (var key in keys.Where(k => !derived.Contains(k)))
            Assert.Matches(KeyPattern(key), row);
        Assert.Equal(24, Regex.Matches(row, "(?m)^\\s+[a-z_]+ = ").Count);
        Assert.Equal(21, keys.Count);
    }

    [Fact]
    public void EveryRecommendationColumnKeyIsEmitted()
    {
        var derived = DerivedKeys();
        var keys = Keys("RECOMMENDATION_COLUMNS");
        var row = RecommendationSlice();
        foreach (var key in keys.Where(k => !derived.Contains(k)))
            Assert.Matches(KeyPattern(key), row);
        // The derived size text is built from this key, so the key itself is pinned too.
        Assert.Matches(KeyPattern("index_size_gb"), row);
        Assert.Matches(KeyPattern("total_reads"), DatabaseRowSlice());
        Assert.Equal(23, Regex.Matches(row, "(?m)^\\s+[a-z_]+( = |,$)").Count);
        Assert.Equal(17, keys.Count);
    }

    [Fact]
    public void TheColumnsAreInTheDesktopGridOrder()
    {
        Assert.Equal(
            "database_name,tables_analyzed,index_count,total_size_gb,total_rows,indexes_to_disable,indexes_to_merge,compressable_indexes,"
            + "unused_indexes,unused_size_gb,compression_min_savings_gb,compression_max_savings_gb,total_min_savings_gb,total_max_savings_gb,"
            + "reads_breakdown,writes,lock_wait_count,avg_lock_wait_ms,latch_wait_count,avg_latch_wait_ms,captured_at",
            string.Join(",", Keys("ROLLUP_COLUMNS")));
        Assert.Equal(
            "action,result_kind,consolidation_rule,database_name,schema_name,table_name,index_name,index_size_gb_text,index_rows,"
            + "index_reads,index_writes,target_index_name,superseded_by,additional_info,original_index_definition,script,captured_at",
            string.Join(",", Keys("RECOMMENDATION_COLUMNS")));
    }

    [Theory]
    [InlineData("overall")]
    [InlineData("databases")]
    [InlineData("notes")]
    [InlineData("recommendations")]
    [InlineData("recommendation_count")]
    [InlineData("truncated")]
    [InlineData("database_count")]
    [InlineData("databases_truncated")]
    [InlineData("uptime_warning")]
    [InlineData("dedupe_only_applied")]
    [InlineData("overall_workload_reason")]
    public void TheEnvelopeKeysTheTabReadsAreEmitted(string key)
    {
        Assert.Matches(KeyPattern(key), EnvelopeSlice());
        Assert.Matches(@"data\." + key + @"\b", Tab());
    }

    [Fact]
    public void TheTypedDatabaseTextSurvivesAPoll()
    {
        var box = Box();
        Assert.Matches(@"addEventListener\(""input"", \(\) => \{\s*choice\.draft = input\.value;", box);
        Assert.Contains("choice.caret = [input.selectionStart, input.selectionEnd];", box);
        Assert.Contains("input.value = choice.draft ?? choice.db;", box);
        Assert.Matches(@"addEventListener\(""change"", \(\) => \{\s*choice\.db = storedSpelling\(input\.value, known\(\)\);\s*choice\.draft = undefined;", box);
    }

    [Fact]
    public void FocusSurvivesAPoll()
    {
        var box = Box();
        Assert.Matches(@"addEventListener\(""focus"", \(\) => \{\s*choice\.focused = true;", box);
        Assert.Matches(@"addEventListener\(""blur"", \(\) => \{\s*setTimeout\(\(\) => \{\s*if \(input\.isConnected\) choice\.focused = false;\s*\}, 0\);", box);
        Assert.Matches(@"if \(choice\.focused\) \{\s*setTimeout\(\(\) => \{\s*if \(input\.isConnected\) \{\s*input\.focus\(\);\s*if \(choice\.caret\) input\.setSelectionRange\(choice\.caret\[0\], choice\.caret\[1\]\);", box);
        Assert.DoesNotContain("requestAnimationFrame", box);
    }

    [Fact]
    public void TheFullTextColumnsKeepTheirLineBreaks()
    {
        var tab = Tab();
        Assert.Contains("{ key: \"original_index_definition\", label: \"Original Definition\", wrap: true, mono: true, pre: true }", tab);
        Assert.Contains("{ key: \"script\", label: \"Script\", wrap: true, mono: true, pre: true }", tab);
        var panels = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js").ReplaceLineEndings("\n");
        Assert.Equal(2, Regex.Matches(panels, @"if \(c\.pre\) (rcls|cls)\.push\(""pre""\);").Count);
        var css = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "app.css").ReplaceLineEndings("\n");
        var wrap = css.IndexOf("table.data td.wrap {", System.StringComparison.Ordinal);
        var pre = css.IndexOf("table.data td.pre { white-space: pre-wrap; }", System.StringComparison.Ordinal);
        Assert.True(wrap >= 0 && pre > wrap, "td.pre must follow td.wrap so it wins");
    }

    [Fact]
    public void TheLeadNotesAreTheUptimeThenDedupeNotesTheServerEmitsFirst()
    {
        var source = ToolSource();
        var uptime = source.IndexOf("notes.Add(IndexAnalysisUptimeNote)", System.StringComparison.Ordinal);
        var dedupe = source.IndexOf("notes.Add(IndexAnalysisDedupeNote)", System.StringComparison.Ordinal);
        var analyzer = source.IndexOf("notes.AddRange(result.Notes)", System.StringComparison.Ordinal);
        Assert.True(uptime >= 0 && dedupe > uptime, "uptime note must come before the dedupe note");
        Assert.True(analyzer > dedupe, "analyzer notes must come after both lead notes");
    }

    [Fact]
    public void TheOverallRowIsAllDatabasesWithNoWorkloadFigures()
    {
        var tab = Tab();
        Assert.Contains("{ ...data.overall, database_name: \"ALL DATABASES\" }", tab);
        Assert.DoesNotContain("\"N/A\"", tab);
    }

    [Fact]
    public void TheReadsBreakdownIsBuiltFromTheEmittedCounters()
    {
        Assert.Contains(
            "reads_breakdown: r.total_reads == null ? null : fmtInt(r.total_reads) + \" (\" + fmtInt(r.user_seeks) + \" seeks, \" + fmtInt(r.user_scans) + \" scans, \" + fmtInt(r.user_lookups) + \" lookups)\"",
            Tab());
    }

    [Fact]
    public void TheSizeShowsThreeDecimalsLikeTheDesktop()
    {
        Assert.Contains("fmtNum(r.index_size_gb, 3)", Tab());
    }

    [Fact]
    public void TheBannerTextComesFromThePayload()
    {
        var tab = Tab();
        Assert.DoesNotContain("under 14 days", tab);
        Assert.DoesNotContain("Dedupe-only mode", tab);
        Assert.Contains("data.notes || []", tab);
    }

    [Fact]
    public void TheTabKeepsThePayloadOrder()
    {
        Assert.DoesNotContain(".sort(", Tab());
    }

    [Fact]
    public void TheTruncatedNoticeNamesShownThenTotal()
    {
        var tab = Tab();
        Assert.Contains("const rowsCut = n > 0 && data.truncated;", tab);
        /* #5238: the count left over is one name, with a singular branch ("1 more is not shown"); the harness test runs the text itself. */
        Assert.Contains("const left = data.recommendation_count - n;", tab);
        Assert.Contains("\"Showing the largest \" + n + \" of \" + data.recommendation_count + \" recommendations. The list stops at \" + n + \", so \" + (left === 1 ? \"1 more is\" : left + \" more are\") + \" not shown.\"", tab);
        Assert.DoesNotContain("The largest \" + n", tab);
    }

    [Fact]
    public void TheReadStatesAreHandled()
    {
        var tab = Tab();
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"aborted\" \\|\\| res\\.kind === \"auth\"\\) return;$", tab);
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"empty\"\\) return mount\\(content, emptyStrip\\(res\\.message\\)\\);$", tab);
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"error\"\\) return mount\\(content, readErrorStrip\\(res\\.message\\)\\);$", tab);
        Assert.Contains("Could not render this tab: ", tab);
    }

    [Fact]
    public void TheTabBuildsItsDomFromTextOnly()
    {
        Assert.DoesNotContain("innerHTML", Tab());
    }

    [Fact]
    public void TheTabImportsOnlyTheSharedHelpers()
    {
        var imports = Regex.Matches(Tab(), "from \"([^\"]+)\";").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "./database-box.js", "../../panels.js", "../../util.js" }));
    }

    [Fact]
    public void TheStubTextIsGone()
    {
        Assert.DoesNotContain("Not on the web yet", Tab());
    }

    [Fact]
    public async System.Threading.Tasks.Task TheTabParsesUnderNode()
    {
        var path = Path.Combine(Path.GetTempPath(), "index-analysis-" + System.Guid.NewGuid().ToString("N") + ".mjs");
        File.WriteAllText(path, Tab());
        try
        {
            var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            psi.ArgumentList.Add("--check");
            psi.ArgumentList.Add(path);
            Process proc;
            try
            {
                proc = Process.Start(psi)!;
            }
            catch (Win32Exception)
            {
                Assert.Skip("Node is not installed, so the tab script cannot be parsed.");
                throw;
            }

            using (proc)
            {
                var error = proc.StandardError.ReadToEndAsync();
                if (!proc.WaitForExit(20000))
                {
                    proc.Kill(entireProcessTree: true);
                    Assert.Fail("node --check did not finish in 20 s");
                }

                Assert.True(proc.ExitCode == 0, "node --check failed: " + await error);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }
}
