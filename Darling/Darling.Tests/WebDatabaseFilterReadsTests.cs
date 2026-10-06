/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// <para>The server page's database filter (#5244, #5245) sorts every SQL Server read the page names into exactly one of four
/// classes, in <c>wwwroot/js/database-filter-reads.js</c>: FILTERED (the route takes the chosen databases), UNFILTERED (a
/// database-scoped read that cannot take them yet), IDENTITY (the query drill and the seven plan-viewer reads, which take
/// <c>database_name</c> as a row's identity) and SERVER_WIDE (the data has no database). These tests hold the sorting
/// together without Node, so they run where the harness behind <see cref="WebDatabaseFilterBehaviourTests"/> cannot.</para>
///
/// <para>Census scope: every module in <c>pages/server.js</c>'s static import closure, followed transitively, not
/// <c>server-tabs.js</c> alone. <c>server-tabs.js</c> imports <c>pages/analysis-findings.js</c> and <c>pages/plan-viewer.js</c>, and
/// a census that rosters its files does not see the file it does not roster (.github/REVIEW-TRAPS.md names the trap). A read
/// named in any of them with no class would otherwise default to "server-wide" by silence.</para>
///
/// <para>The two pins hold the JS list to the server: the catalog rows that carry <c>PDatabases(</c> are exactly FILTERED, and the
/// dispatch entries that call <c>DatabaseNames(c)</c> are exactly those rows. Until the first read is wired, all three sets
/// are empty, so the parsing is also exercised on text built here, wrapped entries and a helper after the method included.</para>
/// </summary>
public sealed class WebDatabaseFilterReadsTests
{
    private static readonly string[] ClassNames = { "FILTERED", "UNFILTERED", "IDENTITY", "SERVER_WIDE" };

    private static string ReadJs(string relative) =>
        ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", relative);

    private static string ReadEndpoints() =>
        ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");

    /// <summary>The four classes as database-filter-reads.js declares them: the quoted names of each
    /// <c>export const NAME = new Set([...]);</c>, in file order, comments dropped.</summary>
    private static Dictionary<string, string[]> Classes()
    {
        var text = ReadJs("database-filter-reads.js");
        var classes = new Dictionary<string, string[]>();
        foreach (var name in ClassNames)
        {
            var declared = Regex.Match(text, @"export const " + name + @" = new Set\(\[(?<body>.*?)\]\);", RegexOptions.Singleline);
            Assert.True(declared.Success, $"database-filter-reads.js no longer declares `export const {name} = new Set([...]);` in the form these tests read.");
            var body = Regex.Replace(declared.Groups["body"].Value, @"//[^\n]*", "");
            classes[name] = Regex.Matches(body, @"""([A-Za-z0-9_]+)""").Select(m => m.Groups[1].Value).ToArray();
        }

        return classes;
    }

    /// <summary>The catalog's read names: each <c>["name"] = R(</c> row of CatalogDescriptors.</summary>
    internal static string[] CatalogNames(string endpoints) =>
        Regex.Matches(endpoints, @"\[""(\w+)""\] = R\(").Select(m => m.Groups[1].Value).ToArray();

    /// <summary>Entries of one dictionary initializer: each runs from its start match to the next entry's start (or the end of
    /// the region), so an entry that wraps over several lines is read whole.</summary>
    private static (string Name, string Body)[] Entries(string region, string startPattern)
    {
        var starts = Regex.Matches(region, startPattern).ToArray();
        return starts.Select((m, i) => (Name: m.Groups[1].Value, Body: region[m.Index..(i + 1 < starts.Length ? starts[i + 1].Index : region.Length)])).ToArray();
    }

    /// <summary>The CatalogDescriptors rows, each with its text. The region ends at the initializer's closing brace, so a helper
    /// declared after it cannot count as a row's text.</summary>
    internal static (string Name, string Body)[] CatalogRows(string endpoints)
    {
        var begin = Regex.Match(endpoints, @"CatalogDescriptors\s*=\s*new Dictionary<string, CatalogRead>");
        Assert.True(begin.Success, "DarlingWebEndpoints.cs no longer declares CatalogDescriptors in the form this pin reads.");
        var end = endpoints.IndexOf("\n        };", begin.Index, StringComparison.Ordinal);
        Assert.True(end > 0, "the end of CatalogDescriptors was not found.");
        return Entries(endpoints[begin.Index..end], @"\[""(\w+)""\] = R\(");
    }

    /// <summary>The BuildReadDispatch entries, each with its text. The last entry ends at the end of the method (its closing
    /// brace at four spaces), so the text of a helper written after it cannot match.</summary>
    internal static (string Name, string Body)[] DispatchEntries(string endpoints)
    {
        var begin = Regex.Match(endpoints, @"BuildReadDispatch\(ILogger\?");
        Assert.True(begin.Success, "DarlingWebEndpoints.cs no longer declares BuildReadDispatch in the form this pin reads.");
        var end = endpoints.IndexOf("\n    }\n", begin.Index, StringComparison.Ordinal);
        Assert.True(end > 0, "the end of BuildReadDispatch was not found.");
        return Entries(endpoints[begin.Index..end], @"\[""(\w+)""\] = \(c, pg, an\) =>");
    }

    private static string[] Sorted(IEnumerable<string> names) => names.OrderBy(n => n, StringComparer.Ordinal).ToArray();

    /// <summary>The modules reachable from pages/server.js by <c>import ... from "./x.js"</c> (and a dynamic import with a literal
    /// path), followed transitively, as repo-relative paths under wwwroot/js with their text.</summary>
    private static Dictionary<string, string> ImportClosure(string entry)
    {
        var seen = new Dictionary<string, string>(StringComparer.Ordinal);
        var queue = new Queue<string>();
        queue.Enqueue(entry);
        var staticImport = new Regex(
            @"(?:^|\n)[ \t]*(?:import|export)\s[^;]*?from\s*[""'](?<spec>\.[^""']+)[""']|import\s*\(\s*[""'](?<spec>\.[^""']+)[""']\s*\)",
            RegexOptions.None, TimeSpan.FromSeconds(5));
        while (queue.Count > 0)
        {
            var module = queue.Dequeue();
            if (seen.ContainsKey(module))
            {
                continue;
            }

            var text = ReadJs(module);
            seen[module] = text;
            var folder = module.Contains('/', StringComparison.Ordinal) ? module[..module.LastIndexOf('/')] : "";
            foreach (Match m in staticImport.Matches(text))
            {
                var parts = new List<string>(folder.Length == 0 ? Array.Empty<string>() : folder.Split('/'));
                foreach (var segment in m.Groups["spec"].Value.Split('/'))
                {
                    if (segment == "..")
                    {
                        parts.RemoveAt(parts.Count - 1);
                    }
                    else if (segment != ".")
                    {
                        parts.Add(segment);
                    }
                }

                queue.Enqueue(string.Join('/', parts));
            }
        }

        return seen;
    }

    /// <summary>
    /// Every SQL Server read a module of the server page names sits in exactly one class. A name is a read when it is a quoted
    /// string that the catalog knows, or any quoted <c>get_</c> name (so a read the catalog lacks is found too). PostgreSQL
    /// reads carry the <c>get_pg_</c> prefix and are left out; there is no other PostgreSQL-only read among them. The roster
    /// itself, database-filter-reads.js, is not scanned: it names every read it lists.
    /// </summary>
    [Fact]
    public void EveryReadAModuleOfTheServerPageNames_IsInExactlyOneClass()
    {
        var closure = ImportClosure("pages/server.js");
        /* Not vacuous: the closure reaches the modules the server tabs import, the two a roster of server-tabs.js alone would miss
           among them, and util.js, which imports the roster. */
        foreach (var expected in new[] { "pages/server.js", "pages/server-tabs.js", "pages/analysis-findings.js", "pages/plan-viewer.js", "util.js", "panels.js", "database-filter-reads.js" })
        {
            Assert.True(closure.ContainsKey(expected), $"{expected} is not in the import closure of pages/server.js, so this census would not read it. Found: {string.Join(", ", closure.Keys)}");
        }

        var classes = Classes();
        var catalog = new HashSet<string>(CatalogNames(ReadEndpoints()), StringComparer.Ordinal);
        Assert.True(catalog.Count > 100, "The read catalog's entries were not found.");

        var named = new SortedDictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var (module, text) in closure)
        {
            if (module == "database-filter-reads.js")
            {
                continue;
            }

            foreach (Match m in Regex.Matches(text, @"[""'`]([A-Za-z_][A-Za-z0-9_]*)[""'`]"))
            {
                var name = m.Groups[1].Value;
                if (name.StartsWith("get_pg_", StringComparison.Ordinal) || !(catalog.Contains(name) || name.StartsWith("get_", StringComparison.Ordinal)))
                {
                    continue;
                }

                if (!named.TryGetValue(name, out var modules))
                {
                    named[name] = modules = new List<string>();
                }

                if (!modules.Contains(module))
                {
                    modules.Add(module);
                }
            }
        }

        Assert.True(named.Count > 60, $"only {named.Count} reads were found in the closure, so the scan reads too little.");

        var unclassified = named.Where(kv => !classes.Values.Any(list => list.Contains(kv.Key))).Select(kv => $"{kv.Key} (named in {string.Join(", ", kv.Value)})").ToArray();
        Assert.True(
            unclassified.Length == 0,
            "These reads are named by a module of the server page and sit in no class of database-filter-reads.js. A database-scoped read goes in UNFILTERED"
            + " (or FILTERED once its route takes the list), a row-identity read in IDENTITY, anything with no database in SERVER_WIDE:\n" + string.Join("\n", unclassified));
    }

    /// <summary>The classes are disjoint, and no class names a read twice.</summary>
    [Fact]
    public void TheClasses_AreDisjoint_AndNoneListsAReadTwice()
    {
        var classes = Classes();
        foreach (var (name, list) in classes)
        {
            Assert.True(list.Length == list.Distinct(StringComparer.Ordinal).Count(), $"{name} lists a read twice: {string.Join(", ", list.GroupBy(n => n).Where(g => g.Count() > 1).Select(g => g.Key))}");
        }

        var both = classes.SelectMany(kv => kv.Value.Select(read => (Read: read, Class: kv.Key)))
            .GroupBy(x => x.Read)
            .Where(g => g.Count() > 1)
            .Select(g => $"{g.Key} is in {string.Join(" and ", g.Select(x => x.Class))}")
            .ToArray();
        Assert.True(both.Length == 0, "A read is in two classes:\n" + string.Join("\n", both));
    }

    /// <summary>Every name in every class is a read the catalog has: a typo in the roster would otherwise sit there unused while
    /// the read it meant stayed unclassified, and the census above would be the only one to say so.</summary>
    [Fact]
    public void EveryClassedRead_IsInTheReadCatalog()
    {
        var catalog = new HashSet<string>(CatalogNames(ReadEndpoints()), StringComparer.Ordinal);
        var unknown = Classes().SelectMany(kv => kv.Value.Select(read => (Read: read, Class: kv.Key))).Where(x => !catalog.Contains(x.Read)).Select(x => $"{x.Class}: {x.Read}").ToArray();
        Assert.True(unknown.Length == 0, "database-filter-reads.js names reads the catalog does not have:\n" + string.Join("\n", unknown));
    }

    /// <summary>The identity reads are the query drill and the seven plan-viewer reads, and no others: the filter is never injected
    /// into them, so a tenth would silently lose its panel's filter. The database-scoped reads are 34 in all (FILTERED and UNFILTERED
    /// together; moving a read from one to the other keeps the total), and FILTERED is a subset of what the plan filters: none of
    /// the three deadlock reads.</summary>
    [Fact]
    public void TheIdentityReads_AreTheNineRowIdentityReads_AndTheDatabaseScopedReadsAreThirtyFour()
    {
        var classes = Classes();
        Assert.Equal(
            Sorted(new[] { "get_query_trend", "get_plan_xml", "get_query_store_plan_xml", "get_active_query_plan_xml", "get_procedure_plan_xml", "get_query_store_query_history", "get_blocking_plan_xml", "get_deadlock_plan_xml", "get_query_repro_script" }),
            Sorted(classes["IDENTITY"]));
        Assert.Equal(34, classes["FILTERED"].Length + classes["UNFILTERED"].Length);
        Assert.DoesNotContain(classes["FILTERED"], read => read is "get_deadlock_trend" or "get_deadlocks" or "get_deadlock_detail");
        foreach (var deadlock in new[] { "get_deadlock_trend", "get_deadlocks", "get_deadlock_detail" })
        {
            Assert.Contains(deadlock, classes["UNFILTERED"]);
        }
    }

    /// <summary>
    /// Pin 1: the reads in FILTERED are exactly the catalog rows that carry <c>PDatabases(</c>, the parameter a route that takes the
    /// list declares. A read cannot be listed in JS without its route taking the list, nor take it without being listed.
    /// </summary>
    [Fact]
    public void Filtered_IsExactlyTheCatalogRowsThatCarryPDatabases()
    {
        var rows = CatalogRows(ReadEndpoints());
        Assert.True(rows.Length > 100, "The read catalog's entries were not found.");
        var carrying = Sorted(rows.Where(r => r.Body.Contains("PDatabases(", StringComparison.Ordinal)).Select(r => r.Name));
        var filtered = Sorted(Classes()["FILTERED"]);
        Assert.True(
            carrying.SequenceEqual(filtered),
            "FILTERED in database-filter-reads.js and the catalog rows that carry PDatabases( differ.\n"
            + "In the catalog only: " + string.Join(", ", carrying.Except(filtered)) + "\nIn FILTERED only: " + string.Join(", ", filtered.Except(carrying)));
    }

    /// <summary>
    /// Pin 2: each such row's dispatch entry calls <c>DatabaseNames(c)</c>, and no other entry does, so a row that declares the
    /// list cannot ship with a route that ignores it, nor the reverse.
    /// </summary>
    [Fact]
    public void TheDispatchEntriesThatCallDatabaseNames_AreExactlyTheRowsThatCarryPDatabases()
    {
        var endpoints = ReadEndpoints();
        var entries = DispatchEntries(endpoints);
        Assert.True(entries.Length > 100, "BuildReadDispatch's entries were not found.");
        var calling = Sorted(entries.Where(e => e.Body.Contains("DatabaseNames(c)", StringComparison.Ordinal)).Select(e => e.Name));
        var carrying = Sorted(CatalogRows(endpoints).Where(r => r.Body.Contains("PDatabases(", StringComparison.Ordinal)).Select(r => r.Name));
        Assert.True(
            calling.SequenceEqual(carrying),
            "The BuildReadDispatch entries that call DatabaseNames(c) and the catalog rows that carry PDatabases( differ.\n"
            + "Dispatch only: " + string.Join(", ", calling.Except(carrying)) + "\nCatalog only: " + string.Join(", ", carrying.Except(calling)));
    }

    private const string SyntheticSource = """
        class W
        {
            private static CatalogParam PDatabases() => new("database_name", TypeText, false, null);
            internal static readonly IReadOnlyDictionary<string, CatalogRead> CatalogDescriptors =
                new Dictionary<string, CatalogRead>(StringComparer.Ordinal)
                {
                    ["get_a"] = R(CatX, "plain.", PServer(), PHours(4)),
                    ["get_b"] = R(CatX, "takes the list, and wraps over lines.",
                        PServer(), PHours(4),
                        PDatabases()),
                    ["get_c"] = R(CatX, "a name today.", PServer(), PText("database_name")),
                };
            private static CatalogRead Later() => R(CatX, "a helper after the catalog.", PDatabases());

            internal static IReadOnlyDictionary<string, ReadToolHandler> BuildReadDispatch(ILogger? logger = null)
            {
                var dispatch = new Dictionary<string, ReadToolHandler>(StringComparer.Ordinal)
                {
                    ["get_a"] = (c, pg, an) => Tools.A(pg, Server(c)),
                    ["get_b"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucket)
                        ? Tools.B(pg, Server(c), DatabaseNames(c))
                        : UnparseableParam("bucket_minutes"),
                    ["get_c"] = (c, pg, an) => Tools.C(pg, Server(c), Str(c, "database_name")),
                };
                return dispatch;
            }

            private static void Helper(HttpContext c) { var names = DatabaseNames(c); }
        }
        """;

    /// <summary>The parsing the two pins stand on, run on text with a wrapped catalog row, a wrapped dispatch entry, a helper
    /// after the catalog and a helper after the method: only the real carriers and callers are found.</summary>
    [Fact]
    public void ThePinsParsing_FindsWrappedRows_AndIgnoresAHelpersText()
    {
        var text = SyntheticSource.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Equal(new[] { "get_a", "get_b", "get_c" }, CatalogRows(text).Select(r => r.Name).ToArray());
        Assert.Equal(new[] { "get_b" }, CatalogRows(text).Where(r => r.Body.Contains("PDatabases(", StringComparison.Ordinal)).Select(r => r.Name).ToArray());
        Assert.Equal(new[] { "get_a", "get_b", "get_c" }, DispatchEntries(text).Select(e => e.Name).ToArray());
        Assert.Equal(new[] { "get_b" }, DispatchEntries(text).Where(e => e.Body.Contains("DatabaseNames(c)", StringComparison.Ordinal)).Select(e => e.Name).ToArray());
        Assert.Equal(new[] { "get_a", "get_b", "get_c" }, CatalogNames(text));
    }
}
