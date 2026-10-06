// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 (statement filter, PR B): a census over every SQL Server collector definition's payload columns. A column
/// whose name looks like it can hold statement text, a plan, an event document or a script (the
/// <see cref="PayloadPattern"/>) must be listed below as one of three things, so a new column cannot reach the
/// store without somebody deciding what happens to it:
/// <list type="bullet">
/// <item><b>Hooked</b>: the collector passes the value through the statement filter before the write. Each entry
/// names its row in the plan's collection-write-path table and the lane that lands the hook. A row whose hook has
/// not landed yet is "pending: Rn"; the integration branch flips <see cref="Pending"/> to false per row.</item>
/// <item><b>NullByDesign</b>: the collector writes NULL there on purpose.</item>
/// <item><b>Exempt</b>: the name matches but the value is not statement content, with the reason spelled out.</item>
/// </list>
/// <para>The pattern also matches <c>input_buffer</c>, <c>batch_text</c>, <c>command</c> and <c>definition</c>.
/// A <c>Hooked</c> entry that is not <see cref="Entry.Pending"/> must have a collection-time case registered in
/// <see cref="StatementCollectionCensusCases"/> (<see cref="EveryNonPendingHookedColumn_HasACollectionCase"/>), so
/// flipping an entry cannot pass without a hook that a test exercises. The census sees definition payload columns
/// only; <see cref="WatchedWriters"/> lists the writers that do not go through a definition (a plan or text store
/// fed outside the collector pipeline, the slow-read log) and a source scan checks each one for a
/// <c>SensitiveStatements</c> call once it is no longer pending.</para>
/// A new matching column fails <see cref="EveryMatchingPayloadColumn_IsListed"/> until it is listed. A listed
/// column that no longer exists or no longer matches fails <see cref="EveryListedColumn_StillMatchesADefinition"/>,
/// so the list cannot rot. PostgreSQL definitions are out of scope: they are a separate engine with their own
/// filter (<c>PgSensitiveStatementFilter</c>).
/// </summary>
public sealed class StatementColumnCensusTests
{
    private static readonly Regex PayloadPattern = new(
        "sql_text$|query_text$|statement_text$|text_data$|_xml$|query_plan|event_xml|implementation_script|^message$|input_buffer|batch_text|command|definition",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private enum Kind
    {
        Hooked,
        NullByDesign,
        Exempt,
    }

    private sealed record Entry(Kind Kind, string Detail, string? Lane = null, bool Pending = false);

    private static Entry Hooked(string row, string lane, bool pending = true) =>
        new(Kind.Hooked, "6.7 row " + row, lane, pending);

    private static Entry Null(string reason) => new(Kind.NullByDesign, reason);

    private static Entry Exempt(string reason) => new(Kind.Exempt, reason);

    /// <summary>Keyed "definition.column" (the definition's Name, which is what schedules and logs call it).</summary>
    private static readonly Dictionary<string, Entry> Listed = new(StringComparer.Ordinal)
    {
        /* Rows 1-4: query_stats and procedure_stats text and plans, inline and deferred fetch (R2). */
        ["query_stats.query_text"] = Hooked("1", "R2"),
        ["query_stats.query_plan_xml"] = Hooked("2 and 3 (deferred plan fetch)", "R2"),
        ["procedure_stats.query_plan_xml"] = Hooked("3 and 4 (deferred plan fetch, inline)", "R2"),
        ["query_stats.query_plan_hash"] = Exempt("a hash of the plan, not plan text; the filter has nothing to read"),
        ["query_stats.query_plan_xml_bytes"] = Exempt("the byte length of the stored plan, a number"),
        ["procedure_stats.query_plan_xml_bytes"] = Exempt("the byte length of the stored plan, a number"),

        /* Row 5: Query Store. Lite stores it live; Darling stores it live and from backfill (R3). */
        ["query_store.query_text"] = Hooked("5", "R3"),
        ["query_store.query_plan_hash"] = Exempt("a hash of the plan, not plan text; the filter has nothing to read"),
        ["query_store.query_plan_text"] = Null("the main Query Store query writes a typed NULL here (QueryStoreCollector.cs:738); plan text reaches the store only through the separate by-ids fetch and the plan writer (6.7 row 7, R3)"),

        /* Row 8: the live snapshot of running requests (R6). */
        ["query_snapshots.query_text"] = Hooked("8", "R6"),
        ["query_snapshots.query_plan"] = Hooked("8", "R6"),
        ["query_snapshots.live_query_plan"] = Hooked("8", "R6"),

        /* Rows 9-11: blocked process reports (R4). */
        ["blocked_process_report.blocked_sql_text"] = Hooked("9", "R4"),
        ["blocked_process_report.blocking_sql_text"] = Hooked("9", "R4"),
        ["blocked_process_report.blocked_process_report_xml"] = Hooked("10", "R4"),
        ["blocked_process_report.blocked_query_plan_xml"] = Hooked("11", "R4"),
        ["blocked_process_report.blocking_query_plan_xml"] = Hooked("11", "R4"),

        /* Rows 12-14: deadlocks (R4). */
        ["deadlocks.victim_sql_text"] = Hooked("12", "R4"),
        ["deadlocks.deadlock_graph_xml"] = Hooked("13", "R4"),
        ["deadlocks.victim_query_plan_xml"] = Hooked("14", "R4"),

        /* Row 15: the DMV blocking snapshot (R4). */
        ["dmv_blocking_snapshot.blocked_sql_text"] = Hooked("15", "R4"),
        ["dmv_blocking_snapshot.blocking_sql_text"] = Hooked("15", "R4"),

        /* Rows 16-18 and 21: the event and history collectors (R5). */
        ["long_query_completions.statement_text"] = Hooked("16", "R5"),
        ["default_trace_events.text_data"] = Hooked("17", "R5"),
        ["system_health_events.event_xml"] = Hooked("18", "R5"),
        ["job_history.message"] = Hooked("21", "R5"),

        /* The widened pattern (input_buffer, batch_text, command, definition) found one more column. Plan 6.7 names it. */
        ["index_object_stats.filter_definition"] = Exempt("an index filter predicate over bracketed column names (IndexObjectStatsCollector.cs), not a statement"),

        /* Row 19: plan correction (R6). */
        ["plan_correction.query_text"] = Hooked("19", "R6"),
        ["plan_correction.implementation_script"] = Hooked("19", "R6"),
    };

    /// <summary>
    /// Columns the pattern does not match but the plan names as exempt, so a later rename into the pattern, or a
    /// removal, is noticed. Each must exist in its definition and must NOT match the pattern (a match belongs in
    /// <see cref="Listed"/>).
    /// </summary>
    private static readonly Dictionary<string, string> WatchedExempt = new(StringComparer.Ordinal)
    {
        ["waiting_tasks.resource_description"] = "the collector writes a literal NULL there (WaitingTasksCollector.cs)",
    };

    private static IEnumerable<(string Definition, string Column)> SqlServerPayloadColumns() =>
        CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer)
            .SelectMany(d => d.PayloadColumns.Select(c => (d.Name, c.Name)));

    /// <summary>The census itself, over any set of (definition, column) pairs; the plant test feeds it a fake one.</summary>
    private static string[] Unlisted(IEnumerable<(string Definition, string Column)> columns) =>
        columns
            .Where(c => PayloadPattern.IsMatch(c.Column))
            .Select(c => c.Definition + "." + c.Column)
            .Where(key => !Listed.ContainsKey(key))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();

    [Fact]
    public void EveryMatchingPayloadColumn_IsListed()
    {
        var unlisted = Unlisted(SqlServerPayloadColumns());

        Assert.True(
            unlisted.Length == 0,
            "These SQL Server payload columns look like statement, plan or event content but are not in " +
            "StatementColumnCensusTests.Listed. Hook each one through the statement filter (and list it as Hooked " +
            "with its plan row and lane), or list it as NullByDesign or Exempt with the reason: " +
            string.Join(", ", unlisted));
    }

    [Fact]
    public void ANewMatchingColumn_IsReportedUntilListed()
    {
        var planted = SqlServerPayloadColumns()
            .Append(("query_stats", "extra_statement_text"))
            .Append(("wait_stats", "wait_type"))
            .ToArray();

        var unlisted = Unlisted(planted);

        Assert.Equal(new[] { "query_stats.extra_statement_text" }, unlisted);
    }

    [Fact]
    public void EveryListedColumn_StillMatchesADefinition()
    {
        var actual = SqlServerPayloadColumns()
            .Where(c => PayloadPattern.IsMatch(c.Column))
            .Select(c => c.Definition + "." + c.Column)
            .ToHashSet(StringComparer.Ordinal);

        var stale = Listed.Keys.Where(k => !actual.Contains(k)).OrderBy(k => k, StringComparer.Ordinal).ToArray();

        Assert.True(
            stale.Length == 0,
            "These census entries name a column that no longer exists in a SQL Server definition or no longer " +
            "matches the payload pattern; remove or fix them: " + string.Join(", ", stale));
    }

    [Fact]
    public void EveryWatchedExemptColumn_ExistsAndStaysOutsideThePattern()
    {
        var all = SqlServerPayloadColumns().Select(c => c.Definition + "." + c.Column).ToHashSet(StringComparer.Ordinal);

        foreach (var (key, reason) in WatchedExempt)
        {
            Assert.False(string.IsNullOrWhiteSpace(reason), key + " needs a reason");
            Assert.True(all.Contains(key), key + " is no longer a SQL Server payload column; remove it from WatchedExempt");

            var column = key[(key.IndexOf('.', StringComparison.Ordinal) + 1)..];
            Assert.False(PayloadPattern.IsMatch(column), key + " now matches the payload pattern; move it into Listed");
        }
    }

    [Fact]
    public void EveryEntry_CarriesItsReasonRowAndLane()
    {
        foreach (var (key, entry) in Listed)
        {
            Assert.False(string.IsNullOrWhiteSpace(entry.Detail), key + " needs a plan row or a reason");

            if (entry.Kind == Kind.Hooked)
            {
                Assert.Matches("^R[0-9]+$", entry.Lane ?? string.Empty);
                Assert.StartsWith("6.7 row ", entry.Detail, StringComparison.Ordinal);
            }
            else
            {
                Assert.Null(entry.Lane);
                Assert.False(entry.Pending, key + ": only a Hooked entry can be pending");
            }
        }
    }

    [Fact]
    public void TheCensusCountsWhatItReports()
    {
        var matched = SqlServerPayloadColumns().Count(c => PayloadPattern.IsMatch(c.Column));
        Assert.Equal(matched, Listed.Count);

        Assert.Equal(matched, Listed.Values.Count(e => e.Kind == Kind.Hooked)
            + Listed.Values.Count(e => e.Kind == Kind.NullByDesign)
            + Listed.Values.Count(e => e.Kind == Kind.Exempt));
    }

    // ── non-pending Hooked entries need a collection-time case ──

    /// <summary>
    /// The one class a registered collection case must sit in: the collection census, the tests that plant the canary
    /// in a collector's input and read what <c>StatementScrubRecordingWriter</c> recorded. It may be a partial class
    /// spread over several files.
    /// </summary>
    internal const string CollectionCensusClass = "StatementCollectionCensusTests";

    /// <summary>A real test: a <c>[Fact]</c> or <c>[Theory]</c> that is not skipped, conditionally skipped or explicit.</summary>
    private static bool IsRunnableTest(MethodInfo method)
    {
        var facts = method.GetCustomAttributes<FactAttribute>(inherit: true).ToArray();
        return facts.Length > 0
            && facts.All(f => string.IsNullOrEmpty(f.Skip)
                && string.IsNullOrEmpty(f.SkipUnless)
                && string.IsNullOrEmpty(f.SkipWhen)
                && !f.Explicit);
    }

    /// <summary>The source of every file that declares <paramref name="type"/>, joined (empty when none is found).</summary>
    private static string CensusClassSource(Type type)
    {
        var dir = Path.GetDirectoryName(RepoFile.PathTo("Darling", "Darling.Tests", "StatementColumnCensusTests.cs"))!;
        var declaration = new Regex(@"\bclass\s+" + Regex.Escape(type.Name) + @"\b");
        return string.Join("\n", Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            .Select(File.ReadAllText)
            .Where(t => declaration.IsMatch(t)));
    }

    /// <summary>
    /// The problems with a registry: a key that is not a Hooked entry, or a case that does not name a runnable test
    /// (a method that exists, carries <c>[Fact]</c>/<c>[Theory]</c> with no skip) in the collection census class whose
    /// source drives <c>StatementScrubRecordingWriter</c>, names the column, and does not skip at run time.
    /// </summary>
    private static string[] RegistryProblems(IReadOnlyDictionary<string, string> cases) =>
        RegistryProblems(cases, typeof(StatementColumnCensusTests).Assembly.GetTypes(), CollectionCensusClass, CensusClassSource);

    private static string[] RegistryProblems(
        IReadOnlyDictionary<string, string> cases, IEnumerable<Type> types, string censusClass, Func<Type, string> sourceOf)
    {
        var problems = new List<string>();
        var typeList = types.ToArray();

        foreach (var (key, test) in cases.OrderBy(c => c.Key, StringComparer.Ordinal))
        {
            if (!Listed.TryGetValue(key, out var entry) || entry.Kind != Kind.Hooked)
            {
                problems.Add(key + " is not a Hooked entry of Listed");
                continue;
            }

            var parts = test.Split('.');
            if (parts.Length != 2 || parts[0] != censusClass)
            {
                problems.Add(key + " names " + test + ", which is not a test in " + censusClass);
                continue;
            }

            var type = typeList.FirstOrDefault(t => t.Name == censusClass);
            var method = type?
                .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
                .FirstOrDefault(m => m.Name == parts[1] && IsRunnableTest(m));
            if (type is null || method is null)
            {
                problems.Add(key + " names " + test + ", which is not a runnable [Fact] or [Theory] (none, skipped, or explicit)");
                continue;
            }

            var source = sourceOf(type);
            var column = key[(key.IndexOf('.') + 1)..];
            if (!source.Contains("StatementScrubRecordingWriter", StringComparison.Ordinal)
                || !source.Contains(column, StringComparison.Ordinal)
                || source.Contains("Assert.Skip", StringComparison.Ordinal))
            {
                problems.Add(key + " names " + test + ", but " + censusClass + " does not drive StatementScrubRecordingWriter " +
                    "for " + column + " (or it skips at run time)");
            }
        }

        return problems.ToArray();
    }

    /// <summary>The Hooked entries that claim their hook landed (not pending) and have no registered case.</summary>
    private static string[] NonPendingWithoutACase(IReadOnlyDictionary<string, string> cases) =>
        Listed.Where(e => e.Value.Kind == Kind.Hooked && !e.Value.Pending && !cases.ContainsKey(e.Key))
            .Select(e => e.Key)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();

    [Fact]
    public void EveryNonPendingHookedColumn_HasACollectionCase()
    {
        var missing = NonPendingWithoutACase(StatementCollectionCensusCases.ByColumn);

        Assert.True(
            missing.Length == 0,
            "These Hooked entries are not pending but have no collection-time case in " +
            "StatementCollectionCensusCases.ByColumn (a test that plants the canary in the collector's input and " +
            "asserts what is written): " + string.Join(", ", missing));
    }

    [Fact]
    public void EveryRegisteredCollectionCase_NamesARunnableTestOnAHookedColumn()
    {
        var problems = RegistryProblems(StatementCollectionCensusCases.ByColumn);

        Assert.True(problems.Length == 0, string.Join("; ", problems));
    }

    // Fixtures for the registry self-test. Not public, so xunit does not run them (xUnit1000 asks for public).
#pragma warning disable xUnit1000
    private sealed class FixtureCensus
    {
        [Fact]
        public void Runs() { }

        [Fact(Skip = "fixture")]
        public void Skipped() { }

        [Fact(Explicit = true)]
        public void NotRunByDefault() { }

        [Theory(Skip = "fixture")]
        [InlineData(1)]
        public void SkippedTheory(int x) => _ = x;

        public void NoAttribute() { }
    }

    private sealed class FixtureOtherClass
    {
        [Fact]
        public void Runs() { }
    }
#pragma warning restore xUnit1000

    [Fact]
    public void TheRegistryChecks_CatchABrokenCaseAndAnUncoveredEntry()
    {
        // With no cases at all, every non-pending Hooked entry is named (none today: all are pending).
        var empty = new Dictionary<string, string>(StringComparer.Ordinal);
        Assert.Equal(
            Listed.Count(e => e.Value.Kind == Kind.Hooked && !e.Value.Pending),
            NonPendingWithoutACase(empty).Length);

        // The real registry check: a missing class, a column that is not Hooked, and a runnable test in a class that is
        // not the collection census, each fail.
        var broken = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["query_stats.query_text"] = "NoSuchClass.NoSuchMethod",
            ["query_stats.query_plan_hash"] = "StatementColumnCensusTests.EveryEntry_CarriesItsReasonRowAndLane",
        };
        Assert.Equal(2, RegistryProblems(broken).Length);

        // Over the fixture census class (named FixtureCensus here): only a runnable test whose source drives the
        // recording writer for the column is accepted.
        var types = new[] { typeof(FixtureCensus), typeof(FixtureOtherClass) };
        string Driving(Type _) => "var w = new StatementScrubRecordingWriter(); // query_text";
        string Unrelated(Type _) => "plain test with no harness";

        string[] Check(string test, Func<Type, string> source) =>
            RegistryProblems(
                new Dictionary<string, string>(StringComparer.Ordinal) { ["query_stats.query_text"] = test },
                types, nameof(FixtureCensus), source);

        Assert.Empty(Check("FixtureCensus.Runs", Driving));
        Assert.Single(Check("FixtureCensus.Skipped", Driving));
        Assert.Single(Check("FixtureCensus.NotRunByDefault", Driving));
        Assert.Single(Check("FixtureCensus.SkippedTheory", Driving));
        Assert.Single(Check("FixtureCensus.NoAttribute", Driving));
        Assert.Single(Check("FixtureCensus.Missing", Driving));
        Assert.Single(Check("FixtureOtherClass.Runs", Driving));
        Assert.Single(Check("FixtureCensus.Runs", Unrelated));
        Assert.Single(Check("FixtureCensus.Runs", _ => "new StatementScrubRecordingWriter(); query_text; Assert.SkipWhen(x, y);"));
    }

    // ── writers outside the definitions ──

    /// <summary>
    /// A writer that can put statement text, a plan or a caller's arguments into a store without going through a
    /// collector definition's payload columns. <c>Sites</c> are the (file, type, method) places the hook may live;
    /// every site must still exist, and a non-pending entry needs a <c>SensitiveStatements</c> call in at least one.
    /// </summary>
    private sealed record Watched(
        string Name, string Row, string Lane, bool Pending, params (string File, string Type, string Method)[] Sites);

    private const string Runner = "Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs";

    private static readonly Watched[] WatchedWriters =
    {
        // 6.7 row 7: Query Store plans by id. The runner fetches and hands them to the plan writer.
        new("query store plans by id", "6.7 row 7", "R3", true,
            (Runner, "DarlingCollectorRunner", "FetchAndStorePlansAsync"),
            ("Darling/PerformanceMonitor.Darling.Storage/QueryStorePlanWriter.cs", "QueryStorePlanWriter", "WriteAsync")),

        // 6.7 row 6: the Query Store text store.
        new("query store text store", "6.7 row 6", "R3", true,
            (Runner, "DarlingCollectorRunner", "FetchAndStoreQueryTextAsync"),
            ("Darling/PerformanceMonitor.Darling.Storage/QueryStoreTextWriter.cs", "QueryStoreTextWriter", "WriteAsync")),

        // 6.7 row 20: the oversized-plan sweep fetches one plan and records it in the backlog.
        new("oversized plan sweep", "6.7 row 20", "R6", true,
            ("Darling/PerformanceMonitor.Darling.Service/OversizedPlanBacklogSweep.cs", "OversizedPlanBacklogSweep", "FetchOnePlanAsync")),

        // The slow-read log stores up to 4 KB of a slow call's arguments (a plan or a statement a caller passed).
        new("slow-read log: the MCP filter's offer", "plan section 1 (slow-read log)", "L7 and L8", true,
            ("Darling/PerformanceMonitor.Darling.Service/Mcp/McpToolLatencyFilter.cs", "McpToolLatencyFilter", "OfferSlow")),
        new("slow-read log: the log's offer", "plan section 1 (slow-read log)", "L7 and L8", true,
            ("Darling/PerformanceMonitor.Darling.Service/SlowReadLog.cs", "SlowReadLog", "Offer")),
    };

    private static readonly Regex SessionCall = new(
        @"SensitiveStatements\s*\.\s*(Session|Text|Xml|Json)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The source with comments, and the inside of string and character literals, blanked to spaces
    /// (newlines kept), so a brace or a name in a comment or a string is not read as code.</summary>
    internal static string CodeOnly(string source)
    {
        var sb = new StringBuilder(source.Length);
        var i = 0;
        while (i < source.Length)
        {
            var c = source[i];
            var next = i + 1 < source.Length ? source[i + 1] : '\0';

            if (c == '/' && next == '/')
            {
                while (i < source.Length && source[i] != '\n')
                {
                    sb.Append(' ');
                    i++;
                }
            }
            else if (c == '/' && next == '*')
            {
                var end = source.IndexOf("*/", i + 2, StringComparison.Ordinal);
                end = end < 0 ? source.Length : end + 2;
                for (; i < end; i++)
                {
                    sb.Append(source[i] == '\n' ? '\n' : ' ');
                }
            }
            else if (c == '"' && source.AsSpan(i).StartsWith("\"\"\"", StringComparison.Ordinal))
            {
                var end = source.IndexOf("\"\"\"", i + 3, StringComparison.Ordinal);
                end = end < 0 ? source.Length : end + 3;
                for (; i < end; i++)
                {
                    sb.Append(source[i] == '\n' ? '\n' : ' ');
                }
            }
            else if (c == '"' || c == '\'')
            {
                var verbatim = c == '"' && i > 0 && (source[i - 1] == '@' || (i > 1 && source[i - 1] == '$' && source[i - 2] == '@'));
                sb.Append(' ');
                i++;
                while (i < source.Length)
                {
                    var d = source[i];
                    if (!verbatim && d == '\\')
                    {
                        sb.Append("  ");
                        i += 2;
                        continue;
                    }

                    if (d == c)
                    {
                        if (verbatim && i + 1 < source.Length && source[i + 1] == c)
                        {
                            sb.Append("  ");
                            i += 2;
                            continue;
                        }

                        sb.Append(' ');
                        i++;
                        break;
                    }

                    if (d == '\n' && !verbatim)
                    {
                        break;
                    }

                    sb.Append(d == '\n' ? '\n' : ' ');
                    i++;
                }
            }
            else
            {
                sb.Append(c);
                i++;
            }
        }

        return sb.ToString();
    }

    /// <summary>The bodies of every declaration of <paramref name="method"/> in <paramref name="source"/> (empty
    /// when the type or the method is not there). A declaration is a line that starts with modifiers or a return
    /// type and then the name and "(", followed by a block body.</summary>
    internal static List<string> BodiesOf(string source, string type, string method)
    {
        var bodies = new List<string>();
        var code = CodeOnly(source);
        if (!Regex.IsMatch(code, @"\b(class|struct|record|interface)\s+" + Regex.Escape(type) + @"\b"))
        {
            return bodies;
        }

        var declaration = new Regex(
            @"^[ \t]*(?!(?:return|await|new|else|throw|yield)\b)(?:[\w<>\[\],.?()]+[ \t]+)+" + Regex.Escape(method) + @"[ \t]*(?:<[^>\r\n]*>)?[ \t]*\(",
            RegexOptions.Multiline | RegexOptions.CultureInvariant);

        foreach (Match m in declaration.Matches(code))
        {
            var depth = 0;
            var i = m.Index + m.Length - 1;
            for (; i < code.Length; i++)
            {
                if (code[i] == '(')
                {
                    depth++;
                }
                else if (code[i] == ')' && --depth == 0)
                {
                    break;
                }
            }

            var open = i;
            while (open < code.Length && code[open] != '{' && code[open] != ';'
                && !(code[open] == '=' && open + 1 < code.Length && code[open + 1] == '>'))
            {
                open++;
            }

            if (open >= code.Length || code[open] != '{')
            {
                continue; // an abstract, interface or expression-bodied member: no block body to scan
            }

            var braces = 0;
            var close = open;
            for (; close < code.Length; close++)
            {
                if (code[close] == '{')
                {
                    braces++;
                }
                else if (code[close] == '}' && --braces == 0)
                {
                    break;
                }
            }

            bodies.Add(code[open..Math.Min(close + 1, code.Length)]);
        }

        return bodies;
    }

    private static bool HasSessionCall(string body) => SessionCall.IsMatch(body);

    /// <summary>The problems with a watched list, reading each site's file through <paramref name="read"/>.</summary>
    private static string[] WatchedProblems(IEnumerable<Watched> writers, Func<string, string?> read)
    {
        var problems = new List<string>();
        foreach (var w in writers)
        {
            if (string.IsNullOrWhiteSpace(w.Lane) || string.IsNullOrWhiteSpace(w.Row))
            {
                problems.Add(w.Name + " needs a plan row and an owning lane");
            }

            var hooked = false;
            foreach (var (file, type, method) in w.Sites)
            {
                var source = read(file);
                var bodies = source is null ? new List<string>() : BodiesOf(source, type, method);
                if (bodies.Count == 0)
                {
                    problems.Add(w.Name + ": " + type + "." + method + " in " + file + " no longer exists; update WatchedWriters");
                    continue;
                }

                hooked |= bodies.Any(HasSessionCall);
            }

            if (!w.Pending && !hooked)
            {
                problems.Add(w.Name + " is not pending but none of its sites calls SensitiveStatements");
            }
        }

        return problems.ToArray();
    }

    private static string? ReadSiteFile(string relative)
    {
        var path = RepoFile.PathTo(relative);
        return File.Exists(path) ? File.ReadAllText(path) : null;
    }

    [Fact]
    public void EveryWatchedWriter_StillExists_AndANonPendingOneCallsTheFilter()
    {
        var problems = WatchedProblems(WatchedWriters, ReadSiteFile);

        Assert.True(problems.Length == 0, string.Join("; ", problems));
        Assert.Equal(WatchedWriters.Length, WatchedWriters.Select(w => w.Name).Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void TheWatchedScan_SeesACallAMissingMethodAndACommentedCall()
    {
        const string Source = @"
namespace N
{
    public sealed class W
    {
        // SensitiveStatements.Session() in a comment is not a call
        public void Plain(string s)
        {
            var braces = ""{ not a block }""; /* SensitiveStatements.Text(s) */
        }

        internal async Task Hooked(string s)
        {
            var session = new SensitiveStatements.Session();
            Use(session.Text(s));
        }
    }
}";
        Assert.Single(BodiesOf(Source, "W", "Plain"));
        Assert.False(HasSessionCall(BodiesOf(Source, "W", "Plain")[0]));
        Assert.True(HasSessionCall(BodiesOf(Source, "W", "Hooked")[0]));
        Assert.Empty(BodiesOf(Source, "W", "Gone"));
        Assert.Empty(BodiesOf(Source, "Other", "Hooked"));

        var one = new Watched("planted", "6.7 row 0", "R0", false, ("f.cs", "W", "Plain"));
        Assert.Single(WatchedProblems(new[] { one }, _ => Source));
        Assert.Empty(WatchedProblems(new[] { one with { Sites = new[] { ("f.cs", "W", "Hooked") } } }, _ => Source));
        Assert.Single(WatchedProblems(new[] { one with { Pending = true, Sites = new[] { ("f.cs", "W", "Gone") } } }, _ => Source));
        Assert.Single(WatchedProblems(new[] { one with { Pending = true } }, _ => null));
    }

    // ── the release gate ──

    private static string[] PendingEntries() =>
        Listed.Where(e => e.Value.Kind == Kind.Hooked && e.Value.Pending)
            .Select(e => "column " + e.Key + " (" + e.Value.Lane + ")")
            .Concat(WatchedWriters.Where(w => w.Pending).Select(w => "writer " + w.Name + " (" + w.Lane + ")"))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();

    /// <summary>
    /// The release-cut gate (see the release-checklist skill). Run by name; with <c>DARLING_RELEASE_CUT=1</c> it
    /// fails while any statement-column census entry or watched writer is pending, and its message is the pending
    /// count and the list. Without the variable it passes and prints the count, so CI is unaffected.
    /// </summary>
    [Fact]
    public void StatementCensus_PendingEntries_BlockARelease()
    {
        var pending = PendingEntries();
        var summary = "Statement-column census pending entries: " + pending.Length +
            (pending.Length == 0 ? string.Empty : " -> " + string.Join("; ", pending));

        Console.WriteLine(summary);

        if (Environment.GetEnvironmentVariable("DARLING_RELEASE_CUT") == "1")
        {
            Assert.True(pending.Length == 0, "A release cut needs every statement-column census entry hooked. " + summary);
        }
    }
}
