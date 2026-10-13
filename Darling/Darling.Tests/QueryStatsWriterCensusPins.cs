/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: the census of everything that writes <c>collect.query_stats</c>. The hourly row-count ledger
/// (<c>collect.query_stats_hour_ledger</c>, <see cref="QueryStatsHourLedger"/>) counts rows at ONE write: the collector
/// runner's COPY, <c>DarlingCollectorRunner.CopyBatchOnceAsync</c>, adds each batch's non-restart count in the COPY's own
/// transaction. The long-window count guard then compares the ledger with the rollup, per (server, hour), and takes a
/// rollup that agrees as complete. A second writer would put rows into raw, and so into the hourly rollup, that no
/// ledger entry accounts for. If those rows are late (below the rollup's watermark, before the next refresh) the rollup
/// and the ledger both lack them, the guard passes, and a panel serves a rollup that is missing rows as exact. That is
/// the unsafe case, a writer the ledger does not count whose rows the rollup also lacks, and the guard cannot see it for
/// itself, so this census guards it for this build's code: the expected writer set is exactly
/// <c>{CopyBatchOnceAsync}</c>, and a second writer fails here with the reason. The census reads this build's source
/// only. It cannot see an older service that still writes raw rows into a V164 store (a rolling upgrade, or a
/// downgrade): the guard catches those rows wherever the rollup holds them and misses them in one known case, an hour
/// the rollup never materialized whose raw rows all came from the older service
/// (<see cref="IntervalRollupCountGuard"/> names it).
///
/// <para>Three arms. (1) A literal scan of every non-test source in the service's <c>ProjectReference</c> closure (see
/// below) for a DML verb aimed at the table. (2) Every
/// <c>BeginBinaryImportAsync</c> site must be on a register, because the real COPY is built at run time
/// (<c>PgCollectorRowWriter.CopyCommandFor(definition)</c> appends <c>schema.TargetTable</c>), so arm 1 cannot see it:
/// the runner's one site over the generic definition, and the four RDS ingestors, each over its own fixed definition
/// with a named target table that is not this one. (3) No migration rung above the ledger's own may run DML on the table.
/// The scan must also be shown to have looked: it asserts the number of files it read and that the files it names were
/// among them, and each arm carries a positive control.</para>
///
/// <para><b>What the scan covers.</b> The scan root is the service's compile closure, not a directory glob: every
/// <c>Darling/PerformanceMonitor.*</c> project and every project those reference through a <c>ProjectReference</c>,
/// transitively. The service and storage projects compile six root libraries into the same binary
/// (<c>PerformanceMonitor.Alerting</c>, <c>.Analysis</c>, <c>.Collectors</c>, <c>.Common</c>, <c>.Notifications</c> and
/// <c>.PlanAnalysis</c>; <c>QueryStatsCollector</c> itself is in <c>.Collectors</c>), and the viewer adds
/// <c>PerformanceMonitor.Ui</c>, so a writer added to any of them is seen. The tests project is outside the closure by
/// construction, since nothing references it.</para>
///
/// <para><b>Known blind spots, stated rather than implied.</b> Dynamic SQL is invisible to a text scan. Arm 2 closes the
/// COPY path, which is how a collector writes. Run-time-built DML outside a COPY, such as <c>INSERT INTO collect.{t}</c>
/// from a table variable, is NOT seen by arm 1, and neither is DML built by an interpolated string, a
/// <c>StringBuilder</c> or a concatenation across literals; the writers of that shape in the tree today are the payload
/// dimensions and the Query Store slice repair, and neither names this table. Dynamic SQL inside a migration rung is
/// closed by <c>MigrationDataMovingRungCensusPins.TheLadderStillContainsNoDynamicSql</c>, which keeps the ladder free of
/// it. SQL in a file that is not <c>.cs</c> is not read; none exists in these projects today (no <c>*.sql</c> file and no
/// SQL <c>EmbeddedResource</c>). Database-side writers are not scanned: a trigger, a rule, a function or a scheduled
/// job, including one a rung above the ledger's own creates with <c>CREATE TRIGGER</c> or <c>CREATE RULE</c>, which arm
/// 3's verb list does not flag. A COPY reached through a method group (<c>Func&lt;...&gt; f = conn.BeginBinaryImportAsync;</c>)
/// has no <c>(</c> after the name, so arm 2 does not see it. An UPDATE in place is invisible to the ledger as it is to
/// the guard it feeds, so arm 1 lists UPDATE to make a new one a decision rather than an accident. Retention's DELETE is
/// deliberately not a writer here: the guard reads no raw rows, so a row-by-row DELETE of raw shows only after the
/// rollup refreshes over it, as a ledger above the rollup, which fails safe; retention never deletes inside a window the
/// route takes (its window starts inside raw's retention).</para>
/// </summary>
public sealed class QueryStatsWriterCensusPins
{
    private const string RunnerFile = "PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs";
    private const string TheWriter = "CopyBatchOnceAsync";
    private const string TableName = "query_stats";
    private const int LedgerRung = 164;

    private const RegexOptions Opts = RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase;

    /// <summary>
    /// A write verb aimed at the table, in code or in a string literal (the SQL lives in literals): INSERT INTO, COPY,
    /// UPDATE or MERGE INTO, the schema prefix optional, either part optionally quoted. Word-bounded after the name with
    /// <c>(?![\w$])</c>, so <c>query_stats_hour_ledger</c>, <c>query_stats_interval_hourly</c> and every other
    /// <c>query_stats_*</c> relation are not this table.
    /// </summary>
    private static readonly Regex s_sourceDml = new(
        @"\b(?:INSERT\s+INTO|COPY|UPDATE|MERGE\s+INTO)\s+(?:ONLY\s+)?(?:[\\""]{0,2}collect[\\""]{0,2}\s*\.\s*)?[\\""]{0,2}" + TableName + @"[\\""]{0,2}(?![\w$])",
        Opts);

    /// <summary>The migration arm's verbs: the four above plus DELETE FROM and TRUNCATE, since a rung that removes rows
    /// from raw is as much a decision for the ledger as one that adds them.</summary>
    private static readonly Regex s_rungDml = new(
        @"\b(?:INSERT\s+INTO|COPY|UPDATE|MERGE\s+INTO|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?)\s+(?:ONLY\s+)?(?:[\\""]{0,2}collect[\\""]{0,2}\s*\.\s*)?[\\""]{0,2}" + TableName + @"[\\""]{0,2}(?![\w$])",
        Opts);

    /// <summary>Every way Npgsql starts a COPY that loads rows: binary or text import, or the raw copy stream.</summary>
    private static readonly Regex s_importSite = new(
        @"\bBegin(?:Binary|Text)Import(?:Async)?\s*\(|\bBeginRawBinaryCopy(?:Async)?\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_copyCommandArgument = new(
        @"^PgCollectorRowWriter\s*\.\s*CopyCommandFor\s*\(\s*(?<def>\w+)\s*\)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_definitionDeclaration = new(
        @"\bvar\s+definition\s*=\s*(?<expr>[\w.]+)\s*;",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_copyBatchOnceDeclaration = new(
        @"\bTask(?:<[^>(]+>)?\s+" + TheWriter + @"\s*(?:<[^>(]*>)?\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>One COPY start found in the source: where it is, what it was handed, and the definition it names.</summary>
    internal sealed record ImportSite(string File, int Line, string Argument, string Definition, int Offset);

    /// <summary>
    /// The register of COPY sites that are NOT the runner's: each RDS ingestor copies over its own fixed collector
    /// definition into a named table that is not <c>query_stats</c>. The table is declared here and compared with the
    /// definition's real <c>TargetTable</c>, so a changed definition fails the fact below rather than passing quietly.
    /// </summary>
    private static readonly SortedDictionary<string, (string Definition, string Table, Func<string> ActualTable)> s_fixedSites =
        new(StringComparer.Ordinal)
        {
            ["PerformanceMonitor.Darling.Service/Targets/RdsCpuIngestor.cs"] =
                ("PgCpuUtilizationCollector.Instance", "pg_cpu_utilization", () => PgCpuUtilizationCollector.Instance.TargetTable),
            ["PerformanceMonitor.Darling.Service/Targets/RdsDeadlockIngestor.cs"] =
                ("PgDeadlocksCollector.Instance", "pg_deadlocks", () => PgDeadlocksCollector.Instance.TargetTable),
            ["PerformanceMonitor.Darling.Service/Targets/RdsLogEventIngestor.cs"] =
                ("PgLogEventsCollector.Instance", "pg_log_events", () => PgLogEventsCollector.Instance.TargetTable),
            ["PerformanceMonitor.Darling.Service/Targets/RdsPlanIngestor.cs"] =
                ("PgPlanCaptureCollector.Instance", "pg_plan_capture", () => PgPlanCaptureCollector.Instance.TargetTable),
        };

    private static string WhyItMatters(string what) =>
        what + " The hourly row-count ledger (collect.query_stats_hour_ledger, #4605) counts non-restart rows at ONE write, "
        + "DarlingCollectorRunner.CopyBatchOnceAsync, in the COPY's own transaction (QueryStatsHourLedger.UpsertSql). A row "
        + "written anywhere else is in raw and in the hourly rollup but not in the ledger, and if it is late (below the "
        + "rollup's watermark, before the next refresh) the count guard passes on a rollup that is missing it and a panel "
        + "serves the result as exact. Count the new writer's rows into the ledger in the same transaction, or write "
        + "through CopyBatchOnceAsync, then update this census and say why.";

    /* ---------------- the scan ---------------- */

    /// <summary>Every non-test source compiled into the Darling binaries: the <c>Darling/PerformanceMonitor.*</c> projects and
    /// every project they reference, transitively (the root <c>PerformanceMonitor.*</c> libraries ship in the same service),
    /// without <c>bin</c> and <c>obj</c>. The tests project is outside the closure because nothing references it. File keys
    /// are <c>ProjectName/relative/path.cs</c>.</summary>
    private static List<(string File, string Text)> LoadSources()
    {
        var pending = new Stack<string>(
            Directory.EnumerateDirectories(RepoFile.PathTo("Darling"), "PerformanceMonitor.*")
                .SelectMany(d => Directory.EnumerateFiles(d, "*.csproj")));
        var projects = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
        while (pending.Count > 0)
        {
            var csproj = Path.GetFullPath(pending.Pop());
            if (!projects.Add(csproj))
            {
                continue;
            }

            foreach (Match reference in Regex.Matches(File.ReadAllText(csproj), @"<ProjectReference\s+Include=""(?<path>[^""]+)"""))
            {
                pending.Push(Path.Combine(
                    Path.GetDirectoryName(csproj)!,
                    reference.Groups["path"].Value.Replace('\\', Path.DirectorySeparatorChar)));
            }
        }

        var sources = new List<(string File, string Text)>();
        foreach (var csproj in projects)
        {
            var projectDir = Path.GetDirectoryName(csproj)!;
            var project = Path.GetFileName(projectDir);
            foreach (var path in Directory.EnumerateFiles(projectDir, "*.cs", SearchOption.AllDirectories))
            {
                var rel = Path.GetRelativePath(projectDir, path).Replace('\\', '/');
                if (rel.StartsWith("obj/", StringComparison.Ordinal) || rel.StartsWith("bin/", StringComparison.Ordinal)
                    || rel.Contains("/obj/", StringComparison.Ordinal) || rel.Contains("/bin/", StringComparison.Ordinal))
                {
                    continue;
                }

                sources.Add((project + "/" + rel, File.ReadAllText(path)));
            }
        }

        return sources;
    }

    /// <summary>Comments blanked, string literals KEPT (the SQL lives in them), newlines preserved.</summary>
    private static string WithoutComments(string text)
    {
        var mask = CSharpSourceWalker.CodeMask(text);
        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
        {
            for (var i = start; i < start + body.Length && i < mask.Length; i++)
            {
                mask[i] = true;
            }
        }

        var chars = text.ToCharArray();
        for (var i = 0; i < chars.Length; i++)
        {
            if (!mask[i] && chars[i] != '\n')
            {
                chars[i] = ' ';
            }
        }

        return new string(chars);
    }

    private static int LineOf(string text, int offset) => 1 + text.AsSpan(0, offset).Count('\n');

    /// <summary>Arm 1: every literal DML hit on the table, as <c>file:line (matched text)</c>.</summary>
    internal static IReadOnlyList<string> FindLiteralWriters(IEnumerable<(string File, string Text)> sources)
    {
        var found = new List<string>();
        foreach (var (file, text) in sources)
        {
            var code = WithoutComments(text);
            foreach (Match hit in s_sourceDml.Matches(code))
            {
                found.Add($"{file}:{LineOf(code, hit.Index).ToString(CultureInfo.InvariantCulture)} ({Regex.Replace(hit.Value, @"\s+", " ")})");
            }
        }

        return found;
    }

    /// <summary>Arm 2: every COPY start in CODE (comments and string literals blanked, so a method named in prose or in
    /// an error message is not a site), with its first argument read from the original text.</summary>
    internal static IReadOnlyList<ImportSite> FindImportSites(IEnumerable<(string File, string Text)> sources)
    {
        var found = new List<ImportSite>();
        foreach (var (file, text) in sources)
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);
            foreach (Match hit in s_importSite.Matches(code))
            {
                var from = hit.Index + hit.Length;
                var depth = 0;
                var end = code.Length;
                for (var i = from; i < code.Length; i++)
                {
                    var c = code[i];
                    if (c == '(')
                    {
                        depth++;
                    }
                    else if (c == ')' && depth-- == 0)
                    {
                        end = i;
                        break;
                    }
                    else if (c == ',' && depth == 0)
                    {
                        end = i;
                        break;
                    }
                }

                var argument = Regex.Replace(text[from..end].Trim(), @"\s+", " ");
                var named = s_copyCommandArgument.Match(argument);
                found.Add(new ImportSite(file, LineOf(code, hit.Index), argument, named.Success ? named.Groups["def"].Value : string.Empty, hit.Index));
            }
        }

        return found;
    }

    /// <summary>The offsets the runner's <c>CopyBatchOnceAsync</c> body spans in the comment-and-string-stripped code, or null.</summary>
    private static (int Start, int End, int Declarations)? CopyBatchOnceSpan(string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var declarations = s_copyBatchOnceDeclaration.Matches(code);
        if (declarations.Count == 0)
        {
            return null;
        }

        var open = code.IndexOf('{', declarations[0].Index);
        return (open, open + CSharpSourceWalker.BraceBalanced(code, open).Length, declarations.Count);
    }

    /// <summary>Names the writer a generic COPY site belongs to: the method when it is the runner's, else its file and line.</summary>
    private static string WriterOf(ImportSite site, IReadOnlyDictionary<string, string> textByFile)
    {
        if (site.File == RunnerFile && CopyBatchOnceSpan(textByFile[site.File]) is { } span
            && site.Offset > span.Start && site.Offset < span.End)
        {
            return TheWriter;
        }

        return $"{site.File}:{site.Line.ToString(CultureInfo.InvariantCulture)}";
    }

    /// <summary>The expected writer set, over any sources: every literal DML hit, every COPY site that is not on the fixed
    /// register, named as the writer it is.</summary>
    internal static SortedSet<string> WriterSet(IReadOnlyList<(string File, string Text)> sources)
    {
        var textByFile = sources.ToDictionary(s => s.File, s => s.Text, StringComparer.Ordinal);
        var writers = new SortedSet<string>(FindLiteralWriters(sources), StringComparer.Ordinal);
        foreach (var site in FindImportSites(sources))
        {
            if (s_fixedSites.TryGetValue(site.File, out var allowed) && site.Definition == "definition"
                && DeclaredDefinitions(textByFile[site.File]).SequenceEqual(new[] { allowed.Definition }))
            {
                continue;
            }

            writers.Add(site.File == RunnerFile && site.Definition == "definition"
                ? WriterOf(site, textByFile)
                : $"unlisted COPY site {site.File}:{site.Line.ToString(CultureInfo.InvariantCulture)} ({site.Argument})");
        }

        return writers;
    }

    /// <summary>The expressions a file assigns to its <c>var definition</c>: the fixed definition an RDS ingestor copies over.</summary>
    private static List<string> DeclaredDefinitions(string text) =>
        s_definitionDeclaration.Matches(WithoutComments(text)).Select(m => m.Groups["expr"].Value).ToList();

    private static string StripSqlComments(string sql) =>
        Regex.Replace(Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline), @"--[^\n]*", " ");

    /* ---------------- the census ---------------- */

    [Fact]
    public void TheWritersOfQueryStats_AreExactly_CopyBatchOnceAsync()
    {
        var sources = LoadSources();

        /* The scan must have looked: a glob that moved or a read that found nothing would otherwise satisfy "no
           second writer" for the wrong reason. The Collectors file is outside Darling/, so finding it shows the
           ProjectReference walk followed the references into the root projects (#4605). */
        Assert.True(sources.Count > 300, $"the census read only {sources.Count.ToString(CultureInfo.InvariantCulture)} source files; its project-reference walk no longer reaches the tree");
        var files = sources.Select(s => s.File).ToHashSet(StringComparer.Ordinal);
        foreach (var known in new[]
                 {
                     RunnerFile,
                     "PerformanceMonitor.Darling.Service/DarlingRetention.cs",
                     "PerformanceMonitor.Darling.Storage/PgCollectorRowWriter.cs",
                     "PerformanceMonitor.Darling.Storage/QueryStatsHourLedger.cs",
                     "PerformanceMonitor.Collectors/QueryStatsCollector.cs",
                 })
        {
            Assert.Contains(known, files);
        }

        var writers = WriterSet(sources);

        Assert.True(
            writers.SetEquals(new[] { TheWriter }),
            WhyItMatters(
                "The writers of collect.query_stats must be exactly {" + TheWriter + "}. Found: {"
                + string.Join(", ", writers) + "}."));
    }

    [Fact]
    public void TheCopySites_AreExactlyTheRunnersAndTheFourRdsIngestors_EachOverAFixedDefinitionWithANamedTable()
    {
        var sources = LoadSources();
        var sites = FindImportSites(sources);

        /* The runner's one site, and one per RDS ingestor, and nothing else. */
        var expected = new SortedSet<string>(s_fixedSites.Keys.Append(RunnerFile), StringComparer.Ordinal);
        var actual = new SortedSet<string>(sites.Select(s => s.File), StringComparer.Ordinal);
        Assert.True(
            actual.SetEquals(expected),
            WhyItMatters(
                "Every COPY start must be on the register. Found COPY starts in: {" + string.Join(", ", actual)
                + "}, expected: {" + string.Join(", ", expected) + "}."));
        Assert.Equal(expected.Count, sites.Count);

        var textByFile = sources.ToDictionary(s => s.File, s => s.Text, StringComparer.Ordinal);
        foreach (var (file, allowed) in s_fixedSites)
        {
            /* The site copies over exactly the definition the register names, and the definition is a field of this
               file, declared where the site can be read. */
            var site = sites.Single(s => s.File == file);
            Assert.Equal("definition", site.Definition);
            var declaration = DeclaredDefinitions(textByFile[file]);
            Assert.True(
                declaration.SequenceEqual(new[] { allowed.Definition }),
                WhyItMatters($"{file} must copy over `{allowed.Definition}` only, found: {string.Join(", ", declaration)}."));

            /* The register's table is the definition's real target table, and is not this one. */
            Assert.Equal(allowed.Table, allowed.ActualTable());
            Assert.NotEqual(TableName, allowed.Table, StringComparer.OrdinalIgnoreCase);
        }

        /* The runner's site copies over the generic definition: the one that may be query_stats. */
        var runnerSite = sites.Single(s => s.File == RunnerFile);
        Assert.Equal("definition", runnerSite.Definition);
        Assert.Equal("PgCollectorRowWriter.CopyCommandFor(definition)", runnerSite.Argument);
    }

    [Fact]
    public void TheRunnersCopySite_SitsInsideCopyBatchOnceAsync_WhichIsDeclaredOnce()
    {
        var text = File.ReadAllText(RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));
        var span = CopyBatchOnceSpan(text);
        Assert.True(span is not null, "DarlingCollectorRunner.CopyBatchOnceAsync is gone or renamed: the writer this census names no longer exists (#4605)");
        Assert.Equal(1, span!.Value.Declarations);

        var inRunner = FindImportSites(new[] { (RunnerFile, text) });
        var only = Assert.Single(inRunner);
        Assert.True(
            only.Offset > span.Value.Start && only.Offset < span.Value.End,
            WhyItMatters("The runner's COPY start moved out of CopyBatchOnceAsync."));
    }

    [Fact]
    public void OnlyTheQueryStatsCollector_HasQueryStatsAsItsTargetTable()
    {
        /* A second definition over the same table would write it through the same runner COPY, but under another
           payload and possibly another notion of a restart row. */
        var definitions = CollectorCatalog.All
            .Where(d => string.Equals(d.TargetTable, TableName, StringComparison.OrdinalIgnoreCase))
            .Select(d => d.GetType().Name)
            .ToList();

        Assert.True(
            definitions.SequenceEqual(new[] { nameof(QueryStatsCollector) }),
            WhyItMatters("Exactly one collector definition may target collect.query_stats. Found: {" + string.Join(", ", definitions) + "}."));
        Assert.Equal(TableName, QueryStatsCollector.Instance.TargetTable);
    }

    [Fact]
    public void NoMigrationRungAboveTheLedgersOwn_RunsDmlOnQueryStats()
    {
        /* The ladder must reach the ledger's rung, or this arm would pass on an empty set for ever. */
        var rung = PgMigrations.Scripts.Single(m => m.Version == LedgerRung);
        Assert.Equal("query-stats-hour-ledger", rung.Name);
        Assert.True(PgMigrations.Scripts.Max(m => m.Version) >= LedgerRung, "the ladder lost the ledger's rung");

        var offenders = PgMigrations.Scripts
            .Where(m => m.Version > LedgerRung)
            .Where(m => s_rungDml.IsMatch(StripSqlComments(m.Sql)))
            .Select(m => $"V{m.Version.ToString(CultureInfo.InvariantCulture)} ({m.Name})")
            .ToList();

        Assert.True(
            offenders.Count == 0,
            WhyItMatters("Rung(s) above V" + LedgerRung.ToString(CultureInfo.InvariantCulture) + " run DML on collect.query_stats: " + string.Join(", ", offenders) + "."));

        /* Control on a real rung: the ledger's own, which names query_stats_hour_ledger and inserts into its state
           table, is not a hit. The word boundary is what keeps that true. */
        Assert.False(s_rungDml.IsMatch(StripSqlComments(rung.Sql)), "the word boundary no longer separates query_stats from query_stats_hour_ledger");
    }

    /* ---------------- the detectors, on fabricated sources ---------------- */

    [Theory]
    [InlineData("var sql = \"INSERT INTO collect.query_stats (a) VALUES (1)\";")]
    [InlineData("var sql = @\"insert   into\r\n query_stats (a) values (1)\";")]
    [InlineData("var sql = \"COPY collect.query_stats (a) FROM STDIN (FORMAT BINARY)\";")]
    [InlineData("var sql = \"UPDATE collect.query_stats SET sample_interval_seconds = 0\";")]
    [InlineData("var sql = \"UPDATE ONLY query_stats SET a = 1\";")]
    [InlineData("var sql = \"MERGE INTO collect.query_stats AS t USING x ON t.a = x.a\";")]
    [InlineData("var sql = \"INSERT INTO \\\"collect\\\".\\\"query_stats\\\" (a) VALUES (1)\";")]
    public void TheLiteralScan_FindsAPlantedWriter_AndNamesItsFileAndLine(string line)
    {
        var found = FindLiteralWriters(new[] { ("PerformanceMonitor.Darling.Service/Planted.cs", "// header\r\n" + line) });

        var hit = Assert.Single(found);
        Assert.StartsWith("PerformanceMonitor.Darling.Service/Planted.cs:2 (", hit, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("var sql = \"INSERT INTO collect.query_stats_hour_ledger (a) VALUES (1)\";")]
    [InlineData("var sql = \"UPDATE collect.query_stats_hour_ledger_state SET a = 1\";")]
    [InlineData("var sql = \"COPY collect.query_stats_interval_hourly FROM STDIN\";")]
    [InlineData("var sql = \"SELECT count(*) FROM collect.query_stats WHERE a = 1\";")]
    [InlineData("var sql = \"DELETE FROM collect.query_stats WHERE collection_time < $1\";")]
    [InlineData("// INSERT INTO collect.query_stats is described here, not run\r\nvar x = 1;")]
    [InlineData("/* COPY query_stats */ var x = 1;")]
    public void TheLiteralScan_IgnoresOtherTables_ReadsRetentionDeletesAndProse(string line)
    {
        Assert.Empty(FindLiteralWriters(new[] { ("Fake.cs", line) }));
    }

    [Fact]
    public void TheCopyScan_FindsAnUnlistedSite_NamesItsArgument_AndIgnoresProseAndStrings()
    {
        var sites = FindImportSites(new[]
        {
            ("A.cs", "using var i = await connection.BeginBinaryImportAsync(\r\n    PgCollectorRowWriter.CopyCommandFor(QueryStatsCollector.Instance), token);"),
            ("B.cs", "using var i = c.BeginTextImport(\"COPY collect.query_stats FROM STDIN\");"),
            ("C.cs", "// BeginBinaryImportAsync is named in prose\r\nvar m = \"the BeginBinaryImportAsync call\";"),
        });

        Assert.Equal(2, sites.Count);
        Assert.Equal("PgCollectorRowWriter.CopyCommandFor(QueryStatsCollector.Instance)", sites[0].Argument);
        Assert.Equal(string.Empty, sites[0].Definition);
        Assert.Equal("\"COPY collect.query_stats FROM STDIN\"", sites[1].Argument);

        var writers = WriterSet(new[]
        {
            ("PerformanceMonitor.Darling.Service/Targets/Planted.cs", "using var i = await c.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), t);"),
        });
        Assert.Contains(writers, w => w.StartsWith("unlisted COPY site PerformanceMonitor.Darling.Service/Targets/Planted.cs:1", StringComparison.Ordinal));
    }

    [Fact]
    public void TheMigrationArm_CatchesDmlOnTheTable_InEveryVerb_AndNothingElse()
    {
        foreach (var sql in new[]
                 {
                     "INSERT INTO collect.query_stats (a) SELECT 1;",
                     "UPDATE query_stats SET a = 1;",
                     "DELETE FROM collect.query_stats WHERE a = 1;",
                     "TRUNCATE TABLE collect.query_stats;",
                     "TRUNCATE query_stats;",
                     "MERGE INTO collect.query_stats t USING x ON true WHEN MATCHED THEN DELETE;",
                     "COPY collect.query_stats FROM STDIN;",
                 })
        {
            Assert.Matches(s_rungDml, StripSqlComments(sql));
        }

        foreach (var sql in new[]
                 {
                     "INSERT INTO collect.query_stats_hour_ledger_state (id) VALUES (1);",
                     "UPDATE collect.query_stats_hour_ledger SET n = 1;",
                     "SELECT * FROM collect.query_stats;",
                     "CREATE INDEX IF NOT EXISTS ix ON collect.query_stats (a);",
                     "-- INSERT INTO collect.query_stats is only a comment\nSELECT 1;",
                     "/* DELETE FROM query_stats */ SELECT 1;",
                 })
        {
            Assert.DoesNotMatch(s_rungDml, StripSqlComments(sql));
        }
    }
}
