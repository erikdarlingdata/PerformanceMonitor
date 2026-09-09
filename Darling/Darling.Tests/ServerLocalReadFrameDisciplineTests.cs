/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3198: <c>default_trace_events.event_time</c> is the monitored server's LOCAL wall clock, and every read
/// that returns or bounds it has to de-skew it to naive UTC first. This is the third instance of the class —
/// #2991/#2992 was <c>query_stats.creation_time</c>, #3198 the MCP tool, and the same PR's sweep found the
/// compose annotation overlay — so what is pinned here is the BOUNDARY rather than another example.
///
/// <para><b>Why the frame cannot be judged by column name.</b> Four of the five annotation sources call
/// their event column <c>event_time</c> too, and all four are UTC: they carry the XE <c>@timestamp</c>. Only
/// the Default Trace ships <c>fn_trace_gettable</c>'s <c>StartTime</c>. <c>CollectorTimestampFrameTests</c>
/// says this outright — "<c>system_health.event_time</c> is UTC under the same column name, which is why
/// <c>StoreSqlClockDisciplineTests</c> cannot judge either" — so a name-keyed rule would either miss the one
/// local column or condemn the four UTC ones. Every fact below is therefore scoped to a TABLE.</para>
///
/// <para><b>What each fact reaches, and what it does not.</b>
/// <see cref="EveryLiteralSqlReadOfTheDefaultTrace_DeSkewsEventTime"/> covers the reads whose table name is a
/// SQL literal — the two Darling constants and Lite's DuckDB read. The compose annotation query builds its
/// table name from the catalog at runtime, so no source scan can see it; that path is covered instead by
/// <see cref="EveryAnnotationSourcesDeclaredFrame_MatchesItsOwnCollectorsQueryText"/>, which checks the
/// DECLARED frame against the owning collector's own SQL, and by
/// <see cref="CompiledAnnotationSql_DeSkewsExactlyTheServerLocalSources"/>, which checks the compiler acts on
/// the declaration. That split is the honest shape of the coverage, not an accident of it.</para>
///
/// <para><b>The discriminators are pinned in both directions</b>
/// (<see cref="TheDiscriminators_FlagABareRead_AndPassADeSkewedOne"/>) because a scan whose matcher has
/// quietly stopped matching reports a clean bill of health, which is worse than no scan. Each named site
/// additionally asserts its ANCHOR is present before judging the file: the bare-read check keys on the
/// <c>dte</c> alias, so a rename would otherwise turn it into an assertion about nothing.</para>
/// </summary>
public sealed class ServerLocalReadFrameDisciplineTests
{
    /* ───────────────────────── the literal-SQL reads of default_trace_events ───────────────────────── */

    /// <summary>A SQL <c>FROM</c> naming the Default Trace table or its passthrough view. This is what makes a
    /// file a READER rather than DDL, prose or a catalog entry — the same table name appears in migrations, in
    /// the MCP instruction text and in <c>MeasureCatalog</c>, none of which touch a timestamp.</summary>
    private static readonly Regex ReadsTheTable =
        new(@"FROM\s+(?:collect\.)?(?:v_)?default_trace_events", RegexOptions.IgnoreCase);

    /// <summary>The Postgres de-skew, spelled on the column exactly as both Darling constants carry it.</summary>
    private static readonly Regex PgDeSkew =
        new(@"event_time\s*-\s*make_interval\s*\(\s*mins\s*=>\s*svr\.offset_minutes\s*\)");

    /// <summary>A bare <c>dte.event_time</c> — projected or compared — with no de-skew attached. The
    /// <c>dte</c> alias is the table scope: both Darling reads alias <c>default_trace_events AS dte</c>, and
    /// nothing else in the tree uses that alias, so this cannot reach the four UTC <c>event_time</c>
    /// columns.</summary>
    private static readonly Regex BareAliasedEventTime =
        new(@"dte\.event_time(?!\s*-\s*make_interval)");

    /// <summary>Lite's de-skew is in C#, not SQL: DuckDB gets the window in the server's local frame and the
    /// loader subtracts the offset from each row. Matched on the subtraction because that is the step that
    /// puts the RETURNED value in UTC; the window helper alone would leave the value local.</summary>
    private static readonly Regex LiteRowDeSkew =
        new(@"GetDateTime\(0\)\.AddMinutes\(\s*-\s*offset\s*\)");

    /// <summary>
    /// Every literal-SQL read of the Default Trace, with the de-skew form it must carry and why it differs.
    /// A floor AND a ceiling per file: a bare total would let a Darling site vanish and a Lite one appear and
    /// still add up, which is precisely the one-sided-port regression #2992 found nothing guarding against.
    /// </summary>
    private static readonly (string RelativePath, int PgSites, bool LiteRowDeSkew, string Why)[] KnownReaders =
    [
        ("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingDefaultTraceReader.cs", 3, false,
            "the get_default_trace_events MCP read: one projection + both window bounds"),
        ("Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.SystemEvents.cs", 3, false,
            "the viewer's System Events tab: the same three, byte-identical to the MCP read above"),
        ("Lite/Services/LocalDataService.SystemEvents.cs", 0, true,
            "Lite's DuckDB read: server-local window via GetTimeRangeServerLocal, then the row de-skewed in C#"),
    ];

    [Fact]
    public void EveryLiteralSqlReadOfTheDefaultTrace_DeSkewsEventTime()
    {
        var scanned = 0;
        var found = new List<string>();

        foreach (var path in ProductionSourceFiles())
        {
            scanned++;

            if (ReadsTheTable.IsMatch(File.ReadAllText(path)))
            {
                found.Add(Relative(path));
            }
        }

        /* A floor, so a broken glob cannot report a clean bill of health. Measured at 713 production .cs
           files across Darling/ and Lite/ when this was written. */
        Assert.True(scanned >= 400, $"scanned only {scanned} production source files — check the globs below");

        Assert.Equal(
            KnownReaders.Select(r => r.RelativePath).OrderBy(x => x, StringComparer.Ordinal).ToArray(),
            found.OrderBy(x => x, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void EveryKnownReader_CarriesItsDeSkew_AndNoBareEventTime()
    {
        foreach (var (relativePath, pgSites, liteRowDeSkew, why) in KnownReaders)
        {
            var path = RepoPath(relativePath);
            Assert.True(File.Exists(path), $"{relativePath} is gone — update this guard deliberately");
            var text = File.ReadAllText(path);

            Assert.Equal(pgSites, PgDeSkew.Matches(text).Count);
            Assert.Equal(liteRowDeSkew, LiteRowDeSkew.IsMatch(text));

            if (pgSites > 0)
            {
                /* The anchor first: the bare-read check below is scoped by the dte alias, so if the alias
                   were renamed the check would pass by matching nothing at all. */
                Assert.Contains("default_trace_events AS dte", text, StringComparison.Ordinal);

                var bare = BareAliasedEventTime.Matches(text).Count;

                Assert.True(
                    bare == 0,
                    $"{relativePath} ({why}) still reads a bare dte.event_time in {bare} place(s). "
                    + "The Default Trace StartTime is the monitored server's LOCAL wall clock, while as_of, "
                    + "collection_time, last_collection and every XE event_time are naive UTC — so an "
                    + "un-de-skewed value is wrong by the server's offset in the direction that inverts "
                    + "causality: 04:28 UTC renders as 00:28 at UTC-4 and reads as having PRECEDED the "
                    + "04:28 error it coincided with. Subtract the collected offset: "
                    + "event_time - make_interval(mins => svr.offset_minutes).");
            }
        }
    }

    /* ───────────────────────── the compose annotation overlay ───────────────────────── */

    /// <summary>The T-SQL assignment of a collector's own event-time column — this codebase writes
    /// <c>alias = expression</c>, so the right-hand side is the provenance.</summary>
    private static Regex TimeColumnAssignment(string column) =>
        new(@"(?<![\w.])" + Regex.Escape(column) + @"\s*=\s*([^\r\n]+)");

    /// <summary>
    /// Each annotation source's DECLARED clock frame, checked against the evidence in its own collector's
    /// query text rather than taken on trust. A column assigned from the XE <c>@timestamp</c> is UTC; one
    /// assigned from <c>fn_trace_gettable</c>'s <c>StartTime</c> is server-local. A collector whose
    /// assignment cannot be found, or whose assignments disagree, fails — an unreadable provenance is not
    /// evidence of a UTC one, and defaulting it to UTC is the failure this whole class is made of.
    /// </summary>
    [Fact]
    public void EveryAnnotationSourcesDeclaredFrame_MatchesItsOwnCollectorsQueryText()
    {
        Assert.NotEmpty(MeasureCatalog.AnnotationSources);

        foreach (var source in MeasureCatalog.AnnotationSources)
        {
            var collector = CollectorCatalog.All.Single(c =>
                string.Equals(c.TargetTable, source.SourceTable, StringComparison.Ordinal));

            var file = collector.GetType().Name + ".cs";
            var path = RepoPath("PerformanceMonitor.Collectors/" + file);
            Assert.True(File.Exists(path), $"{source.Key}: collector source not found at {path}");

            var sql = WithoutComments(File.ReadAllText(path));
            var assignments = TimeColumnAssignment(source.TimeColumn).Matches(sql)
                .Select(m => m.Groups[1].Value)
                .ToList();

            Assert.True(
                assignments.Count > 0,
                $"{source.Key}: found no '{source.TimeColumn} = ...' assignment in {file}, so this guard "
                + "cannot see where the column comes from. Do not read that as UTC — a frame nobody can "
                + "read is the state the de-skew defects shipped in.");

            var frames = assignments
                .Select(rhs => rhs.Contains("@timestamp", StringComparison.Ordinal)
                    ? AnnotationClockFrame.Utc
                    : rhs.Contains("StartTime", StringComparison.Ordinal)
                        ? AnnotationClockFrame.ServerLocal
                        : (AnnotationClockFrame?)null)
                .ToList();

            Assert.DoesNotContain(null, frames);
            Assert.Single(frames.Distinct());

            Assert.Equal(frames[0], source.Frame);
        }

        /* And the census: exactly one source is server-local today. Named as a count so that adding a
           second one — or silently demoting this one to UTC — has to be a deliberate edit here. */
        Assert.Equal(
            new[] { "default_trace_events" },
            MeasureCatalog.AnnotationSources
                .Where(a => a.Frame == AnnotationClockFrame.ServerLocal)
                .Select(a => a.Key)
                .ToArray());
    }

    /// <summary>
    /// The compiler acts on the declaration: a server-local source both RETURNS and BOUNDS the de-skewed
    /// expression, and a UTC source is left alone. Both halves matter — bounding on the de-skewed value while
    /// returning the raw one is the exact half-fix #3198 reported, and it looks correct from the filter's
    /// side because the row SELECTION is right either way.
    /// </summary>
    [Fact]
    public void CompiledAnnotationSql_DeSkewsExactlyTheServerLocalSources()
    {
        foreach (var source in MeasureCatalog.AnnotationSources)
        {
            var sql = CompiledAnnotation(source.Key);
            var deSkewed = $"f.{source.TimeColumn} - make_interval(mins => COALESCE(o.utc_offset_minutes, 0))";
            var bare = $"f.{source.TimeColumn}";

            if (source.Frame == AnnotationClockFrame.ServerLocal)
            {
                Assert.Contains($"{deSkewed} AS ts", sql, StringComparison.Ordinal);
                Assert.Contains($"{deSkewed} >= $1", sql, StringComparison.Ordinal);
                Assert.Contains($"{deSkewed} <= $2", sql, StringComparison.Ordinal);
                /* The per-server join, not one scalar for the whole overlay: a panel routinely spans servers
                   at different offsets, and a single offset would de-skew all of them by one of them. */
                Assert.Contains("DISTINCT ON (server_name) server_name, utc_offset_minutes", sql, StringComparison.Ordinal);
                Assert.Contains("LEFT JOIN", sql, StringComparison.Ordinal);
                /* No bare occurrence survives anywhere — projection or bound. */
                Assert.DoesNotContain($"{bare} AS ts", sql, StringComparison.Ordinal);
                Assert.DoesNotContain($"{bare} >= $1", sql, StringComparison.Ordinal);
                Assert.DoesNotContain($"{bare} <= $2", sql, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains($"{bare} AS ts", sql, StringComparison.Ordinal);
                Assert.Contains($"{bare} >= $1", sql, StringComparison.Ordinal);
                Assert.Contains($"{bare} <= $2", sql, StringComparison.Ordinal);
                Assert.DoesNotContain("make_interval", sql, StringComparison.Ordinal);
                Assert.DoesNotContain("utc_offset_minutes", sql, StringComparison.Ordinal);
            }

            /* Unchanged by the de-skew: the offset table and column are compiler constants, so the join
               introduces no parameter and the window/server binds keep their ordinals. */
            Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
        }
    }

    /* ───────────────────────── the discriminators, both directions ───────────────────────── */

    [Fact]
    public void TheDiscriminators_FlagABareRead_AndPassADeSkewedOne()
    {
        /* ReadsTheTable: the FROM forms that shipped, and the non-reader mentions it must not drag in. */
        foreach (var sql in new[]
        {
            "        FROM default_trace_events AS dte, svr",
            "FROM v_default_trace_events",
            "FROM collect.default_trace_events AS f",
        })
        {
            Assert.True(ReadsTheTable.IsMatch(sql), $"ReadsTheTable missed a real read: {sql}");
        }

        foreach (var notARead in new[]
        {
            /* DDL, retention and the compose catalog all name the table without reading a timestamp. */
            "CREATE TABLE IF NOT EXISTS default_trace_events (",
            "new ComposeDimension(\"default_trace_events\", \"event_name\", \"event_name\", Likeable: true),",
            "\"default_trace_events\", \"event_time\", \"event_name\"",
        })
        {
            Assert.False(ReadsTheTable.IsMatch(notARead), $"ReadsTheTable dragged in a non-read: {notARead}");
        }

        /* BareAliasedEventTime: the three forms that shipped in the defect, and the fixed forms. */
        foreach (var sql in new[]
        {
            "            dte.event_time,",
            "        AND   dte.event_time >= $2 + make_interval(mins => svr.offset_minutes)",
            "        ORDER BY dte.event_time DESC",
        })
        {
            Assert.True(BareAliasedEventTime.IsMatch(sql), $"BareAliasedEventTime missed the hazard: {sql}");
        }

        foreach (var sql in new[]
        {
            "            dte.event_time - make_interval(mins => svr.offset_minutes) AS event_time_utc,",
            "        AND   dte.event_time - make_interval(mins => svr.offset_minutes) >= $2",
            "        ORDER BY event_time_utc DESC",
            /* The four UTC columns of the same name, which this must never reach. */
            "AND   event_time >= $2",
            "    f.event_time AS ts,",
        })
        {
            Assert.False(BareAliasedEventTime.IsMatch(sql), $"BareAliasedEventTime dragged in a benign form: {sql}");
        }

        /* PgDeSkew and LiteRowDeSkew must actually recognise the fix, or the site census above passes by
           finding nothing rather than by the sites being there. */
        Assert.True(PgDeSkew.IsMatch("dte.event_time - make_interval(mins => svr.offset_minutes) AS event_time_utc,"));
        Assert.False(PgDeSkew.IsMatch("            dte.event_time,"));
        Assert.False(PgDeSkew.IsMatch("dte.event_time >= $2 + make_interval(mins => svr.offset_minutes)"));
        Assert.True(LiteRowDeSkew.IsMatch("reader.GetDateTime(0).AddMinutes(-offset);"));
        Assert.False(LiteRowDeSkew.IsMatch("reader.GetDateTime(0);"));

        /* TimeColumnAssignment: the alias-on-left form, and the projections it must not mistake for one. */
        Assert.Equal(
            "ft.StartTime,",
            TimeColumnAssignment("event_time").Match("    event_time = ft.StartTime,").Groups[1].Value);
        Assert.False(TimeColumnAssignment("event_time").IsMatch("    x.event_time,"));
        Assert.False(TimeColumnAssignment("deadlock_time").IsMatch("    b.deadlock_time,"));
    }

    /* ───────────────────────── plumbing ───────────────────────── */

    private static string CompiledAnnotation(string key)
    {
        var json = "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\","
            + "\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"" + key + "\"]}";
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(json)!, Array.Empty<string>());
        Assert.True(error is null, error);
        Assert.NotNull(plan);

        var start = new DateTime(2026, 7, 18, 0, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2026, 7, 18, 6, 0, 0, DateTimeKind.Utc);
        var compiled = ComposeCompiler.CompileAnnotations(
            plan!,
            new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, RollupAvailability.All, end, RollupCoverage.Unknown));

        return Assert.Single(compiled).Compiled.Sql;
    }

    /// <summary>Comment spans out, string literals kept — the SQL under test IS a verbatim literal, and this
    /// codebase carries its reasoning in comments that name the very tokens matched here.</summary>
    private static string WithoutComments(string text)
    {
        var withoutBlocks = Regex.Replace(text, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        var withoutSlashes = Regex.Replace(withoutBlocks, @"//[^\r\n]*", " ");

        return Regex.Replace(withoutSlashes, @"--[^\r\n]*", " ");
    }

    private static IEnumerable<string> ProductionSourceFiles()
    {
        foreach (var root in new[] { RepoPath("Darling"), RepoPath("Lite") })
        {
            foreach (var path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                if (Segments(path).Any(s =>
                        string.Equals(s, "bin", StringComparison.Ordinal)
                        || string.Equals(s, "obj", StringComparison.Ordinal)
                        || string.Equals(s, "Darling.Tests", StringComparison.Ordinal)
                        || string.Equals(s, "Lite.Tests", StringComparison.Ordinal)))
                {
                    continue;
                }

                yield return path;
            }
        }
    }

    private static IEnumerable<string> Segments(string path) =>
        path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

    private static string Relative(string absolute) =>
        Path.GetRelativePath(RepoPath("."), absolute).Replace(Path.DirectorySeparatorChar, '/');

    private static string RepoPath(string relative, [CallerFilePath] string thisFile = "")
    {
        /* This file lives at <repo>/Darling/Darling.Tests/, so the repo root is two levels up. */
        var repoRoot = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
        return Path.GetFullPath(Path.Combine(repoRoot, relative.Replace('/', Path.DirectorySeparatorChar)));
    }
}
