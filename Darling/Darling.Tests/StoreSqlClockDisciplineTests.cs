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
using System.Text;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// No store-side SQL literal may compare a naive collector timestamp column against a BARE clock function.
/// Every timestamp column in both stores is <c>timestamp without time zone</c> holding naive UTC — measured,
/// there is not one <c>timestamptz</c> column in the schema — while <c>now()</c> is <c>timestamptz</c>. The
/// mixed comparison is not an error: PostgreSQL resolves it by converting the NAIVE side at the store
/// SESSION's TimeZone, which initdb takes from the host OS and which
/// <c>DarlingManagedPostgres.BuildConfAppend</c> does not pin, so a managed store on a Windows host inherits
/// the machine's local zone and a bring-your-own store inherits whatever it was built with.
///
/// <para><b>The measurement.</b> On <c>timescale/timescaledb:latest-pg17</c> seeded one row per 52 seconds,
/// <c>WHERE collection_time &gt; now() - interval '1 hour'</c> returned 66 rows under
/// <c>TimeZone='UTC'</c> and 343 under <c>TimeZone='America/New_York'</c> — the same one-hour predicate
/// silently spanning five. West of UTC a window widens and grades stale data; east of UTC it narrows and can
/// return nothing at all, which is the direction that matters, because two of the sites this pin was written
/// for are alert reads and an alert that returns no rows never fires.</para>
///
/// <para><b>Why a source pin.</b> Every store anyone develops or tests against runs UTC, so the defect is
/// invisible in CI, invisible in a live-store probe, and invisible in review — the query reads exactly like
/// what it was meant to say. <see cref="PgReadKindDisciplineTests"/> already holds the C# BIND side of this
/// same discipline (a <c>Kind=Utc</c> DateTime makes Npgsql infer timestamptz); this holds the SQL LITERAL
/// side, which that scan cannot see.</para>
///
/// <para><b>What it does not claim.</b> It flags comparisons and clamps, not writes: a bare clock in a
/// <c>SET</c>, a <c>VALUES</c> row or a projection is a different defect shape (a local-time value stored,
/// rather than a window skewed) and Lite has several that are inert today because nothing reads those
/// columns back. Widening this to the write side is a separate change with a data-migration question
/// attached, so it is deliberately out of scope here rather than silently allowlisted.</para>
/// </summary>
public sealed class StoreSqlClockDisciplineTests
{
    /* Floors, so the scan cannot pass by finding nothing. Measured on dev at the time of writing: 563 files,
       19,604 string literals, 1,626 of them SQL-shaped, 58 naive timestamp column names. Set well below
       those so ordinary growth never trips them, but high enough that a broken glob or a desynchronised
       literal walk fails instead of reporting a clean bill of health. The column-vocabulary floor is the one
       that matters most: the discriminator needs those names, and a DDL scrape that quietly returned four of
       them would make every offender below invisible while still reporting thousands of literals scanned. */
    private const int MinimumFilesScanned = 200;
    private const int MinimumLiteralsScanned = 12_000;
    private const int MinimumSqlLiteralsScanned = 1_200;
    private const int MinimumTimestampColumnsKnown = 40;

    /// <summary>
    /// The one bare <c>now()</c> left in the store's SQL, and it is waived on ARITHMETIC, not on being
    /// harmless: across UTC-12..UTC+14 its 48-hour window delivers 34–60 hours, 34h still covers the refresh
    /// cadence (startup, then riding the 24-hour purge) and 60h is still inside the 4-day raw retention. The
    /// query's own remarks carry the derivation and the two numbers that have to keep holding.
    /// </summary>
    private static readonly HashSet<string> Waived = new(StringComparer.Ordinal)
    {
        "DarlingModuleMap.cs:RefreshSql",
    };

    [Fact]
    public void NoStoreSqlComparesANaiveTimestampToABareClock()
    {
        var columns = NaiveTimestampColumns();

        Assert.True(
            columns.Count >= MinimumTimestampColumnsKnown,
            $"only {columns.Count} naive timestamp column names were scraped from the store DDL (floor "
            + $"{MinimumTimestampColumnsKnown}) — the discriminator is blind without them, so this scan "
            + "would pass vacuously. Check the DDL globs below.");

        /* collection_time is the column every windowed read is written against; naming it explicitly means a
           scrape that collects 40 obscure names and misses the load-bearing one still fails. */
        Assert.Contains("collection_time", columns);

        var files = 0;
        var literals = 0;
        var sqlLiterals = 0;
        var offenders = new List<string>();

        foreach (var path in StoreSourceFiles())
        {
            files++;
            var text = File.ReadAllText(path);
            var name = Path.GetFileName(path);

            foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
            {
                literals++;

                if (!LooksLikeSql(body))
                {
                    continue;
                }

                sqlLiterals++;

                foreach (var finding in MixedClockComparisons(body, columns))
                {
                    var member = EnclosingMember(text, start);

                    if (Waived.Contains(name + ":" + member))
                    {
                        continue;
                    }

                    offenders.Add($"{name}:{member} — {finding}");
                }
            }
        }

        Assert.True(files >= MinimumFilesScanned, $"scanned only {files} store source files (floor {MinimumFilesScanned})");
        Assert.True(literals >= MinimumLiteralsScanned, $"scanned only {literals} string literals (floor {MinimumLiteralsScanned})");
        Assert.True(sqlLiterals >= MinimumSqlLiteralsScanned, $"scanned only {sqlLiterals} SQL literals (floor {MinimumSqlLiteralsScanned})");

        Assert.True(
            offenders.Count == 0,
            "store SQL compares a naive collector timestamp against a bare clock function:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders) + Environment.NewLine
            + "The store session's TimeZone shifts the naive side, so the window widens west of UTC and can "
            + "return nothing east of it. Bind the bound as a Kind-Unspecified DateTime parameter computed "
            + "from DateTime.UtcNow (the clock that stamped collection_time), the way every other windowed "
            + "read here does.");
    }

    /// <summary>
    /// The discriminator, pinned in BOTH directions against literals written for the purpose. A scan like the
    /// one above is only worth its runtime if it flags the thing it names and leaves everything else alone;
    /// each positive here is a form that shipped in this repo, and each negative is a form the corpus
    /// contains and must not be dragged in with them.
    /// </summary>
    [Fact]
    public void TheDiscriminator_FlagsTheHazard_AndNothingElse()
    {
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "collection_time", "updated_at", "last_seen" };

        string[] hazards =
        [
            /* The four spellings named in the rule. */
            "SELECT 1 FROM query_store_stats AS qs WHERE qs.server_id = $1 AND qs.collection_time > now() - interval '2 hours'",
            "SELECT 1 FROM t WHERE collection_time >= CURRENT_TIMESTAMP - interval '2 days'",
            "SELECT 1 FROM t WHERE collection_time >= LOCALTIMESTAMP - interval '10 minutes'",
            "SELECT 1 FROM t WHERE collection_time > now()::timestamp - interval '1 hour'",
            /* Column on the RIGHT of the operator. */
            "SELECT 1 FROM t WHERE now() - interval '1 hour' < collection_time",
            /* The clamp shape: no comparison operator anywhere, GREATEST does the comparing. */
            "SELECT time_bucket('1 hour', GREATEST((SELECT min(collection_time) FROM collect.query_stats), now()::timestamp - INTERVAL '35 days')) AS need_from",
            "SELECT LEAST(max(collection_time), now()) FROM t",
            /* BETWEEN, and a qualified alias. */
            "SELECT 1 FROM t AS x WHERE x.collection_time BETWEEN now() - interval '1 day' AND now()",
            /* HAVING and a JOIN's ON are predicate contexts too. */
            "SELECT 1 FROM t GROUP BY a HAVING max(collection_time) > now() - interval '1 hour'",
            "SELECT 1 FROM a JOIN b ON b.collection_time > now() - interval '1 hour'",
            /* Bare predicate FRAGMENTS — the shape these readers interpolate, with no statement keyword. */
            "AND r.collection_time >= NOW() - INTERVAL '10 MINUTES'",
            "AND   qs.collection_time > now() - interval '2 hours'",
        ];

        foreach (var sql in hazards)
        {
            Assert.True(
                MixedClockComparisons(sql, columns).Any(),
                "the discriminator MISSED a hazard it is named for: " + sql);
        }

        string[] benign =
        [
            /* The fix: the bound is a parameter. */
            "SELECT 1 FROM query_store_stats AS qs WHERE qs.server_id = $1 AND qs.collection_time > $2",
            "SELECT time_bucket('1 hour', GREATEST((SELECT min(collection_time) FROM collect.query_stats), $1)) AS need_from",
            /* The other legitimate spelling — both sides naive UTC. Rescued whether or not it is parenthesised
               and whether or not the timestamptz cast is written out. */
            "SELECT 1 FROM t WHERE collection_time > (now() AT TIME ZONE 'UTC') - interval '2 hours'",
            "SELECT 1 FROM t WHERE collection_time > now() AT TIME ZONE 'UTC' - interval '2 hours'",
            "SELECT 1 FROM t WHERE collection_time > now()::timestamptz AT TIME ZONE 'UTC' - interval '2 hours'",
            /* WRITES, not comparisons — the deliberate scope boundary. Lite ships all three shapes. */
            "UPDATE config_database_state_expected SET expected_state = 'ONLINE', updated_at = now()::TIMESTAMP WHERE server_id = $1",
            "INSERT INTO t (server_id, updated_at) SELECT $1, now()::TIMESTAMP FROM database_states WHERE collection_time = $2",
            "INSERT INTO t (completed_at, rows_removed) VALUES (CURRENT_TIMESTAMP, 4)",
            "INSERT INTO t (a, updated_at) SELECT $1, now() ON CONFLICT (a) DO UPDATE SET updated_at = now()",
            /* A clock with no naive column anywhere near it: TimescaleDB's own catalog is timestamptz, and its
               scheduler takes a timestamptz. Both ship in TimescaleSupport. */
            "SELECT alter_job($1::integer, next_start => now())",
            "SELECT j.hypertable_name FROM timescaledb_information.jobs AS j WHERE j.next_start < now()",
            /* The clock is spelled only in a SQL comment. Four wrong counts were produced here by scans that
               did not strip these. */
            "SELECT 1 FROM t WHERE collection_time > $1 /* not now() - interval '2 hours': the session zone shifts it */",
            "SELECT 1 FROM t WHERE collection_time > $1 -- was now() - interval '2 hours'",
            /* Fragments that are already right: the bound form, and the rescued spelling the viewer's
               monitored-servers upsert actually ships. */
            "AND r.collection_time >= $4",
            "(now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC')",
        ];

        foreach (var sql in benign)
        {
            Assert.False(
                MixedClockComparisons(sql, columns).Any(),
                "the discriminator FLAGGED a benign form: " + sql
                    + " => " + string.Join("; ", MixedClockComparisons(sql, columns)));
        }
    }

    /* ---------------- the discriminator ---------------- */

    /// <summary>Bare clock reads. A store-local <c>now()::timestamp</c> is the same hazard spelled as a cast,
    /// so the cast is matched as part of the occurrence rather than left to look like a rescue — but the
    /// <c>\b</c> after it is load-bearing: without it the optional cast eats the first eleven characters of
    /// <c>::timestamptz</c>, the rescue that follows no longer sits at the match's end, and
    /// <c>now()::timestamptz AT TIME ZONE 'UTC'</c> — a CORRECT form — reads as an offender.</summary>
    private static readonly Regex ClockRegex = new(
        @"\b(?:now\s*\(\s*\)|current_timestamp|localtimestamp|statement_timestamp\s*\(\s*\)|transaction_timestamp\s*\(\s*\)|clock_timestamp\s*\(\s*\))(?:\s*::\s*timestamp\b)?",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary><c>AT TIME ZONE 'UTC'</c> immediately after the clock makes both sides naive UTC. It binds
    /// tighter than the arithmetic that follows, so the unparenthesised form is equally safe.</summary>
    private static readonly Regex RescueRegex = new(
        @"\G\s*(?:::\s*timestamptz\s*)?at\s+time\s+zone\s+'utc'",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex ComparisonRegex = new(
        @"(?:>=|<=|<>|!=|>|<|=|\bbetween\b)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>Keywords that open a PREDICATE. A clock reached from one of these is being compared; a clock
    /// reached from <c>SET</c>, <c>SELECT</c> or <c>VALUES</c> is being stored, which is a different defect
    /// and not this pin's business.</summary>
    private static readonly HashSet<string> PredicateOpeners = new(StringComparer.OrdinalIgnoreCase)
    {
        "WHERE", "AND", "OR", "HAVING", "ON", "WHEN", "THEN", "ELSE",
    };

    private static readonly HashSet<string> SpanBoundaries = new(StringComparer.OrdinalIgnoreCase)
    {
        "WHERE", "AND", "OR", "HAVING", "ON", "WHEN", "THEN", "ELSE", "SELECT", "SET", "VALUES",
        "ORDER", "GROUP", "LIMIT", "OFFSET", "RETURNING", "INSERT", "UPDATE", "DELETE", "FROM",
        "JOIN", "UNION", "EXCEPT", "INTERSECT", "CONFLICT", "DO", "AS", "BY", "INTO",
    };

    /// <summary>Clamp functions: they compare their arguments without any operator being written, so the
    /// predicate walk cannot see them. <c>BaselineBackfillProbeSql</c> was exactly this shape.</summary>
    private static readonly Regex ClampRegex = new(
        @"\b(?:greatest|least)\s*\(", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// Every place <paramref name="sql"/> puts a bare clock in a comparison or a clamp with one of
    /// <paramref name="columns"/>. Internal so the corpus scan and the control pin share one implementation
    /// — a copy would let the controls certify behaviour the corpus scan does not have.
    /// </summary>
    internal static IEnumerable<string> MixedClockComparisons(string sql, ISet<string> columns)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(columns);

        var text = StripSqlComments(sql);
        var findings = new List<string>();

        foreach (Match clock in ClockRegex.Matches(text))
        {
            if (RescueRegex.Match(text, clock.Index + clock.Length).Success)
            {
                continue;
            }

            var span = ClampArgumentsAround(text, clock.Index) ?? PredicateSpanAround(text, clock.Index);

            if (span is null || !MentionsColumn(span, columns))
            {
                continue;
            }

            findings.Add(Collapse(span));
        }

        return findings;
    }

    /// <summary>The innermost <c>GREATEST</c>/<c>LEAST</c> argument list containing <paramref name="index"/>,
    /// or null when the clock is not inside one.</summary>
    private static string? ClampArgumentsAround(string text, int index)
    {
        foreach (Match clamp in ClampRegex.Matches(text))
        {
            var open = clamp.Index + clamp.Length - 1;

            if (open >= index)
            {
                continue;
            }

            var depth = 0;

            for (var i = open; i < text.Length; i++)
            {
                if (text[i] == '(')
                {
                    depth++;
                }
                else if (text[i] == ')')
                {
                    depth--;

                    if (depth == 0)
                    {
                        if (index < i)
                        {
                            return text[open..(i + 1)];
                        }

                        break;
                    }
                }
            }
        }

        return null;
    }

    /// <summary>
    /// The predicate <paramref name="index"/> sits in: outward to the nearest boundary keyword, an
    /// unbalanced parenthesis, or a comma at this depth. Returns null unless the predicate was OPENED by a
    /// boolean keyword and actually contains a comparison — that pair is what separates
    /// <c>WHERE col = now()</c> from <c>SET col = now()</c>.
    /// </summary>
    private static string? PredicateSpanAround(string text, int index)
    {
        var (left, opener) = WalkLeft(text, index);
        var right = WalkRight(text, index);

        if (opener is null || !PredicateOpeners.Contains(opener))
        {
            return null;
        }

        var span = text[left..right];

        return ComparisonRegex.IsMatch(span) ? span : null;
    }

    private static (int Start, string? Opener) WalkLeft(string text, int index)
    {
        var depth = 0;
        var i = index;

        while (i > 0)
        {
            var c = text[i - 1];

            if (c == ')')
            {
                depth++;
            }
            else if (c == '(')
            {
                if (depth == 0)
                {
                    return (i, null);
                }

                depth--;
            }
            else if (depth == 0 && (c == ',' || c == ';'))
            {
                return (i, null);
            }
            else if (depth == 0 && char.IsLetter(c))
            {
                var end = i;
                var start = end;

                while (start > 0 && (char.IsLetterOrDigit(text[start - 1]) || text[start - 1] == '_'))
                {
                    start--;
                }

                var word = text[start..end];

                if (SpanBoundaries.Contains(word))
                {
                    return (end, word);
                }

                i = start;
                continue;
            }

            i--;
        }

        return (0, null);
    }

    private static int WalkRight(string text, int index)
    {
        var depth = 0;
        var i = index;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                if (depth == 0)
                {
                    return i;
                }

                depth--;
            }
            else if (depth == 0 && (c == ',' || c == ';'))
            {
                return i;
            }
            else if (depth == 0 && char.IsLetter(c))
            {
                var start = i;
                var end = start;

                while (end < text.Length && (char.IsLetterOrDigit(text[end]) || text[end] == '_'))
                {
                    end++;
                }

                var word = text[start..end];

                if (SpanBoundaries.Contains(word) && start != index)
                {
                    return start;
                }

                i = end;
                continue;
            }

            i++;
        }

        return text.Length;
    }

    private static bool MentionsColumn(string span, ISet<string> columns)
    {
        foreach (Match word in Regex.Matches(span, @"[A-Za-z_][A-Za-z0-9_]*"))
        {
            if (columns.Contains(word.Value))
            {
                return true;
            }
        }

        return false;
    }

    /* ---------------- corpus ---------------- */

    /// <summary>
    /// Column names declared <c>timestamp</c> / <c>TIMESTAMP</c> (never <c>timestamptz</c>) by either store's
    /// DDL. Scraped from the DDL's own literals rather than listed here, so a column added by a new migration
    /// rung is covered the day it lands — and read through the literal walk, so a name written in a comment
    /// cannot enter the vocabulary.
    /// </summary>
    private static HashSet<string> NaiveTimestampColumns([CallerFilePath] string thisFile = "")
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var declaration = new Regex(
            @"(?:^|[(,])\s*(?<name>[A-Za-z_][A-Za-z0-9_]*)\s+timestamp\b(?!\s*(?:with|tz))",
            RegexOptions.IgnoreCase | RegexOptions.Multiline);

        foreach (var path in DdlFiles(thisFile))
        {
            var text = File.ReadAllText(path);

            foreach (var (_, body) in CSharpSourceWalker.StringLiteralBodies(text))
            {
                foreach (Match match in declaration.Matches(body))
                {
                    names.Add(match.Groups["name"].Value);
                }
            }
        }

        /* `timestamp` is the type keyword, not a column; it turns up when a cast is written across a line
           break inside a DDL literal. Left in, it would match every SQL literal that casts anything. */
        names.Remove("timestamp");

        /* sample_time is EXCLUDED, and not because it is safe. It is the one name in the store whose frame
           depends on the table: cpu_utilization_stats.sample_time is deliberately the MONITORED SERVER's
           local wall clock (#1262 de-skews it per batch, Lite windows it through GetTimeRangeServerLocal),
           while memory_pressure_events.sample_time must be UTC. CollectorTimestampFrameTests pins both,
           per column, against the read path each protects — and its own remarks record that its first cut
           was a store-wide "all naive timestamps are UTC" rule that would have forbidden the CPU
           collector's intentional local clock. This scan keys on column NAMES, so it cannot tell those two
           apart and must not claim to; that column's frame is that pin's business, not this one's. */
        names.Remove("sample_time");

        return names;
    }

    private static IEnumerable<string> DdlFiles(string thisFile)
    {
        foreach (var dir in new[] { StorageDir(thisFile), LiteDir(thisFile, "Database") })
        {
            foreach (var path in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
            {
                yield return path;
            }
        }
    }

    /// <summary>
    /// Both apps' store-side source: everything that composes SQL against a Darling (PostgreSQL) or Lite
    /// (DuckDB) store. The collectors are deliberately NOT here — their SQL runs against a monitored SQL
    /// Server, whose clock policy is a separate question with at least one deliberately local caller.
    /// </summary>
    private static IEnumerable<string> StoreSourceFiles([CallerFilePath] string thisFile = "")
    {
        var roots = new[]
        {
            StorageDir(thisFile),
            ServiceDir(thisFile),
            ViewerDir(thisFile),
            LiteDir(thisFile, "Services"),
            LiteDir(thisFile, "Database"),
            LiteDir(thisFile, "Analysis"),
        };

        foreach (var root in roots)
        {
            foreach (var path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                if (path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return path;
            }
        }
    }

    private static string DarlingDir(string thisFile) =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    private static string StorageDir(string thisFile) =>
        Directory.Exists(Path.Combine(DarlingDir(thisFile), "PerformanceMonitor.Darling.Storage"))
            ? Path.Combine(DarlingDir(thisFile), "PerformanceMonitor.Darling.Storage")
            : throw new DirectoryNotFoundException("Darling Storage project not found from " + thisFile);

    private static string ServiceDir(string thisFile) =>
        Path.Combine(DarlingDir(thisFile), "PerformanceMonitor.Darling.Service");

    private static string ViewerDir(string thisFile) =>
        Path.Combine(DarlingDir(thisFile), "PerformanceMonitor.Darling.Viewer");

    private static string LiteDir(string thisFile, string leaf) =>
        Path.GetFullPath(Path.Combine(DarlingDir(thisFile), "..", "Lite", leaf));

    /* ---------------- text helpers ---------------- */

    /// <summary>
    /// Whether a literal is worth reading as SQL: a whole statement, OR a bare predicate FRAGMENT.
    ///
    /// <para>The fragment arm is not padding. These readers assemble filters as standalone literals and
    /// interpolate them — <c>LocalDataService.WaitStats</c> builds five that way — so a fragment like
    /// <c>"AND r.collection_time &gt;= NOW() - INTERVAL '10 MINUTES'"</c> carries no statement keyword at
    /// all, and a statement-only filter skips the shape most likely to reintroduce this. A fragment is
    /// recognised by carrying both a clock and a comparison, which is all the discriminator needs to judge
    /// it.</para>
    ///
    /// <para>Known limit, stated rather than papered over: the scan reads ONE literal at a time, so a
    /// predicate whose column sits in one literal and whose clock sits in another — welded together only by
    /// concatenation — is outside it. No such split exists in the corpus today; the one fragment pairing a
    /// clock with a column, <c>ViewerDataService.MonitoredServers</c>, already uses the rescued
    /// <c>AT TIME ZONE 'UTC'</c> form. Catching it would mean resolving concatenation rather than reading
    /// literals.</para>
    /// </summary>
    private static bool LooksLikeSql(string body) =>
        body.Contains("SELECT ", StringComparison.OrdinalIgnoreCase)
        || body.Contains("INSERT ", StringComparison.OrdinalIgnoreCase)
        || body.Contains("UPDATE ", StringComparison.OrdinalIgnoreCase)
        || body.Contains("DELETE ", StringComparison.OrdinalIgnoreCase)
        || body.Contains("CREATE ", StringComparison.OrdinalIgnoreCase)
        || body.Contains(" FROM ", StringComparison.OrdinalIgnoreCase)
        || (ClockRegex.IsMatch(body) && ComparisonRegex.IsMatch(body));

    /// <summary>Blanks SQL comments, preserving length so an offset still points where it did. The repo's SQL
    /// is heavily commented and those comments discuss <c>now()</c> constantly — including in the waiver this
    /// pin allows — so a scan that skips this step measures the prose.</summary>
    private static string StripSqlComments(string sql)
    {
        var sb = new StringBuilder(sql);

        for (var i = 0; i < sb.Length - 1; i++)
        {
            if (sb[i] == '-' && sb[i + 1] == '-')
            {
                while (i < sb.Length && sb[i] != '\n')
                {
                    sb[i++] = ' ';
                }

                continue;
            }

            if (sb[i] == '/' && sb[i + 1] == '*')
            {
                var close = sql.IndexOf("*/", i + 2, StringComparison.Ordinal);
                var end = close < 0 ? sb.Length : close + 2;

                while (i < end)
                {
                    if (sb[i] != '\n')
                    {
                        sb[i] = ' ';
                    }

                    i++;
                }

                i--;
            }
        }

        return sb.ToString();
    }

    /// <summary>The member a literal belongs to, for the waiver key and the failure message: the nearest
    /// declaration above it.</summary>
    private static string EnclosingMember(string text, int offset)
    {
        var head = text[..Math.Min(offset, text.Length)];

        var matches = Regex.Matches(
            head,
            @"(?:const\s+string|static\s+string|string)\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?:=|=>)|(?<name2>[A-Za-z_][A-Za-z0-9_]*)\s*\([^)]*\)\s*(?:=>|\{)");

        for (var i = matches.Count - 1; i >= 0; i--)
        {
            var name = matches[i].Groups["name"].Success ? matches[i].Groups["name"].Value : matches[i].Groups["name2"].Value;

            if (!string.IsNullOrEmpty(name))
            {
                return name;
            }
        }

        return "<unknown>";
    }

    private static string Collapse(string span)
    {
        var one = Regex.Replace(span.Trim(), @"\s+", " ");

        return one.Length <= 160 ? one : one[..160] + "…";
    }
}
