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
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V138 (partial #3691, the "per-database settings" collector line): TWO nullable columns
/// on the existing <c>collect.pg_server_config</c> — <c>database_name</c> and <c>role_name</c> — so a stored
/// setting row says WHOSE setting it is. No new table, no new hypertable
/// (<see cref="TimescaleSupport.HypertableCount"/> stays 72), no DEFAULT, no backfill, no passthrough (the
/// table has no <c>v_</c> view), no Lite twin (Lite has no PostgreSQL collectors at all). The shape is
/// <see cref="QsCaptureModeRouteKnobToastRungTests"/>' (V137) column half, one table instead of three.
///
/// <para><b>The lie it ends.</b> <c>pg_settings</c> is the RESOLVED view for the collector's own backend, so
/// a cluster where one database carries <c>ALTER DATABASE … SET work_mem</c> stored nothing about it and the
/// <c>CONFIG_PG_*</c> facts graded a value that database's sessions never use. The collector now
/// <c>UNION ALL</c>s <c>pg_db_role_setting</c> onto <c>pg_settings</c>, and these two columns are what tell
/// the two populations apart in the stored rows.</para>
///
/// <para><b>The correctness edge is the READS, which is why the census below is the centre of this file.</b>
/// Every shipped read of this table is a latest-snapshot, one-row-per-setting-NAME shape — several loading a
/// dictionary keyed by name, one taking a single row by <c>ORDER BY … LIMIT 1</c>, one partitioning a
/// <c>LAG</c> by name — so an override row arriving with a duplicate name would shadow the server-wide value,
/// throw on a duplicate key, or manufacture a configuration change. Nine reads therefore carry
/// <c>database_name IS NULL AND role_name IS NULL</c> and exactly ONE selects the overrides. The census
/// asserts that partition over the tree rather than over a list, so the NEXT reader has to choose an arm
/// deliberately.</para>
///
/// <para>This file carried the "I am the top rung" claims that moved off
/// <see cref="QsCaptureModeRouteKnobToastRungTests"/> (V137) when this rung landed, and handed them on to
/// <see cref="PostmasterStartTimeRungTests"/> (V139, #3955) when that one did. What stays is everything true of
/// this rung wherever it sits: its name, its DDL, its probe sentinel at its own ordinal, and that a store which
/// stopped here maps to exactly 138.</para>
///
/// <para>The collector's <c>pg_settings</c> behaviour and its two original reads are
/// <see cref="PgServerConfigTests"/> (V102); the page contract and the snapshot counts are
/// <c>McpPageContractTests</c>. This file is the RUNG: the ladder, the DDL, the V102 restatement, the
/// collector's payload tail and UNION, the probe, the reader census, and the live round trip through
/// <c>get_pg_server_config</c>.</para>
/// </summary>
[Trait("Stage", "Guard")] // #5798: walks the repo tree, so the guard stage runs it
public sealed class PgServerConfigScopeRungTests
{
    private const int RungVersion = 138;
    private const int PreviousVersion = 137;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V139 (#3955, the
    /// postmaster start time) appended its own — so this is a position within the signature rather than its end,
    /// the handoff <see cref="QsCaptureModeRouteKnobToastRungTests"/> made to this file one rung ago.</summary>
    private const int ProbeOrdinal = 113;

    private const string Table = "pg_server_config";

    private static readonly string[] ScopeColumns = { "database_name", "role_name" };

    private static PgMigrations.Migration V138 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("pg-server-config-database-role-overrides", V138.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped
           being true when V139 landed. The invariant that outlives the handoff is that the LADDER's top and
           the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /* ---- the collector ------------------------------------------------------------------------------- */

    /* ---- the reader census (the correctness edge) ----------------------------------------------------- */

    /// <summary>
    /// A <c>FROM pg_server_config</c> occurrence, with the alias if it has one.
    /// </summary>
    private static readonly Regex FromTable = new(
        @"FROM\s+(?:collect\.)?pg_server_config(?:\s+AS\s+(\w+))?", RegexOptions.Compiled);

    /// <summary>
    /// <b>The census.</b> Every product read of <c>pg_server_config</c> either EXCLUDES the overrides
    /// (<c>database_name IS NULL</c> and <c>role_name IS NULL</c>) or is the ONE read that selects them
    /// (<c>database_name IS NOT NULL OR role_name IS NOT NULL</c>). Twelve anchors, named individually rather
    /// than matched in bulk, because the shapes differ and each one's reason does: nine latest-per-name reads,
    /// the one override read, and the two <c>FROM</c>s of the scoped change feed (#3937), which selects the
    /// overrides too.
    ///
    /// <para>The inner <c>MAX(collection_time)</c> subqueries are deliberately NOT required to carry the
    /// predicate — every one of the seven is per SERVER, not per name, so an override row cannot move the
    /// anchor, and a predicate there would be noise asserting a shape that does not matter. That is why this
    /// pin counts OUTER statements by anchor instead of counting <c>FROM</c> occurrences: the occurrence count
    /// is 21 across the tree and only 12 of them are reads.</para>
    ///
    /// <para>The last clause is the one that catches the NEXT reader: any <c>FROM pg_server_config</c> in a
    /// product file whose statement carries neither arm fails, so a thirteenth read has to choose. The scan is
    /// over the five product projects with comments stripped, so a comment quoting the table (there are
    /// several, including this rung's own) is not mistaken for a read.</para>
    /// </summary>
    [Fact]
    public void EveryProductReadOfTheConfigTable_EitherExcludesTheOverrides_OrIsTheOneThatSelectsThem()
    {
        var filtering = new (string File, string Anchor)[]
        {
            ("PgTargetFactCollector.Config.cs", "FROM pg_server_config AS c"),
            ("PgTargetFactCollector.Memory.cs", "FROM pg_server_config AS c"),
            ("PgTargetFactCollector.Blocking.cs", "FROM pg_server_config AS c"),
            ("PgTargetFactCollector.Posture.cs", "FROM pg_server_config AS c"),
            ("PgTargetBaselineProvider.Clock.cs", "FROM pg_server_config AS c"),
            ("DarlingPgLoggingAuditReader.cs", "FROM pg_server_config AS c"),
            ("DarlingPgTrendReader.cs", "FROM pg_server_config"),
            ("PgTargetStatementsEvictionRead.cs", "FROM pg_server_config AS c"),
            ("DarlingPgStatementReader.cs", "FROM pg_server_config"),
        };

        var sources = ProductSources().ToDictionary(p => Path.GetFileName(p.File), p => p.Text, StringComparer.Ordinal);

        foreach (var (file, anchor) in filtering)
        {
            var text = StripComments(sources[file]).Replace("\r\n", "\n", StringComparison.Ordinal);
            var at = text.IndexOf(anchor, StringComparison.Ordinal);
            Assert.True(at >= 0, $"{file}: no read of {Table} at all — this census entry is guarding nothing");
            foreach (var column in ScopeColumns)
            {
                Assert.Contains($"{column} IS NULL", text[at..], StringComparison.Ordinal);
            }
        }

        /* The shared reader carries five of the twelve in one file: two filtering statements, the one
           override read, and the scoped change feed's two FROMs. Asserted on the constants themselves rather
           than on the file's text, so a predicate moved between them fails here. */
        foreach (var (name, sql) in new[]
        {
            (nameof(DarlingPgServerConfigReader.CurrentConfigSql), DarlingPgServerConfigReader.CurrentConfigSql),
            (nameof(DarlingPgServerConfigReader.ConfigChangesSql), DarlingPgServerConfigReader.ConfigChangesSql),
        })
        {
            Assert.Contains("c.database_name IS NULL", sql, StringComparison.Ordinal);
            Assert.Contains("c.role_name IS NULL", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("IS NOT NULL OR", sql, StringComparison.Ordinal);
            _ = name;
        }

        /* The ONE read that asks for them, by the positive arm and by ordering per scope. */
        var overrideSql = DarlingPgServerConfigReader.OverrideSql;
        Assert.Contains("(c.database_name IS NOT NULL OR c.role_name IS NOT NULL)", overrideSql, StringComparison.Ordinal);
        Assert.DoesNotContain("database_name IS NULL", overrideSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY c.database_name NULLS LAST, c.role_name NULLS LAST, c.name", overrideSql, StringComparison.Ordinal);
        /* Same newest-snapshot anchor as the server-wide read, as its own subquery rather than a threaded
           instant, and it projects no cap: an override list truncated is worse than none. */
        Assert.Contains("SELECT MAX(collection_time)", overrideSql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", overrideSql, StringComparison.Ordinal);

        /* #3937: the scoped change feed is the census's second override-SELECTING statement. Its two reads of
           the table are one statement: `snapshots` reads the server's snapshot INSTANTS off every row (a
           snapshot with no override left in it still has to pair, or a RESET of the last override is never
           seen), so that FROM is per server rather than per name and carries neither arm on its own; the
           statement's `overrides` CTE carries the positive arm, and the census's 2000-character statement
           window from that first FROM reaches it. Both counted, which is the 10 -> 12. Asserted here so the
           positive arm cannot quietly leave the CTE while the window still sees it somewhere else. */
        var scopedSql = StripComments(DarlingPgServerConfigReader.ScopedConfigChangesSql);
        Assert.Equal(2, FromTable.Matches(scopedSql).Count);
        Assert.Contains("AND   (c.database_name IS NOT NULL OR c.role_name IS NOT NULL)", scopedSql, StringComparison.Ordinal);
        Assert.DoesNotContain("database_name IS NULL", scopedSql, StringComparison.Ordinal);
        Assert.DoesNotContain("role_name IS NULL", scopedSql, StringComparison.Ordinal);
        Assert.True(
            scopedSql.IndexOf("(c.database_name IS NOT NULL OR c.role_name IS NOT NULL)", StringComparison.Ordinal)
                - scopedSql.IndexOf("FROM pg_server_config AS c", StringComparison.Ordinal) < 2000,
            "the scoped feed's positive arm has moved out of the census's statement window from its first FROM");

        /* The clause that catches the thirteenth reader. Counted over OUTER statements: an occurrence whose
           surrounding statement carries one of the two arms is accounted for, and one that carries neither
           is a read nothing has decided about. */
        var undecided = new List<string>();
        var reads = 0;
        foreach (var (file, text) in ProductSources())
        {
            var clean = StripComments(text).Replace("\r\n", "\n", StringComparison.Ordinal);
            foreach (var match in FromTable.Matches(clean).Cast<Match>())
            {
                /* The inner per-server anchor subqueries: `FROM pg_server_config` with no alias immediately
                   followed by `WHERE server_id = $1)` — or by the as-of form `WHERE server_id = $1 AND
                   collection_time <= $2)` the fact collectors anchor on, and #3928's lower bound
                   `AND collection_time >= $N` before it — per SERVER, never per name, so they need no
                   predicate and are not reads. */
                var tail = clean[match.Index..Math.Min(clean.Length, match.Index + 200)];
                if (Regex.IsMatch(tail, @"^FROM\s+(?:collect\.)?pg_server_config\s*\n?\s*WHERE\s+server_id = \$1(?:\s*\n?\s*AND\s+collection_time >= \$\d+)?(?:\s*\n?\s*AND\s+collection_time <= \$\d+)?\)"))
                {
                    continue;
                }

                reads++;
                var statement = clean[match.Index..Math.Min(clean.Length, match.Index + 2000)];
                var excludes = statement.Contains("database_name IS NULL", StringComparison.Ordinal)
                            && statement.Contains("role_name IS NULL", StringComparison.Ordinal);
                var selects = statement.Contains("database_name IS NOT NULL OR", StringComparison.Ordinal);

                /* #4348 S1b: the one-time secret scrub is the census's third answer, not a missed choice.
                   A secret can land in a per-database or per-role override exactly as easily as in the
                   server-wide setting (the ruling's own test shape seeds both), so the scrub's candidate
                   read must carry NEITHER arm — it wants every row, whichever scope it is, and decides
                   row-by-row in C# via PgSettingRedactor rather than in this SQL. */
                var scrubsEveryScopeDeliberately = Path.GetFileName(file) == "PgSettingScrub.cs";

                if (!excludes && !selects && !scrubsEveryScopeDeliberately)
                {
                    undecided.Add($"{Path.GetFileName(file)} @ {match.Index}");
                }
            }
        }

        /* 10 -> 12 (#3937): ScopedConfigChangesSql's `snapshots` subquery and `overrides` CTE, both aliased
           `FROM pg_server_config AS c`, both classified "selects" above. 12 -> 13 (#4348 S1b): the scrub's
           one candidate read, classified above as the deliberate third answer. 13 -> 15 (#4677): the eviction
           finding's pg_stat_statements.max read and the top-queries reader's, both server-wide (they exclude the overrides).
           15 -> 16 (#4938): the worker's server-clock read (DarlingWorker.ReadPgServerClockSql), the newest server-wide
           TimeZone setting of a PostgreSQL target, which excludes the overrides the same way. */
        Assert.Equal(16, reads);
        Assert.True(undecided.Count == 0,
            "a pg_server_config read carries neither arm of the V138 scope split, so it will see per-database "
          + "and per-role override rows as if they were the server's settings: ["
          + string.Join(", ", undecided) + "]. Add `AND database_name IS NULL AND role_name IS NULL` to a "
          + "server-wide read, or select the overrides explicitly the way DarlingPgServerConfigReader."
          + nameof(DarlingPgServerConfigReader.OverrideSql) + " does.");
    }

    /* ---- the probe (top arm) -------------------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel at its own ordinal, and the map's arm for it
    /// returns 138. The probe asks the question, the caller reads the answer, the map has the parameter — a
    /// sentinel present at only some of them shifts every LATER ordinal onto the wrong column. The top-arm half of
    /// this claim moved to <see cref="PostmasterStartTimeRungTests"/> (V139) with the top.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        Assert.Contains(
            $"table_name = '{Table}'\n                                                     AND   column_name = 'database_name'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        /* The "nothing past me" half of this claim moved to V139's test with the top ordinal; what stays is
           that this rung's sentinel is read at its OWN ordinal, which is what keeps every later one on the
           right column. */
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPgServerConfigDatabaseRoleOverrides", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A position within the signature, not its end: `ProbeOrdinal == arity - 1` asserted this rung is the
           NEWEST sentinel, which stopped being true the moment V139 appended its own. */
        Assert.True(ProbeOrdinal < arity - 1);
        Assert.Equal("hasPgServerConfigDatabaseRoleOverrides", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns this rung's version. */
        var thisArm = viewer.IndexOf("if (hasPgServerConfigDatabaseRoleOverrides)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasQsCaptureModeRouteKnobToast)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V138 sentinel arm — a store stopped here would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V138 arm sits below the previous rung's, so a V138 store maps one rung low");
        /* This rung's own literal, not the build's version: the "returns StorageVersion.SchemaVersion" half of
           the top-arm claim moved to V139's test with the top. */
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table and the columns are named in the probe line and nowhere in the arm's
           prose — the coverage ratchet strips information_schema lines but cannot strip a comment. */
        Assert.DoesNotContain(Table, viewer[thisArm..previousArm], StringComparison.Ordinal);
        foreach (var column in ScopeColumns)
        {
            Assert.DoesNotContain(column, viewer[thisArm..previousArm], StringComparison.Ordinal);
        }
    }

    /* ---- the tool's section --------------------------------------------------------------------------- */

    /* ---- the live round trip -------------------------------------------------------------------------- */

    private static IEnumerable<(string File, string Text)> ProductSources()
    {
        foreach (var project in new[]
        {
            Path.Combine("Darling", "PerformanceMonitor.Darling.Analysis"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Storage"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer"),
            "PerformanceMonitor.Collectors",
        })
        {
            var directory = Path.Combine(RepoFile.Root, project);
            foreach (var file in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                         && !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                .OrderBy(f => f, StringComparer.Ordinal))
            {
                var text = File.ReadAllText(file);
                if (text.Contains(Table, StringComparison.Ordinal))
                {
                    yield return (file, text);
                }
            }
        }
    }

    /// <summary>
    /// Block and line comments out, so a comment naming the table — this rung's own doc blocks name it a
    /// dozen times — is not mistaken for a read. The bound this rests on: every read is SQL inside a string
    /// literal, and no string literal in these files contains <c>/*</c> other than the reads' own inline
    /// comments, which are exactly what should be stripped.
    /// </summary>
    private static string StripComments(string source)
    {
        var noBlocks = Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(noBlocks, @"^\s*///?.*$", string.Empty, RegexOptions.Multiline);
    }

    /// <summary>
    /// #4251 round-1 review, L1: nothing offline kept <see cref="PgServerConfigCollector"/>'s two query texts
    /// (plain, and the <c>pg_file_settings</c> route) in step — only a live run with a granted role ever
    /// exercised the second one. This pins that the two BuildQuery outputs differ in exactly one span: the
    /// <c>pending_restart</c> column. A column added to one text alone, or a file-settings predicate that
    /// drifted from what <c>PayloadColumns</c> expects, fails this without needing a live database.
    /// </summary>
    [Fact]
    public void TheFileSettingsQuery_IsTheDefaultQuery_WithOnlyThePendingRestartColumnChanged()
    {
        static string Text(bool readable) => PgServerConfigCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = -4251,
            ServerName = "pin-4251",
            CollectionTime = DateTime.UtcNow,
            Deltas = NoDeltas.Instance,
            PgFileSettingsReadable = readable,
        }).Text;

        var plain = Text(false);
        var withFile = Text(true);
        const string plainColumn = "    s.pending_restart                       AS pending_restart,";
        Assert.Single(Regex.Matches(plain, Regex.Escape(plainColumn)));

        var start = withFile.IndexOf("    (s.pending_restart", StringComparison.Ordinal);
        Assert.True(start >= 0);
        const string tail = "AS pending_restart,";
        var end = withFile.IndexOf(tail, start, StringComparison.Ordinal) + tail.Length;
        Assert.Equal(plain, withFile[..start] + plainColumn + withFile[end..]);
    }

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public static readonly NoDeltas Instance = new();

        public long CalculateDelta(int serverId, string key, string metric, long current, DateTime? at = null, int i = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string key, string metric, long current, out int seconds, DateTime? at = null, int i = 0)
        {
            seconds = 60;
            return 0;
        }
    }
}
