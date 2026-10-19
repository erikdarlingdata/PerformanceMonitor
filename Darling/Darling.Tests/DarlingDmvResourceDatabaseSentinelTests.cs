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
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1893, Darling half: the DMV blocking snapshot's sentinel now names the database the LOCK RESOURCE
/// lives in, so a cross-database lock fingerprints the same from either collector.
///
/// <para>
/// #1876 made the two collectors' labels agree at the incident identity and corrected the report side's
/// sentinel to name <c>resource_database_id</c>. The DMV side could not follow, because the normalizer runs
/// in C# against a stored row whose only database name is <c>database_name</c> — which the collector writes
/// as the BLOCKED SESSION's database. For a cross-database lock the session runs in one database while the
/// contended object lives in another, so the two sides still disagreed.
/// </para>
///
/// <para>
/// The fix is server-side and cheap: the resource's database id is read from
/// <c>sys.dm_os_waiting_tasks.resource_description</c>, whose documented format carries a <c>dbid=</c>
/// token for EVERY lock resource type, and <c>DB_NAME()</c> runs in the same query. The DMV side still does
/// NOT resolve the OBJECT behind a KEY, PAGE or RID lock — that is the expensive per-database lookup the
/// report side needs a cursor and #1865's permission screen for, and this sweep runs on a much tighter
/// cadence.
/// </para>
///
/// <para>Lite.Tests' <c>DmvResourceDatabaseSentinelTests</c> pins the identical expectations, so editing one app's copy alone fails a build.</para>
/// </summary>
public sealed class DarlingDmvResourceDatabaseSentinelTests
{
    /* ── the fingerprint, which is the whole point ── */

    [Fact]
    public void A_Cross_Database_Lock_Fingerprints_The_Same_From_Either_Collector()
    {
        /* The exact strings both collectors produced for ONE live cross-database KEY lock on SQL 2022
           (session in pm1893_ctx, resource in pm1893_res). Red before #1893: the DMV row carried the raw
           resource, which #1876's normalizer could only name with the SESSION's database. */
        var report = Group(Event(ReportSentinel, SessionDatabase));
        var snapshot = Group(Event(DmvSentinelAfter, SessionDatabase));

        Assert.Equal(ReportSentinel, DmvSentinelAfter);
        Assert.Equal(report[0].Incident.DedupKey, snapshot[0].Incident.DedupKey);
    }

    [Fact]
    public void The_Old_Raw_Resource_Could_Only_Ever_Have_Named_The_Session_Database()
    {
        /* Why the fix had to be server-side, demonstrated rather than asserted: hand #1876's normalizer the
           value the DMV collector used to store, and the best it can do is the session's database — the
           resource's id is right there in the string, but C# cannot turn 11 into a name. */
        var normalized = ContentiousObjectLabel.Normalize(DmvValueBefore, SessionDatabase);

        Assert.Equal("Unresolved: key lock, database: pm1893_ctx", normalized);
        Assert.NotEqual(ReportSentinel, normalized);
        Assert.NotEqual(Group(Event(ReportSentinel, SessionDatabase))[0].Incident.DedupKey,
                        Group(Event(DmvValueBefore, SessionDatabase))[0].Incident.DedupKey);
    }

    [Fact]
    public void The_New_Sentinel_Passes_Through_The_Normalizer_Untouched()
    {
        /* The collector now emits report form, so #1876's read-side transform must be a no-op on it — if it
           ever started re-deriving the database from the row, it would put the session's name back and undo
           this fix silently. Checked with a DIFFERENT database name passed in, so a transform that rewrote
           the value would visibly change it. */
        Assert.Equal(DmvSentinelAfter, ContentiousObjectLabel.Normalize(DmvSentinelAfter, SessionDatabase));
        Assert.Equal(DmvSentinelAfter, ContentiousObjectLabel.Normalize(DmvSentinelAfter, "some-other-database"));
    }

    /* ── the parse, and its documented basis ── */

    [Fact]
    public void The_Resource_Database_Comes_From_Resource_Descriptions_Documented_Dbid_Token()
    {
        /* MS Learn documents every lock resource_description as carrying dbid=<db-id> — keylock, pagelock,
           ridlock, objectlock, databaselock, filelock, extentlock, applicationlock, metadatalock, hobtlock
           and allocunitlock — so ONE parse covers every lock shape, where splitting wait_resource
           positionally would need a per-type branch. Verified live on SQL 2022:
           'keylock hobtid=72057594045726720 dbid=11 id=lock... mode=X associatedObjectId=...'. */
        var sql = Sql();

        Assert.Contains("CHARINDEX(N'dbid=', wt.resource_description)", sql, StringComparison.Ordinal);
        Assert.Contains("wt.resource_description", sql, StringComparison.Ordinal);
        /* Digits only, up to the first non-digit: 'dbid=11 id=lock...' must yield 11, not 11 plus whatever
           follows. The appended '.' guarantees PATINDEX finds a terminator even at end of string. */
        Assert.Contains("PATINDEX(N'%[^0-9]%', d.tail + N'.')", sql, StringComparison.Ordinal);
        Assert.Contains("DB_NAME(resparse.resource_database_id)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void The_Parse_Is_Restricted_To_Lock_Waits_That_Have_A_Resource()
    {
        /* Latch and RESOURCE_SEMAPHORE rows have no 'TYPE: ' resource shape and no dbid= to read — their
           resource_description is a bare '<db-id>:<file-id>:<page>' or a latch class. Gating on the wait
           TYPE rather than sniffing the string keeps every one of those rows byte-identical to before, and
           is honest about why: this is a lock-resource parse. */
        var sql = Sql();

        Assert.Contains("WHERE wt.wait_type LIKE N'LCK[_]%'", sql, StringComparison.Ordinal);
        Assert.Contains("AND   der_b.wait_resource IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("AND   der_b.wait_resource <> N''", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Without_A_Dbid_The_Old_Value_Is_Kept_Exactly()
    {
        /* The fallback is unchanged on purpose: a row this cannot name is better left carrying the raw
           resource than relabelled with a database nobody verified. */
        var sql = Sql();

        Assert.Contains("ELSE der_b.wait_resource", sql, StringComparison.Ordinal);
        var sentinel = sql.IndexOf("WHEN resparse.resource_database_id IS NOT NULL", StringComparison.Ordinal);
        var fallback = sql.IndexOf("ELSE der_b.wait_resource", StringComparison.Ordinal);
        Assert.True(sentinel > 0 && fallback > sentinel, "the sentinel must be tried before the raw fallback");
    }

    [Fact]
    public void The_Object_Resolution_Branch_Is_Untouched()
    {
        /* #1893 deliberately does NOT give this collector KEY/PAGE/RID object resolution: that is the
           per-database lookup the report side needs a cursor and a permission screen (#1865) for, and this
           sweep runs on a far tighter cadence. Only the naming changed. */
        var sql = Sql();

        Assert.Contains("WHEN objparse.object_id IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("QUOTENAME(OBJECT_SCHEMA_NAME(objparse.object_id, objparse.database_id))", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sys.partitions", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sys.dm_db_page_info", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sp_executesql", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CURSOR", sql, StringComparison.Ordinal);
    }

    /* ── the shape must be the report collector's, not merely similar ── */

    [Fact]
    public void Both_Collectors_Classify_Lock_Types_With_The_Same_Ordered_Rules()
    {
        /* The sentinel's lock word has to match or the labels differ by a token and fingerprint apart. Both
           sides test KEY, OBJECT, RID then PAGE in that order and fall back to the leading token upper-cased
           and capped at 32 — the order matters because a compound resource can embed more than one. */
        foreach (var sql in new[] { Sql(), ReportSql() })
        {
            var order = Regex.Matches(sql, @"LIKE N'%(KEY|OBJECT|RID|PAGE): %'")
                .Select(m => m.Groups[1].Value)
                .ToArray();

            Assert.Equal(new[] { "KEY", "OBJECT", "RID", "PAGE" }, order);
            Assert.Contains("CHARINDEX(N':', ", sql, StringComparison.Ordinal);
            Assert.Contains("), 32)", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Both_Collectors_Build_The_Sentinel_From_The_Same_Literal_Pieces()
    {
        /* Three literals, spelled the same on both sides. A stray capital or a missing comma here is a
           silent fingerprint split, which is exactly the class of defect #1876 and #1893 exist to close. */
        foreach (var sql in new[] { Sql(), ReportSql() })
        {
            Assert.Contains("N'Unresolved: '", sql, StringComparison.Ordinal);
            Assert.Contains("N' lock, '", sql, StringComparison.Ordinal);
            Assert.Contains("N'database: '", sql, StringComparison.Ordinal);
            Assert.Contains("N'unknown'", sql, StringComparison.Ordinal);
        }

        /* And both fall back to the event/session database before giving up, in the same order. */
        Assert.Contains("COALESCE(DB_NAME(resparse.resource_database_id), DB_NAME(der_b.database_id), N'unknown')", Sql(), StringComparison.Ordinal);
        Assert.Contains("COALESCE(DB_NAME(b.resource_database_id), DB_NAME(b.database_id), N'unknown')", ReportSql(), StringComparison.Ordinal);

        /* Both LOWER the lock word. The fingerprint would survive a case split (it lower-cases before
           hashing), so this is about the label a human reads: one collector writing 'KEY lock' beside
           another's 'key lock' for the same contention is the drift this pair of issues is about. */
        Assert.Contains("LOWER(resparse.lock_type)", Sql(), StringComparison.Ordinal);
        Assert.Contains("LOWER(b.lock_type)", ReportSql(), StringComparison.Ordinal);
    }

    /* ── the deprecated Dashboard's SQL twin ── */

    [Fact]
    public void The_Dashboard_Install_Script_Carries_The_Same_Change()
    {
        /* install/56 is the Dashboard's hand-maintained copy of this collector. It has drifted before, and a
           store that labels the same lock differently from Lite's and Darling's is the divergence this
           repo's parity rule exists to stop. */
        var install = File.ReadAllText(FindRepoFile(Path.Combine("install", "56_collect_dmv_blocking_snapshot.sql")));

        Assert.Contains("WHEN resparse.resource_database_id IS NOT NULL", install, StringComparison.Ordinal);
        Assert.Contains("N'Unresolved: '", install, StringComparison.Ordinal);
        Assert.Contains("COALESCE(DB_NAME(resparse.resource_database_id), DB_NAME(der_b.database_id), N'unknown')", install, StringComparison.Ordinal);
        Assert.Contains("CHARINDEX(N'dbid=', wt.resource_description)", install, StringComparison.Ordinal);
        Assert.Contains("PATINDEX(N'%[^0-9]%', d.tail + N'.')", install, StringComparison.Ordinal);
        Assert.Contains("WHERE wt.wait_type LIKE N'LCK[_]%'", install, StringComparison.Ordinal);
    }

    [Fact]
    public void The_Change_Is_Query_Text_Only_And_Stores_No_New_Column()
    {
        /* #1893 changed the query text only, so it added no column of its own (the collector wrote 19 columns then,
           and carries more since, because later issues appended theirs; #5632's four head-blocker columns are pinned in
           PerfmonAndDmvBlockingCollectorDefinitionTests). It is the contentious_object value that got better. */
        var names = DmvBlockingSnapshotCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("contentious_object", names);
        Assert.DoesNotContain("wait_resource", names);
        Assert.DoesNotContain("resource_database_id", names);
    }

    /* ── fixtures: the literal strings the two collectors produced for ONE live lock ── */

    /// <summary>What the DMV collector stored for that lock BEFORE #1893 — the raw wait resource.</summary>
    private const string DmvValueBefore = "KEY: 11:72057594045726720 (8194443284a0)";

    /// <summary>What it stores now, read off the live run.</summary>
    private const string DmvSentinelAfter = "Unresolved: key lock, database: pm1893_res";

    /// <summary>What the report collector produced for the same lock when it could not resolve the object.</summary>
    private const string ReportSentinel = "Unresolved: key lock, database: pm1893_res";

    /// <summary>The database the BLOCKED SESSION was running in — deliberately not the resource's.</summary>
    private const string SessionDatabase = "pm1893_ctx";

    /* ── helpers ── */

    private static string Sql() =>
        DmvBlockingSnapshotCollector.Instance.BuildQuery(Context()).Text.ReplaceLineEndings("\n");

    private static string ReportSql() =>
        BlockedProcessReportCollector.Instance.BuildQuery(Context()).Text.ReplaceLineEndings("\n");

    private static CollectorContext Context() => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 7, 30, 12, 0, 0, DateTimeKind.Utc),
        Deltas = null!,
        Target = new CollectorTargetInfo(),
    };

    private static BlockingIncidentGrouper.BlockedEvent[] Event(string contentiousObject, string database) =>
        new[]
        {
            new BlockingIncidentGrouper.BlockedEvent(
                database, contentiousObject,
                "UPDATE pm1893_res.dbo.locktarget SET v = v + 100 WHERE id = 1;",
                "UPDATE pm1893_res.dbo.locktarget SET v = v + 1 WHERE id = 1;",
                13882, "X"),
        };

    private static List<BlockingIncidentGrouper.BlockingGroup> Group(BlockingIncidentGrouper.BlockedEvent[] events) =>
        BlockingIncidentGrouper.Group("SQL2022", events);

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
