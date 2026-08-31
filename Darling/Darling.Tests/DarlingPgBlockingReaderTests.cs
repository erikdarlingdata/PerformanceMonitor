/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the blocking read: chains assembled from the stored edge list, roots attributed, and the cycle
/// case that the chain query structurally cannot see.
///
/// <para>Two of these pin defects that live probing found and that no C# test could have caught on its own:
/// the missing <c>WITH RECURSIVE</c> (a runtime error on the first call, because PostgreSQL scopes the
/// keyword to the whole WITH clause) and the unaliased output columns. Both were correct-looking text.</para>
/// </summary>
public class DarlingPgBlockingReaderTests
{
    private static string ChainsSql => DarlingPgBlockingReader.PgBlockingChainsSql;

    private static string CyclesSql => DarlingPgBlockingReader.PgBlockingCyclesSql;

    /// <summary>
    /// <c>WITH RECURSIVE</c>, not <c>WITH</c>. PostgreSQL scopes <c>RECURSIVE</c> to the entire WITH clause
    /// rather than to the one CTE that needs it, so a self-referencing CTE behind a plain <c>WITH</c> fails
    /// with <c>relation "chain" does not exist</c> — at runtime, on the first call, never at build time.
    /// Both queries here recurse.
    /// </summary>
    [Fact]
    public void BothRecursiveQueriesDeclareWithRecursive()
    {
        Assert.StartsWith("WITH RECURSIVE", ChainsSql.TrimStart(), StringComparison.Ordinal);
        Assert.StartsWith("WITH RECURSIVE", CyclesSql.TrimStart(), StringComparison.Ordinal);
    }

    /// <summary>
    /// A root is a backend that blocks something and is not itself blocked — found by absence, which is the
    /// only way to find it and the reason the collector stores the whole edge set per capture rather than
    /// only the pairs someone asked about.
    /// </summary>
    [Fact]
    public void FindsRootsByAbsenceOfAnUpstreamBlocker()
    {
        Assert.Contains("WHERE NOT EXISTS (", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("upstream.blocked_pid = e.blocking_pid", ChainsSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// BOTH recursions must be depth-capped. A cycle in the edge set would otherwise run an uncapped
    /// recursive CTE until it exhausted memory — and cycles are reachable, since PostgreSQL only resolves
    /// them after <c>deadlock_timeout</c> and a capture can land inside that window.
    /// </summary>
    [Fact]
    public void BothRecursionsAreDepthCapped()
    {
        Assert.Contains("depth < 32", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("depth < 32", CyclesSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The CHAIN recursion must refuse to revisit a backend already on its walk. Without it, a cycle hanging
    /// off an otherwise legitimate root is walked to the depth cap: root A blocks B while B/C/D cycle among
    /// themselves, A still qualifies as a root, and the walk goes B → C → D → B → … until depth 32. The cap
    /// stops the runaway, but the reader then sees <c>max_depth = 32</c> and a worst victim drawn from
    /// repeated revisits — indistinguishable from a genuine 32-deep chain.
    /// <para>Demonstrated against live Aurora on a fixture with exactly that shape: the unguarded recursion
    /// reported <c>max_depth = 32</c> where the guarded one reports 2.</para>
    /// </summary>
    [Fact]
    public void TheChainRecursionRefusesToRevisitABackend()
    {
        Assert.Contains("ARRAY[r.blocking_pid, e.blocked_pid] AS visited", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("e.blocked_pid <> ALL(c.visited)", ChainsSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Recurrence must exclude the vanished-blocker sentinel. The collector stores
    /// <c>coalesce(blocker.backend_id, 0)</c>, so every root whose own row had already left
    /// <c>pg_stat_activity</c> lands on id <c>0</c> — and grouping those together counts unrelated one-off
    /// incidents from different captures as repeat appearances of one backend. That is exactly the
    /// conflation the synthetic backend id exists to prevent, arriving through the fallback rather than
    /// through pid reuse.
    /// <para>Excluded rather than counted, so the outer LEFT JOIN yields NULL and the read reports
    /// recurrence as UNKNOWN. "Seen once" and "cannot tell" are different claims and the tool says which.</para>
    /// </summary>
    [Fact]
    public void RecurrenceExcludesTheVanishedBlockerSentinel()
    {
        Assert.Contains("WHERE blocking_backend_id <> 0", ChainsSql, StringComparison.Ordinal);
        /* And the projection must NOT coalesce the resulting NULL into a number. */
        Assert.DoesNotContain("coalesce(c.samples_as_root, 1)", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("c.samples_as_root", ChainsSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The cycle query must be scoped to its own participants and grouped per COMPONENT, not per capture.
    /// A one-minute capture snapshots the whole instance, so one collection routinely holds several
    /// unrelated situations — the case none of the original probe scenarios covered, since each was isolated.
    /// <para>Both failure modes were demonstrated live on a fixture holding a cycle in <c>zz_cycle_db</c>
    /// beside an unrelated chain in <c>aa_other_db</c>, plus two disjoint cycles in one capture: the
    /// unscoped join reported the deadlock's database as <c>aa_other_db</c> (alphabetically first across the
    /// whole capture), and grouping on <c>collection_id</c> alone merged the two disjoint cycles into one
    /// bogus four-participant component.</para>
    /// </summary>
    [Fact]
    public void TheCycleQueryIsScopedToItsOwnParticipantsAndGroupedPerComponent()
    {
        /* Scoped join: the edge rows feeding min(database_name) must belong to the cycle. */
        Assert.Contains("e.blocked_pid = ANY(c.members)", CyclesSql, StringComparison.Ordinal);
        /* Per-component grouping, canonicalised so each participant's rotation collapses to one row. */
        Assert.Contains("array_agg(m ORDER BY m)", CyclesSql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY e.collection_id, e.collection_time, c.members", CyclesSql, StringComparison.Ordinal);
        /* And the walk must not wander into a foreign cycle on the way. */
        Assert.Contains("e.blocking_pid <> ALL(w.members)", CyclesSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The cycle walk must also stop when it returns to where it started, not lean on the depth cap alone.
    /// The cap bounds the damage; this is what makes the query correct rather than merely finite.
    /// </summary>
    [Fact]
    public void TheCycleWalkStopsOnClosingTheLoop()
    {
        Assert.Contains("w.at_pid <> w.start_pid", CyclesSql, StringComparison.Ordinal);
        /* And the detection itself: reachable from yourself. Unaliased — it lives in the `closed` CTE,
           which selects straight from `walk`. */
        Assert.Contains("WHERE at_pid = start_pid", CyclesSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Recurrence is counted on the synthetic backend id, NOT the pid. That is the whole reason the collector
    /// computes the id: a pid is reused, so a 30-day count keyed on it silently merges two different
    /// backends, and "the same stuck session all afternoon" and "a succession of different ones" have
    /// different remedies.
    /// </summary>
    [Fact]
    public void RecurrenceIsKeyedOnTheBackendIdNotThePid()
    {
        Assert.Contains(
            "count(DISTINCT collection_id) AS samples_as_root", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY blocking_backend_id", ChainsSql, StringComparison.Ordinal);
        Assert.DoesNotContain("GROUP BY blocking_pid", ChainsSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Direct victims and total victims are different numbers and both are reported: one root blocking
    /// thirty sessions directly is a different shape of problem from a thirty-deep chain, and a single
    /// count cannot tell them apart.
    /// </summary>
    [Fact]
    public void SeparatesDirectVictimsFromTotalVictims()
    {
        Assert.Contains(
            "count(DISTINCT blocked_pid)::int AS total_victims", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE depth = 1)", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("max(depth)::int AS max_depth", ChainsSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ordered worst-first, not newest-first. Under a row limit a newest-first ordering answers a different
    /// question than the one asked, and can omit the incident entirely.
    /// </summary>
    [Fact]
    public void OrdersWorstFirstSoARowLimitCannotHideTheIncident()
    {
        Assert.Contains(
            "ORDER BY s.total_victims DESC, s.max_depth DESC, d.collection_time DESC",
            ChainsSql,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Every output column of both queries carries an explicit alias. The C# readers are positional so this
    /// changes no behaviour — but three unaliased <c>coalesce()</c> expressions came back as three columns
    /// all named "coalesce", which makes the query unreadable in the one tool anyone debugging it will
    /// actually reach for.
    /// </summary>
    [Fact]
    public void EveryOutputColumnIsAliased()
    {
        foreach (var (name, sql) in new[] { ("chains", ChainsSql), ("cycles", CyclesSql) })
        {
            /* The final SELECT is the last one in the text; its items are the output columns. Any that is a
               bare function call with no AS is the hazard. */
            var lastSelect = sql.LastIndexOf("SELECT", StringComparison.Ordinal);
            Assert.True(lastSelect > 0, $"the {name} query must have a final SELECT");
            var finalSelect = sql[lastSelect..];

            Assert.False(
                Regex.IsMatch(
                    finalSelect,
                    @"^\s+(?:coalesce|count|min|max|array_agg)\(.*\)\s*,?\s*$",
                    RegexOptions.Multiline),
                $"the {name} query has an unaliased function in its output list — it comes back named after "
                + "the function, and several such columns collide into identical headings");
        }
    }

    /// <summary>
    /// The capture-count read must come from <c>collection_log</c>, not from the edge table. The edge table
    /// cannot tell "no blocking" from "not collected" — both are an absence of rows — and that distinction
    /// is the difference between an all-clear and knowing nothing at all.
    /// </summary>
    [Fact]
    public void CaptureCountsComeFromTheCollectionLog()
    {
        var sql = DarlingPgBlockingReader.PgBlockingCaptureCountsSql;

        Assert.Contains("FROM collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("l.collector_name = 'pg_blocking'", sql, StringComparison.Ordinal);
        Assert.Contains("l.status = 'SUCCESS'", sql, StringComparison.Ordinal);
        /* Both halves of the denominator: captures that found blocking, and captures at all. */
        Assert.Contains("count(*) FILTER (WHERE l.rows_collected > 0)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every parameter is positional and bound, never interpolated — including the row limit, which was a
    /// hardcoded <c>LIMIT 50</c> in an earlier PostgreSQL reader and had to be corrected.
    /// </summary>
    [Fact]
    public void TheRowLimitIsAParameter()
    {
        Assert.Contains("LIMIT $4", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", CyclesSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both queries scope to one server and one window. A read that forgot either would silently mix
    /// servers' pids together, and a pid is only unique within one instance.
    /// </summary>
    [Fact]
    public void BothQueriesScopeToOneServerAndWindow()
    {
        foreach (var sql in new[] { ChainsSql, CyclesSql })
        {
            Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
            Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
            Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Sessions queued BEHIND a cycle must be reported. Before this, a "lollipop" — X blocked by A where
    /// A/B/C form a cycle — put X in neither read: <c>chains</c> cannot reach it because no cycle member
    /// qualifies as a root, and the cycle walk cannot either because a walk starting at X's edge extends into
    /// the cycle and is barred from closing on its own start, so it never lands in <c>closed</c>. A real,
    /// captured blocking relationship appeared nowhere at all — the one outcome this collector's whole design
    /// forbids.
    /// <para>Transitive, and members are excluded, so a session two hops behind the cycle still counts and a
    /// participant is never double-reported as its own victim. Verified live: 910 and 911 appear as neither a
    /// chain root nor a cycle participant, and surface only through this count.</para>
    /// </summary>
    [Fact]
    public void SessionsQueuedBehindACycleAreReported()
    {
        Assert.Contains("behind AS (", CyclesSql, StringComparison.Ordinal);
        /* Transitive: the CTE must recurse on itself, not just take direct victims. */
        Assert.Contains("FROM behind AS b", CyclesSql, StringComparison.Ordinal);
        /* Members are never counted as being behind their own cycle. */
        Assert.Contains("e.blocked_pid <> ALL(c.members)", CyclesSql, StringComparison.Ordinal);
        Assert.Contains("blocked_behind_count", CyclesSql, StringComparison.Ordinal);
        /* Zero, not NULL, for a cycle with nothing behind it — an absent count would read as unknown. */
        Assert.Contains("coalesce(max(b.blocked_behind_count), 0)", CyclesSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two columns the collector computes per EDGE must be aggregated over all of the root's edges, never
    /// taken from the one edge <c>DISTINCT ON</c> happens to pick. <c>database_name</c> is
    /// <c>coalesce(blocked.datname, blocker.datname)</c> and <c>query_text_may_be_truncated</c> is an OR
    /// across both queries, so an arbitrary pick attributes a victim's database, or a victim's clipped text,
    /// to the root. Everything else on <c>root_detail</c> is genuinely backend-constant and the pick is fine.
    /// </summary>
    [Fact]
    public void PerEdgeColumnsAreAggregatedNotArbitrarilyPicked()
    {
        Assert.Contains("root_edge_agg AS (", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("array_agg(DISTINCT e.database_name) AS databases", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("bool_or(e.query_text_may_be_truncated)", ChainsSql, StringComparison.Ordinal);
        /* And root_detail must no longer carry either of them. */
        var rootDetail = ChainsSql[ChainsSql.IndexOf("root_detail AS (", StringComparison.Ordinal)..
                                   ChainsSql.IndexOf("root_edge_agg AS (", StringComparison.Ordinal)];
        Assert.DoesNotContain("e.database_name", rootDetail, StringComparison.Ordinal);
        Assert.DoesNotContain("e.query_text_may_be_truncated", rootDetail, StringComparison.Ordinal);
    }

    /// <summary>
    /// A chain that hit the walk cap must say so. With the revisit guard in place a <c>max_depth</c> of 32 is
    /// no longer a masked cycle — it is a genuinely 32-level walk the cap stopped, which makes
    /// <c>total_victims</c> and the worst victim FLOORS rather than totals, indistinguishable from a complete
    /// answer. Implausible in practice and reported anyway, for the same reason the sampling caveat is:
    /// a short answer must never pass for the whole picture.
    /// </summary>
    [Fact]
    public void ATruncatedChainAnnouncesItself()
    {
        Assert.Contains("chain_may_be_truncated", ChainsSql, StringComparison.Ordinal);
        Assert.Contains("(s.max_depth >= 32)", ChainsSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every <c>RemedyFor</c> branch, pinned individually — the same treatment
    /// <c>DarlingMcpPgXminTools.RemedyFor</c> already gets, and for the same reason: this text IS the
    /// product's answer, and the branches are ordered, so reordering the aborted-state check relative to the
    /// boolean flag would silently change which remedy a caller receives.
    /// <para>The distinctions are not cosmetic. Idle-in-transaction is an application defect, active is a
    /// tuning problem, aborted is a client that ignored an error it already had, and a null state means the
    /// chain resolved itself. Prescribing any one of those for another wastes the reader's time or loses
    /// work.</para>
    /// </summary>
    [Fact]
    public void EveryRemedyBranchIsDistinctAndNamesItsOwnAction()
    {
        var idle = DarlingMcpPgBlockingTools.RemedyFor("idle in transaction", false, 240_000);
        var idleByFlag = DarlingMcpPgBlockingTools.RemedyFor(null, true, 0);
        var aborted = DarlingMcpPgBlockingTools.RemedyFor("idle in transaction (aborted)", false, 0);
        var active = DarlingMcpPgBlockingTools.RemedyFor("active", false, 8_400);
        var unknown = DarlingMcpPgBlockingTools.RemedyFor(null, false, 0);
        var other = DarlingMcpPgBlockingTools.RemedyFor("fastpath function call", false, 0);

        Assert.Contains("IDLE IN TRANSACTION", idle, StringComparison.Ordinal);
        Assert.Contains("idle_in_transaction_session_timeout", idle, StringComparison.Ordinal);

        /* The boolean flag alone must reach the same branch — the collector stamps it, and a root whose
           state string was not captured can still be known to have been idle in transaction. */
        Assert.Equal(idle, idleByFlag);

        Assert.Contains("ABORTED", aborted, StringComparison.Ordinal);
        Assert.Contains("ROLLBACK", aborted, StringComparison.Ordinal);
        /* Ordering pin: 'idle in transaction (aborted)' must NOT fall into the plain idle branch, which its
           prefix would match under a StartsWith or a reordered check. */
        Assert.NotEqual(idle, aborted);

        Assert.Contains("ACTIVE", active, StringComparison.Ordinal);
        /* The duration is only mentioned when there is one to mention. */
        Assert.Contains("8400 ms", active, StringComparison.Ordinal);
        Assert.DoesNotContain("ms", DarlingMcpPgBlockingTools.RemedyFor("active", false, 0), StringComparison.Ordinal);

        Assert.Contains("was not captured", unknown, StringComparison.Ordinal);

        Assert.Contains("fastpath function call", other, StringComparison.Ordinal);

        /* All six answers distinct — a collapsed branch is the failure this test exists for. */
        var all = new[] { idle, aborted, active, unknown, other };
        Assert.Equal(all.Length, all.Distinct(StringComparer.Ordinal).Count());
    }

    private static string DedupedByRootSql => DarlingPgBlockingReader.PgBlockingChainsDedupedByRootSql;

    /// <summary>
    /// #2714: <see cref="PgBlockingChainsSql"/> is unchanged — same candidate CTEs, same severity-ordered
    /// LIMIT — proving the shared factoring didn't alter what the MCP tool and the Viewer's blocking grid
    /// already ship. The behavioral fix lives entirely in the new, separate
    /// <see cref="PgBlockingChainsDedupedByRootSql"/> query these callers do not use.
    /// </summary>
    [Fact]
    public void ChainsSql_IsUnchangedByTheDedupRefactor()
    {
        Assert.StartsWith("WITH RECURSIVE", ChainsSql.TrimStart(), StringComparison.Ordinal);
        Assert.EndsWith(
            "ORDER BY s.total_victims DESC, s.max_depth DESC, d.collection_time DESC\nLIMIT $4",
            ChainsSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2714: the dedup happens BEFORE the row-count LIMIT, not after — <c>DISTINCT ON</c> must appear
    /// ahead of the final <c>LIMIT $4</c> in the query text, or this is the same bug re-typed. Also pins the
    /// sentinel-aware partition key (root_backend_id = 0 falls back to root_pid, mirroring
    /// <c>DarlingWorker.WorstPgBlockingChainPerRoot</c>'s own C# dedup identity) so the two layers of
    /// defense-in-depth cannot silently disagree about what "the same root" means.
    /// </summary>
    [Fact]
    public void DedupedByRootSql_DedupesBeforeLimit_UsingTheSameSentinelAwareRootIdentity()
    {
        var distinctOnIndex = DedupedByRootSql.IndexOf("DISTINCT ON (root_key)", StringComparison.Ordinal);
        var limitIndex = DedupedByRootSql.LastIndexOf("LIMIT $4", StringComparison.Ordinal);

        Assert.True(distinctOnIndex >= 0, "expected a DISTINCT ON (root_key) dedup step");
        Assert.True(limitIndex >= 0, "expected a trailing LIMIT $4");
        Assert.True(distinctOnIndex < limitIndex,
            "DISTINCT ON must run BEFORE the row-count LIMIT, or this is #2714 re-typed");

        Assert.Contains("WHEN root_backend_id = 0", DedupedByRootSql, StringComparison.Ordinal);
        Assert.Contains("'pid:' || root_pid::text", DedupedByRootSql, StringComparison.Ordinal);
        Assert.Contains("'bid:' || root_backend_id::text", DedupedByRootSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2714: the deduped query must select the exact same output columns, in the exact same order, as
    /// <see cref="PgBlockingChainsSql"/> — <see cref="DarlingPgBlockingReader.MapChainRow"/> reads both by
    /// ordinal, and a mismatch here is the exact "two lists that must agree, and nothing makes them" hazard
    /// that method's own doc comment already warns about for the original query's projection.
    /// </summary>
    [Fact]
    public void DedupedByRootSql_ProjectsTheSameColumnsInTheSameOrder_AsChainsSql()
    {
        string[] expectedColumns =
        [
            "captured_at", "root_backend_id", "root_pid", "databases", "root_username",
            "root_application_name", "root_state", "root_query", "root_is_idle_in_transaction",
            "root_xact_duration_ms", "root_query_duration_ms", "total_victims", "direct_victims",
            "max_depth", "worst_victim_wait_ms", "worst_victim_query", "samples_as_root",
            "query_text_may_be_truncated", "chain_may_be_truncated",
        ];

        var finalSelectStart = DedupedByRootSql.LastIndexOf("SELECT", StringComparison.Ordinal);
        Assert.True(finalSelectStart >= 0, "expected a final SELECT projecting the deduped rows");
        var finalSelect = DedupedByRootSql[finalSelectStart..];

        var lastIndex = -1;
        foreach (var column in expectedColumns)
        {
            var index = finalSelect.IndexOf(column, StringComparison.Ordinal);
            Assert.True(index > lastIndex, $"expected '{column}' to appear, in order, in the final SELECT");
            lastIndex = index;
        }
    }

    /// <summary>
    /// #2714: the wiring, not just the query shape. Neither <c>EvaluatePgBlockingAsync</c> nor
    /// <c>WorstPgBlockingChainPerRoot</c> is otherwise exercised by a fast unit test — both talk to Postgres
    /// directly and are covered only by the gated live test above — so nothing else would catch a future
    /// edit quietly reverting the alert's call site back to the raw, pre-#2714
    /// <see cref="DarlingPgBlockingReader.GetPgBlockingChainsAsync"/> without also reverting the query it
    /// calls. Pinned at source, the same idiom <c>DatabaseMismatchTripwireTests</c> already uses for this
    /// exact class of cross-artifact contract.
    /// </summary>
    [Fact]
    public void EvaluatePgBlockingAsync_CallsTheDedupedByRootReader_NotTheRawOne()
    {
        var source = ReadWorkerSource();
        var methodStart = source.IndexOf("private async Task EvaluatePgBlockingAsync(", StringComparison.Ordinal);
        Assert.True(methodStart >= 0, "expected to find EvaluatePgBlockingAsync in DarlingWorker.cs");

        var methodEnd = source.IndexOf("\n    private", methodStart + 1, StringComparison.Ordinal);
        Assert.True(methodEnd > methodStart, "expected to find the next method after EvaluatePgBlockingAsync");
        var body = source[methodStart..methodEnd];

        Assert.Contains("DarlingPgBlockingReader.GetPgBlockingChainsDedupedByRootAsync", body, StringComparison.Ordinal);
        Assert.DoesNotContain("DarlingPgBlockingReader.GetPgBlockingChainsAsync(", body, StringComparison.Ordinal);
    }

    private static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) proof of #2714 against a real instance: a severe root sampled repeatedly across
/// the window really does crowd a second, distinct root entirely out of <see cref="DarlingPgBlockingReader.
/// PgBlockingChainsSql"/>'s severity-ordered LIMIT — proven RED here — and
/// <see cref="DarlingPgBlockingReader.PgBlockingChainsDedupedByRootSql"/> keeps both roots under the exact
/// same LIMIT, proven GREEN. The text pins above prove the query SHAPE; this proves the query DOES what the
/// shape claims against PostgreSQL's actual planner and DISTINCT ON semantics, which no text assertion can.
/// </summary>
[Collection("live-postgres")]
public sealed class PgBlockingChainsDedupedByRootLivePostgresTests
{
    private const string ServerName = "darling-pgblocking-dedup-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* Root A: severe (3 victims/sample), sampled on 20 separate captures — enough to fill a LIMIT of 20 on
       its own. Root B: distinct, less severe (1 victim), sampled ONCE, at a time no earlier than Root A's
       samples, so recency cannot accidentally rescue it — only severity-before-dedup can, or #2714 is back. */
    private const int RootABackendId = 900_001;
    private const int RootAPid = 51_000;
    private const int RootBBackendId = 900_002;
    private const int RootBPid = 51_100;
    private const int SampleCount = 20;
    private const int LimitUnderTest = SampleCount;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ASeverePersistentRoot_DoesNotCrowdADistinctRoot_OutOfTheDedupedRead()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #2714 dedup-before-limit test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var collectionId = 0L;

            for (var i = 0; i < SampleCount; i++)
            {
                collectionId++;
                var capturedAt = now.AddMinutes(-SampleCount + i);
                for (var victim = 0; victim < 3; victim++)
                {
                    await PlantEdgeAsync(
                        connection, collectionId, capturedAt, RootAPid, RootABackendId,
                        blockedPid: RootAPid + 100 + victim, ct);
                }
            }

            collectionId++;
            await PlantEdgeAsync(
                connection, collectionId, now, RootBPid, RootBBackendId, blockedPid: RootBPid + 100, ct);

            var windowStart = now.AddHours(-2);
            var windowEnd = now.AddMinutes(1);

            /* RED: the pre-#2714 shape. Severity-ordered LIMIT, applied before any per-root dedup, spends
               its whole budget on Root A's 20 equally-severe repeat samples and never reaches Root B at
               all — not merely under-ranks it, DROPS it, so no downstream dedup step can recover it. */
            var raw = await DarlingPgBlockingReader.GetPgBlockingChainsAsync(
                postgres, ServerId, windowStart, windowEnd, LimitUnderTest, ct);

            Assert.Equal(LimitUnderTest, raw.Count);
            Assert.All(raw, row => Assert.Equal(RootABackendId, row.RootBackendId));
            Assert.DoesNotContain(raw, row => row.RootBackendId == RootBBackendId);

            /* GREEN: #2714's fix. Same LIMIT, but dedup-by-root runs BEFORE it, so the budget is spent on
               root DIVERSITY — both roots survive even though Root A alone could have filled it twice over. */
            var deduped = await DarlingPgBlockingReader.GetPgBlockingChainsDedupedByRootAsync(
                postgres, ServerId, windowStart, windowEnd, LimitUnderTest, ct);

            Assert.Equal(2, deduped.Count);
            Assert.Contains(deduped, row => row.RootBackendId == RootABackendId && row.TotalVictims == 3);
            Assert.Contains(deduped, row => row.RootBackendId == RootBBackendId && row.TotalVictims == 1);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    private static async Task PlantEdgeAsync(
        NpgsqlConnection connection, long collectionId, DateTime capturedAt, int blockingPid, int blockingBackendId,
        int blockedPid, System.Threading.CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO collect.pg_blocking_edges
                (collection_id, collection_time, server_id, server_name, blocked_pid, blocking_pid,
                 blocking_backend_id, database_name, blocked_query, blocking_query,
                 query_text_may_be_truncated, blocking_is_idle_in_transaction)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, 'appdb', 'select victim', 'update root set x = 1', false, false)
            """;
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(capturedAt, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(blockedPid);
        command.Parameters.AddWithValue(blockingPid);
        command.Parameters.AddWithValue((long)blockingBackendId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM collect.pg_blocking_edges WHERE server_id = $1";
        command.Parameters.AddWithValue(ServerId);
        await command.ExecuteNonQueryAsync(ct);
    }
}
