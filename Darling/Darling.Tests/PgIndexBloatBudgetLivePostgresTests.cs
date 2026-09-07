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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>pg_index_bloat</c>'s work budget bounds what the statement READS, measured by running the shipped
/// query against a live PostgreSQL and counting how many times <c>pgstatindex</c> was actually invoked.
///
/// <para><b>Why this test executes instead of asserting on query text.</b> The budget has been introduced
/// twice — a count (#2617) and a byte figure (#2997) — and both shipped as a further qual on a
/// <c>LEFT JOIN LATERAL</c> to the function. Both were pinned by <c>Assert.Contains</c>-style checks that
/// found the qual and passed. Neither bounded anything: the planner cannot skip an inner side it has not
/// evaluated, so the function ran once per candidate row and the qual only decided whether its answer was
/// kept. The output was correct every time — right row count, right <c>skipped_reason</c> on every skipped
/// index — while the statement read every b-tree index on the instance and died on its deadline having
/// reported nothing. <b>No assertion on the text of the query can tell those two apart</b>, because the
/// broken one and the fixed one differ in what the executor does, not in what the SQL says.</para>
///
/// <para><b>Why it lives in Darling.Tests.</b> Darling is what runs this collector in production —
/// <c>DarlingWorker</c> registers <c>pg_index_bloat</c> against
/// <see cref="PgIndexBloatCollector.Instance"/> — and the target where the collector has never returned a
/// row is a Darling target. This suite is also the only one with a live PostgreSQL to execute against.</para>
///
/// <para><b>The invariant, and why it cannot pass vacuously.</b> Three numbers are read out of the shipped
/// query itself rather than written here: <c>C</c>, the candidate rows it returns; <c>M</c>, those it
/// reports as measured; and <c>L</c>, the <c>Actual Loops</c> on the <c>pgstatindex</c> function-scan node
/// of its own <c>EXPLAIN ANALYZE</c>. The test asserts <c>L == M</c> — the function was invoked only for
/// the indexes the budget admitted — and <c>M &lt; C</c>, which is what makes the first assertion
/// meaningful: on a database where everything fits the budget, <c>L == M == C</c> holds under the broken
/// shape too. Measured both ways on PostgreSQL 17.11 with 215 candidates against the 200-index count
/// bound: the ON-clause form gives <c>C=215, M=200, L=215</c>, this one <c>C=215, M=200, L=200</c>.</para>
///
/// <para>Nothing about the query is rewritten for the test — not the literals, not the shape. The count
/// bound is the one made to bind, because pushing the candidate set past 200 costs a few hundred empty
/// indexes while pushing it past the byte budget would cost gigabytes. The two bounds are enforced by the
/// same <c>WHERE</c> on the same relation, so exercising either exercises the mechanism.</para>
///
/// <para>Creates its own schema and drops it, so it leaves nothing for the fixture's residue diff — which
/// watches <c>collect</c> and would not see this schema anyway.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgIndexBloatBudgetLivePostgresTests
{
    /// <summary>
    /// Enough throwaway b-tree indexes to carry the candidate set past the collector's 200-index count
    /// bound on its own, without depending on how many indexes the live store happens to contain. They are
    /// built on a tiny table so each is a page or two and <c>pgstatindex</c> over the admitted 200 is
    /// trivial work.
    /// </summary>
    private const int ProbeIndexCount = 210;

    private const string ProbeSchema = "darling_test_pg_index_bloat_budget";

    [Fact]
    public async Task TheWorkBudgetBoundsWhatIsRead_NotOnlyWhatIsReported_AgainstLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the pg_index_bloat work-budget test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        /* pgstattuple is contrib and ships with the bundled runtime. SCHEMA public is not decoration: the
           store's search_path is "collect, config, public", so a bare CREATE EXTENSION installs the
           extension into collect, and the collector - which qualifies the function public.pgstatindex on
           purpose - then fails with 42883 while the extension is, by every other measure, installed. That
           schema is the product's real precondition, so this establishes it rather than working around it. */
        await TryExecuteAsync(connection, "CREATE EXTENSION IF NOT EXISTS pgstattuple SCHEMA public", ct);

        /* IF NOT EXISTS makes the SCHEMA clause a no-op against a store that already installed it
           elsewhere, so the placement is repaired rather than assumed. */
        if (!await PgstatindexResolvesInPublicAsync(connection, ct))
        {
            await TryExecuteAsync(connection, "ALTER EXTENSION pgstattuple SET SCHEMA public", ct);
        }

        /* The precondition is asserted as the FUNCTION being callable where the collector calls it, never
           as CREATE EXTENSION having returned without error. Those two came apart on the first CI run of
           this test - the create succeeded and the function was unreachable - which is why this reads the
           catalog instead of trusting the DDL. A rig that still cannot offer it is a rig limitation and
           skips with that reason stated. */
        Assert.SkipWhen(
            !await PgstatindexResolvesInPublicAsync(connection, ct),
            "public.pgstatindex is not callable on this rig, so the collector's query cannot run at all.");

        /* The SHIPPED query, from the collector the service dispatches - not a copy of it. Literals,
           shape and all. Since #3153 it also carries BOUND parameters when a rotation cursor exists, so
           the plan travels rather than just its text: executing the text alone would fail with an
           unsupplied parameter the moment a cursor is stored, which is the one state this suite must be
           able to reach. */
        var plan = PgIndexBloatCollector.Instance.BuildQuery(MakeContext());
        var sql = plan.Text;

        var bodySucceeded = false;
        try
        {
            await ExecuteAsync(connection, $"DROP SCHEMA IF EXISTS {ProbeSchema} CASCADE", ct);
            await ExecuteAsync(connection, $"CREATE SCHEMA {ProbeSchema}", ct);
            await ExecuteAsync(
                connection,
                $"CREATE TABLE {ProbeSchema}.candidates AS SELECT g AS id FROM generate_series(1, 200) AS g",
                ct);

            /* Distinct expression indexes rather than distinct columns, so one narrow table carries all of
               them. */
            await ExecuteAsync(
                connection,
                $"""
                 DO $$
                 BEGIN
                     FOR n IN 1..{ProbeIndexCount.ToString(CultureInfo.InvariantCulture)} LOOP
                         EXECUTE format(
                             'CREATE INDEX ix_probe_%s ON {ProbeSchema}.candidates ((id + %s))',
                             lpad(n::text, 4, '0'), n);
                     END LOOP;
                 END $$
                 """,
                ct);

            /* C and M come from the query's own output. M is counted by skipped_reason rather than by a
               measurement column: the CASE arms are the exact complement of the gate, so a NULL reason IS
               "this index was admitted", and it stays correct however pgstatindex renders an odd index
               (an empty one reports avg_leaf_density as NaN, which is not NULL and would count either
               way). */
            var counts = await ReadCountsAsync(connection, plan, ct);

            Assert.True(
                counts.Candidates > counts.Measured,
                $"the probe left every one of {counts.Candidates} candidates inside the budget, so this run "
                + "cannot tell a budget that bounds reads from one that only labels rows - L == M == C holds "
                + "either way. Raise ProbeIndexCount above the collector's count bound");

            /* Every admitted index must have come back WITH a measurement. A gate that filters the
               function's input rather than its output can drop an admitted row if the join back is wrong,
               and that row would arrive carrying neither a measurement nor a reason - reported, and
               silently blank. */
            Assert.Equal(0, counts.AdmittedButUnmeasured);

            /* And L: how many times the executor actually entered pgstatindex. This is the whole test. */
            var loops = await ReadPgstatindexLoopsAsync(connection, plan, ct);

            Assert.Equal(counts.Measured, loops);

            bodySucceeded = true;
        }
        finally
        {
            /* #1902: teardown goes through the shared helper, on its own connection and its own lifetime.
               Opening one by hand leaves the throw-from-finally that replaces the body's exception, which
               is how a real failure gets reported as cleanup noise. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await ExecuteAsync(cleanup, $"DROP SCHEMA IF EXISTS {ProbeSchema} CASCADE", cleanupCt);
            });
        }
    }


    /// <summary>
    /// The measured set really does ROTATE, and a full pass really does cover every candidate — measured
    /// by driving the shipped query through consecutive cycles and feeding each cycle's own cursor back in
    /// exactly as the host does (#3153).
    ///
    /// <para><b>Why this executes rather than reading the query.</b> The pre-#3153 collector produced
    /// correct-looking output on every cycle — right row count, a <c>skipped_reason</c> on every skipped
    /// index — while measuring the SAME index forever, because the ordering was a pure function of the
    /// target catalog and measuring an index does not change its size. No assertion on the text can tell
    /// "rotates" from "does not": the two differ in what a SECOND cycle selects. So this asserts on the
    /// union of what several cycles measured, which is the property the <c>skipped_reason</c> claims.</para>
    ///
    /// <para><b>The three things it pins, and why each can fail on its own.</b> Cycle 2 must measure a
    /// DISJOINT set from cycle 1 — a cursor that did not advance re-measures the same rows. The union over
    /// a pass must be EVERY candidate — a cursor that advanced too far skips rows, and a wrap that fired
    /// early strands them. And no row, on any cycle, may carry a null measurement with a null reason —
    /// which is what a gate conjunct without a matching <c>CASE</c> arm produces, silently.</para>
    ///
    /// <para>The COUNT bound is what is made to bind, as in the sibling test above and for the same
    /// reason: pushing the candidate set past 200 costs a few hundred single-page indexes, while pushing
    /// it past the byte budget would cost gigabytes. Both bounds are enforced by the same <c>WHERE</c> on
    /// the same relation, so exercising either exercises the mechanism.</para>
    /// </summary>
    [Fact]
    public async Task TheMeasuredSetRotates_AndAFullPassCoversEveryCandidate_AgainstLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the pg_index_bloat rotation test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        await TryExecuteAsync(connection, "CREATE EXTENSION IF NOT EXISTS pgstattuple SCHEMA public", ct);

        if (!await PgstatindexResolvesInPublicAsync(connection, ct))
        {
            await TryExecuteAsync(connection, "ALTER EXTENSION pgstattuple SET SCHEMA public", ct);
        }

        Assert.SkipWhen(
            !await PgstatindexResolvesInPublicAsync(connection, ct),
            "public.pgstatindex is not callable on this rig, so the collector's query cannot run at all.");

        /* The connected database's real name, because the cursor is keyed by it and the statement selects
           its own row with current_database(). Read from the server rather than parsed out of the
           connection string, which may not name it at all. */
        await using var whoami = new NpgsqlCommand("SELECT current_database()", connection);
        var databaseName = (string)(await whoami.ExecuteScalarAsync(ct))!;

        var bodySucceeded = false;
        try
        {
            await ExecuteAsync(connection, $"DROP SCHEMA IF EXISTS {RotationSchema} CASCADE", ct);
            await ExecuteAsync(connection, $"CREATE SCHEMA {RotationSchema}", ct);
            await ExecuteAsync(
                connection,
                $"CREATE TABLE {RotationSchema}.candidates AS SELECT g AS id FROM generate_series(1, 200) AS g",
                ct);
            await ExecuteAsync(
                connection,
                $"""
                 DO $$
                 BEGIN
                     FOR n IN 1..{ProbeIndexCount.ToString(CultureInfo.InvariantCulture)} LOOP
                         EXECUTE format(
                             'CREATE INDEX ix_rot_%s ON {RotationSchema}.candidates ((id + %s))',
                             lpad(n::text, 4, '0'), n);
                     END LOOP;
                 END $$
                 """,
                ct);

            /* The host's own loop, reduced to what this property needs: build from the stored state, read
               through the definition, then land what the definition asked to persist. */
            var state = new Dictionary<string, string>(StringComparer.Ordinal);
            var perCycle = new List<HashSet<long>>();
            var candidateCounts = new List<int>();
            var cursors = new List<string>();

            for (var cycle = 0; cycle < RotationCycles; cycle++)
            {
                var plan = PgIndexBloatCollector.Instance.BuildQuery(MakeContext(state, databaseName));
                var context = MakeContext(state, databaseName);

                List<PgIndexBloatCollector.Row> rows;
                await using (var command = ProviderCommand(connection, plan, plan.Text))
                await using (var reader = await command.ExecuteReaderAsync(ct))
                {
                    rows = await PgIndexBloatCollector.Instance.ReadAsync(reader, context, ct);
                }

                /* The invariant that holds on EVERY cycle: a row is measured or it says why. A gate
                   conjunct with no matching CASE arm returns rows with neither, and an empty index really
                   does report a null density — so "blank measurement" alone is not detectable at read
                   time, which is what makes this the load-bearing check rather than a nicety. */
                Assert.DoesNotContain(
                    rows,
                    r => r.SkippedReason is null && r.LeafPages is null && r.AvgLeafDensity is null);

                candidateCounts.Add(rows.Count);
                perCycle.Add(rows.Where(r => r.SkippedReason is null).Select(r => r.IndexOid).ToHashSet());

                foreach (var entry in context.PendingState)
                {
                    state[entry.Key] = entry.Value;
                }

                cursors.Add(state[PgIndexBloatCollector.RotationCursorKeyPrefix + databaseName]);
            }

            /* The census never moves: rotation bounds what is MEASURED, never what is reported. */
            Assert.Single(candidateCounts.Distinct());

            /* The count bound has to have bitten, or nothing below distinguishes rotation from a database
               that simply fits in one cycle. */
            Assert.True(
                perCycle[0].Count < candidateCounts[0],
                $"cycle 1 measured all {candidateCounts[0]} candidates, so this run cannot tell a rotating "
                + "cursor from a stationary one. Raise ProbeIndexCount above the collector's count bound");

            /* THE assertion: cycle 2 measured different indexes. Against the pre-#3153 collector these two
               sets are identical, because nothing carried the first cycle's position forward. */
            Assert.NotEmpty(perCycle[1]);
            Assert.Empty(perCycle[0].Intersect(perCycle[1]));

            /* And the pass covers everything, which "measures something different" alone does not imply:
               a cursor that jumped too far would also satisfy the check above while stranding rows. */
            var covered = new HashSet<long>();
            foreach (var cycleSet in perCycle)
            {
                covered.UnionWith(cycleSet);
            }

            Assert.Equal(candidateCounts[0], covered.Count);

            /* The pass ENDS, and says so with the marker rather than by parking the cursor forever. */
            Assert.Contains(PgIndexBloatCollector.RotationPassCompleteMarker, cursors);

            /* And it starts again: the cycle after the marker measures a non-empty set, so a completed
               pass is a wrap and not a stop. */
            var wrappedAt = cursors.IndexOf(PgIndexBloatCollector.RotationPassCompleteMarker);
            Assert.True(
                wrappedAt < perCycle.Count - 1,
                "the pass completed on the last cycle, so this run never observed it start over. Raise "
                + "RotationCycles");
            Assert.NotEmpty(perCycle[wrappedAt + 1]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await ExecuteAsync(cleanup, $"DROP SCHEMA IF EXISTS {RotationSchema} CASCADE", cleanupCt);
            });
        }
    }

    /// <summary>Its own schema, so the two live tests in this file cannot see each other's indexes.</summary>
    private const string RotationSchema = "darling_test_pg_index_bloat_rotation";

    /// <summary>
    /// Enough cycles to see a full pass END and the next one BEGIN, at 210 candidates against a 200-index
    /// bound: 200, then 10, then the empty cycle that detects the pass is over and wraps, then the pass
    /// again. Four would do; six leaves room for the store to hold an index or two of its own inside the
    /// candidate set without the wrap sliding off the end of the run.
    /// </summary>
    private const int RotationCycles = 6;

    /// <summary>
    /// Wraps the shipped query so its own output supplies the expectations, rather than this test carrying
    /// a second copy of the budget arithmetic that could agree with a wrong answer.
    /// </summary>
    private static async Task<(int Candidates, int Measured, int AdmittedButUnmeasured)> ReadCountsAsync(
        NpgsqlConnection connection, CollectorQuery plan, CancellationToken cancellationToken)
    {
        await using var command = ProviderCommand(
            connection,
            plan,
            $"""
             SELECT count(*)::int                                                              AS candidates,
                    count(*) FILTER (WHERE q.skipped_reason IS NULL)::int                      AS measured,
                    count(*) FILTER (WHERE q.skipped_reason IS NULL
                                     AND q.avg_leaf_density IS NULL)::int                      AS admitted_unmeasured
             FROM ({plan.Text}) AS q
             """);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        Assert.True(await reader.ReadAsync(cancellationToken));

        return (reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2));
    }

    /// <summary>
    /// Builds the command through the SAME provider the service dispatches with, then swaps in the
    /// wrapper text. The parameter mapping is the shipped one rather than one retyped here — a test that
    /// bound its own types could pass while <c>PostgresTargetProvider</c>'s mapping did not resolve,
    /// which is precisely the class of failure this suite exists to catch.
    /// </summary>
    private static NpgsqlCommand ProviderCommand(
        NpgsqlConnection connection, CollectorQuery plan, string wrappedText)
    {
        var command = (NpgsqlCommand)PostgresTargetProvider.Instance.CreateCommand(
            plan, connection, PgIndexBloatCollector.Instance.CommandTimeoutSecondsOverride ?? 300);

        command.CommandText = wrappedText;

        return command;
    }

    /// <summary>
    /// <c>Actual Loops</c> on the <c>pgstatindex</c> function-scan node, which is the executor's own count
    /// of how many times it entered the function. JSON rather than the text plan because the number is
    /// being asserted on: a regex over indented plan text is one formatting change away from matching the
    /// wrong node, or nothing.
    /// </summary>
    private static async Task<int> ReadPgstatindexLoopsAsync(
        NpgsqlConnection connection, CollectorQuery plan, CancellationToken cancellationToken)
    {
        await using var command = ProviderCommand(
            connection,
            plan,
            $"EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF, COSTS OFF, SUMMARY OFF)\n{plan.Text}");

        var planJson = (string?)await command.ExecuteScalarAsync(cancellationToken);
        Assert.False(string.IsNullOrEmpty(planJson));

        using var document = JsonDocument.Parse(planJson!);
        var loops = FunctionScanLoops(document.RootElement[0].GetProperty("Plan"), "pgstatindex").ToArray();

        /* Exactly one such node: the query calls the function once, in one place. Two would mean the
           measurement below is describing only part of the work. */
        Assert.Single(loops);

        return loops[0];
    }

    private static System.Collections.Generic.IEnumerable<int> FunctionScanLoops(
        JsonElement node, string functionName)
    {
        if (node.TryGetProperty("Function Name", out var name)
            && string.Equals(name.GetString(), functionName, StringComparison.Ordinal))
        {
            yield return node.GetProperty("Actual Loops").GetInt32();
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                foreach (var loops in FunctionScanLoops(child, functionName))
                {
                    yield return loops;
                }
            }
        }
    }

    private static async Task ExecuteAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Whether the collector's own qualified call target exists. Overloads are not distinguished — any
    /// <c>pgstatindex</c> in <c>public</c> means the extension is where the query expects it.
    /// </summary>
    private static async Task<bool> PgstatindexResolvesInPublicAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT count(*)::int                AS overloads
            FROM pg_catalog.pg_proc AS p
            JOIN pg_catalog.pg_namespace AS n
              ON n.oid = p.pronamespace
            WHERE n.nspname = 'public'
            AND   p.proname = 'pgstatindex'
            """,
            connection);

        return (int)(await command.ExecuteScalarAsync(cancellationToken))! > 0;
    }

    /// <summary>
    /// Runs a precondition statement whose success is verified by the catalog rather than by its own
    /// return, so a failure here is not the finding — an unreachable function is, and it is checked
    /// directly.
    /// </summary>
    private static async Task TryExecuteAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        try
        {
            await ExecuteAsync(connection, sql, cancellationToken);
        }
        catch (PostgresException)
        {
        }
    }

    private static CollectorContext MakeContext(
        IReadOnlyDictionary<string, string>? state = null, string? currentDatabase = null) => new()
    {
        ServerId = 42,
        ServerName = "pg-target",
        CollectionTime = new DateTime(2026, 9, 7, 12, 0, 0, DateTimeKind.Utc),
        Deltas = NoDeltas.Instance,
        Target = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = 17,
        },
        ExcludedDatabases = Array.Empty<string>(),
        State = state ?? CollectorContext.NoState,
        CurrentDatabaseName = currentDatabase,
    };

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public static readonly NoDeltas Instance = new();

        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key,
            long currentValue, out int intervalSeconds, DateTime? collectionTime = null,
            int maxGapSeconds = 0)
        {
            intervalSeconds = 60;
            return 0;
        }
    }
}
