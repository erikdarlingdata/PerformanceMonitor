/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
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

        /* pgstattuple is contrib and ships with the bundled runtime, so this is expected to succeed. A rig
           without it is a rig limitation rather than a product defect, hence skip rather than fail — but
           the reason is stated, because a silently-skipped execution test is the thing this test exists to
           object to. */
        var pgstattupleAvailable = true;
        try
        {
            await ExecuteAsync(connection, "CREATE EXTENSION IF NOT EXISTS pgstattuple", ct);
        }
        catch (PostgresException)
        {
            pgstattupleAvailable = false;
        }

        Assert.SkipWhen(!pgstattupleAvailable,
            "pgstattuple is not installable on this rig, so pgstatindex cannot be invoked at all.");

        /* The SHIPPED string, from the collector the service dispatches - not a copy of it. Literals,
           shape and all. */
        var sql = PgIndexBloatCollector.Instance.BuildQuery(MakeContext()).Text;

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
            var counts = await ReadCountsAsync(connection, sql, ct);

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
            var loops = await ReadPgstatindexLoopsAsync(connection, sql, ct);

            Assert.Equal(counts.Measured, loops);
        }
        finally
        {
            await using var cleanup = new NpgsqlConnection(connectionString);
            await cleanup.OpenAsync(CancellationToken.None);
            await ExecuteAsync(cleanup, $"DROP SCHEMA IF EXISTS {ProbeSchema} CASCADE", CancellationToken.None);
        }
    }

    /// <summary>
    /// Wraps the shipped query so its own output supplies the expectations, rather than this test carrying
    /// a second copy of the budget arithmetic that could agree with a wrong answer.
    /// </summary>
    private static async Task<(int Candidates, int Measured, int AdmittedButUnmeasured)> ReadCountsAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT count(*)::int                                                              AS candidates,
                    count(*) FILTER (WHERE q.skipped_reason IS NULL)::int                      AS measured,
                    count(*) FILTER (WHERE q.skipped_reason IS NULL
                                     AND q.avg_leaf_density IS NULL)::int                      AS admitted_unmeasured
             FROM ({sql}) AS q
             """,
            connection);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        Assert.True(await reader.ReadAsync(cancellationToken));

        return (reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2));
    }

    /// <summary>
    /// <c>Actual Loops</c> on the <c>pgstatindex</c> function-scan node, which is the executor's own count
    /// of how many times it entered the function. JSON rather than the text plan because the number is
    /// being asserted on: a regex over indented plan text is one formatting change away from matching the
    /// wrong node, or nothing.
    /// </summary>
    private static async Task<int> ReadPgstatindexLoopsAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF, COSTS OFF, SUMMARY OFF)\n{sql}",
            connection);

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

    private static CollectorContext MakeContext() => new()
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
