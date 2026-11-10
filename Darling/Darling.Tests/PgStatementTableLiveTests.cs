/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8400: the run <see cref="PgStatementTable.RunAsync"/> returns is one the eviction pass missed. The pass a forcing run
/// brings on between a body's statements and its first read takes the entries run once first, the body's own among them,
/// so the body's read of its own entry fails inside the run. That failure is the pass's doing: the run is voided, the body
/// runs again, and the run returned read the entry its statement made. The read's count on each run is recorded, so a
/// pass that left the body's entry in place fails the test instead of passing it on a voided run that finished.
/// </summary>
/* pg-cluster-roles, not live-postgres: the test resets the cluster-wide pg_stat_statements first, so that every entry in
   the table at the forced pass is the test's own and the pass takes the body's entry for certain. The collection runs
   alone, so no other class has entries in the table to lose to the reset. */
[Collection("pg-cluster-roles")]
public sealed class PgStatementTableLiveTests
{
    [Fact]
    public async Task ARunWhoseOwnEntryTheForcedPassTakes_IsVoidedAndRunAgain_AndTheRunReturnedReadTheEntryItsStatementMade()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the statement table test.");
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: CREATE EXTENSION would leave residue in the shared store; a scratch database of its own goes with it. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        try
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            /* A table of this test's entries only. The pass removes the five percent of the table with the least usage, the
               entries run once before any run twice; with fewer entries run once than that, the body's own is among them. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT pg_stat_statements_reset()");
        }
        catch (PostgresException ex) when (ex.SqlState is "55000" or "58P01" or "0A000" or "42501")
        {
            Assert.Skip("pg_stat_statements is not available on this server: " + ex.Message);
        }

        /* The pass takes the least-used five percent of the table, the entries run once before any run twice. At the pass a
           forcing run brings on, the entries run once are the body's own and part of one fill batch; with the pass bigger
           than that, the body's own is among the entries it takes for certain, and otherwise only sometimes. */
        var max = await PgStatementTable.MaxEntriesAsync(connection, ct);
        Assert.SkipWhen(max * 5 / 100 < PgStatementTable.FillBatch + PgStatementTable.Headroom,
            $"pg_stat_statements.max is {max}: a pass takes {max * 5 / 100} entries, not comfortably more than the {PgStatementTable.FillBatch} shapes of a fill batch, so the forced pass does not take the body's own entry for certain.");

        var runs = new List<int>();
        var reads = new List<int>();
        var run = await PgStatementTable.RunAsync(connection, ct, async pass =>
        {
            runs.Add(pass.Number);
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT 840900 + 1 AS pm8409_own_entry");

            await pass.BeforeReadAsync(ct);

            var texts = new List<string>();
            await using (var read = new NpgsqlCommand(
                "SELECT query FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND query LIKE '%pm8409\\_own\\_entry%'",
                connection))
            await using (var reader = await read.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    texts.Add(reader.GetString(0));
                }
            }

            /* The read of the entry the statement above made; on the run the pass landed in, the entry is gone. */
            reads.Add(texts.Count);
            Assert.Single(texts);
        }, forcePass: true);

        PgStatementTable.AssertNoPassInside(run);
        Assert.Equal(new[] { 1, 2 }, runs);
        /* The first run's read found nothing, so the second run is the retry after a read that failed; a first read that
           found the entry would mean the pass missed it, and the run was voided for the pass alone. */
        Assert.Equal(new[] { 0, 1 }, reads);
        Assert.Equal(2, run.Number);
    }

    /// <summary>
    /// <see cref="PgStatementTable.ForcePassAsync"/> takes the entries run once in the connection's database whatever else the
    /// table holds. A pass removes the least-used five percent of the table and lowers the usage of every entry it leaves,
    /// so an entry run once before a pass sorts below one run once since it. With more of the former in the table than one
    /// pass removes, a fill that stops at the first pass leaves the body's own entry in place; the fill has to go on until
    /// the database's entries run once are read back gone.
    /// </summary>
    [Fact]
    public async Task TheForcedPass_TakesTheDatabasesEntriesRunOnce_WhenMoreEntriesRunOnceBeforeThanOnePassRemovesAreInTheTable()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the statement table test.");
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: CREATE EXTENSION would leave residue in the shared store; two scratch databases of its own go with it,
           one for the body's entry and one for the entries that compete with it for the pass. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var other = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var competitor = new NpgsqlConnection(other.ConnectionString);
        await competitor.OpenAsync(ct);
        try
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            await DarlingMcpTestData.ExecAsync(competitor, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT pg_stat_statements_reset()");
        }
        catch (PostgresException ex) when (ex.SqlState is "55000" or "58P01" or "0A000" or "42501")
        {
            Assert.Skip("pg_stat_statements is not available on this server: " + ex.Message);
        }

        var max = await PgStatementTable.MaxEntriesAsync(connection, ct);
        var passSize = max * 5 / 100;

        /* Three passes' worth of entries run once in the other database, then exactly one pass, brought on by shapes run
           twice: the module lowers the usage of every entry the pass leaves, so the competitors that remain sort below any
           entry run once from here on. */
        var next = 0;
        for (; next < 3 * passSize;)
        {
            await DarlingMcpTestData.ExecAsync(competitor, ct, CompetitorBatch(ref next));
        }

        var before = await PgStatementTable.DeallocAsync(connection, ct);
        for (var dealloc = before; dealloc == before; dealloc = await PgStatementTable.DeallocAsync(connection, ct))
        {
            Assert.True(next < 2 * max, $"{next} statement shapes brought no eviction pass on a table of {max} entries.");
            var batch = CompetitorBatch(ref next);
            await DarlingMcpTestData.ExecAsync(competitor, ct, batch);
            await DarlingMcpTestData.ExecAsync(competitor, ct, batch);
        }

        var competitorsLeft = await EntriesRunOnceAsync(competitor, ct);
        Assert.True(competitorsLeft > passSize,
            $"{competitorsLeft} competitors are left after the first pass, not more than the {passSize} one pass removes; the table cannot show whether the fill stops at one pass.");

        /* The body's own entry, run once after that pass, and the fill that has to take it. */
        await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT 843200 + 1 AS pm8432_own_entry");
        await PgStatementTable.ForcePassAsync(connection, ct);

        await using var read = new NpgsqlCommand(
            "SELECT count(*)::int FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND query LIKE '%pm8432\\_own\\_entry%'",
            connection);
        var own = (int)(await read.ExecuteScalarAsync(ct))!;
        Assert.True(own == 0,
            $"the forced pass left the body's own entry in place: {competitorsLeft} entries run once before the previous pass were in the table, more than the {passSize} one pass removes.");
    }

    /// <summary>How many entries run once the connection's database has in the table, this read aside.</summary>
    private static async Task<int> EntriesRunOnceAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            "SELECT count(*)::int AS pm8432_run_once FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND calls = 1 AND query NOT LIKE '%pm8432\\_run\\_once%'",
            connection);
        return (int)(await read.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// With <c>callsAtMost</c> 2, <see cref="PgStatementTable.ForcePassAsync"/> takes the entries run at most twice in the
    /// connection's database: the fill's shapes run three times, so they sort above an entry run twice, and the fill goes on
    /// until no entry run once or twice is left. A fill that takes the entries run once only stops while the body's entry
    /// run twice is in place, among the fill's own shapes.
    /// </summary>
    [Fact]
    public async Task TheForcedPass_TakesTheDatabasesEntriesRunTwice_WhenAskedForTheEntriesRunAtMostTwice()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the statement table test.");
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: CREATE EXTENSION would leave residue in the shared store; two scratch databases of its own go with it. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var other = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var competitor = new NpgsqlConnection(other.ConnectionString);
        await competitor.OpenAsync(ct);
        try
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT pg_stat_statements_reset()");
        }
        catch (PostgresException ex) when (ex.SqlState is "55000" or "58P01" or "0A000" or "42501")
        {
            Assert.Skip("pg_stat_statements is not available on this server: " + ex.Message);
        }

        /* Six passes' worth of entries run once in the other database: the passes a fill that takes the entries run once
           brings on take these, never the entries run twice. */
        var max = await PgStatementTable.MaxEntriesAsync(connection, ct);
        var next = 0;
        for (; next < 6 * (max * 5 / 100);)
        {
            await DarlingMcpTestData.ExecAsync(competitor, ct, CompetitorBatch(ref next));
        }

        await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT 843201 + 1 AS pm8432_twice");
        await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT 843201 + 1 AS pm8432_twice");
        await PgStatementTable.ForcePassAsync(connection, ct, callsAtMost: 2);

        Assert.True(await MarkedEntriesAsync(connection, "pm8432_twice", ct) == 0, "the forced pass left the body's entry run twice in place.");
    }

    /// <summary>
    /// With <c>database</c> named, <see cref="PgStatementTable.ForcePassAsync"/> reads back the entries of that database, so a
    /// body whose statements run in a database other than the one the counter is read from gets its own entries taken. A
    /// fill that reads back the counter's own database stops at its first pass when that database has nothing run once, and
    /// the body's entry, run once since the last pass, sorts above the entries run once before it and stays.
    /// </summary>
    [Fact]
    public async Task TheForcedPass_TakesTheNamedDatabasesEntries_WhenTheBodysStatementsRunInAnotherDatabaseThanTheCounter()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the statement table test.");
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: CREATE EXTENSION would leave residue in the shared store; three scratch databases of its own go with
           it: the counter's, the body's and the competitors'. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var bodyDb = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var other = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var again = new NpgsqlConnection(scratch.ConnectionString);
        await again.OpenAsync(ct);
        await using var body = new NpgsqlConnection(bodyDb.ConnectionString);
        await body.OpenAsync(ct);
        await using var competitor = new NpgsqlConnection(other.ConnectionString);
        await competitor.OpenAsync(ct);
        try
        {
            /* Twice each, so the counter's database has no entry run once of its own: what the fill reads back must then be
               the body's database's. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            await DarlingMcpTestData.ExecAsync(again, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT pg_stat_statements_reset()");
            await DarlingMcpTestData.ExecAsync(again, ct, "SELECT pg_stat_statements_reset()");
        }
        catch (PostgresException ex) when (ex.SqlState is "55000" or "58P01" or "0A000" or "42501")
        {
            Assert.Skip("pg_stat_statements is not available on this server: " + ex.Message);
        }

        /* The fill's own first-time statements on the counter's database run once here, and again inside the fill below. */
        await PgStatementTable.ForcePassAsync(connection, ct);

        /* Six passes' worth of entries run once in the competitors' database, then exactly one pass, so they sort below any
           entry run once from here on. */
        var max = await PgStatementTable.MaxEntriesAsync(connection, ct);
        var next = 0;
        for (; next < 6 * (max * 5 / 100);)
        {
            await DarlingMcpTestData.ExecAsync(competitor, ct, CompetitorBatch(ref next));
        }

        var before = await PgStatementTable.DeallocAsync(connection, ct);
        for (var dealloc = before; dealloc == before; dealloc = await PgStatementTable.DeallocAsync(connection, ct))
        {
            Assert.True(next < 2 * max, $"{next} statement shapes brought no eviction pass on a table of {max} entries.");
            var batch = CompetitorBatch(ref next);
            await DarlingMcpTestData.ExecAsync(competitor, ct, batch);
            await DarlingMcpTestData.ExecAsync(competitor, ct, batch);
        }

        await DarlingMcpTestData.ExecAsync(body, ct, "SELECT 843202 + 1 AS pm8432_elsewhere");
        await PgStatementTable.ForcePassAsync(connection, ct, database: body.Database);

        Assert.True(await MarkedEntriesAsync(connection, "pm8432_elsewhere", ct) == 0, "the forced pass left the body's entry in the other database in place.");
    }

    /// <summary>How many entries of any database carry <paramref name="marker"/> in their text.</summary>
    private static async Task<int> MarkedEntriesAsync(NpgsqlConnection connection, string marker, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand("SELECT count(*)::int FROM pg_stat_statements WHERE query LIKE $1", connection);
        read.Parameters.AddWithValue("%" + marker + "%");
        return (int)(await read.ExecuteScalarAsync(ct))!;
    }

    /// <summary>One fill batch's worth of competitor shapes, from <paramref name="next"/> on, as one command text.</summary>
    private static string CompetitorBatch(ref int next)
    {
        var batch = new System.Text.StringBuilder();
        for (var i = 0; i < PgStatementTable.FillBatch; i++)
        {
            batch.Append(CompetitorShape(next++)).Append(';');
        }

        return batch.ToString();
    }

    /// <summary>
    /// The <paramref name="n"/>-th shape of the competitors' family: two or more columns of <c>1</c>, each a product of some
    /// more, under one of eight WHERE/LIMIT/OFFSET suffixes. Products, so the family shares no entry with the fill's sums or
    /// the headroom family's differences.
    /// </summary>
    private static string CompetitorShape(int n)
    {
        var variant = n & 7;
        var rest = n >> 3;
        var items = rest / 8 + 2;
        var terms = rest % 8 + 1;
        var item = "1" + string.Concat(System.Linq.Enumerable.Repeat("*1", terms));
        var suffix = ((variant & 1) != 0 ? " WHERE true" : string.Empty)
            + ((variant & 2) != 0 ? " LIMIT 1" : string.Empty)
            + ((variant & 4) != 0 ? " OFFSET 0" : string.Empty);
        return "SELECT " + string.Join(",", System.Linq.Enumerable.Repeat(item, items)) + suffix;
    }
}
