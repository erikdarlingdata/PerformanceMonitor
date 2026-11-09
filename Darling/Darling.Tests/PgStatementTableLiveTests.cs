/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8400: the run <see cref="PgStatementTable.RunAsync"/> returns is one the eviction pass missed. The pass a forcing run
/// brings on between a body's statements and its first read takes the entries run once first, the body's own among them,
/// so the body's read of its own entry fails inside the run. That failure is the pass's doing: the run is voided, the body
/// runs again, and the run returned read the entry its statement made.
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

        var runs = new List<int>();
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
            Assert.Single(texts);
        }, forcePass: true);

        PgStatementTable.AssertNoPassInside(run);
        Assert.Equal(new[] { 1, 2 }, runs);
        Assert.Equal(2, run.Number);
    }
}
