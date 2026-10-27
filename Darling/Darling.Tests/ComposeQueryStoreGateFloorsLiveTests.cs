/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6807: the composed Query Store gate reads each server's interval-table floor (<c>MIN(first_execution_time)</c>) in ONE statement for the
/// whole fleet, not one statement per server, and each server's floor is what the one-server statement answers: a server with rows, a
/// server with none (NULL), and a repeated id.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to CREATE and DROP its own database. */
public sealed class ComposeQueryStoreGateFloorsLiveTests
{
    [Fact]
    public async Task TheGateReadsTheTableFloorsInOneStatementForTheFleet_AndEachFloorEqualsTheOneServerAnswer()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct, servers: 6, build: false);
        var bodySucceeded = false;
        try
        {
            /* Servers 4 to 6 are enrolled with a coverage claim but hold no wide rows; give server 6 a row far back so the floors differ. */
            await ComposeStampLiveSupport.InsertRowAsync(connection, 6, hourNow.AddDays(-12), 8_000_006, ct);

            var end = DateTime.UtcNow;
            var start = end.AddHours(-24);
            await using var count = new NpgsqlCommandScope(scratch.DatabaseName);
            var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct);
            Assert.True(resolution.Eligible, "the seeded store must route the panel to the wide table, or this test proves nothing");
            Assert.Equal(1, count.FloorReads);

            /* The fleet statement's floors, each equal to the one-server answer. An id with no coverage row never reaches the floor step,
               so the statement leaves it out, and the gate would read its floor alone if it ever got there. */
            var ask = new[] { 1, 2, 3, 4, 5, 6, -6807999, 1 };
            var floors = new System.Collections.Generic.Dictionary<int, DateTime?>();
            await using (var fleet = new NpgsqlCommand(QueryStoreIntervalWide.PlainTableFloorsSql, connection))
            {
                fleet.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, Value = ask });
                fleet.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
                fleet.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DBNull.Value });
                await using var reader = await fleet.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    floors.Add(reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1));
                }
            }

            foreach (var id in ask.Distinct().Where(id => id > 0))
            {
                await using var one = new NpgsqlCommand(QueryStoreIntervalWide.PlainTableFloorSql, connection);
                one.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = id });
                var expected = await one.ExecuteScalarAsync(ct) as DateTime?;
                Assert.True(floors.TryGetValue(id, out var floor), $"server {id} has no answer");
                Assert.Equal(expected, floor);
            }

            Assert.False(floors.ContainsKey(-6807999));
            Assert.Equal(6, floors.Count);
            Assert.NotNull(floors[6]);
            Assert.NotNull(floors[1]);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    /// <summary>
    /// #6807 review: a server that refuses ahead of the floor step (here, every server holds a pending batch) costs no floor read at
    /// all, in either form, as before #6807. And when the fleet statement fails, the gate still decides every server, each through its
    /// own one-server floor read, and gives the answer it gives with the statement.
    /// </summary>
    [Fact]
    public async Task AServerThatRefusesAheadOfTheFloorStep_CostsNoFloorRead_AndAFailedFleetStatementFallsBackToEachServersOwnRead()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct, servers: 6, build: false);
        var bodySucceeded = false;
        try
        {
            var end = DateTime.UtcNow;
            var start = end.AddHours(-24);
            var withStatement = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct);

            QueryStoreIntervalWide.StoreWideInputsCache.TableFloorsSqlForTests.Value =
                "SELECT (1 / 0)::integer AS server_id, NULL::timestamp AS floor WHERE $1::integer[] IS NOT NULL AND $2::timestamp IS NOT NULL AND ($3::timestamp IS NULL OR TRUE)";
            try
            {
                await using var count = new NpgsqlCommandScope(scratch.DatabaseName);
                var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct);
                Assert.True(resolution.Eligible, "a failed fleet floor statement must leave each server to its own floor read, not refuse the panel");
                Assert.Equal(withStatement.WideStart, resolution.WideStart);
                Assert.Equal(withStatement.RawServerIds ?? [], resolution.RawServerIds ?? []);
                Assert.Equal(6, count.FloorReads);
            }
            finally
            {
                QueryStoreIntervalWide.StoreWideInputsCache.TableFloorsSqlForTests.Value = null;
            }

            for (var s = 1; s <= 6; s++)
            {
                await using var pending = new NpgsqlCommand(
                    "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at) VALUES ($1, $2, 'db0', $2)", connection);
                pending.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = s });
                pending.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(hourNow.AddHours(-1), DateTimeKind.Unspecified) });
                await pending.ExecuteNonQueryAsync(ct);
            }

            await using (var count = new NpgsqlCommandScope(scratch.DatabaseName))
            {
                var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct);
                Assert.False(resolution.Eligible, "every server holds a pending batch, so none may read the interval table");
                Assert.Equal(0, count.FloorReads);
            }

            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    /// <summary>
    /// Counts the statements that read a table floor, in either form, scoped to one database, as the other live tests count statements.
    /// One fragment matches both forms, so the count is 1 for the fleet statement and one per server without it. A count that reads 0
    /// where 1 is expected fails loudly, so no separate control is needed.
    /// </summary>
    private sealed class NpgsqlCommandScope : IAsyncDisposable
    {
        private readonly NpgsqlCommandCounter _floors;

        public NpgsqlCommandScope(string databaseName)
        {
            _floors = new NpgsqlCommandCounter(databaseName, "MIN(t.first_execution_time)");
        }

        /// <summary>Statements that read the table floor: one fleet statement, or one per server.</summary>
        public long FloorReads => _floors.Count;

        public ValueTask DisposeAsync()
        {
            _floors.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
