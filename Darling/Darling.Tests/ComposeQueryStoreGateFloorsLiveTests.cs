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

            var ask = new[] { 1, 2, 3, 4, 5, 6, -6807999, 1 };
            var cache = new QueryStoreIntervalWide.StoreWideInputsCache();
            await cache.PrefetchTableFloorsAsync(connection, ask, 60, ct);
            var floors = cache.TableFloors;
            Assert.NotNull(floors);
            foreach (var id in ask.Distinct())
            {
                await using var one = new NpgsqlCommand(QueryStoreIntervalWide.PlainTableFloorSql, connection);
                one.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = id });
                var expected = await one.ExecuteScalarAsync(ct) as DateTime?;
                Assert.True(floors!.TryGetValue(id, out var floor), $"server {id} has no answer");
                Assert.Equal(expected, floor);
            }

            Assert.Null(floors![-6807999]);
            Assert.NotNull(floors[6]);
            Assert.NotNull(floors[1]);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    /// <summary>Counts the floor statement (either form) and a control, scoped to one database, as the other live tests count statements.</summary>
    private sealed class NpgsqlCommandScope : IAsyncDisposable
    {
        private readonly NpgsqlCommandCounter _floors;
        private readonly NpgsqlCommandCounter _perServer;

        public NpgsqlCommandScope(string databaseName)
        {
            _floors = new NpgsqlCommandCounter(databaseName, "first_execution_time");
            _perServer = new NpgsqlCommandCounter(databaseName, "MIN(t.first_execution_time)");
        }

        /// <summary>Statements that read the table floor: one fleet statement, or one per server.</summary>
        public long FloorReads => _perServer.Count;

        public ValueTask DisposeAsync()
        {
            _floors.Dispose();
            _perServer.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
