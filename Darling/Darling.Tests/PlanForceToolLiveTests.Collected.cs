/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/* The store check of a force or unforce: the database, query_id and plan_id the caller names must sit together in a Query Store row Darling collected for
   that server. Query Store ids are per-database sequences that start at 1, so the same pair exists in nearly every database on a server, and a wrong database
   would force an unrelated plan on an unrelated query. The check runs on the store alone, before the server is read, so these tests need only the store. */
public sealed partial class PlanForceToolLiveTests
{
    private const string PlanNotCollected = "plan_not_collected";

    /// <summary>One collected Query Store row for the server under test: the three values a caller copies from one get_query_store_top row.</summary>
    private static async Task SeedCollectedPlanAsync(Ctx c, string database, long queryId, long planId)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, 7, 'srv-7', $3, $4, $5, 'Regular', $2, 'mod', md5($3 || $4::text || $5::text), 10, 500, 300, 900, 700, 'PRIMARY', 1, $2)", c.Connection) { CommandTimeout = 60 };
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = now.Ticks, NpgsqlDbType = NpgsqlDbType.Bigint });
        insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlDbType.Timestamp });
        insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = database, NpgsqlDbType = NpgsqlDbType.Text });
        insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queryId, NpgsqlDbType = NpgsqlDbType.Bigint });
        insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = planId, NpgsqlDbType = NpgsqlDbType.Bigint });
        await insert.ExecuteNonQueryAsync(c.Ct);
    }

    /// <summary>The tool called with the test database's name and the ids as given, with nothing seeded on the caller's behalf.</summary>
    private static Task<string> CallForceAsync(Ctx c, bool force, long queryId, long planId, bool dryRun) =>
        DarlingMcpPlanForceTools.RunAsync(c.Source, c.Source, c.Log, force, "srv-7", c.Sql.Database, queryId, planId, c.Arg, dryRun, c.Ct);

    private static Task<string?> RefusedRowsAsync(Ctx c, string reason) =>
        PgScalarAsync(c.Connection, $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = '{reason}'", c.Ct);

    /// <summary>
    /// The store holds the pair for another database only. The call is refused before the server is read, on a dry run and on a write, for a force and an
    /// unforce alike; the message names the database the store does have, so the caller can see which row the ids came from; the refusal is journaled and
    /// no action row is.
    /// </summary>
    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task AForceOrUnforceOfAPairTheStoreHoldsForAnotherDatabase_IsRefusedAsPlanNotCollected_AndNamesThatDatabase(bool force, bool dryRun)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SeedCollectedPlanAsync(c, "other_db", 5, 7);

            var json = Parse(await CallForceAsync(c, force, 5, 7, dryRun));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal(PlanNotCollected, (string?)json["reason"]);
            var message = (string?)json["message"];
            Assert.Contains("other_db", message, StringComparison.Ordinal);
            Assert.Contains(c.Sql.Database, message, StringComparison.Ordinal);
            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await RefusedRowsAsync(c, PlanNotCollected));
        }, storeOnly: true);
    }

    /// <summary>The store holds the pair for no database at all: refused the same way, and the message says the rows may have aged out of the store.</summary>
    [Fact]
    public async Task AForceOfAPairTheStoreHoldsForNoDatabase_IsRefusedAsPlanNotCollected_AndSaysTheRowsMayHaveAgedOut()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var json = Parse(await CallForceAsync(c, force: true, 5, 7, dryRun: true));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal(PlanNotCollected, (string?)json["reason"]);
            var message = (string?)json["message"];
            Assert.Contains(c.Sql.Database, message, StringComparison.Ordinal);
            Assert.Contains("aged out", message, StringComparison.Ordinal);
            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await RefusedRowsAsync(c, PlanNotCollected));
        }, storeOnly: true);
    }

    /// <summary>
    /// The store holds the pair for the named database, spelled in another case: the check passes, the call goes on to the server (here a closed port, so it
    /// ends there), and nothing is journaled as not collected. Database names compare without regard to case, as the exclusion list does.
    /// </summary>
    [Fact]
    public async Task AForceOfAPairTheStoreHoldsForThatDatabase_PassesTheStoreCheck_WhateverTheCase()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SeedCollectedPlanAsync(c, c.Sql.Database.ToUpperInvariant(), 5, 7);

            var json = Parse(await CallForceAsync(c, force: true, 5, 7, dryRun: true));
            Assert.NotEqual(PlanNotCollected, (string?)json["reason"]);
            Assert.Equal("0", await RefusedRowsAsync(c, PlanNotCollected));
        }, storeOnly: true);
    }

    /// <summary>A pair collected under the ten-day interval table alone, with no raw row left, is still a collected pair.</summary>
    [Fact]
    public async Task AForceOfAPairOnlyTheIntervalTableHolds_PassesTheStoreCheck()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_wide (server_id, collection_time, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc)
VALUES (7, $2, $1, 1, 7, 5, 'PRIMARY', $2, 'Regular')", c.Connection) { CommandTimeout = 60 })
            {
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = c.Sql.Database, NpgsqlDbType = NpgsqlDbType.Text });
                insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
                await insert.ExecuteNonQueryAsync(c.Ct);
            }

            var json = Parse(await CallForceAsync(c, force: true, 5, 7, dryRun: true));
            Assert.NotEqual(PlanNotCollected, (string?)json["reason"]);
            Assert.Equal("0", await RefusedRowsAsync(c, PlanNotCollected));
        }, storeOnly: true);
    }
}
