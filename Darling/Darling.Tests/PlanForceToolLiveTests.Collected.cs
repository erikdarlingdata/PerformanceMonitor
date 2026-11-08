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
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/* The store check of a force or unforce: the database, query_id and plan_id the caller names must sit together in a Query Store row Darling collected for
   that server. Query Store ids are per-database sequences that start at 1, so the same pair exists in nearly every database on a server, and a wrong database
   would force an unrelated plan on an unrelated query. The check runs on the store alone, before the server is read, so these tests need only the store. */
public sealed partial class PlanForceToolLiveTests
{
    private const string PlanNotCollected = "plan_not_collected";
    private const string PlanQueryChanged = "plan_query_changed";

    /// <summary>A <c>query_hash</c> in the store's <c>0x</c> text form that no query on the test server has.</summary>
    private const string OtherQueryHash = "0x0000000000000001";

    /// <summary>
    /// One collected Query Store row for the server under test: the three values a caller copies from one get_query_store_top row, with the query's
    /// <c>query_hash</c> as the server holds it when the row is for the test's own SQL Server database (a placeholder for a database that exists only in the store).
    /// <paramref name="queryHash"/> overrides it, for a row collected from a query that is no longer the one behind the ids.
    /// </summary>
    private static async Task SeedCollectedPlanAsync(Ctx c, string database, long queryId, long planId, string? queryHash = null)
    {
        if (queryHash is null && !c.StoreOnly && string.Equals(database, c.Sql.Database, StringComparison.Ordinal))
            queryHash = await c.Sql.QueryHashAsync(queryId, c.Ct);
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, 7, 'srv-7', $3, $4, $5, 'Regular', $2, 'mod', COALESCE($6, md5($3 || $4::text || $5::text)), 10, 500, 300, 900, 700, 'PRIMARY', 1, $2)", c.Connection) { CommandTimeout = 60 };
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = now.Ticks, NpgsqlDbType = NpgsqlDbType.Bigint });
        insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlDbType.Timestamp });
        insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = database, NpgsqlDbType = NpgsqlDbType.Text });
        insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queryId, NpgsqlDbType = NpgsqlDbType.Bigint });
        insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = planId, NpgsqlDbType = NpgsqlDbType.Bigint });
        insert.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = queryHash, NpgsqlDbType = NpgsqlDbType.Text });
        await insert.ExecuteNonQueryAsync(c.Ct);
    }

    /// <summary>The tool called with the ids as given and <paramref name="database"/> (the test database's name by default), with nothing seeded on the caller's behalf.</summary>
    private static Task<string> CallForceAsync(Ctx c, bool force, long queryId, long planId, bool dryRun, string? database = null) =>
        DarlingMcpPlanForceTools.RunAsync(c.Source, c.Source, c.Log, force, "srv-7", database ?? c.Sql.Database, queryId, planId, c.Arg, dryRun, c.Ct);

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
    /// The store holds the pair for the named database, spelled in another case. The check passes, and the server is read and written in the database as the
    /// store spells it, not as the caller spelled it: on a server whose collation is case-sensitive those are two databases, and the collected row came from the
    /// store's. The before image, the statement and the journaled target all carry the store's spelling, so the undo goes back to the same database.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AForceOfAPairTheStoreHoldsForThatDatabase_SpelledInAnotherCase_RunsInTheDatabaseAsTheStoreSpellsIt(bool dryRun)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pfc1", c.Ct);
            await SeedCollectedPlanAsync(c, c.Sql.Database, query, planA);

            var json = Parse(await CallForceAsync(c, force: true, query, planA, dryRun, c.Sql.Database.ToUpperInvariant()));
            Assert.Equal(dryRun ? "dry_run" : "done", (string?)json["status"]);
            Assert.Equal(c.Sql.Database, (string?)json["before"]!["database"]);
            Assert.Equal(PlanForceStatement.For(c.Sql.Database, query, planA, force: true), (string?)json["statement"]);
            Assert.Equal("0", await RefusedRowsAsync(c, PlanNotCollected));
            if (dryRun)
                return;
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(c.Sql.Database, (string?)called["target"]!["database"]);
        });
    }

    /// <summary>
    /// The store holds the pair for two spellings of the database that differ by case alone: two databases on a server whose collation is case-sensitive. A third
    /// spelling names neither and is refused, with both named, so the caller can copy one; the exact spelling passes the check and the call goes on to the server
    /// (a closed port here, so it ends there).
    /// </summary>
    [Fact]
    public async Task AForceOfAPairTheStoreHoldsForTwoSpellingsOfTheDatabase_IsRefusedUnlessTheSpellingIsExact()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var lower = c.Sql.Database;
            var upper = lower.ToUpperInvariant();
            var mixed = char.ToUpperInvariant(lower[0]) + lower[1..];
            await SeedCollectedPlanAsync(c, lower, 5, 7);
            await SeedCollectedPlanAsync(c, upper, 5, 7);

            var json = Parse(await CallForceAsync(c, force: true, 5, 7, dryRun: true, mixed));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal(PlanNotCollected, (string?)json["reason"]);
            var message = (string?)json["message"];
            Assert.Contains(lower, message, StringComparison.Ordinal);
            Assert.Contains(upper, message, StringComparison.Ordinal);
            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await RefusedRowsAsync(c, PlanNotCollected));

            var exact = Parse(await CallForceAsync(c, force: true, 5, 7, dryRun: true, upper));
            Assert.NotEqual(PlanNotCollected, (string?)exact["reason"]);
            Assert.Equal("1", await RefusedRowsAsync(c, PlanNotCollected));
        }, storeOnly: true);
    }

    /// <summary>
    /// The refusal names the databases the store holds the pair for, less those the server's entry excludes: a database excluded after its rows were collected is
    /// one Darling does not act in, so it is not offered as the one the ids came from.
    /// </summary>
    [Fact]
    public async Task TheRefusal_NamesNoDatabaseTheServerEntryExcludes()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await PgExecAsync(c.Connection, "UPDATE config.config_monitored_servers SET excluded_databases = ARRAY['gone_db'] WHERE server_id = 7", c.Ct);
            await SeedCollectedPlanAsync(c, "other_db", 5, 7);
            await SeedCollectedPlanAsync(c, "gone_db", 5, 7);

            var json = Parse(await CallForceAsync(c, force: true, 5, 7, dryRun: true));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal(PlanNotCollected, (string?)json["reason"]);
            var message = (string?)json["message"];
            Assert.Contains("other_db", message, StringComparison.Ordinal);
            Assert.DoesNotContain("gone_db", message, StringComparison.Ordinal);
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

    /// <summary>
    /// The store holds the triple, but its row's <c>query_hash</c> is not the hash of the query the server has behind that query_id now: a database restored or
    /// re-created inside the store's window restarts its Query Store ids, so the same pair names another query's plan. The call is refused before anything is
    /// written, on a dry run and on a write, for a force and an unforce alike; the message names both hashes; the refusal is journaled and no action row is.
    /// </summary>
    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task AForceOrUnforceOfAPairWhoseCollectedQueryHashIsNotTheServers_IsRefusedAsPlanQueryChanged_AndWritesNothing(bool force, bool dryRun)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pfh1", c.Ct);
            if (!force)
                await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);
            var serverHash = await c.Sql.QueryHashAsync(query, c.Ct);
            Assert.NotNull(serverHash);
            await SeedCollectedPlanAsync(c, c.Sql.Database, query, planA, OtherQueryHash);

            var json = Parse(await CallForceAsync(c, force, query, planA, dryRun));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal(PlanQueryChanged, (string?)json["reason"]);
            var message = (string?)json["message"];
            Assert.Contains(serverHash, message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(OtherQueryHash, message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(force ? null : planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await RefusedRowsAsync(c, PlanQueryChanged));
        });
    }

    /// <summary>The collected row's <c>query_hash</c> is the server's: the check passes, and the before image carries the hash the server has, so the journal shows which query the ids named.</summary>
    [Fact]
    public async Task AForceOfAPairWhoseCollectedQueryHashIsTheServers_Runs_AndTheBeforeImageCarriesTheHash()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pfh2", c.Ct);
            var serverHash = await c.Sql.QueryHashAsync(query, c.Ct);
            Assert.NotNull(serverHash);
            await SeedCollectedPlanAsync(c, c.Sql.Database, query, planA, serverHash);

            var json = Parse(await CallForceAsync(c, force: true, query, planA, dryRun: true));
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.Equal(serverHash, (string?)json["before"]!["query_hash"], StringComparer.OrdinalIgnoreCase);
            Assert.Equal("0", await RefusedRowsAsync(c, PlanQueryChanged));
        });
    }

    /// <summary>
    /// The store holds the pair in rows from before and after the query behind the ids changed: the server's hash is among the collected ones, so the pair is
    /// one the store collected for this query and the check passes. A collected row with no hash at all says nothing about the query and does not refuse either.
    /// </summary>
    [Fact]
    public async Task AForceOfAPairWhoseCollectedRowsIncludeTheServersHash_OrCarryNone_PassesTheQueryCheck()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pfh3", c.Ct);
            await SeedCollectedPlanAsync(c, c.Sql.Database, query, planA, OtherQueryHash);
            await SeedCollectedPlanAsync(c, c.Sql.Database, query, planA);
            await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_wide (server_id, collection_time, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc)
VALUES (7, $2, $1, 1, $4, $3, 'PRIMARY', $2, 'Regular')", c.Connection) { CommandTimeout = 60 })
            {
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = c.Sql.Database, NpgsqlDbType = NpgsqlDbType.Text });
                insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
                insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = query, NpgsqlDbType = NpgsqlDbType.Bigint });
                insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = planA, NpgsqlDbType = NpgsqlDbType.Bigint });
                await insert.ExecuteNonQueryAsync(c.Ct);
            }

            var json = Parse(await CallForceAsync(c, force: true, query, planA, dryRun: true));
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.Equal("0", await RefusedRowsAsync(c, PlanQueryChanged));
        });
    }
}
