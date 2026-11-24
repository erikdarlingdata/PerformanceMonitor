/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #9171: the dropped-database read behind the top-queries reads (<c>CurrentDatabaseScope.ReadDroppedAsync</c>) read the heap row of every
/// <c>database_states</c> snapshot row in its window, because the only index carried no <c>database_name</c>. The tuning pass now builds a
/// covering index for it and drops the narrow one once the cover is valid. The answer must not change, and a database created and
/// dropped inside the window must still be found.
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseStatesCoverIndexLiveTests
{
    private const string ServerName = "cover-states";
    private const int ServerId = 91710;

    private static async Task<int> ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] args)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var a in args)
        {
            cmd.Parameters.AddWithValue(a);
        }

        return await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection c, string name, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", c);
        cmd.Parameters.AddWithValue(name);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }

    private static async Task<uint> IndexOidAsync(NpgsqlConnection c, string name, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT c.oid::int8 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relname = $1", c);
        cmd.Parameters.AddWithValue(name);
        return (uint)(long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>A store as an older build leaves it: the narrow index only, with snapshots in which Gone left, Flash was created and dropped, and Live stayed.</summary>
    private static async Task<(NpgsqlConnection Body, DateTime Now)> SeedOlderStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        await ExecAsync(body, "DROP INDEX IF EXISTS collect." + PgTableTuning.DatabaseStatesNameCoverIndexName, ct);
        await ExecAsync(body, "CREATE INDEX IF NOT EXISTS " + PgTableTuning.DatabaseStatesNarrowIndexName + " ON collect.database_states (server_id, collection_time)", ct);
        var now = DateTime.SpecifyKind(LiveClock.AnchoredEnd(), DateTimeKind.Unspecified);
        var snapshots = new (TimeSpan Age, string[] Names)[]
        {
            (TimeSpan.FromHours(20), new[] { "Live", "Gone" }),
            (TimeSpan.FromHours(10), new[] { "Live", "Gone", "Flash" }),
            (TimeSpan.FromHours(5), new[] { "Live", "Gone" }),
            (TimeSpan.FromMinutes(5), new[] { "Live" }),
        };
        foreach (var (age, names) in snapshots)
        {
            var id = 1;
            foreach (var name in names)
            {
                await ExecAsync(body,
                    "INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,$6,'ONLINE',FALSE)",
                    ct, CollectionIdGenerator.Next(), now - age, ServerId, ServerName, name, id++);
            }
        }

        return (body, now);
    }

    private static async Task<List<string>> DroppedAsync(ScratchPostgres scratch, DateTime nowUtc, CancellationToken ct)
    {
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var dropped = await CurrentDatabaseScope.ReadDroppedAsync(
            dataSource, ServerId, nowUtc: DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc), windowStartUtc: DateTime.SpecifyKind(nowUtc.AddHours(-24), DateTimeKind.Utc),
            logger: null, cancellationToken: ct);
        Assert.NotNull(dropped);
        return dropped!.OrderBy(n => n, StringComparer.Ordinal).ToList();
    }

    [Fact]
    public async Task TheTuningPass_BuildsTheCover_DropsTheNarrowIndex_KeepsTheAnswer_AndASecondPassChangesNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live database_states cover index test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var (body, now) = await SeedOlderStoreAsync(scratch, ct);
        await using var bodyConnection = body;

        var before = await DroppedAsync(scratch, now, ct);
        /* Flash was created and dropped between two snapshots inside the window, so the answer must name it as well as Gone. */
        Assert.Equal(new[] { "Flash", "Gone" }, before);

        await TuningStartPasses.ConvergeAsync(body, ct);

        Assert.True(await IndexExistsAsync(body, PgTableTuning.DatabaseStatesNameCoverIndexName, ct), "the cover is built");
        Assert.False(await IndexExistsAsync(body, PgTableTuning.DatabaseStatesNarrowIndexName, ct), "the narrow index it replaces is dropped");
        Assert.Equal(before, await DroppedAsync(scratch, now, ct));

        var oid = await IndexOidAsync(body, PgTableTuning.DatabaseStatesNameCoverIndexName, ct);
        await TuningStartPasses.ConvergeAsync(body, ct);
        await PgTableTuning.ApplyAsync(body, NullLogger.Instance, hourly: true, ct);
        Assert.Equal(oid, await IndexOidAsync(body, PgTableTuning.DatabaseStatesNameCoverIndexName, ct));
        Assert.False(await IndexExistsAsync(body, PgTableTuning.DatabaseStatesNarrowIndexName, ct));
    }

    [Fact]
    public async Task TheHourlyPass_NeverBuildsTheCover_SoItKeepsTheNarrowIndexThatTheReadsStillUse()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live database_states cover index test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var (body, now) = await SeedOlderStoreAsync(scratch, ct);
        await using var bodyConnection = body;

        await PgTableTuning.ApplyAsync(body, NullLogger.Instance, hourly: true, ct);

        Assert.False(await IndexExistsAsync(body, PgTableTuning.DatabaseStatesNameCoverIndexName, ct), "the hourly pass builds no index");
        Assert.True(await IndexExistsAsync(body, PgTableTuning.DatabaseStatesNarrowIndexName, ct), "with no cover, the narrow index stays");
        Assert.Equal(new[] { "Flash", "Gone" }, await DroppedAsync(scratch, now, ct));
    }

    [Fact]
    public async Task WithTheCover_TheDroppedDatabaseRead_RunsAsAnIndexOnlyScanOverIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live database_states cover index test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var (body, now) = await SeedOlderStoreAsync(scratch, ct);
        await using var bodyConnection = body;
        await TuningStartPasses.ConvergeAsync(body, ct);
        await ExecAsync(body, "VACUUM (ANALYZE) collect.database_states", ct);

        /* The planner chooses by cost on a few rows, so the other paths are switched off to ask only whether the cover can serve the read. */
        await ExecAsync(body, "SET enable_seqscan = off", ct);
        await ExecAsync(body, "SET enable_bitmapscan = off", ct);
        await using var explain = new NpgsqlCommand("EXPLAIN " + QueryStoreBackfillState.LastSeenPerDatabaseSql, body);
        explain.Parameters.AddWithValue(ServerId);
        explain.Parameters.AddWithValue(now.AddHours(-24));
        var plan = new List<string>();
        await using (var reader = await explain.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                plan.Add(reader.GetString(0));
            }
        }

        Assert.Contains(plan, l => l.Contains("Index Only Scan", StringComparison.Ordinal)
            && l.Contains(PgTableTuning.DatabaseStatesNameCoverIndexName, StringComparison.Ordinal));
    }
}
