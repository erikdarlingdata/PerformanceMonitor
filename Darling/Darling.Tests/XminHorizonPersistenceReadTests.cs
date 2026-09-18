/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3537: the xmin read's two window figures, against a real Postgres — the horizon-arm numerator (winning
/// age at/above the threshold, holder ignored) and its denominator (the collector's OWN successful runs,
/// off <c>collection_log</c>, so quiet captures count).
///
/// <para><b>Why live rather than a source pin.</b> <c>XminSql</c> has no execution coverage anywhere else:
/// a pin can assert the <c>captures</c> CTE exists, but only a store can prove the filters count — that an
/// ERROR run, another collector's run and another server's run all stay out of the denominator while a
/// zero-row healthy run stays in, and that a below-threshold winner stays out of the numerator while still
/// counting as a holder-bearing observation. Each seeded row here is one of those predicates.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class XminHorizonPersistenceReadTests
{
    private const int ServerId = -353701;
    private const int OtherServerId = -353702;
    private const string ServerName = "xmin-persistence-read";
    private const string Collector = "pg_xmin_horizon";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// One rotating-holder window, end to end: four held collections under three distinct winning pids
    /// (one below the age bar), two quiet captures, and three log rows that must not count. The read's
    /// figures are asserted exactly, then handed to the evaluator to prove the fire this issue exists for:
    /// the horizon arm, under the stable rotating-holders subject, where the identity fraction (1 of 4)
    /// never could.
    /// </summary>
    [Fact]
    public async Task TheXminRead_CountsCapturesAndAboveThreshold_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live xmin-persistence test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var adapter = new DarlingPostgresAlertReadAdapter(postgres);

            /* The empty store first: no holder rows means no row at all, the healthy null — and the one
               execution of the full statement that cannot hide behind seeded data if the SQL stops
               parsing. */
            Assert.Null(await adapter.GetXminHorizonAsync(ServerId, ct));

            /* Four collections, three distinct winning pids — the parade. The 30M winner sits below the
               50M evaluator threshold, so it is a holder-bearing observation that must NOT count as an
               above-threshold one. The t-10 loser row shares its winner's collection_time, pinning that
               extra rows per collection inflate nothing. */
            await SeedHolderAsync(connection, ct, MinutesAgo(10), "session", 60_000_000, "101", "state=idle in transaction", isWinner: true);
            await SeedHolderAsync(connection, ct, MinutesAgo(10), "replication_slot", 10_000_000, "slot_a", null, isWinner: false);
            await SeedHolderAsync(connection, ct, MinutesAgo(8), "session", 70_000_000, "102", null, isWinner: true);
            await SeedHolderAsync(connection, ct, MinutesAgo(6), "session", 30_000_000, "103", null, isWinner: true);
            await SeedHolderAsync(connection, ct, MinutesAgo(4), "session", 80_000_000, "104", "state=idle in transaction", isWinner: true);

            /* The capture denominator: the four holder-bearing runs, two QUIET (zero-row, healthy) runs
               the holder table cannot see — the whole reason the denominator lives in collection_log —
               and three rows that must stay out of it: a run that failed, another collector's run, and
               another server's. */
            await SeedLogAsync(connection, ct, MinutesAgo(10), ServerId, Collector, "SUCCESS", 2);
            await SeedLogAsync(connection, ct, MinutesAgo(8), ServerId, Collector, "SUCCESS", 1);
            await SeedLogAsync(connection, ct, MinutesAgo(6), ServerId, Collector, "SUCCESS", 1);
            await SeedLogAsync(connection, ct, MinutesAgo(4), ServerId, Collector, "SUCCESS", 1);
            await SeedLogAsync(connection, ct, MinutesAgo(2), ServerId, Collector, "SUCCESS", 0);
            await SeedLogAsync(connection, ct, MinutesAgo(1), ServerId, Collector, "SUCCESS", 0);
            await SeedLogAsync(connection, ct, MinutesAgo(3), ServerId, Collector, "ERROR", 0);
            await SeedLogAsync(connection, ct, MinutesAgo(5), ServerId, "pg_database_stats", "SUCCESS", 4);
            await SeedLogAsync(connection, ct, MinutesAgo(7), OtherServerId, Collector, "SUCCESS", 1);

            var info = await adapter.GetXminHorizonAsync(ServerId, ct);

            Assert.NotNull(info);
            Assert.Equal("session", info!.Source);
            Assert.Equal("104", info.Identifier);
            Assert.Equal(80_000_000L, info.XminAge);
            Assert.Equal(1, info.ObservationsHeld);
            Assert.Equal(4, info.ObservationsTotal);
            Assert.Equal(3, info.ObservationsAboveThreshold);
            Assert.Equal(6, info.CapturesInWindow);

            /* And the consequence the figures exist for: 3 of 6 captures is the horizon arm's majority
               where the identity fraction is 1 of 4 — the rotating-holder fire, under the subject the
               host's per-subject cooldown can actually hold across rotations. */
            var finding = PostgresAlertEvaluator.EvaluateXmin(info);
            Assert.NotNull(finding);
            Assert.Equal(PostgresAlertEvaluator.XminRotatingHoldersSubject, finding!.Subject);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /* ── helpers ── */

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static Task SeedHolderAsync(
        NpgsqlConnection connection, CancellationToken ct,
        DateTime collectionTimeUtc, string source, long xminAge, string holder, string? detail, bool isWinner) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_xmin_horizon
    (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), collectionTimeUtc, ServerId, ServerName,
            source, xminAge, holder, detail, isWinner);

    private static Task SeedLogAsync(
        NpgsqlConnection connection, CancellationToken ct,
        DateTime collectionTimeUtc, int serverId, string collectorName, string status, int rowsCollected) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), serverId, ServerName, collectorName,
            collectionTimeUtc, 50, status, null, rowsCollected, 40, 10);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM pg_xmin_horizon WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM collection_log WHERE server_id IN ($1, $2)", ServerId, OtherServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM servers WHERE server_id = $1", ServerId);
    }
}
