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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live, end-to-end proof of the #4348 S1b one-time scrub against a REAL compressed TimescaleDB chunk.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (<see cref="ScratchPostgres"/>) rather than
/// sharing the live fixture, so it is deliberately NOT in the <c>live-postgres</c> collection: it creates its
/// own hypertable/compression shape on <c>collect.pg_server_config</c>, which the shared fixture must never
/// inherit from a test.</para>
/// </summary>
public sealed class PgSettingScrubLiveTests
{
    /// <summary>
    /// Pins round 2's H1 fix: batches are grouped by (server, day), not day alone, and every UPDATE carries a
    /// constant <c>server_id</c> predicate alongside the day range — not just the join equality — so
    /// TimescaleDB can exclude every other server's compressed segment in that day's chunk. Two servers each
    /// get a secret-bearing row on the SAME day, in the SAME compressed chunk. The connection's
    /// <c>Options=-c timescaledb.max_tuples_decompressed_per_dml_transaction=N</c> is set to one target's row
    /// count plus one: a day-only batch (the pre-H1 shape) would decompress BOTH servers' rows in the shared
    /// chunk and trip the limit with 53400; the (server, day) batch decompresses only the one server's
    /// segment and stays under it.
    /// </summary>
    [Fact]
    public async Task TheScrubBatchesPerServerAndDay_NotDayAlone()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4351 H1 batching pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        /* Seed and shape the chunk using an UNCAPPED connection: capping max_tuples_decompressed at
           connection scope would also block the seeding compress_chunk() call itself. */
        await using (var setupConnection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setupConnection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setupConnection, ct);

            Assert.True(await TimescaleSupport.TryEnableAsync(setupConnection, null, ct),
                "the dev fixture is expected to have TimescaleDB installed");
            await ExecAsync(setupConnection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
            await ExecAsync(setupConnection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

            const int serverA = -444445;
            const int serverB = -444446;

            /* One secret-bearing row per server, same day, same chunk. */
            await InsertRowAsync(setupConnection, serverA, OldTime, "primary_conninfo", Secret, databaseName: null, roleName: null, ct);
            await InsertRowAsync(setupConnection, serverB, OldTime, "primary_conninfo", Secret, databaseName: null, roleName: null, ct);

            await ExecAsync(setupConnection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);

            Assert.True(await ContainsSecretAsync(setupConnection, ct), "seeding failed to plant the secret this test exists to catch");
        }

        /* One target's own day has 1 candidate row. Cap at 1 (target-row-count) so a day-only batch, which
           would decompress both servers' rows sharing this chunk, trips 53400 with room to spare; a
           (server, day) batch touches only 1 row and stays at the cap. */
        var cappedConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Options = "-c timescaledb.max_tuples_decompressed_per_dml_transaction=1",
        }.ConnectionString;

        await using var postgres = NpgsqlDataSource.Create(cappedConnectionString);

        var summary = await PgSettingScrub.RunAsync(postgres, logger: null, ct);

        Assert.False(summary.AlreadyDone);
        Assert.Equal(2, summary.RowsUpdated);

        await using var verifyConnection = new NpgsqlConnection(scratch.ConnectionString);
        await verifyConnection.OpenAsync(ct);
        Assert.False(await ContainsSecretAsync(verifyConnection, ct), "the raw password survived the scrub under a tight decompress cap");
    }

    /// <summary>
    /// M3 (a): a single target's single day exceeds the decompress limit on its own — no cross-server
    /// sharing needed. 200 secret-bearing rows for ONE server on ONE day, capped at
    /// <c>max_tuples_decompressed_per_dml_transaction=50</c> (well under 200, and also under
    /// <see cref="PgSettingScrub.MaxKeysPerUpdate"/> so the cap alone is what would fail without the SET
    /// LOCAL override). Before the fix this trips 53400 on the very first batch of its own day; after the
    /// fix, SET LOCAL = 0 inside each batch's transaction lets it complete regardless of the cap.
    /// </summary>
    [Fact]
    public async Task TheScrubCompletes_WhenOneTargetsOneDayExceedsTheDecompressLimit()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4351 M3(a) decompress-limit pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        const int serverId = -444447;
        const int rowCount = 200;

        await using (var setupConnection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setupConnection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setupConnection, ct);

            Assert.True(await TimescaleSupport.TryEnableAsync(setupConnection, null, ct),
                "the dev fixture is expected to have TimescaleDB installed");
            await ExecAsync(setupConnection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
            await ExecAsync(setupConnection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

            for (var i = 0; i < rowCount; i++)
            {
                await InsertRowAsync(setupConnection, serverId, OldTime, $"custom.setting_{i}", Secret, databaseName: null, roleName: null, ct);
            }

            await ExecAsync(setupConnection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);

            Assert.True(await ContainsSecretAsync(setupConnection, ct), "seeding failed to plant the secret this test exists to catch");
        }

        var cappedConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Options = "-c timescaledb.max_tuples_decompressed_per_dml_transaction=50",
        }.ConnectionString;

        await using var postgres = NpgsqlDataSource.Create(cappedConnectionString);

        var summary = await PgSettingScrub.RunAsync(postgres, logger: null, ct);

        Assert.False(summary.AlreadyDone);
        Assert.Equal(rowCount, summary.RowsUpdated);

        await using var verifyConnection = new NpgsqlConnection(scratch.ConnectionString);
        await verifyConnection.OpenAsync(ct);
        Assert.False(await ContainsSecretAsync(verifyConnection, ct), "the raw password survived the scrub under a tight decompress cap on a single target's single day");

        var markerValue = await ScalarTextAsync(verifyConnection,
            "SELECT state_value FROM collect.collector_state WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'",
            0, DateTime.MinValue, ct);
        Assert.Equal(PgSettingRedactor.RulesVersion.ToString(CultureInfo.InvariantCulture), markerValue);
    }

    /// <summary>
    /// M3 (b): a forced failure on one target leaves the OTHER targets scrubbed and the marker unset; after
    /// the cause is removed, the next run completes and sets it. The failure is injected through a real
    /// database error the scrub's own UPDATE would hit — a CHECK constraint that rejects only
    /// <c>badServer</c>'s redacted value, so the scrub's catch-per-server path sees an honest 23514 from
    /// Postgres, not a mocked exception. Dropping the constraint after the first run is "the cause removed";
    /// the second run then completes badServer too and sets the marker.
    /// </summary>
    [Fact]
    public async Task TheScrub_IsolatesAFailingServer_AndRetriesOnTheNextRun()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4351 M3(b) per-server isolation pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        /* badServer < goodServer is deliberate: SortedDictionary orders byServerDay by server_id first, so
           badServer's batch runs FIRST on the connection and rolls back (see "What holds": await using
           disposes an uncompleted transaction). goodServer's batch then runs right after that rollback on
           the SAME connection, pinning that a failed-and-rolled-back batch never leaves the connection in a
           state (e.g. a failed 25P02 transaction) that would poison the next server's batch. */
        const int goodServer = -444448;
        const int badServer = -444449;

        await using var setupConnection = new NpgsqlConnection(scratch.ConnectionString);
        await setupConnection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(setupConnection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(setupConnection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await ExecAsync(setupConnection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);

        /* Rejects only badServer's post-redaction row — an honest constraint violation (23514) the scrub's
           batch UPDATE actually hits, standing in for any real per-row failure without touching the
           product's own logic. */
        await ExecAsync(setupConnection,
            $"ALTER TABLE collect.pg_server_config ADD CONSTRAINT chk_4351_forced_failure CHECK (server_id <> {badServer} OR setting LIKE '%hunter2%')", ct);

        await ExecAsync(setupConnection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

        await InsertRowAsync(setupConnection, goodServer, OldTime, "primary_conninfo", Secret, databaseName: null, roleName: null, ct);
        await InsertRowAsync(setupConnection, badServer, OldTime, "primary_conninfo", Secret, databaseName: null, roleName: null, ct);

        await ExecAsync(setupConnection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);
        Assert.True(await ContainsSecretAsync(setupConnection, ct), "seeding failed to plant the secret this test exists to catch");

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var first = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(first.AlreadyDone);

        // goodServer's row is scrubbed even though badServer's failed; the marker is withheld.
        var goodValue = await ScalarTextAsync(setupConnection,
            "SELECT setting FROM collect.pg_server_config WHERE server_id = $1 AND collection_time = $2 AND name = 'primary_conninfo'",
            goodServer, OldTime, ct);
        Assert.NotNull(goodValue);
        Assert.DoesNotContain("hunter2", goodValue);

        var markerValue = await ScalarTextAsync(setupConnection,
            "SELECT state_value FROM collect.collector_state WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'",
            0, DateTime.MinValue, ct);
        Assert.Null(markerValue);

        var stillSecretBad = await ScalarTextAsync(setupConnection,
            "SELECT setting FROM collect.pg_server_config WHERE server_id = $1 AND collection_time = $2 AND name = 'primary_conninfo'",
            badServer, OldTime, ct);
        Assert.Contains("hunter2", stillSecretBad);

        // Remove the cause, then the next run completes badServer too and sets the marker.
        await ExecAsync(setupConnection, "ALTER TABLE collect.pg_server_config DROP CONSTRAINT chk_4351_forced_failure", ct);

        var second = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(second.AlreadyDone);
        Assert.False(await ContainsSecretAsync(setupConnection, ct), "the raw password survived the retried run");

        // Retry half of the claim: badServer's one row is what the second run picks up; the marker is now set.
        Assert.Equal(1, first.RowsUpdated);
        Assert.Equal(1, second.RowsUpdated);

        var markerValueAfterRetry = await ScalarTextAsync(setupConnection,
            "SELECT state_value FROM collect.collector_state WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'",
            0, DateTime.MinValue, ct);
        Assert.Equal(PgSettingRedactor.RulesVersion.ToString(CultureInfo.InvariantCulture), markerValueAfterRetry);
    }

    /// <summary>
    /// W1 text pin: a pre-2.14 TimescaleDB store (the GUC arrived in timescale/timescaledb PR #6566) has no
    /// <c>timescaledb.max_tuples_decompressed_per_dml_transaction</c> setting at all. A bare
    /// <c>SET LOCAL ... = 0</c> would raise 42704 on every batch on such a store, so the SET must be guarded
    /// by a <c>current_setting(name, true) IS NOT NULL</c> check — <c>true</c> makes it non-throwing, and
    /// <c>set_config(..., true)</c> is the SET LOCAL equivalent, scoped to the same transaction.
    /// </summary>
    [Fact]
    public void TheSetLocalIsGuardedForAPre214TimescaleDbStore()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(),
            "Darling", "PerformanceMonitor.Darling.Service", "PgSettingScrub.cs"));

        Assert.Contains(
            "SELECT set_config('timescaledb.max_tuples_decompressed_per_dml_transaction', '0', true)",
            source, StringComparison.Ordinal);
        Assert.Contains(
            "WHERE current_setting('timescaledb.max_tuples_decompressed_per_dml_transaction', true) IS NOT NULL",
            source, StringComparison.Ordinal);

        /* The un-guarded shape this replaces — must not come back. */
        Assert.DoesNotContain(
            "\"SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0\"",
            source, StringComparison.Ordinal);
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }

    private const string Mask = "********";

    private const int ServerId = -444444;
    private const string ServerName = "darling-setting-scrub-e2e";
    private const string Secret = "host=replica1.internal port=5432 user=replicator password=hunter2 sslmode=require";
    private static readonly DateTime OldTime = DateTime.SpecifyKind(new DateTime(2026, 1, 1, 0, 0, 0), DateTimeKind.Unspecified);
    private static readonly DateTime NewTime = DateTime.SpecifyKind(new DateTime(2026, 1, 1, 1, 0, 0), DateTimeKind.Unspecified);

    [Fact]
    public async Task TheScrubRedactsOldRows_LeavesHistoryUnchanged_AndIsIdempotent()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4348 scrub test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await ExecAsync(connection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
        await ExecAsync(connection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

        /* Arm 1: a server-wide pg_settings row (database_name/role_name NULL), the OLD unredacted shape. */
        await InsertRowAsync(connection, ServerId, OldTime, "primary_conninfo", Secret, databaseName: null, roleName: null, ct);
        /* Arm 2: a database_name override row (V138) — the standby's replication setting scoped to one database. */
        await InsertRowAsync(connection, ServerId, OldTime, "primary_conninfo", Secret, databaseName: "appdb", roleName: null, ct);
        /* A NEWER row S1a's redactor already masked — same underlying value, already-redacted shape, server-wide. */
        await InsertRowAsync(connection, ServerId, NewTime, "primary_conninfo", "host=replica1.internal port=5432 user=replicator password=******** sslmode=require", databaseName: null, roleName: null, ct);
        /* A control row with no secret at all — must come out byte-identical. */
        await InsertRowAsync(connection, ServerId, OldTime, "shared_buffers", "128MB", databaseName: null, roleName: null, ct);

        await ExecAsync(connection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);

        /* ── Prove the assertion below actually depends on the job: the secret is really there, compressed, before it runs. ── */
        Assert.True(await ContainsSecretAsync(connection, ct), "seeding failed to plant the secret this test exists to catch");

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var first = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(first.AlreadyDone);
        Assert.True(first.RowsUpdated >= 2, $"expected at least the two secret-bearing rows to be updated, got {first.RowsUpdated}");

        /* ── No column of any row contains the secret. ── */
        Assert.False(await ContainsSecretAsync(connection, ct), "the raw password survived the scrub");

        /* ── The control row is byte-identical. ── */
        var controlValue = await ScalarTextAsync(connection,
            "SELECT setting FROM collect.pg_server_config WHERE server_id = $1 AND collection_time = $2 AND name = 'shared_buffers'",
            ServerId, OldTime, ct);
        Assert.Equal("128MB", controlValue);

        /* ── The change history shows no CHANGE between the old (now-scrubbed) row and the newer (already-redacted) row. ── */
        await using (var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            var changes = await DarlingPgServerConfigReader.GetConfigChangesAsync(
                dataSource, ServerId, OldTime.AddHours(-1), NewTime.AddHours(1), 50, ct);
            foreach (var change in changes)
            {
                if (change.Name == "primary_conninfo" && change.DatabaseName is null)
                {
                    Assert.NotEqual(DarlingPgServerConfigReader.PgConfigChangeKind.Changed, change.ChangeKind);
                }
            }
        }

        /* ── A second start does nothing: the marker holds. ── */
        var second = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
        Assert.True(second.AlreadyDone);

        /* ── An older marker (standing in for a future RulesVersion bump) runs the scrub again. ── */
        await ExecAsync(connection,
            "UPDATE collect.collector_state SET state_value = '0' WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'", ct);
        var third = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(third.AlreadyDone);
    }

    /// <summary>
    /// Full hour-slicing: one server-day of 24 hours × 5 secret-bearing rows, with
    /// <see cref="PgSettingScrub.TestOnlyHourSliceCandidateThresholdOverride"/> forced to 10 so the day's
    /// 120 candidates take the sliced path (one transaction per hour) rather than the single-range path.
    /// Every row ends up redacted and the marker is set to the current <see cref="PgSettingRedactor.RulesVersion"/>.
    /// </summary>
    [Fact]
    public async Task TheScrubRedactsEveryRow_WhenTheDayIsSlicedByHour()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4348 hour-slice pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        try
        {
            PgSettingScrub.TestOnlyHourSliceCandidateThresholdOverride = 10;

            const int serverId = -444450;
            var day = new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Unspecified);

            await using (var setupConnection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await setupConnection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(setupConnection, ct);

                Assert.True(await TimescaleSupport.TryEnableAsync(setupConnection, null, ct),
                    "the dev fixture is expected to have TimescaleDB installed");
                await ExecAsync(setupConnection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
                await ExecAsync(setupConnection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

                for (var hour = 0; hour < 24; hour++)
                {
                    for (var row = 0; row < 5; row++)
                    {
                        var collectionTime = day.AddHours(hour).AddMinutes(row);
                        await InsertRowAsync(setupConnection, serverId, collectionTime, $"custom.setting_{hour}_{row}", "password=hunter2", databaseName: null, roleName: null, ct);
                    }
                }

                await ExecAsync(setupConnection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);

                Assert.True(await ContainsSecretAsync(setupConnection, "hunter2", ct), "seeding failed to plant the secret this test exists to catch");
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var summary = await PgSettingScrub.RunAsync(postgres, logger: null, ct);

            Assert.False(summary.AlreadyDone);
            Assert.Equal(120, summary.RowsUpdated);

            await using var verifyConnection = new NpgsqlConnection(scratch.ConnectionString);
            await verifyConnection.OpenAsync(ct);
            Assert.False(await ContainsSecretAsync(verifyConnection, "hunter2", ct), "a setting value is still unmasked after the hour-sliced scrub");

            var markerValue = await ScalarTextAsync(verifyConnection,
                "SELECT state_value FROM collect.collector_state WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'",
                0, DateTime.MinValue, ct);
            Assert.Equal(PgSettingRedactor.RulesVersion.ToString(CultureInfo.InvariantCulture), markerValue);
        }
        finally
        {
            PgSettingScrub.TestOnlyHourSliceCandidateThresholdOverride = null;
        }
    }

    /// <summary>
    /// Interrupted hour-slicing: the same shape as above, but
    /// <see cref="PgSettingScrub.TestOnlyAfterSliceCommitted"/> throws right after the 5th slice's transaction
    /// has already committed. The already-committed slices' rows are redacted, the rest of the day's rows are
    /// still plaintext, and the marker is withheld. A second run (the seam disarmed) redacts exactly the
    /// remaining rows, sets the marker, and changes zero rows in the slices the first run finished — proving
    /// resumability rather than assuming it.
    /// </summary>
    [Fact]
    public async Task TheScrub_ResumesExactlyTheUnfinishedSlices_AfterAMidDayInterruption()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4348 interrupted-slice pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        try
        {
            PgSettingScrub.TestOnlyHourSliceCandidateThresholdOverride = 10;

            const int serverId = -444451;
            var day = new DateTime(2026, 2, 2, 0, 0, 0, DateTimeKind.Unspecified);

            await using var setupConnection = new NpgsqlConnection(scratch.ConnectionString);
            await setupConnection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setupConnection, ct);

            Assert.True(await TimescaleSupport.TryEnableAsync(setupConnection, null, ct),
                "the dev fixture is expected to have TimescaleDB installed");
            await ExecAsync(setupConnection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
            await ExecAsync(setupConnection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

            for (var hour = 0; hour < 24; hour++)
            {
                for (var row = 0; row < 5; row++)
                {
                    var collectionTime = day.AddHours(hour).AddMinutes(row);
                    await InsertRowAsync(setupConnection, serverId, collectionTime, $"custom.setting_{hour}_{row}", "password=hunter2", databaseName: null, roleName: null, ct);
                }
            }

            await ExecAsync(setupConnection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);
            Assert.True(await ContainsSecretAsync(setupConnection, "hunter2", ct), "seeding failed to plant the secret this test exists to catch");

            var slicesCommitted = 0;
            PgSettingScrub.TestOnlyAfterSliceCommitted = () =>
            {
                slicesCommitted++;
                if (slicesCommitted == 5)
                {
                    throw new InvalidOperationException("forced mid-day interruption after the 5th committed slice");
                }
            };

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            await Assert.ThrowsAsync<InvalidOperationException>(() => PgSettingScrub.RunAsync(postgres, logger: null, ct));

            // Hours 0..4 (5 slices, 25 rows) committed before the throw; hours 5..23 (95 rows) are still plaintext.
            var stillPlaintextCount = await ScalarLongAsync(setupConnection,
                "SELECT count(*) FROM collect.pg_server_config WHERE server_id = $1 AND setting = 'password=hunter2'", serverId, ct);
            Assert.Equal(95, stillPlaintextCount);

            var redactedSoFarCount = await ScalarLongAsync(setupConnection,
                "SELECT count(*) FROM collect.pg_server_config WHERE server_id = $1 AND setting <> 'password=hunter2'", serverId, ct);
            Assert.Equal(25, redactedSoFarCount);

            var markerAfterInterruption = await ScalarTextAsync(setupConnection,
                "SELECT state_value FROM collect.collector_state WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'",
                0, DateTime.MinValue, ct);
            Assert.Null(markerAfterInterruption);

            // Disarm the seam, then the second run redacts exactly the unfinished rows.
            PgSettingScrub.TestOnlyAfterSliceCommitted = null;

            var second = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
            Assert.False(second.AlreadyDone);
            Assert.Equal(95, second.RowsUpdated);

            Assert.False(await ContainsSecretAsync(setupConnection, "hunter2", ct), "a setting value is still unmasked after the resumed run");

            var markerAfterResume = await ScalarTextAsync(setupConnection,
                "SELECT state_value FROM collect.collector_state WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'",
                0, DateTime.MinValue, ct);
            Assert.Equal(PgSettingRedactor.RulesVersion.ToString(CultureInfo.InvariantCulture), markerAfterResume);
        }
        finally
        {
            PgSettingScrub.TestOnlyHourSliceCandidateThresholdOverride = null;
            PgSettingScrub.TestOnlyAfterSliceCommitted = null;
        }
    }

    /// <summary>
    /// One row per redactor rule shape (#4348), all seeded under marker=1 so the run has to redact every one
    /// of them to reach the current marker: a percent-encoded query key, a fully percent-encoded query key, a
    /// key-quoted assignment, <c>curl -u</c>, <c>curl --proxy-user</c>, <c>sshpass -p</c>, a SAS-style
    /// <c>sig=</c> query parameter, and an <c>X-Amz-Signature</c> query parameter. Every row is masked and
    /// the marker lands on <see cref="PgSettingRedactor.RulesVersion"/>.
    /// </summary>
    [Fact]
    public async Task TheScrubMasksEveryRedactorRuleShape()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG … to run the per-rule scrub test");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await ExecAsync(connection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
        await ExecAsync(connection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

        const int serverId = -444460;

        var forms = new (string Name, string Value)[]
        {
            ("primary_conninfo", "postgresql://alice@host/db?pass%77ord=hunter2"),
            ("primary_conninfo", "postgresql://alice@host/db?%70ass%77ord=hunter2"),
            ("custom.json_blob", "{\"password\" = \"hunter2\", \"host\" = \"foo\"}"),
            ("archive_command", "curl -u user:hunter2 https://x"),
            ("archive_command", "curl --proxy-user user:hunter2 https://x"),
            ("archive_command", "sshpass -p hunter2 ssh user@host"),
            ("primary_conninfo", "https://acct.blob.core.windows.net/c/f?sv=2024&sig=hunter2"),
            ("primary_conninfo", "https://b.s3.amazonaws.com/f?X-Amz-Signature=hunter2"),
        };

        var collectionTime = OldTime;
        foreach (var (name, value) in forms)
        {
            await InsertRowAsync(connection, serverId, collectionTime, name, value, databaseName: null, roleName: null, ct);
            collectionTime = collectionTime.AddMinutes(1);
        }

        await ExecAsync(connection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);

        Assert.True(await ContainsSecretAsync(connection, "hunter2", ct), "seeding failed to plant the secret this test exists to catch");

        /* Seeded at 1 (matching PgSettingScrub's own marker row shape, server_id = 0) so the run has to
           redact every form to reach RulesVersion rather than trivially no-op on an already-current marker. */
        await ExecAsync(connection,
            "INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at) " +
            "VALUES (0, 'pg_setting_scrub', 'rules_version', '1', now())", ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var summary = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(summary.AlreadyDone);
        Assert.Equal(forms.Length, summary.RowsUpdated);

        Assert.False(await ContainsSecretAsync(connection, "hunter2", ct), "a setting value is still unmasked after the scrub");

        var markerValue = await ScalarTextAsync(connection,
            "SELECT state_value FROM collect.collector_state WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'",
            0, DateTime.MinValue, ct);
        Assert.Equal(PgSettingRedactor.RulesVersion.ToString(CultureInfo.InvariantCulture), markerValue);
    }

    /// <summary>
    /// A client-side command timeout on one target, with the other target unaffected: two servers, a
    /// pre-update <c>pg_sleep</c> scoped to ONE server id via
    /// <see cref="PgSettingScrub.TestOnlyPreUpdateDelayServerId"/>, and the UPDATE's command timeout forced
    /// to 1 second via <see cref="PgSettingScrub.TestOnlyUpdateCommandTimeoutSecondsOverride"/>. The delayed
    /// server's row stays plaintext (its batch times out and is skipped, per the type remarks' per-server
    /// isolation), the other server's row is fully redacted, and the marker is withheld because one target
    /// failed. <see cref="CapturingTestLogger"/> is used (not <c>logger: null</c>) so the timeout's own log
    /// line can be asserted.
    ///
    /// <para>The slow server's id is deliberately the smaller of the two, so it sorts FIRST in
    /// <c>byServerDay</c>'s ordering. It also carries a SECOND day, so once its first day's batch times out
    /// and <c>failedServerIds</c> records the failure, its remaining day is skipped for the rest of THIS run
    /// (per the type remarks' per-server isolation) rather than retried — both of the slow server's days stay
    /// plaintext, while the other server, later in the ordering, is unaffected and fully redacted.</para>
    /// </summary>
    [Fact]
    public async Task TheScrubTimesOutOnOneServer_AndStillRedactsTheOther()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4348 timeout+reconnect pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        try
        {
            const int slowServer = -444453;
            const int fastServer = -444452;
            var day = new DateTime(2026, 2, 3, 0, 0, 0, DateTimeKind.Unspecified);
            var secondDay = day.AddDays(1);

            await using var setupConnection = new NpgsqlConnection(scratch.ConnectionString);
            await setupConnection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setupConnection, ct);

            Assert.True(await TimescaleSupport.TryEnableAsync(setupConnection, null, ct),
                "the dev fixture is expected to have TimescaleDB installed");
            await ExecAsync(setupConnection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
            await ExecAsync(setupConnection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

            await InsertRowAsync(setupConnection, slowServer, day, "primary_conninfo", "password=hunter2", databaseName: null, roleName: null, ct);
            await InsertRowAsync(setupConnection, slowServer, secondDay, "primary_conninfo", "password=hunter2", databaseName: null, roleName: null, ct);
            await InsertRowAsync(setupConnection, fastServer, day, "primary_conninfo", "password=hunter2", databaseName: null, roleName: null, ct);

            await ExecAsync(setupConnection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);
            Assert.True(await ContainsSecretAsync(setupConnection, "hunter2", ct), "seeding failed to plant the secret this test exists to catch");

            PgSettingScrub.TestOnlyUpdateCommandTimeoutSecondsOverride = 1;
            PgSettingScrub.TestOnlyPreUpdateDelaySeconds = 3;
            PgSettingScrub.TestOnlyPreUpdateDelayServerId = slowServer;
            PgSettingScrub.TestOnlyPreUpdateDelayOnce = true;

            var capturingLogger = new CapturingTestLogger();

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var summary = await PgSettingScrub.RunAsync(postgres, capturingLogger, ct);

            Assert.False(summary.AlreadyDone);

            var slowFirstDayValue = await ScalarTextAsync(setupConnection,
                "SELECT setting FROM collect.pg_server_config WHERE server_id = $1 AND collection_time = $2 AND name = 'primary_conninfo'",
                slowServer, day, ct);
            Assert.Equal("password=hunter2", slowFirstDayValue);

            /* The slow server's SECOND day is skipped, not timed out — TestOnlyPreUpdateDelayOnce clears the
               delay seam after the first batch, so if the second day's batch ran at all it would succeed and
               redact. It stays plaintext only because the first day's timeout already marked the server
               failed, per the type remarks' per-server isolation. */
            var slowSecondDayValue = await ScalarTextAsync(setupConnection,
                "SELECT setting FROM collect.pg_server_config WHERE server_id = $1 AND collection_time = $2 AND name = 'primary_conninfo'",
                slowServer, secondDay, ct);
            Assert.Equal("password=hunter2", slowSecondDayValue);

            var fastValue = await ScalarTextAsync(setupConnection,
                "SELECT setting FROM collect.pg_server_config WHERE server_id = $1 AND collection_time = $2 AND name = 'primary_conninfo'",
                fastServer, day, ct);
            Assert.NotEqual("password=hunter2", fastValue);

            var markerValue = await ScalarTextAsync(setupConnection,
                "SELECT state_value FROM collect.collector_state WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'",
                0, DateTime.MinValue, ct);
            Assert.Null(markerValue);

            /* The fixture's CapturingTestLogger records the real formatted line; assert it names the timeout
               rather than a generic client SQLSTATE. Exactly one line, since TestOnlyPreUpdateDelayOnce
               clears the delay seam after the first batch, so the second day's batch (skipped, not run)
               never gets a chance to time out again. */
            var timeoutLines = capturingLogger.Lines.Count(line => line.Contains("timeout", StringComparison.OrdinalIgnoreCase));
            Assert.Equal(1, timeoutLines);
        }
        finally
        {
            PgSettingScrub.TestOnlyUpdateCommandTimeoutSecondsOverride = null;
            PgSettingScrub.TestOnlyPreUpdateDelaySeconds = null;
            PgSettingScrub.TestOnlyPreUpdateDelayServerId = null;
            PgSettingScrub.TestOnlyPreUpdateDelayOnce = false;
        }
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, int serverId, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        cmd.Parameters.AddWithValue(serverId);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task<bool> ContainsSecretAsync(NpgsqlConnection connection, string marker, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            $"SELECT count(*) FROM collect.pg_server_config WHERE setting LIKE '%{marker}%' OR boot_val LIKE '%{marker}%' OR reset_val LIKE '%{marker}%'",
            connection)
        { CommandTimeout = 60 };
        var count = (long)(await cmd.ExecuteScalarAsync(ct))!;
        return count > 0;
    }

    private static async Task<bool> ContainsSecretAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT count(*) FROM collect.pg_server_config WHERE setting LIKE '%hunter2%' OR boot_val LIKE '%hunter2%' OR reset_val LIKE '%hunter2%'",
            connection)
        { CommandTimeout = 60 };
        var count = (long)(await cmd.ExecuteScalarAsync(ct))!;
        return count > 0;
    }

    private static async Task<string?> ScalarTextAsync(NpgsqlConnection connection, string sql, int serverId, DateTime collectionTime, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(collectionTime);
        return (await cmd.ExecuteScalarAsync(ct)) as string;
    }

    private static async Task InsertRowAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, string name, string value,
        string? databaseName, string? roleName, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.pg_server_config
  (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype,
   source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc, database_name, role_name)
VALUES
  ($1, $2, $3, $4, $5, $6, NULL, 'category', 'sighup', 'string', 'configuration file', $6, $6, NULL, NULL, false, 'desc', $7, $8)", connection)
        { CommandTimeout = 60 };
        cmd.Parameters.AddWithValue((long)(collectionTime.Ticks ^ (long)name.GetHashCode() ^ (databaseName ?? "").GetHashCode()));
        cmd.Parameters.AddWithValue(collectionTime);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(ServerName);
        cmd.Parameters.AddWithValue(name);
        cmd.Parameters.AddWithValue(value);
        cmd.Parameters.AddWithValue((object?)databaseName ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)roleName ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
