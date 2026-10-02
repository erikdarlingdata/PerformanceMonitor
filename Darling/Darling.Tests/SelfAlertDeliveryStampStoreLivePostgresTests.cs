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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches
   nothing on the shared one, so serializing it against the live-postgres collection would cost suite time
   and buy no isolation. */

/// <summary>
/// The half of #3580's stamp store no unit pin reaches: that the two statements PARSE against a migrated
/// store, that a stamp round-trips to the tick with its Kind, that the upsert replaces rather than
/// duplicates, that the row sits where the class remarks say it sits (<c>server_id = 0</c>,
/// <c>collector_name = 'self_alert'</c>, <c>updated_at</c> a naive UTC write time), and that a value the
/// store cannot parse reads as no stamp rather than as a throw — the <see cref="FleetSweepStoreLivePostgresTests"/>
/// shape, over a table that has existed since V44 and so needs no rung of its own.
/// </summary>
public sealed class SelfAlertDeliveryStampStoreLivePostgresTests
{
    [Fact]
    public async Task AStampRoundTrips_Replaces_AndSitsUnderTheFleetSentinel()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live delivery-stamp round-trip (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var log = new CapturingTestLogger();
        var store = new PgSelfAlertDeliveryStampStore(postgres, log);

        /* A fresh store answers null for both keys — no throw, no phantom row. */
        Assert.Null(await store.GetDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.CostDigestStateKey, ct));
        Assert.Null(await store.GetDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey, ct));

        /* Round-trip to the tick, Kind Utc coming back — and the two keys are independent rows. */
        var digestAt = new DateTime(2026, 9, 17, 23, 30, 12, DateTimeKind.Utc).AddTicks(1234567);
        await store.RecordDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.CostDigestStateKey, digestAt, ct);
        var readBack = await store.GetDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.CostDigestStateKey, ct);
        Assert.Equal(digestAt, readBack);
        Assert.Equal(DateTimeKind.Utc, readBack!.Value.Kind);
        Assert.Null(await store.GetDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey, ct));

        /* A Kind-Unspecified caller value (the evaluator's clock seam can hand one in) is stamped Utc on the
           way in, so it reads back Utc and equal. */
        var rollupAt = new DateTime(2026, 9, 18, 0, 15, 0, DateTimeKind.Unspecified);
        await store.RecordDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey, rollupAt, ct);
        var rollupBack = await store.GetDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey, ct);
        Assert.Equal(rollupAt, rollupBack);
        Assert.Equal(DateTimeKind.Utc, rollupBack!.Value.Kind);

        /* The upsert REPLACES: a second delivery a day later overwrites the digest's row, and the table holds
           exactly two rows under this owner — one per document — not three. */
        var laterDigestAt = digestAt.AddDays(1);
        await store.RecordDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.CostDigestStateKey, laterDigestAt, ct);
        Assert.Equal(laterDigestAt, await store.GetDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.CostDigestStateKey, ct));

        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            await using var rows = new NpgsqlCommand(@"
SELECT server_id, collector_name, state_key, state_value, updated_at
FROM collect.collector_state
WHERE collector_name = $1
ORDER BY state_key", connection);
            rows.Parameters.AddWithValue(PgSelfAlertDeliveryStampStore.StateCollectorName);
            await using var reader = await rows.ExecuteReaderAsync(ct);

            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(PgSelfAlertDeliveryStampStore.FleetServerId, reader.GetInt32(0));
            Assert.Equal("self_alert", reader.GetString(1));
            Assert.Equal(PgSelfAlertDeliveryStampStore.CostDigestStateKey, reader.GetString(2));
            /* The value is the round-trip text with its Z — what makes the read side's Kind honest. */
            Assert.EndsWith("Z", reader.GetString(3), StringComparison.Ordinal);
            /* updated_at is the WRITE time, naive UTC: within a minute of now, read as Unspecified from a
               `timestamp` column, and never the server's local rendering of a timestamptz cast (the trap the
               runner's own comment measured at exactly one zone offset). */
            var updatedAt = reader.GetDateTime(4);
            Assert.Equal(DateTimeKind.Unspecified, updatedAt.Kind);
            Assert.InRange(DateTime.UtcNow - DateTime.SpecifyKind(updatedAt, DateTimeKind.Utc), TimeSpan.FromMinutes(-1), TimeSpan.FromMinutes(1));

            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey, reader.GetString(2));

            Assert.False(await reader.ReadAsync(ct), "the upsert duplicated a stamp row instead of replacing it");
        }

        /* A value this build cannot parse — a hand edit, or a writer this build does not know — reads as NO
           stamp with a warning, so the gate falls back to memory for the tick rather than the pass dying on a
           FormatException; the next delivery overwrites it. */
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            await using var poison = new NpgsqlCommand(@"
UPDATE collect.collector_state
SET state_value = 'yesterday-ish'
WHERE server_id = $1 AND collector_name = $2 AND state_key = $3", connection);
            poison.Parameters.AddWithValue(PgSelfAlertDeliveryStampStore.FleetServerId);
            poison.Parameters.AddWithValue(PgSelfAlertDeliveryStampStore.StateCollectorName);
            poison.Parameters.AddWithValue(PgSelfAlertDeliveryStampStore.CostDigestStateKey);
            Assert.Equal(1, await poison.ExecuteNonQueryAsync(ct));
        }

        Assert.Null(await store.GetDeliveredAtUtcAsync(PgSelfAlertDeliveryStampStore.CostDigestStateKey, ct));
        Assert.Contains("not a round-trip UTC instant", log.Joined, StringComparison.Ordinal);
        Assert.Contains("yesterday-ish", log.Joined, StringComparison.Ordinal);
    }
}
