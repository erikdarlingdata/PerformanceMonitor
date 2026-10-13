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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The service's side of the external alert queue against a real store (#5654): the claim takes each row once, a run that
/// dies holding a claim leaves the row interrupted and never re-sent, expiry, the delivered and muted marks, the firing
/// state across a fire and a clear, and retention trimming finished rows and never live ones.
/// </summary>
public sealed class ExternalAlertStoreLiveTests
{
    private const int Deadline = 30;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheStatements_AreParameterizedAndSchemaQualified_AndTheClaimIsSkipLockedWithAStatusGuard()
    {
        foreach (var sql in new[]
                 {
                     ExternalAlertStore.ClaimSql, ExternalAlertStore.MarkSql, ExternalAlertStore.ReleaseForRetrySql,
                     ExternalAlertStore.ExpireSql, ExternalAlertStore.ReapSql, ExternalAlertStore.GetStateSql,
                     ExternalAlertStore.RecordFireSql, ExternalAlertStore.RecordClearSql,
                 })
        {
            Assert.DoesNotContain("external_alert_pending", sql.Replace("config.external_alert_pending", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
            Assert.DoesNotContain("external_alert_state", sql.Replace("config.external_alert_state", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        }

        Assert.Contains("FOR UPDATE OF p SKIP LOCKED", ExternalAlertStore.ClaimSql, StringComparison.Ordinal);
        Assert.Contains("SET status = 'claimed'", ExternalAlertStore.ClaimSql, StringComparison.Ordinal);
        Assert.Contains("AND t.status = 'pending'", ExternalAlertStore.ClaimSql, StringComparison.Ordinal);
        Assert.Contains("pg_advisory_xact_lock($1)", ExternalAlertStore.ClaimLockSql, StringComparison.Ordinal);
        Assert.Contains("SET status = 'interrupted'", ExternalAlertStore.ReapSql, StringComparison.Ordinal);
        Assert.DoesNotContain("'pending'", ExternalAlertStore.ReapSql.Replace("status = 'interrupted'", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains("AND status = 'claimed'", ExternalAlertStore.MarkSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoConcurrentClaimers_TakeEveryRowExactlyOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the external alert store's live facts (each mints its own scratch database).");

        await WithStoreAsync(async (seed, connectionString, ct) =>
        {
            var sourceId = await AddSourceAsync(seed, "claim_svc", ct);
            const int Rows = 120;
            for (var i = 0; i < Rows; i++)
            {
                await SeedAsync(seed, sourceId, "fire", "k" + i.ToString(CultureInfo.InvariantCulture), ct: ct);
            }

            await using var dataSourceA = NpgsqlDataSource.Create(connectionString);
            await using var dataSourceB = NpgsqlDataSource.Create(connectionString);
            var a = new ExternalAlertStore(dataSourceA, Deadline);
            var b = new ExternalAlertStore(dataSourceB, Deadline);

            async Task<List<long>> DrainAsync(ExternalAlertStore store, string instance)
            {
                var taken = new List<long>();
                while (true)
                {
                    var batch = await store.ClaimAsync(instance, 7, ct);
                    if (batch.Count == 0)
                    {
                        return taken;
                    }

                    taken.AddRange(batch.Select(r => r.PendingId));

                    /* A claim that hands a row out twice would never run dry; the count assertion below reports it. */
                    if (taken.Count > Rows)
                    {
                        return taken;
                    }
                }
            }

            var results = await Task.WhenAll(DrainAsync(a, "host-a:1"), DrainAsync(b, "host-b:2"));
            var all = results[0].Concat(results[1]).ToList();

            Assert.Equal(Rows, all.Count);
            Assert.Equal(Rows, all.Distinct().Count());
            Assert.True(results[0].Count > 0 && results[1].Count > 0, "both claimers should have taken part of the queue");
            Assert.Equal(Rows.ToString(CultureInfo.InvariantCulture), await ScalarAsync(seed, "SELECT count(*) FROM config.external_alert_pending WHERE status = 'claimed'", ct));
            Assert.Empty(await a.ClaimAsync("host-a:1", 50, ct));
        });
    }

    [Fact]
    public async Task AClaimAndTheRunThatTookItDying_LeavesTheRowInterrupted_AndNoRunClaimsItAgain()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the external alert store's live facts (each mints its own scratch database).");

        await WithStoreAsync(async (seed, connectionString, ct) =>
        {
            var sourceId = await AddSourceAsync(seed, "reap_svc", ct);
            var pendingId = await SeedAsync(seed, sourceId, "fire", "k1", ct: ct);
            var freshId = await SeedAsync(seed, sourceId, "fire", "k2", ct: ct);

            /* The first run claims, then dies: its data source is gone and it reported nothing. */
            IReadOnlyList<ExternalAlertPendingRow> claimed;
            var firstRun = NpgsqlDataSource.Create(connectionString);
            try
            {
                claimed = await new ExternalAlertStore(firstRun, Deadline).ClaimAsync("host:100", 1, ct);
            }
            finally
            {
                await firstRun.DisposeAsync();
            }

            Assert.Equal(pendingId, Assert.Single(claimed).PendingId);

            /* The next run reaps with a grace: a claim younger than the grace is a live run's and is left alone. */
            await using var secondRun = NpgsqlDataSource.Create(connectionString);
            var store = new ExternalAlertStore(secondRun, Deadline);
            Assert.Equal(0, await store.ReapAsync(TimeSpan.FromMinutes(5), ct));
            Assert.Equal("claimed", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct));

            /* Past the grace it is interrupted, finished, and never claimed again. */
            Assert.Equal(1, await store.ReapAsync(TimeSpan.Zero, ct));
            Assert.Equal("interrupted", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct));
            Assert.Equal("true", await ScalarAsync(seed, $"SELECT (completed_at IS NOT NULL)::text FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct));
            Assert.Equal(0, await store.ReapAsync(TimeSpan.Zero, ct));

            var next = await store.ClaimAsync("host:200", 50, ct);
            Assert.Equal(freshId, Assert.Single(next).PendingId);
            Assert.Empty(await store.ClaimAsync("host:200", 50, ct));
            Assert.Equal("interrupted", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct));

            /* A late mark from the run that was thought dead changes nothing: the row is no longer claimed. */
            Assert.False(await store.MarkDeliveredAsync(pendingId, ct));
            Assert.Equal("interrupted", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct));
        });
    }

    [Fact]
    public async Task APendingRowPastItsDeadline_IsExpired_AndNeverClaimed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the external alert store's live facts (each mints its own scratch database).");

        await WithStoreAsync(async (seed, connectionString, ct) =>
        {
            var sourceId = await AddSourceAsync(seed, "expiry_svc", ct);
            var stale = await SeedAsync(seed, sourceId, "fire", "old", expiresInMinutes: -1, ct: ct);
            var live = await SeedAsync(seed, sourceId, "fire", "new", expiresInMinutes: 10, ct: ct);
            var claimedBeforeDeadline = await SeedAsync(seed, sourceId, "fire", "held", expiresInMinutes: -1, status: "claimed", ct: ct);

            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            var store = new ExternalAlertStore(dataSource, Deadline);

            /* An expired row is never handed out, even before the expiry pass marks it. */
            Assert.Equal(live, Assert.Single(await store.ClaimAsync("host:1", 50, ct)).PendingId);

            Assert.Equal(1, await store.ExpireAsync(ct));
            Assert.Equal("expired", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {stale}", ct));
            Assert.Equal("true", await ScalarAsync(seed, $"SELECT (completed_at IS NOT NULL)::text FROM config.external_alert_pending WHERE pending_id = {stale}", ct));

            /* Expiry ends only pending rows: a claimed row is the reaper's, a finished row is final. */
            Assert.Equal("claimed", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {claimedBeforeDeadline}", ct));
            Assert.Equal("claimed", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {live}", ct));
            Assert.Equal(0, await store.ExpireAsync(ct));
        });
    }

    [Fact]
    public async Task DeliveredAndMutedMarks_FinishAClaimedRow_AndARetryReleaseWaitsForItsTime()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the external alert store's live facts (each mints its own scratch database).");

        await WithStoreAsync(async (seed, connectionString, ct) =>
        {
            var sourceId = await AddSourceAsync(seed, "mark_svc", ct);
            var one = await SeedAsync(seed, sourceId, "fire", "k1", ct: ct);
            var two = await SeedAsync(seed, sourceId, "fire", "k2", ct: ct);
            var three = await SeedAsync(seed, sourceId, "fire", "k3", ct: ct);

            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            var store = new ExternalAlertStore(dataSource, Deadline);

            /* Marking a row nobody claimed does nothing. */
            Assert.False(await store.MarkDeliveredAsync(one, ct));
            Assert.Equal("pending", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {one}", ct));

            var claimed = await store.ClaimAsync("host:1", 50, ct);
            Assert.Equal(new[] { one, two, three }, claimed.Select(r => r.PendingId).ToArray());

            Assert.True(await store.MarkDeliveredAsync(one, ct));
            Assert.True(await store.MarkMutedAsync(two, "mute rule m1", ct));
            Assert.Equal("delivered", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {one}", ct));
            Assert.Equal("muted|mute rule m1", await ScalarAsync(seed, $"SELECT status || '|' || reason FROM config.external_alert_pending WHERE pending_id = {two}", ct));
            Assert.Equal("true", await ScalarAsync(seed, $"SELECT (completed_at IS NOT NULL)::text FROM config.external_alert_pending WHERE pending_id = {two}", ct));

            /* A finished row cannot be marked again. */
            Assert.False(await store.MarkMutedAsync(one, "late", ct));
            Assert.Equal("delivered", await ScalarAsync(seed, $"SELECT status FROM config.external_alert_pending WHERE pending_id = {one}", ct));

            /* A failed send goes back to pending, counts the attempt, and is not claimed before its time. */
            Assert.True(await store.ReleaseForRetryAsync(three, DateTime.UtcNow.AddMinutes(5), "webhook refused", ct));
            Assert.Equal("pending|1", await ScalarAsync(seed, $"SELECT status || '|' || attempts FROM config.external_alert_pending WHERE pending_id = {three}", ct));
            Assert.Empty(await store.ClaimAsync("host:1", 50, ct));
            await ExecAsync(seed, $"UPDATE config.external_alert_pending SET next_attempt_at = now() - interval '1 second' WHERE pending_id = {three}", ct);
            var again = Assert.Single(await store.ClaimAsync("host:1", 50, ct));
            Assert.Equal((three, 1), (again.PendingId, again.Attempts));
        });
    }

    [Fact]
    public async Task AFireThenAClearOnOneKey_ReachTheDrainInOrder_AndUpdateTheFiringState()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the external alert store's live facts (each mints its own scratch database).");

        await WithStoreAsync(async (seed, connectionString, ct) =>
        {
            var sourceId = await AddSourceAsync(seed, "state_svc", ct);
            var fire = await SeedAsync(seed, sourceId, "fire", "disk:c", severity: "critical", ct: ct);
            var clear = await SeedAsync(seed, sourceId, "clear", "disk:c", ct: ct);
            var other = await SeedAsync(seed, sourceId, "fire", "disk:d", ct: ct);

            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            var store = new ExternalAlertStore(dataSource, Deadline);

            /* The clear waits behind its fire; a different key is not held up. */
            var first = await store.ClaimAsync("host:1", 50, ct);
            Assert.Equal(new[] { fire, other }, first.Select(r => r.PendingId).ToArray());
            Assert.Equal("state_svc", first[0].SourceName);
            Assert.Empty(await store.ClaimAsync("host:1", 50, ct));

            Assert.Null(await store.GetStateAsync(sourceId, "disk:c", 0, ct));
            Assert.Null(await store.RecordClearAsync(sourceId, "disk:c", 0, ct));

            Assert.Equal((false, (string?)null), await store.RecordFireAsync(sourceId, "disk:c", 0, "critical", ct));
            var firing = await store.GetStateAsync(sourceId, "disk:c", 0, ct);
            Assert.NotNull(firing);
            Assert.True(firing!.Firing);
            Assert.Equal("critical", firing.FiredSeverity);
            Assert.NotNull(firing.LastFiredAtUtc);
            Assert.Equal((true, "critical"), await store.RecordFireAsync(sourceId, "disk:c", 0, "warning", ct));
            Assert.Equal("warning", (await store.GetStateAsync(sourceId, "disk:c", 0, ct))!.FiredSeverity);

            /* The same key on another server is its own subject. */
            Assert.Null(await store.GetStateAsync(sourceId, "disk:c", 7, ct));

            Assert.True(await store.MarkDeliveredAsync(fire, ct));
            var second = Assert.Single(await store.ClaimAsync("host:1", 50, ct));
            Assert.Equal(clear, second.PendingId);
            Assert.Equal("clear", second.Kind);

            Assert.Equal("warning", await store.RecordClearAsync(sourceId, "disk:c", 0, ct));
            var cleared = await store.GetStateAsync(sourceId, "disk:c", 0, ct);
            Assert.False(cleared!.Firing);
            Assert.Null(await store.RecordClearAsync(sourceId, "disk:c", 0, ct));
        });
    }

    [Fact]
    public async Task Retention_TrimsOldFinishedRows_AndNeverPendingOrClaimedOnes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the external alert store's live facts (each mints its own scratch database).");

        await WithStoreAsync(async (seed, connectionString, ct) =>
        {
            var sourceId = await AddSourceAsync(seed, "keep_svc", ct);
            var ageDays = DarlingRetention.ExternalAlertRetentionDays + 10;
            var oldIds = new List<long>();
            foreach (var status in new[] { "delivered", "muted", "no_channel", "failed", "expired", "interrupted", "ignored" })
            {
                oldIds.Add(await SeedAsync(seed, sourceId, "fire", "old-" + status, status: status, submittedDaysAgo: ageDays, ct: ct));
            }

            var oldPending = await SeedAsync(seed, sourceId, "fire", "old-pending", status: "pending", submittedDaysAgo: ageDays, ct: ct);
            var oldClaimed = await SeedAsync(seed, sourceId, "fire", "old-claimed", status: "claimed", submittedDaysAgo: ageDays, ct: ct);
            var recentDone = await SeedAsync(seed, sourceId, "fire", "recent-done", status: "delivered", submittedDaysAgo: 1, ct: ct);

            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            await DarlingRetention.PurgeAsync(dataSource, timescaleAvailable: false, logger: null, ct);

            var remaining = (await ScalarAsync(seed, "SELECT string_agg(pending_id::text, ',' ORDER BY pending_id) FROM config.external_alert_pending", ct))!;
            Assert.Equal(string.Join(",", new[] { oldPending, oldClaimed, recentDone }.OrderBy(i => i)), remaining);
            Assert.Empty(oldIds.Select(i => i.ToString(CultureInfo.InvariantCulture)).Intersect(remaining.Split(',')));

            /* The registry and the firing state are not trimmed by age. */
            Assert.Equal("1", await ScalarAsync(seed, "SELECT count(*) FROM config.external_alert_source", ct));
        });
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, string, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var seed = new NpgsqlConnection(scratch.ConnectionString);
        await seed.OpenAsync(ct);
        await PgMigrations.MigrateAsync(seed, ct);

        var bodySucceeded = false;
        try
        {
            await body(seed, scratch.ConnectionString, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<int> AddSourceAsync(NpgsqlConnection connection, string name, CancellationToken ct) =>
        Convert.ToInt32(
            await ScalarAsync(connection,
                $"INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('{name}', 'role_{name}') RETURNING source_id", ct),
            CultureInfo.InvariantCulture);

    private static async Task<long> SeedAsync(
        NpgsqlConnection connection, int sourceId, string kind, string key, string severity = "warning",
        int expiresInMinutes = 15, string status = "pending", int submittedDaysAgo = 0, CancellationToken ct = default)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, title, expires_at, status, submitted_at, claimed_at)"
            + " VALUES ($1, $2, $3, $4, 'seeded', now() + make_interval(mins => $5), $6, now() - make_interval(days => $7),"
            + " CASE WHEN $6 = 'claimed' THEN now() ELSE NULL END) RETURNING pending_id", connection)
        {
            CommandTimeout = Deadline,
        };
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sourceId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = kind });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = severity });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = expiresInMinutes });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = status });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = submittedDaysAgo });
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = Deadline };
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = Deadline };
        await command.ExecuteNonQueryAsync(ct);
    }
}
