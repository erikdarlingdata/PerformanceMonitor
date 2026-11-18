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
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #8988: the hourly text fetch rewrote every statement still in the target's view, each time, because the conflict arm set
/// <c>last_seen</c> on every fetch and <c>last_seen</c> is indexed twice (no update of it can be HOT). The upsert now writes a
/// row only when its text or own-entry list changed or its <c>last_seen</c> is past the refresh guard, and the due check reads a
/// per-server fetch marker so the cadence does not depend on <c>last_seen</c>. Driven through the real batch write
/// (<see cref="PgStatementText.WriteFetchAsync"/>) and the real due statement.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to create and drop its own database. */
public sealed class PgStatementTextRefreshGuardLiveTests
{
    private static readonly DateTime T0 = new(2026, 11, 18, 4, 0, 0, DateTimeKind.Unspecified);
    private static readonly TimeSpan Guard = TimeSpan.FromHours(PayloadDimensions.LastSeenRefreshGuardHours);

    private sealed record Batch(List<long> Ids, List<string> Texts, List<string?> Own, List<string?> Candidates)
    {
        public static Batch Of(int count, Func<long, string> text, Func<long, string?>? own = null)
        {
            var ids = Enumerable.Range(1, count).Select(i => (long)i).ToList();
            return new Batch(ids, ids.Select(text).ToList(), ids.Select(i => own?.Invoke(i)).ToList(), ids.Select(_ => (string?)null).ToList());
        }
    }

    private static Task<int> WriteAsync(NpgsqlDataSource postgres, int serverId, Batch batch, DateTime at, CancellationToken ct) =>
        PgStatementText.WriteFetchAsync(postgres, serverId, batch.Ids, batch.Texts, batch.Own, batch.Candidates, at, 120, ct);

    private static async Task<(string Xmin, DateTime LastSeen, string Text, string? Own)> RowAsync(NpgsqlConnection c, int serverId, long queryId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT xmin::text, last_seen, query_text, own_entries::text FROM collect.pg_statement_text WHERE server_id = $1 AND queryid = $2", c);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(queryId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetString(0), reader.GetDateTime(1), reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    private static async Task<bool> IsDueAsync(NpgsqlConnection c, int serverId, DateTime now, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PgStatementText.IsDueSql, c);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(PgStatementText.Naive(now - PgStatementText.RefreshInterval));
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<(long Updates, long HotUpdates)> CountersAsync(string connectionString, CancellationToken ct)
    {
        /* A backend reports its counters when it exits, so the writer's data source is disposed before this reads. */
        await using var c = new NpgsqlConnection(connectionString);
        await c.OpenAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE relid = 'collect.pg_statement_text'::regclass", c);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    [Fact]
    public async Task AnUnchangedRefetch_InsideTheWindow_WritesNoRow_AndTheCountersShowNoUpdate()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var store = await OpenStoreAsync(scratch, ct);
        var batch = Batch.Of(3000, i => "select " + i, i => i % 2 == 0 ? "[[10,20]]" : null);

        int firstWritten;
        await using (var first = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            firstWritten = await WriteAsync(first, 7, batch, T0, ct);
        }

        var before = await RowAsync(store, 7, 2, ct);
        var countersBefore = await CountersAsync(scratch.ConnectionString, ct);
        int secondWritten;
        await using (var second = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            secondWritten = await WriteAsync(second, 7, batch, T0 + TimeSpan.FromHours(1), ct);
        }

        var after = await RowAsync(store, 7, 2, ct);
        var countersAfter = await CountersAsync(scratch.ConnectionString, ct);
        Assert.True(
            countersAfter.Updates == countersBefore.Updates && countersAfter.HotUpdates == countersBefore.HotUpdates,
            $"second cycle: n_tup_upd +{countersAfter.Updates - countersBefore.Updates}, n_tup_hot_upd +{countersAfter.HotUpdates - countersBefore.HotUpdates}");
        Assert.Equal(before, after);
        Assert.Equal(T0, after.LastSeen);
        Assert.Equal(0, await CountAsync(store, "SELECT count(*) FROM collect.pg_statement_text WHERE last_seen <> timestamp '2026-11-18 04:00:00'", ct));

        /* The return value is the rows the upsert wrote: every new row, then none for the unchanged refetch, then the one
           row whose text changed. */
        Assert.Equal(3000, firstWritten);
        Assert.Equal(0, secondWritten);
        var oneChanged = Batch.Of(3000, i => i is 5 ? "select changed" : "select " + i, i => i % 2 == 0 ? "[[10,20]]" : null);
        await using var third = NpgsqlDataSource.Create(scratch.ConnectionString);
        Assert.Equal(1, await WriteAsync(third, 7, oneChanged, T0 + TimeSpan.FromHours(2), ct));
    }

    [Fact]
    public async Task AChangedText_AndEachKindOfChangedOwnEntryList_AreWritten_AndTheUnchangedRowsAreNot()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var store = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Row 1 and 6 stay as they are, row 2's text changes, row 3 gains an own entry (NULL before), row 4 loses its entry
           (NULL after), row 5 swaps one entry for another. */
        var first = Batch.Of(6, i => "select " + i, i => i is 4 ? "[[1,2]]" : i is 5 ? "[[3,4]]" : null);
        await WriteAsync(postgres, 7, first, T0, ct);
        var untouched = await RowAsync(store, 7, 1, ct);
        var control = await RowAsync(store, 7, 6, ct);

        var later = T0 + TimeSpan.FromHours(1);
        var second = Batch.Of(6, i => i is 2 ? "select changed" : "select " + i, i => i is 3 ? "[[5,6]]" : i is 5 ? "[[7,8]]" : null);
        await WriteAsync(postgres, 7, second, later, ct);

        Assert.Equal(untouched, await RowAsync(store, 7, 1, ct));
        Assert.Equal(control, await RowAsync(store, 7, 6, ct));

        var changedText = await RowAsync(store, 7, 2, ct);
        Assert.Equal("select changed", changedText.Text);
        Assert.Equal(later, changedText.LastSeen);

        var gained = await RowAsync(store, 7, 3, ct);
        Assert.Equal("[[5, 6]]", gained.Own);
        Assert.Equal(later, gained.LastSeen);

        var lost = await RowAsync(store, 7, 4, ct);
        Assert.Null(lost.Own);
        Assert.Equal(later, lost.LastSeen);

        var swapped = await RowAsync(store, 7, 5, ct);
        Assert.Equal("[[7, 8]]", swapped.Own);
        Assert.Equal(later, swapped.LastSeen);
    }

    [Fact]
    public async Task ARowPastTheRefreshWindow_GetsItsLastSeenAdvanced_ARowInsideItDoesNot_AndAnOlderStampNeverMovesItBack()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var store = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var batch = Batch.Of(10, i => "select " + i);
        await WriteAsync(postgres, 7, batch, T0, ct);

        await WriteAsync(postgres, 7, batch, T0 + Guard - TimeSpan.FromMinutes(1), ct);
        Assert.Equal(T0, (await RowAsync(store, 7, 3, ct)).LastSeen);

        var past = T0 + Guard + TimeSpan.FromMinutes(1);
        await WriteAsync(postgres, 7, batch, past, ct);
        Assert.Equal(past, (await RowAsync(store, 7, 3, ct)).LastSeen);

        /* A retried batch from earlier never moves a row backwards. */
        await WriteAsync(postgres, 7, batch, T0, ct);
        Assert.Equal(past, (await RowAsync(store, 7, 3, ct)).LastSeen);
    }

    [Fact]
    public async Task TheDueCheck_FollowsTheFetchMarker_NotLastSeen()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var store = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var batch = Batch.Of(5, i => "select " + i);

        await WriteAsync(postgres, 7, batch, T0, ct);
        Assert.False(await IsDueAsync(store, 7, T0 + TimeSpan.FromMinutes(59), ct));
        Assert.True(await IsDueAsync(store, 7, T0 + TimeSpan.FromMinutes(61), ct));

        /* The second fetch leaves every last_seen at T0, so newest last_seen says the last fetch was 70 minutes ago. The marker says 20. */
        var secondAt = T0 + TimeSpan.FromMinutes(50);
        await WriteAsync(postgres, 7, batch, secondAt, ct);
        Assert.Equal(T0, (await RowAsync(store, 7, 1, ct)).LastSeen);
        Assert.False(await IsDueAsync(store, 7, T0 + TimeSpan.FromMinutes(70), ct));
        Assert.False(await IsDueAsync(store, 7, secondAt + TimeSpan.FromMinutes(59), ct));
        Assert.True(await IsDueAsync(store, 7, secondAt + TimeSpan.FromMinutes(61), ct));

        /* One server's marker says nothing about another's. */
        Assert.True(await IsDueAsync(store, 8, T0 + TimeSpan.FromMinutes(1), ct));
    }

    [Fact]
    public async Task WithNoMarker_TheDueCheckFallsBackToTheNewestLastSeen()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var store = await OpenStoreAsync(scratch, ct);

        /* A store from before the marker: rows, and no collector_state row. */
        await ExecAsync(store, @"
INSERT INTO collect.pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
VALUES (7, 1, 'select 1', timestamp '2026-11-18 03:00:00', timestamp '2026-11-18 03:00:00'),
       (7, 2, 'select 2', timestamp '2026-11-18 03:00:00', timestamp '2026-11-18 04:00:00')", ct);
        Assert.Equal(0, await CountAsync(store, "SELECT count(*) FROM collect.collector_state WHERE collector_name = '" + PgStatementText.CollectorName + "'", ct));

        Assert.False(await IsDueAsync(store, 7, T0 + TimeSpan.FromMinutes(59), ct));
        Assert.True(await IsDueAsync(store, 7, T0 + TimeSpan.FromMinutes(61), ct));
        /* No rows and no marker: the first fetch. */
        Assert.True(await IsDueAsync(store, 8, T0, ct));
    }

    [Fact]
    public async Task AFetchWhoseUpsertFails_LeavesTheOldMarker()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var store = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var batch = Batch.Of(3, i => "select " + i);
        await WriteAsync(postgres, 7, batch, T0, ct);

        /* A NULL text breaks the NOT NULL column, so the upsert throws before the marker is written. */
        var broken = new Batch(batch.Ids, batch.Texts.Select(_ => (string)null!).ToList(), batch.Own, batch.Candidates);
        await Assert.ThrowsAnyAsync<Exception>(() => WriteAsync(postgres, 7, broken, T0 + TimeSpan.FromMinutes(50), ct));

        Assert.True(await IsDueAsync(store, 7, T0 + TimeSpan.FromMinutes(61), ct));
    }
}
