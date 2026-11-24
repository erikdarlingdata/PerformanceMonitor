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
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #8979: the sightings upsert rewrote every re-sighted row to add to <c>seen_count</c>, tens of millions of heap row versions
/// a day on a busy store. A conflict now writes only when it fills an object name or text sample or the
/// <c>last_seen</c> move is due. #9456 widened that move from an hour to <see cref="QueryTagSightingWriter.ReSightingRefreshWidth"/>. Driven through the real writer (<see cref="QueryTagSightingWriter.RecordAsync"/>) and read back
/// from the table and from <c>pg_stat_user_tables</c>.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to create and drop its own database. */
public sealed class QueryTagSightingsRefreshGuardLiveTests
{
    private const int Server = 7;

    private static readonly string BackdateSql = string.Create(
        CultureInfo.InvariantCulture, $"interval '{(long)QueryTagSightingWriter.ReSightingBackdate.TotalSeconds} seconds'");

    private static string KeyOf(int i) => "0x" + i.ToString("X8", CultureInfo.InvariantCulture);

    private static List<QueryTagSighting> Batch(int count, Func<int, string?> text, Func<int, string?>? objectName = null) =>
        Enumerable.Range(1, count)
            .Select(i => new QueryTagSighting(
                QueryTagKeyKind.Hash,
                KeyOf(i),
                "Sales",
                objectName?.Invoke(i) is { } name ? new QueryTagObject("Sales", "dbo", name) : null,
                "App",
                "svc",
                "H1",
                text(i)))
            .ToList();

    private static Task<int> WriteAsync(NpgsqlDataSource postgres, List<QueryTagSighting> sightings, CancellationToken ct) =>
        QueryTagSightingWriter.RecordAsync(postgres, Server, sightings, null, ct);

    private static async Task<(string Xmin, DateTime LastSeen, long Count, string? ObjectName, string? Text)> RowAsync(NpgsqlConnection c, int i, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT xmin::text, last_seen, seen_count, object_name, text_sample FROM collect.query_tag_sightings WHERE server_id = $1 AND key = $2", c);
        command.Parameters.AddWithValue(Server);
        command.Parameters.AddWithValue(KeyOf(i));
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetString(0), reader.GetDateTime(1), reader.GetInt64(2), reader.IsDBNull(3) ? null : reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetString(4));
    }

    private static async Task<(long Updates, long HotUpdates)> CountersAsync(string connectionString, CancellationToken ct)
    {
        /* A backend reports its counters when it exits, so the writer's data source is disposed before this reads. */
        await using var c = new NpgsqlConnection(connectionString);
        await c.OpenAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE relid = 'collect.query_tag_sightings'::regclass", c);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    [Fact]
    public async Task AnUnchangedReSighting_InsideTheRefreshWidth_WritesNoRow_AndTheCountersShowNoUpdate()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var store = await OpenStoreAsync(scratch, ct);
        var batch = Batch(3000, i => "select " + i, i => i % 2 == 0 ? "Proc" + i : null);

        await using (var first = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            Assert.Equal(3000, await WriteAsync(first, batch, ct));
        }

        var before = await RowAsync(store, 2, ct);
        var countersBefore = await CountersAsync(scratch.ConnectionString, ct);
        await using (var second = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            await WriteAsync(second, batch, ct);
        }

        var after = await RowAsync(store, 2, ct);
        var countersAfter = await CountersAsync(scratch.ConnectionString, ct);
        Assert.True(
            countersAfter.Updates == countersBefore.Updates && countersAfter.HotUpdates == countersBefore.HotUpdates,
            $"second flush: n_tup_upd +{countersAfter.Updates - countersBefore.Updates}, n_tup_hot_upd +{countersAfter.HotUpdates - countersBefore.HotUpdates}");
        Assert.Equal(before, after);
        Assert.Equal(1, after.Count);
        Assert.Equal(3000, await CountAsync(store, "SELECT count(*) FROM collect.query_tag_sightings WHERE seen_count = 1 AND last_seen = first_seen", ct));
    }

    [Fact]
    public async Task AReSighting_ThatFillsAMissingObjectName_OrTextSample_IsWritten_AndAFilledRowIsNot()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var store = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Row 1 lacks an object name, row 2 lacks a text sample, row 3 lacks both, row 4 lacks both and still does, row 5 is complete. */
        await WriteAsync(postgres, Batch(5, i => i is 2 or 3 or 4 ? null : "select " + i, i => i is 1 or 3 or 4 ? null : "Proc" + i), ct);
        await ExecAsync(store, "UPDATE collect.query_tag_sightings SET last_seen = now() - interval '10 minutes'", ct);
        var untouched4 = await RowAsync(store, 4, ct);
        var untouched5 = await RowAsync(store, 5, ct);

        await WriteAsync(postgres, Batch(5, i => i is 4 ? null : "later " + i, i => i is 4 ? null : "Later" + i), ct);

        var one = await RowAsync(store, 1, ct);
        Assert.Equal("Sales.dbo.Later1", one.ObjectName);
        Assert.Equal("select 1", one.Text);
        Assert.Equal(2, one.Count);
        var two = await RowAsync(store, 2, ct);
        Assert.Equal("later 2", two.Text);
        Assert.Equal("Sales.dbo.Proc2", two.ObjectName);
        var three = await RowAsync(store, 3, ct);
        Assert.Equal("later 3", three.Text);
        Assert.Equal("Sales.dbo.Later3", three.ObjectName);
        /* A fill moves last_seen to now(), which the map's changed-keys read relies on. */
        Assert.Equal(3, await CountAsync(store, "SELECT count(*) FROM collect.query_tag_sightings WHERE last_seen > now() - interval '5 minutes'", ct));
        Assert.Equal(untouched4, await RowAsync(store, 4, ct));
        Assert.Equal(untouched5, await RowAsync(store, 5, ct));
    }

    [Fact]
    public async Task ARowPastTheRefreshWidth_GetsItsLastSeenAdvancedOnce_ARowInsideItIsNotWritten_AndLastSeenNeverMovesBack()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var store = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var batch = Batch(6, i => "select " + i);
        await WriteAsync(postgres, batch, ct);

        /* Row 1 is 5 hours 59 minutes inside the 6-hour width, row 2 is 6 hours 1 minute past it, row 3 is a day ahead of the clock,
           row 4 is 3 hours back (the hourly width rewrote it, #9456), row 5 is 61 minutes back (due at the old hourly width). */
        await ExecAsync(store, $"UPDATE collect.query_tag_sightings SET last_seen = now() - {BackdateSql} - interval '5 hours 59 minutes' WHERE key = '{KeyOf(1)}'", ct);
        await ExecAsync(store, $"UPDATE collect.query_tag_sightings SET last_seen = now() - {BackdateSql} - interval '6 hours 1 minute' WHERE key = '{KeyOf(2)}'", ct);
        await ExecAsync(store, $"UPDATE collect.query_tag_sightings SET last_seen = now() + interval '1 day' WHERE key = '{KeyOf(3)}'", ct);
        await ExecAsync(store, $"UPDATE collect.query_tag_sightings SET last_seen = now() - {BackdateSql} - interval '3 hours' WHERE key = '{KeyOf(4)}'", ct);
        await ExecAsync(store, $"UPDATE collect.query_tag_sightings SET last_seen = now() - {BackdateSql} - interval '61 minutes' WHERE key = '{KeyOf(5)}'", ct);
        var inside = await RowAsync(store, 1, ct);
        var ahead = await RowAsync(store, 3, ct);
        var threeHours = await RowAsync(store, 4, ct);
        var oldWidth = await RowAsync(store, 5, ct);

        await WriteAsync(postgres, batch, ct);

        /* A re-sighting inside the width writes nothing: the row keeps its xmin, last_seen and count. */
        Assert.Equal(inside, await RowAsync(store, 1, ct));
        Assert.Equal(ahead, await RowAsync(store, 3, ct));
        Assert.Equal(threeHours, await RowAsync(store, 4, ct));
        Assert.Equal(oldWidth, await RowAsync(store, 5, ct));
        var moved = await RowAsync(store, 2, ct);
        Assert.Equal(2, moved.Count);
        Assert.Equal(1, await CountAsync(
            store,
            $"SELECT count(*) FROM collect.query_tag_sightings WHERE key = '{KeyOf(2)}' AND last_seen <= now() - {BackdateSql} AND last_seen > now() - {BackdateSql} - interval '1 minute'",
            ct));

        /* Moved once: the stamp is fresh now, so the same flush again writes nothing more for the row. */
        var movedAgain = await RowAsync(store, 2, ct);
        await WriteAsync(postgres, batch, ct);
        Assert.Equal(movedAgain, await RowAsync(store, 2, ct));
    }
}
