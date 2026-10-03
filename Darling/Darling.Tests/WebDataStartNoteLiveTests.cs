/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The server page's Waiting Tasks grid over a store (#4966): a server added two days ago, read over 7 days, answers
/// with where its data starts; a server collected for a month whose first waiting task comes late in the window
/// answers with nothing, because the store covered the whole window. The answer is the tool's own payload, so the
/// notice is added to rows the tool really returned.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class WebDataStartNoteLiveTests
{
    private const int NewServerId = -496601;
    private const string NewServerName = "web-data-start-added-two-days-ago";
    private const int QuietServerId = -496602;
    private const string QuietServerName = "web-data-start-quiet-start";

    [Fact]
    public async Task AServerAddedTwoDaysAgo_GetsANoteThatNamesItsFirstCollection_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var added = store.End.AddDays(-2);
        await store.SeedAsync(NewServerId, NewServerName, added, firstRow: store.End.AddDays(-1), ct);

        var answer = await store.AskAsync(NewServerName, hours: 168, ct);

        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        var effectiveStart = DateTime.Parse(answer["effective_start"]!.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        Assert.True(Math.Abs((effectiveStart - added).TotalSeconds) < 1, "the data starts at the server's first collection, not its first row");
        var note = answer["truncation_note"]!.GetValue<string>();
        Assert.StartsWith("partial window:", note, StringComparison.Ordinal);
        Assert.Contains(added.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC", note, StringComparison.Ordinal);
        Assert.NotNull(answer["tasks"]);
    }

    [Fact]
    public async Task AWindowTheStoreCovered_GetsNoNote_WhenTheFirstRowComesLate_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        await store.SeedAsync(QuietServerId, QuietServerName, store.End.AddDays(-30), firstRow: store.End.AddDays(-6), ct);

        var week = await store.AskAsync(QuietServerName, hours: 168, ct);
        var threeDays = await store.AskAsync(QuietServerName, hours: 72, ct);

        Assert.Null(week["window_truncated"]);
        Assert.Null(week["truncation_note"]);
        Assert.NotNull(week["tasks"]);
        Assert.Null(threeDays["window_truncated"]);
    }

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private Store(ScratchPostgres scratch, NpgsqlDataSource dataSource, DateTime end)
        {
            _scratch = scratch;
            DataSource = dataSource;
            End = end;
        }

        public NpgsqlDataSource DataSource { get; }

        /// <summary>The minute the seeded history ends at, naive UTC.</summary>
        public DateTime End { get; }

        public static async Task<Store> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);

                var now = DateTime.UtcNow;
                var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                return new Store(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /// <summary>A server first collected at <paramref name="added"/> whose waiting tasks run from
        /// <paramref name="firstRow"/> to the end, one every 30 minutes; the collector's runs are logged from the first
        /// collection, whether or not anything waited.</summary>
        public async Task SeedAsync(int serverId, string serverName, DateTime added, DateTime firstRow, CancellationToken ct)
        {
            await using var connection = await DataSource.OpenConnectionAsync(ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);

            await using (var update = new NpgsqlCommand("UPDATE collect.servers SET created_date = $2 WHERE server_id = $1", connection))
            {
                update.Parameters.AddWithValue(serverId);
                update.Parameters.AddWithValue(DateTime.SpecifyKind(added, DateTimeKind.Unspecified));
                await update.ExecuteNonQueryAsync(ct);
            }

            await using (var log = new NpgsqlCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT row_number() OVER (), $1, $2, 'waiting_tasks', t, 12, 'SUCCESS', 0
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection))
            {
                log.Parameters.AddWithValue(serverId);
                log.Parameters.AddWithValue(serverName);
                log.Parameters.AddWithValue(DateTime.SpecifyKind(added, DateTimeKind.Unspecified));
                log.Parameters.AddWithValue(DateTime.SpecifyKind(End, DateTimeKind.Unspecified));
                await log.ExecuteNonQueryAsync(ct);
            }

            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.waiting_tasks (collection_id, collection_time, server_id, server_name, wait_type, wait_duration_ms, database_name)
SELECT row_number() OVER (), t, $1, $2, 'LCK_M_X', 250, 'WebDb'
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstRow, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(End, DateTimeKind.Unspecified));
            await insert.ExecuteNonQueryAsync(ct);
        }

        /// <summary>What the web mirror answers for the grid: the tool's own payload, then the data-start note.</summary>
        public async Task<JsonObject> AskAsync(string server, int hours, CancellationToken ct)
        {
            var payload = await DarlingMcpSessionTools.GetWaitingTasks(DataSource, server, hours, 30, null, ct);
            var answered = await WebDataStartNote.AddAsync(DataSource, "get_waiting_tasks", server, hours, null, payload, null, ct);
            return Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        }

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
