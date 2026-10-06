/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5245 (part of #5244), PR6 lane R2: get_database_config, get_health_parser_severe_errors and get_default_trace_events
/// read a SET of databases. Each tool gains an internal overload that takes a <see cref="DatabaseFilter"/>; the public method
/// keeps passing one name (config) or no name (the other two). The seed is three databases (A, B and C) on one server:
/// [A, B] returns only A and B, in each read's own filter (C# over the latest snapshot with OrdinalIgnoreCase for config,
/// C# over the resolved name for severe errors, SQL for the default trace), and each empty or truncated answer says
/// "the chosen databases" rather than a verdict about one database.
///
/// <para>Skips without <c>DARLING_TEST_PG</c>, like its siblings.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class EventsDatabaseFilterLiveTests
{
    private const string ServerName = "darling-events-dbfilter-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string DbA = "FilterDbA";
    private const string DbB = "FilterDbB";
    private const string DbC = "FilterDbC";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void DefaultTraceSql_IsTheListForm_WithNoNameSplicedIn()
    {
        var sql = DarlingDefaultTraceReader.EventsByWindowSql;
        Assert.Contains("($5::text[] IS NULL OR dte.database_name = ANY($5))", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$5::text IS NULL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("{{", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FilterToDatabases_ListMatchesAnyName_IgnoringCase_AndAllKeepsEveryRow()
    {
        DarlingCurrentConfigReader.DatabaseConfigReadRow Row(string name) => new(
            name, "ONLINE", 160, "c", "FULL", false, false, false, false, false, false, false, "OFF", false, false, false, false,
            false, false, false, false, "NOTHING", "CHECKSUM", 60, "DISABLED", false, false, null);
        var rows = new[] { Row(DbA), Row(DbB), Row(DbC) };

        Assert.Equal(new[] { DbA, DbB }, DarlingCurrentConfigReader.FilterToDatabases(rows, DatabaseFilter.Of([DbA, DbB])).Select(r => r.DatabaseName));
        Assert.Equal(new[] { DbC }, DarlingCurrentConfigReader.FilterToDatabases(rows, DatabaseFilter.One(DbC.ToUpperInvariant())).Select(r => r.DatabaseName));
        Assert.Equal(3, DarlingCurrentConfigReader.FilterToDatabases(rows, DatabaseFilter.All).Count);
        Assert.Equal(3, DarlingCurrentConfigReader.FilterToDatabases(rows, DatabaseFilter.One("   ")).Count);
        Assert.Empty(DarlingCurrentConfigReader.FilterToDatabases(rows, DatabaseFilter.Of(["NoSuchDb"])));
    }

    [Fact]
    public async Task DatabaseConfig_Overload_TwoNames_ReturnsOnlyThoseDatabases_WhitespaceMeansAll_NoMatchIsNotUnavailable()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live events database-filter test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, ct);

            var ab = JsonDocument.Parse(await DarlingMcpConfigTools.GetDatabaseConfig(postgres, ServerName, DatabaseFilter.Of([DbA, DbB]), ct)).RootElement;
            Assert.Equal(2, ab.GetProperty("database_count").GetInt32());
            Assert.Equal(new[] { DbA, DbB }, ab.GetProperty("databases").EnumerateArray().Select(d => d.GetProperty("database_name").GetString()).Order().ToArray());

            /* One name behaves as it did, ignoring case, and the public method that wraps it returns the same page. */
            var one = JsonDocument.Parse(await DarlingMcpConfigTools.GetDatabaseConfig(postgres, ServerName, DatabaseFilter.One(DbC.ToLowerInvariant()), ct)).RootElement;
            Assert.Equal(new[] { DbC }, one.GetProperty("databases").EnumerateArray().Select(d => d.GetProperty("database_name").GetString()).ToArray());
            var viaPublic = JsonDocument.Parse(await DarlingMcpConfigTools.GetDatabaseConfig(postgres, ServerName, DbC, ct)).RootElement;
            Assert.Equal(1, viaPublic.GetProperty("database_count").GetInt32());

            /* [M3] No name and a whitespace-only name are every database (a whitespace-only name used to filter to nothing). */
            foreach (var blank in new string?[] { null, "   " })
            {
                var all = JsonDocument.Parse(await DarlingMcpConfigTools.GetDatabaseConfig(postgres, ServerName, blank, ct)).RootElement;
                Assert.Equal(3, all.GetProperty("database_count").GetInt32());
            }

            /* A set that matches nothing is a quiet answer from a snapshot that exists, with its capture stamp: never "unavailable". */
            var none = JsonDocument.Parse(await DarlingMcpConfigTools.GetDatabaseConfig(postgres, ServerName, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), ct)).RootElement;
            Assert.False(none.TryGetProperty("status", out _));
            Assert.Equal(0, none.GetProperty("database_count").GetInt32());
            Assert.True(none.TryGetProperty("captured_at", out _));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task SevereErrors_Overload_TwoNames_FiltersOnTheResolvedName_BeforeTheCountsAndThePageCap()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live events database-filter test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, ct);

            var all = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName, 24, 50, DatabaseFilter.All, null, null, ct)).RootElement;
            Assert.Equal(4, all.GetProperty("error_count").GetInt32());

            var ab = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName, 24, 50, DatabaseFilter.Of([DbA, DbB]), null, null, ct)).RootElement;
            Assert.Equal(2, ab.GetProperty("error_count").GetInt32());
            Assert.Equal(new[] { DbA, DbB }, ab.GetProperty("errors").EnumerateArray().Select(e => e.GetProperty("database_name").GetString()).Order().ToArray());

            /* The count is the set's, and the page cap applies after the filter: one shown of the two held. */
            var cut = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName, 24, 1, DatabaseFilter.Of([DbA, DbB]), null, null, ct)).RootElement;
            Assert.Equal(2, cut.GetProperty("error_count").GetInt32());
            Assert.Equal(1, cut.GetProperty("shown").GetInt32());

            /* One name, ordinal: a differently cased name matches nothing, and the no-context error (id 0) is in no chosen database. */
            var oneC = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName, 24, 50, DatabaseFilter.One(DbC), null, null, ct)).RootElement;
            Assert.Equal(1, oneC.GetProperty("error_count").GetInt32());
            var lower = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName, 24, 50, DatabaseFilter.One(DbC.ToLowerInvariant()), null, null, ct)).RootElement;
            Assert.Equal("empty", lower.GetProperty("status").GetString());

            /* [M4] Nothing in the chosen databases while errors WERE captured: the empty answer says so, and no database is named as a verdict. */
            var none = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName, 24, 50, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), null, null, ct)).RootElement;
            Assert.Equal("empty", none.GetProperty("status").GetString());
            var message = none.GetProperty("message").GetString()!;
            Assert.Contains("none was a significant severe error", message, StringComparison.Ordinal);
            Assert.Contains("in the chosen databases", message, StringComparison.Ordinal);
            Assert.DoesNotContain("NoSuchDb", message, StringComparison.Ordinal);
            Assert.Equal(4, none.GetProperty("events_in_window").GetInt32());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task DefaultTrace_Overload_TwoNames_ReturnsOnlyThoseDatabases_AndTheEmptyAnswerSaysTheChosenDatabases()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live events database-filter test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, ct);
            var end = DateTime.UtcNow;
            var start = end.AddHours(-1);

            /* Reader level: the list reaches SQL as one text[]; an event with no database name is in no chosen database. */
            var ab = await DarlingDefaultTraceReader.ReadEventsAsync(postgres, ServerId, start, end, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { DbA, DbB }, ab.Select(r => r.DatabaseName).Order().ToArray());
            var ba = await DarlingDefaultTraceReader.ReadEventsAsync(postgres, ServerId, start, end, DatabaseFilter.Of([DbB, DbA]), ct);
            Assert.Equal(ab.Select(r => r.TextData), ba.Select(r => r.TextData));
            Assert.Equal(4, (await DarlingDefaultTraceReader.ReadEventsAsync(postgres, ServerId, start, end, DatabaseFilter.All, ct)).Count);
            Assert.Equal(4, (await DarlingDefaultTraceReader.ReadEventsAsync(postgres, ServerId, start, end, ct)).Count);
            Assert.Empty(await DarlingDefaultTraceReader.ReadEventsAsync(postgres, ServerId, start, end, DatabaseFilter.Of(["NoSuchDb", DbA.ToLowerInvariant()]), ct));

            /* Tool level: total_events is the set's, and the page limit applies after the filter. */
            var abTool = JsonDocument.Parse(await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, ServerName, 24, 100, DatabaseFilter.Of([DbA, DbB]), null, null, ct)).RootElement;
            Assert.Equal(2, abTool.GetProperty("total_events").GetInt32());
            var cut = JsonDocument.Parse(await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, ServerName, 24, 1, DatabaseFilter.Of([DbA, DbB]), null, null, ct)).RootElement;
            Assert.Equal(2, cut.GetProperty("total_events").GetInt32());
            Assert.Equal(1, cut.GetProperty("shown").GetInt32());
            var allTool = JsonDocument.Parse(await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, ServerName, 24, 100, DatabaseFilter.All, null, null, ct)).RootElement;
            Assert.Equal(4, allTool.GetProperty("total_events").GetInt32());

            /* [M4] Nothing in the chosen databases: empty, for the chosen databases; with no filter, the sentence it always was. */
            var none = JsonDocument.Parse(await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, ServerName, 24, 100, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), null, null, ct)).RootElement;
            Assert.Equal("empty", none.GetProperty("status").GetString());
            var message = none.GetProperty("message").GetString()!;
            Assert.Equal("No significant default trace events found in the requested time range for the chosen databases.", message);
            Assert.DoesNotContain("NoSuchDb", message, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static string ErrorReportedXml(int errorNumber, int databaseId) =>
        $"<event name=\"error_reported\" package=\"sqlserver\" timestamp=\"2026-09-01T00:00:00.000Z\">" +
        $"<data name=\"error_number\"><value>{errorNumber}</value></data>" +
        "<data name=\"severity\"><value>20</value></data>" +
        "<data name=\"state\"><value>1</value></data>" +
        "<data name=\"message\"><value>db-filter probe</value></data>" +
        $"<action name=\"database_id\"><value>{databaseId}</value></action>" +
        "</event>";

    /// <summary>
    /// Seeds one server with three databases (A, B and C): a latest database_config snapshot, a database-id map
    /// (11, 12 and 13), four severe errors (one per database plus one with no database context), and four significant
    /// default-trace ErrorLog rows (one per database plus one with no database name).
    /// </summary>
    private static async Task SeedAsync(string cs, CancellationToken ct)
    {
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

        foreach (var db in new[] { DbA, DbB, DbC })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level, collation_name, recovery_model,
    is_read_only, is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on,
    is_read_committed_snapshot_on, snapshot_isolation_state, is_parameterization_forced, is_query_store_on, is_encrypted, is_trustworthy_on,
    is_db_chaining_on, is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on, log_reuse_wait_desc, page_verify_option,
    target_recovery_time_seconds, delayed_durability, is_accelerated_database_recovery_on, is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, db, "ONLINE", 160, "SQL_Latin1_General_CP1_CI_AS", "FULL",
                false, false, false, true, true, false,
                true, "OFF", false, true, false, false,
                false, true, false, false, "NOTHING", "CHECKSUM",
                60, "DISABLED", false, false, false);
        }

        var ids = new[] { (DbA, 11), (DbB, 12), (DbC, 13) };
        foreach (var (db, id) in ids)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id)
VALUES ($1,$2,$3,$4,$5,$6)", CollectionIdGenerator.Next(), t, ServerId, ServerName, db, id);
        }

        var errorSeeds = new[] { (50011, 11), (50012, 12), (50013, 13), (50014, 0) };
        foreach (var (number, id) in errorSeeds)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, t, SystemHealthParser.ErrorReportedEvent, ErrorReportedXml(number, id));
        }

        foreach (var db in new string?[] { DbA, DbB, DbC, null })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO default_trace_events
(default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, database_name, severity, text_data)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, t, "ErrorLog", (object?)db ?? DBNull.Value, 20, "marker-" + (db ?? "none"));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql = $"DELETE FROM default_trace_events WHERE server_id = {ServerId};" +
                  $" DELETE FROM system_health_events WHERE server_id = {ServerId};" +
                  $" DELETE FROM database_size_stats WHERE server_id = {ServerId};" +
                  $" DELETE FROM database_config WHERE server_id = {ServerId};" +
                  $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
