/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5228: the three row-keyed plan reads the web grids call (get_active_query_plan_xml,
/// get_query_store_plan_xml, get_procedure_plan_xml) return the EXACT stored XML for the row's own key and
/// answer <c>unavailable</c> on a key that matches nothing. The Active Queries read is keyed by the row's
/// natural key, so its collection_time must survive the trip through <c>ToString("o")</c> with the
/// microseconds Postgres keeps, and a snapshot row collected with no request_id must answer to request_id 0.
/// Query Store plans are seeded through the inline <c>query_plan_text</c> column. Fixed anchors only.
/// </summary>
[Collection("live-postgres")]
public sealed class RowPlanReadLivePostgresTests
{
    private const string ServerName = "darling-row-plan-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>A second registered server for the cross-server proof. Its name is not a substring of
    /// <see cref="ServerName"/> (or the reverse), so neither can resolve as the other's partial match.</summary>
    private const string OtherServerName = "darling-twin-plan-e2e";
    private static readonly int OtherServerId = ServerIdHelper.GetDeterministicHashCode(OtherServerName);

    private const string Db = "RowPlanDb";
    private const string SqlHandle = "0xA0B1C2D3E4F50617";

    /// <summary>Fixed anchor with a non-zero microsecond part (123456 us), naive UTC as the store keeps it.</summary>
    private static readonly DateTime Anchor = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Unspecified).AddTicks(1_234_560);

    private static string PlanXml(string marker) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\">" +
        $"<BatchSequence><Batch><Statements><StmtSimple StatementText=\"SELECT '{marker}'\" StatementId=\"1\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }

    /// <summary>The answer's message, decoded (the JSON writer escapes an apostrophe, so a raw substring test would miss one).</summary>
    private static string MessageOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("message").GetString()!;
    }

    [Fact]
    public async Task EachRowPlanTool_ReturnsTheExactStoredXml_AndUnavailableOnAWrongKey()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live row-plan test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            var estimated = PlanXml("estimated");
            var live = PlanXml("live");
            var noRequest = PlanXml("no-request-id");
            var queryStore = PlanXml("query-store");
            var procedure = PlanXml("procedure");

            await InsertSnapshotAsync(connection, Anchor, 51, 3, estimated, live, ct);
            await InsertSnapshotAsync(connection, Anchor, 52, null, noRequest, null, ct);
            await InsertQueryStoreAsync(connection, Anchor, 4242, 7, queryStore, ct);
            await InsertProcedureAsync(connection, Anchor, procedure, ct);

            /* ---- get_active_query_plan_xml: the collection_time the grid hands back is Anchor.ToString("o"). */
            var key = Anchor.ToString("o", CultureInfo.InvariantCulture);
            Assert.Equal(estimated, await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 51, ServerName, 3, cancellationToken: ct));
            Assert.Equal(live, await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 51, ServerName, 3, live: true, cancellationToken: ct));

            /* The microseconds are the key: the same instant rounded to the second matches nothing. */
            var rounded = Anchor.AddTicks(-(Anchor.Ticks % TimeSpan.TicksPerSecond)).ToString("o", CultureInfo.InvariantCulture);
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, rounded, 51, ServerName, 3, cancellationToken: ct)));

            /* A row collected with a NULL request_id answers to request_id 0 (the grids carry it as 0). */
            Assert.Equal(noRequest, await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 52, ServerName, 0, cancellationToken: ct));
            Assert.Equal(noRequest, await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 52, ServerName, cancellationToken: ct));

            /* Wrong keys, and a row with no live plan, are unavailable. */
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 999, ServerName, 3, cancellationToken: ct)));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 51, ServerName, 4, cancellationToken: ct)));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 52, ServerName, 0, live: true, cancellationToken: ct)));

            /* ---- get_query_store_plan_xml, through the inline query_plan_text column. */
            Assert.Equal(queryStore, await DarlingMcpPlanTools.GetQueryStorePlanXml(postgres, Db, 4242, ServerName, cancellationToken: ct));
            Assert.Equal(queryStore, await DarlingMcpPlanTools.GetQueryStorePlanXml(postgres, Db, 4242, ServerName, plan_id: 7, cancellationToken: ct));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetQueryStorePlanXml(postgres, Db, 9999, ServerName, cancellationToken: ct)));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetQueryStorePlanXml(postgres, Db, 4242, ServerName, plan_id: 8, cancellationToken: ct)));

            /* ---- get_procedure_plan_xml. */
            Assert.Equal(procedure, await DarlingMcpPlanTools.GetProcedurePlanXml(postgres, SqlHandle, ServerName, cancellationToken: ct));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetProcedurePlanXml(postgres, "0xDEADBEEF", ServerName, cancellationToken: ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task RegisterServerAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct) =>
        RegisterServerAsync(connection, ServerId, ServerName, ct);

    private static async Task RegisterServerAsync(
        NpgsqlConnection connection, int serverId, string serverName, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name,
    is_enabled = TRUE, modified_date = EXCLUDED.modified_date;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(Anchor, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertSnapshotAsync(
        NpgsqlConnection connection, DateTime collectionTime, int sessionId, int? requestId, string? plan, string? livePlan,
        System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, request_id, query_plan, live_query_plan)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(sessionId);
        command.Parameters.AddWithValue((object?)requestId ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)plan ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)livePlan ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertQueryStoreAsync(
        NpgsqlConnection connection, DateTime collectionTime, long queryId, long planId, string planText,
        System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_plan_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue(planText);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertProcedureAsync(
        NpgsqlConnection connection, DateTime collectionTime, string planXml, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle, query_plan_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue("dbo");
        command.Parameters.AddWithValue("usp_RowPlan");
        command.Parameters.AddWithValue(SqlHandle);
        command.Parameters.AddWithValue(planXml);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// #5236: the blocking and deadlock plan reads fetch the plan of the row the LIST printed, by the strings the list
    /// emitted (as_of is set, so the window is fixed). Blocked-process reports: one with both plans, one with the blocked plan
    /// only, one with none, and one whose plan is an empty string (the flag and the read must agree it is absent). Two
    /// deadlocks share BOTH stamps and differ in victim, so victim_process_id picks the right twin; a third has no plan and a
    /// fourth an empty one. The flags are written only when true. The event_time carries microseconds, and the key is matched
    /// exactly. The DMV-snapshot arm has no plan columns and is covered by the reader mapping test.
    /// </summary>
    [Fact]
    public async Task BlockingAndDeadlockRows_FetchTheirOwnPlans_ByTheKeysTheListReadsEmit()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live row-plan test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            var bothBlocked = PlanXml("both-blocked");
            var bothBlocking = PlanXml("both-blocking");
            var onlyBlocked = PlanXml("only-blocked");
            var victimOne = PlanXml("victim-one");
            var victimTwo = PlanXml("victim-two");

            await InsertReportAsync(connection, Anchor, 51, 52, bothBlocked, bothBlocking, ct);
            await InsertReportAsync(connection, Anchor.AddMinutes(1), 53, 54, onlyBlocked, null, ct);
            await InsertReportAsync(connection, Anchor.AddMinutes(2), 55, 56, null, null, ct);
            await InsertReportAsync(connection, Anchor.AddMinutes(3), 57, 58, "", "", ct);

            var twinCollected = Anchor.AddMinutes(10);
            var twinStamp = twinCollected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, twinCollected, twinStamp, "process1a2b", victimOne, ct);
            await InsertDeadlockAsync(connection, twinCollected, twinStamp, "process3c4d", victimTwo, ct);
            await InsertDeadlockAsync(connection, Anchor.AddMinutes(20), Anchor.AddMinutes(20).AddSeconds(-7), "process5e6f", null, ct);
            await InsertDeadlockAsync(connection, Anchor.AddMinutes(30), Anchor.AddMinutes(30).AddSeconds(-7), "process7a8b", "", ct);

            var asOf = Anchor.AddHours(1).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

            /* ---- blocking: the list's own strings feed the tool. */
            using var blockingList = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(
                postgres, ServerName, hours_back: 24, as_of: asOf, cancellationToken: ct));
            var events = blockingList.RootElement.GetProperty("events");

            JsonElement Event(int blockedSpid) => events.EnumerateArray().Single(e => e.GetProperty("blocked_spid").GetInt32() == blockedSpid);

            /* The row's own strings, its database_name included (the web sends the row's database when it has one). */
            async Task<string> BlockingPlan(JsonElement e, string? side = null) => await DarlingMcpPlanTools.GetBlockingPlanXml(
                postgres, e.GetProperty("event_time").GetString()!, e.GetProperty("blocked_spid").GetInt32(), e.GetProperty("blocking_spid").GetInt32(),
                ServerName, e.GetProperty("blocked_ecid").GetInt32(), e.GetProperty("blocking_ecid").GetInt32(), side ?? "blocked", DatabaseOf(e), ct);

            var both = Event(51);
            Assert.True(both.GetProperty("has_blocked_plan").GetBoolean());
            Assert.True(both.GetProperty("has_blocking_plan").GetBoolean());
            Assert.Equal(bothBlocked, await BlockingPlan(both));
            Assert.Equal(bothBlocking, await BlockingPlan(both, "blocking"));
            Assert.Equal(bothBlocking, await BlockingPlan(both, "BLOCKING"));

            /* The microseconds are the key: the same instant rounded to the second matches nothing. */
            var rounded = Anchor.AddTicks(-(Anchor.Ticks % TimeSpan.TicksPerSecond)).ToString("o", CultureInfo.InvariantCulture);
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetBlockingPlanXml(postgres, rounded, 51, 52, ServerName, cancellationToken: ct)));

            var blockedOnly = Event(53);
            Assert.True(blockedOnly.GetProperty("has_blocked_plan").GetBoolean());
            Assert.False(blockedOnly.TryGetProperty("has_blocking_plan", out _));
            Assert.Equal(onlyBlocked, await BlockingPlan(blockedOnly));
            Assert.Equal("unavailable", StatusOf(await BlockingPlan(blockedOnly, "blocking")));

            /* No plan, and an empty-string plan: neither flag, and the reads agree. */
            foreach (var spid in new[] { 55, 57 })
            {
                var none = Event(spid);
                Assert.False(none.TryGetProperty("has_blocked_plan", out _));
                Assert.False(none.TryGetProperty("has_blocking_plan", out _));
                Assert.Equal("unavailable", StatusOf(await BlockingPlan(none)));
                Assert.Equal("unavailable", StatusOf(await BlockingPlan(none, "blocking")));
            }

            /* ---- deadlocks: twins share both stamps, so the victim picks the plan. */
            using var deadlockList = JsonDocument.Parse(await DarlingMcpBlockingTools.GetDeadlocks(
                postgres, ServerName, hours_back: 24, as_of: asOf, cancellationToken: ct));
            var deadlocks = deadlockList.RootElement.GetProperty("deadlocks");

            JsonElement Deadlock(string victim) => deadlocks.EnumerateArray().Single(d => d.GetProperty("victim_process_id").GetString() == victim);

            async Task<string> VictimPlan(JsonElement d, string? victim) => await DarlingMcpPlanTools.GetDeadlockPlanXml(
                postgres, d.GetProperty("collection_time").GetString()!, d.GetProperty("deadlock_time").GetString()!, ServerName, victim, DatabaseOf(d), ct);

            var one = Deadlock("process1a2b");
            var two = Deadlock("process3c4d");
            Assert.Equal(one.GetProperty("collection_time").GetString(), two.GetProperty("collection_time").GetString());
            Assert.Equal(one.GetProperty("deadlock_time").GetString(), two.GetProperty("deadlock_time").GetString());
            Assert.True(one.GetProperty("has_victim_plan").GetBoolean());
            Assert.True(two.GetProperty("has_victim_plan").GetBoolean());
            Assert.Equal(victimOne, await VictimPlan(one, "process1a2b"));
            Assert.Equal(victimTwo, await VictimPlan(two, "process3c4d"));
            Assert.Equal("unavailable", StatusOf(await VictimPlan(one, "process-nobody")));

            /* Without the tiebreak the stamps cannot say which twin was meant, so the read picks neither: it refuses, and the
               refusal names victim_process_id (the new test below pins the whole shape). */
            var untied = await VictimPlan(one, null);
            Assert.Equal("invalid", StatusOf(untied));
            Assert.Contains("victim_process_id", untied, StringComparison.Ordinal);

            foreach (var victim in new[] { "process5e6f", "process7a8b" })
            {
                var none = Deadlock(victim);
                Assert.False(none.TryGetProperty("has_victim_plan", out _));
                Assert.Equal("unavailable", StatusOf(await VictimPlan(none, victim)));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #5236: a plan read for one server never returns another server's plan. A second server holds a blocked-process report
    /// with the same event_time, spids and ecids as the first server's, collected a minute EARLIER (so it is the earliest copy
    /// of that key), and a deadlock with the same stamps and the same victim, inserted FIRST (so it has the lower deadlock_id).
    /// Each server's reads return its own plan, whichever side or victim form is asked. The first server's reads are the ones
    /// that would return the other server's plan if a read lost its server predicate or bound the wrong id.
    /// </summary>
    [Fact]
    public async Task BlockingAndDeadlockPlanReads_NeverReturnAnotherServersPlan()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live row-plan test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ServerId, ServerName, ct);
            await RegisterServerAsync(connection, OtherServerId, OtherServerName, ct);

            var ownBlocked = PlanXml("own-blocked");
            var ownBlocking = PlanXml("own-blocking");
            var ownVictim = PlanXml("own-victim");
            var otherBlocked = PlanXml("other-blocked");
            var otherBlocking = PlanXml("other-blocking");
            var otherVictim = PlanXml("other-victim");

            /* The same event on both servers: equal event_time, spids and ecids. The other server's copy is collected a minute
               earlier than this server's, so without the server predicate it would be the earliest copy of the key. */
            var ownCollected = Anchor.AddSeconds(5);
            await InsertReportAsync(connection, OtherServerId, OtherServerName, ownCollected.AddMinutes(-1), Anchor, Db, 51, 52, otherBlocked, otherBlocking, ct);
            await InsertReportAsync(connection, ServerId, ServerName, ownCollected, Anchor, Db, 51, 52, ownBlocked, ownBlocking, ct);

            /* The same deadlock stamps and victim on both servers, the other server's inserted first so its deadlock_id is lower. */
            var collected = Anchor.AddMinutes(10);
            var stamp = collected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, OtherServerId, OtherServerName, collected, stamp, "process1a2b", otherVictim, Db, ct);
            await InsertDeadlockAsync(connection, ServerId, ServerName, collected, stamp, "process1a2b", ownVictim, Db, ct);

            var eventTime = Anchor.ToString("o", CultureInfo.InvariantCulture);
            var collectionTime = collected.ToString("o", CultureInfo.InvariantCulture);
            var deadlockTime = stamp.ToString("o", CultureInfo.InvariantCulture);

            foreach (var (server, blocked, blocking, victim) in new[]
            {
                (ServerName, ownBlocked, ownBlocking, ownVictim),
                (OtherServerName, otherBlocked, otherBlocking, otherVictim),
            })
            {
                Assert.Equal(blocked, await DarlingMcpPlanTools.GetBlockingPlanXml(postgres, eventTime, 51, 52, server, cancellationToken: ct));
                Assert.Equal(blocking, await DarlingMcpPlanTools.GetBlockingPlanXml(postgres, eventTime, 51, 52, server, side: "blocking", cancellationToken: ct));
                Assert.Equal(victim, await DarlingMcpPlanTools.GetDeadlockPlanXml(postgres, collectionTime, deadlockTime, server, "process1a2b", cancellationToken: ct));
                Assert.Equal(victim, await DarlingMcpPlanTools.GetDeadlockPlanXml(postgres, collectionTime, deadlockTime, server, cancellationToken: ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #5236: without a victim_process_id, two deadlocks that share both stamps and name different victims are never guessed
    /// between. The read refuses with an <c>invalid</c> answer that names victim_process_id and carries neither plan, for a
    /// missing victim and an empty one alike; naming the victim picks either twin. A deadlock that is alone for its stamps still
    /// answers with no victim, and so do two stored copies of ONE deadlock (they name the same victim), by the lower deadlock_id.
    /// </summary>
    [Fact]
    public async Task DeadlockPlanRead_WithoutAVictim_RefusesTwinsThatNameDifferentVictims_AndStillAnswersAloneDeadlocks()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live row-plan test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            var twinOne = PlanXml("twin-one");
            var twinTwo = PlanXml("twin-two");
            var lone = PlanXml("lone");
            var copyFirst = PlanXml("copy-first");
            var copySecond = PlanXml("copy-second");

            var twinCollected = Anchor.AddMinutes(10);
            var twinStamp = twinCollected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, twinCollected, twinStamp, "process1a2b", twinOne, ct);
            await InsertDeadlockAsync(connection, twinCollected, twinStamp, "process3c4d", twinTwo, ct);

            var loneCollected = Anchor.AddMinutes(20);
            var loneStamp = loneCollected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, loneCollected, loneStamp, "process5e6f", lone, ct);

            var copyCollected = Anchor.AddMinutes(30);
            var copyStamp = copyCollected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, copyCollected, copyStamp, "process7a8b", copyFirst, ct);
            await InsertDeadlockAsync(connection, copyCollected, copyStamp, "process7a8b", copySecond, ct);

            async Task<string> Read(DateTime collected, DateTime stamp, string? victim) => await DarlingMcpPlanTools.GetDeadlockPlanXml(
                postgres, collected.ToString("o", CultureInfo.InvariantCulture), stamp.ToString("o", CultureInfo.InvariantCulture), ServerName, victim,
                cancellationToken: ct);

            /* Twins with no victim: refused, naming victim_process_id, with neither plan in the answer. */
            foreach (var none in new string?[] { null, "" })
            {
                var refused = await Read(twinCollected, twinStamp, none);
                using var doc = JsonDocument.Parse(refused);
                Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal("victim_process_id", doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
                Assert.Contains("victim_process_id", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain("ShowPlanXML", refused, StringComparison.Ordinal);
            }

            /* Naming the victim picks either twin, and a victim that names neither is a plain miss, not a refusal. */
            Assert.Equal(twinOne, await Read(twinCollected, twinStamp, "process1a2b"));
            Assert.Equal(twinTwo, await Read(twinCollected, twinStamp, "process3c4d"));
            Assert.Equal("unavailable", StatusOf(await Read(twinCollected, twinStamp, "process-nobody")));

            /* A deadlock alone for its stamps answers with or without its victim. */
            Assert.Equal(lone, await Read(loneCollected, loneStamp, null));
            Assert.Equal(lone, await Read(loneCollected, loneStamp, "process5e6f"));

            /* Two copies of one deadlock name the same victim: they are not twins, so the lower deadlock_id answers. */
            Assert.Equal(copyFirst, await Read(copyCollected, copyStamp, null));
            Assert.Equal(copyFirst, await Read(copyCollected, copyStamp, "process7a8b"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #5236: both reads take an optional database_name, because one server_id can collect several databases (an Azure master
    /// target), and each database numbers its own sessions. Two databases hold a report with the same event_time, spids and
    /// ecids, and a deadlock with the same stamps AND the same victim, and each database's plans differ. With the row's database
    /// sent, each read returns that database's plan, through the tools and through the web read route (which echoes it). With
    /// none sent, or an empty one, the answer is the one the reads gave before the filter existed (the earliest copy of the
    /// report, the lower deadlock_id). A database that has no such row is a miss that says which database.
    /// </summary>
    [Fact]
    public async Task PlanReads_NarrowToTheRowsDatabase_WhenItIsSent_AndKeepTheOldAnswerWhenItIsNot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live row-plan test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            const string dbOne = "RowPlanDbOne";
            const string dbTwo = "RowPlanDbTwo";
            var oneBlocked = PlanXml("one-blocked");
            var oneBlocking = PlanXml("one-blocking");
            var oneVictim = PlanXml("one-victim");
            var twoBlocked = PlanXml("two-blocked");
            var twoBlocking = PlanXml("two-blocking");
            var twoVictim = PlanXml("two-victim");

            /* The same event key in both databases; the first database's copy is collected first, so it is the earliest copy. */
            await InsertReportAsync(connection, ServerId, ServerName, Anchor.AddSeconds(5), Anchor, dbOne, 51, 52, oneBlocked, oneBlocking, ct);
            await InsertReportAsync(connection, ServerId, ServerName, Anchor.AddSeconds(8), Anchor, dbTwo, 51, 52, twoBlocked, twoBlocking, ct);

            /* The same stamps and the same victim in both databases, the first database's inserted first (the lower deadlock_id). */
            var collected = Anchor.AddMinutes(10);
            var stamp = collected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, ServerId, ServerName, collected, stamp, "process1a2b", oneVictim, dbOne, ct);
            await InsertDeadlockAsync(connection, ServerId, ServerName, collected, stamp, "process1a2b", twoVictim, dbTwo, ct);

            var eventTime = Anchor.ToString("o", CultureInfo.InvariantCulture);
            var collectionTime = collected.ToString("o", CultureInfo.InvariantCulture);
            var deadlockTime = stamp.ToString("o", CultureInfo.InvariantCulture);

            async Task<string> Blocked(string? database, string? side = null) => await DarlingMcpPlanTools.GetBlockingPlanXml(
                postgres, eventTime, 51, 52, ServerName, side: side, database_name: database, cancellationToken: ct);
            async Task<string> Victim(string? database, string? victim = "process1a2b") => await DarlingMcpPlanTools.GetDeadlockPlanXml(
                postgres, collectionTime, deadlockTime, ServerName, victim, database, ct);

            /* The row's database sent: that database's plan. */
            Assert.Equal(oneBlocked, await Blocked(dbOne));
            Assert.Equal(oneBlocking, await Blocked(dbOne, "blocking"));
            Assert.Equal(twoBlocked, await Blocked(dbTwo));
            Assert.Equal(twoBlocking, await Blocked(dbTwo, "blocking"));
            Assert.Equal(oneVictim, await Victim(dbOne));
            Assert.Equal(twoVictim, await Victim(dbTwo));
            Assert.Equal(twoVictim, await Victim(dbTwo, victim: null));

            /* No database, or an empty one: the answer the reads gave before the filter existed. */
            foreach (var none in new string?[] { null, "" })
            {
                Assert.Equal(oneBlocked, await Blocked(none));
                Assert.Equal(oneBlocking, await Blocked(none, "blocking"));
                Assert.Equal(oneVictim, await Victim(none));
            }

            /* A database with no such row is a miss that names it, not another database's plan. */
            var blockedMiss = await Blocked("NoSuchDb");
            Assert.Equal("unavailable", StatusOf(blockedMiss));
            Assert.Contains("in database 'NoSuchDb'", MessageOf(blockedMiss), StringComparison.Ordinal);
            var victimMiss = await Victim("NoSuchDb");
            Assert.Equal("unavailable", StatusOf(victimMiss));
            Assert.Contains("in database 'NoSuchDb'", MessageOf(victimMiss), StringComparison.Ordinal);

            /* The web read route forwards the row's database_name to the tool and echoes it with the plan. */
            async Task<JsonElement> Web(string tool, string query)
            {
                var context = new Microsoft.AspNetCore.Http.DefaultHttpContext();
                context.Request.QueryString = new Microsoft.AspNetCore.Http.QueryString("?" + query);
                using var doc = JsonDocument.Parse(await DarlingWebEndpoints.BuildReadDispatch()[tool](context, postgres, null!));
                return doc.RootElement.Clone();
            }

            var blockedKey = $"server={ServerName}&event_time={eventTime}&blocked_spid=51&blocking_spid=52";
            var webTwo = await Web("get_blocking_plan_xml", blockedKey + "&database_name=" + dbTwo);
            Assert.Equal(twoBlocked, webTwo.GetProperty("plan_xml").GetString());
            Assert.Equal(dbTwo, webTwo.GetProperty("database_name").GetString());
            var webNone = await Web("get_blocking_plan_xml", blockedKey);
            Assert.Equal(oneBlocked, webNone.GetProperty("plan_xml").GetString());
            Assert.Equal(JsonValueKind.Null, webNone.GetProperty("database_name").ValueKind);

            var victimKey = $"server={ServerName}&collection_time={collectionTime}&deadlock_time={deadlockTime}&victim_process_id=process1a2b";
            var webVictimTwo = await Web("get_deadlock_plan_xml", victimKey + "&database_name=" + dbTwo);
            Assert.Equal(twoVictim, webVictimTwo.GetProperty("plan_xml").GetString());
            Assert.Equal(dbTwo, webVictimTwo.GetProperty("database_name").GetString());
            Assert.Equal(oneVictim, (await Web("get_deadlock_plan_xml", victimKey)).GetProperty("plan_xml").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The row's database_name as the list printed it, or null when the row has none (the web then sends none).</summary>
    private static string? DatabaseOf(JsonElement row) =>
        row.TryGetProperty("database_name", out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static Task InsertReportAsync(
        NpgsqlConnection connection, DateTime eventTime, int blockedSpid, int blockingSpid, string? blockedPlan, string? blockingPlan,
        System.Threading.CancellationToken ct) =>
        InsertReportAsync(connection, ServerId, ServerName, eventTime.AddSeconds(5), eventTime, Db, blockedSpid, blockingSpid, blockedPlan, blockingPlan, ct);

    private static async Task InsertReportAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime collectionTime, DateTime eventTime, string? database,
        int blockedSpid, int blockingSpid, string? blockedPlan, string? blockingPlan, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid,
     blocked_ecid, blocking_ecid, blocking_status, database_name, blocked_query_plan_xml, blocking_query_plan_xml)
VALUES ($1, $2, $3, $4, $5, 12000, $6, $7, 0, 0, 'suspended', $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(blockingSpid);
        command.Parameters.AddWithValue(blockedSpid);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)blockedPlan ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)blockingPlan ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task InsertDeadlockAsync(
        NpgsqlConnection connection, DateTime collectionTime, DateTime deadlockTime, string victimProcessId, string? victimPlan,
        System.Threading.CancellationToken ct) =>
        InsertDeadlockAsync(connection, ServerId, ServerName, collectionTime, deadlockTime, victimProcessId, victimPlan, null, ct);

    private static async Task InsertDeadlockAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime collectionTime, DateTime deadlockTime,
        string victimProcessId, string? victimPlan, string? database, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_query_plan_xml, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(victimProcessId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)victimPlan ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id IN ({ServerId}, {OtherServerId}); " +
            $"DELETE FROM deadlocks WHERE server_id IN ({ServerId}, {OtherServerId}); " +
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM procedure_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id IN ({ServerId}, {OtherServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// #5236: the twin check looks at EVERY deadlock the stamps match, not only the ones that captured a plan. Two deadlocks that
    /// share both stamps and name different victims are refused when no victim is sent, whichever of them captured a plan (the
    /// plan of the other one must not answer for it); with each victim sent, the one that captured a plan answers with it and
    /// the other is the plain not-captured miss. Two stored copies of ONE deadlock (the same victim) still answer from the copy
    /// that captured a plan, even when the lower deadlock_id is the copy that did not.
    /// </summary>
    [Fact]
    public async Task DeadlockPlanRead_TwinsRefuse_WhetherOrNotEachCapturedAPlan_AndACopyWithoutAPlanNeverHidesItsSibling()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live row-plan test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            var firstPlan = PlanXml("first-captured");
            var secondPlan = PlanXml("second-captured");
            var copyPlan = PlanXml("copy-captured");

            /* Only the first twin captured a plan. */
            var firstOnlyCollected = Anchor.AddMinutes(40);
            var firstOnlyStamp = firstOnlyCollected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, firstOnlyCollected, firstOnlyStamp, "process1a2b", firstPlan, ct);
            await InsertDeadlockAsync(connection, firstOnlyCollected, firstOnlyStamp, "process3c4d", null, ct);

            /* Only the second twin captured a plan. */
            var secondOnlyCollected = Anchor.AddMinutes(50);
            var secondOnlyStamp = secondOnlyCollected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, secondOnlyCollected, secondOnlyStamp, "process1a2b", "", ct);
            await InsertDeadlockAsync(connection, secondOnlyCollected, secondOnlyStamp, "process3c4d", secondPlan, ct);

            /* Two copies of one deadlock: the lower deadlock_id captured no plan, the higher one did. */
            var copyCollected = Anchor.AddMinutes(60);
            var copyStamp = copyCollected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, copyCollected, copyStamp, "process7a8b", null, ct);
            await InsertDeadlockAsync(connection, copyCollected, copyStamp, "process7a8b", copyPlan, ct);

            async Task<string> Read(DateTime collected, DateTime stamp, string? victim) => await DarlingMcpPlanTools.GetDeadlockPlanXml(
                postgres, collected.ToString("o", CultureInfo.InvariantCulture), stamp.ToString("o", CultureInfo.InvariantCulture), ServerName, victim,
                cancellationToken: ct);

            void AssertRefused(string answer)
            {
                using var doc = JsonDocument.Parse(answer);
                Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal("victim_process_id", doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
                Assert.Contains("victim_process_id", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain("ShowPlanXML", answer, StringComparison.Ordinal);
            }

            /* No victim: refused when only the first twin captured a plan, and when only the second did. */
            foreach (var none in new string?[] { null, "" })
            {
                AssertRefused(await Read(firstOnlyCollected, firstOnlyStamp, none));
                AssertRefused(await Read(secondOnlyCollected, secondOnlyStamp, none));
            }

            /* Each victim sent: the twin that captured a plan answers with it, the other is the not-captured miss. */
            Assert.Equal(firstPlan, await Read(firstOnlyCollected, firstOnlyStamp, "process1a2b"));
            Assert.Equal("unavailable", StatusOf(await Read(firstOnlyCollected, firstOnlyStamp, "process3c4d")));
            Assert.Equal("unavailable", StatusOf(await Read(secondOnlyCollected, secondOnlyStamp, "process1a2b")));
            Assert.Equal(secondPlan, await Read(secondOnlyCollected, secondOnlyStamp, "process3c4d"));

            /* Copies of one deadlock answer from the copy that captured a plan, with no victim or with theirs. */
            Assert.Equal(copyPlan, await Read(copyCollected, copyStamp, null));
            Assert.Equal(copyPlan, await Read(copyCollected, copyStamp, "process7a8b"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
