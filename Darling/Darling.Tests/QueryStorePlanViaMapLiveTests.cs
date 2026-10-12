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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5257: the collector ships <c>query_store_stats.query_plan_text</c> as a NULL placeholder since #2210; the plan
/// lives once in <c>query_plan_dim</c>, reached through <c>collect.query_store_plan_map</c>. The Service
/// (<c>analyze_query_store_plan</c>) and Viewer readers must find a plan there, keep reading old inline rows, and
/// treat a NULL-digest map row (a marker for a plan that will never exist) as absent.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStorePlanViaMapLiveTests
{
    private const string ServerName = "darling-qs-map-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "MapPlanDb";

    private static string Plan(string label) =>
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        $"<StmtSimple StatementText=\"SELECT '{label}'\" StatementId=\"1\" StatementType=\"SELECT\" />" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static readonly string TextPlan = Plan("map-text");
    private static readonly string GzPlan = Plan("map-gz");
    private static readonly string OlderPlan = Plan("map-older");
    private static readonly string InlineOnly = Plan("inline-only");
    private static readonly string InlineBehindMarker = Plan("inline-behind-marker");
    private static readonly string MapNewer = Plan("map-newer");
    private static readonly string InlineOlder = Plan("inline-older");
    private static readonly string MapQ7 = Plan("map-q7");
    private static readonly string OrphanDigestPlan = Plan("orphan-digest-no-dim-row");
    private static readonly string InlineBehindOrphan = Plan("inline-behind-orphan");
    private static readonly string Stale = Plan("map-stale-8d");

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void ViewerAndServiceReaders_ShareOneSqlShape_AndTheMapReadNeverTouchesTheFactTable()
    {
        static string Norm(string s) => string.Join(' ', s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        Assert.Equal(Norm(DarlingStoredPlanReader.QueryStorePlanViaMapSql), Norm(ViewerDataService.QueryStorePlanViaMapSql));
        foreach (var sql in new[] { DarlingStoredPlanReader.QueryStorePlanViaMapSql, ViewerDataService.QueryStorePlanViaMapSql })
        {
            Assert.Contains("FROM collect.query_store_plan_map", sql, StringComparison.Ordinal);
            Assert.Contains("LEFT JOIN query_plan_dim", sql, StringComparison.Ordinal);
            Assert.Contains("m.plan_id = $3", sql, StringComparison.Ordinal);
            /* Two index lookups: the fact table is never read once a plan_id is known. */
            Assert.DoesNotContain("query_store_stats", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheUnpinnedCandidateRead_IsTimeBounded_NewestFirst_WithAPlanIdTiebreak()
    {
        var bounded = DarlingStoredPlanReader.QueryStorePlanCandidatesSql;
        Assert.Contains("collection_time >= $4", bounded, StringComparison.Ordinal);
        Assert.Contains("ORDER BY last_collected DESC, plan_id DESC", bounded, StringComparison.Ordinal);

        var unbounded = DarlingStoredPlanReader.QueryStorePlanCandidatesUnboundedSql;
        Assert.DoesNotContain("collection_time >=", unbounded, StringComparison.Ordinal);
        Assert.Contains("ORDER BY last_collected DESC, plan_id DESC", unbounded, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BothReaders_FindMapOnlyPlans_ReadInlineRows_AndTreatMarkersAsAbsent_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live query-store plan-map test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        await using var viewer = new ViewerDataService(cs!);

        var bodySucceeded = false;
        try
        {
            /* Fixed anchors, never the wall clock. */
            var t0 = new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified);
            var t1 = t0.AddHours(1);

            await RegisterServerAsync(connection, ct);

            /* 1: text-stored plan. */
            await InsertFactAsync(connection, t0, queryId: 1, planId: 11, inline: null, ct);
            await InsertDimAsync(connection, TextPlan, gz: false, ct);
            await InsertMapAsync(connection, 11, TextPlan, ct);

            /* 2: gzip-stored plan. */
            await InsertFactAsync(connection, t0, queryId: 2, planId: 21, inline: null, ct);
            await InsertDimAsync(connection, GzPlan, gz: true, ct);
            await InsertMapAsync(connection, 21, GzPlan, ct);

            /* 3: an older real plan, then a newer plan_id whose map row is a NULL-digest marker. */
            await InsertFactAsync(connection, t0, queryId: 3, planId: 31, inline: null, ct);
            await InsertDimAsync(connection, OlderPlan, gz: false, ct);
            await InsertMapAsync(connection, 31, OlderPlan, ct);
            await InsertFactAsync(connection, t1, queryId: 3, planId: 32, inline: null, ct);
            await InsertMapAsync(connection, 32, null, ct);

            /* 4: an old row that carries the text inline and has no map row. */
            await InsertFactAsync(connection, t0, queryId: 4, planId: 41, inline: InlineOnly, ct);

            /* 5: a marker on the only plan_id, with the text still inline: the inline value answers. */
            await InsertFactAsync(connection, t0, queryId: 5, planId: 51, inline: InlineBehindMarker, ct);
            await InsertMapAsync(connection, 51, null, ct);

            /* 6: an OLDER plan that lives inline and a NEWER plan that lives in the map, for one query. */
            await InsertFactAsync(connection, t0, queryId: 6, planId: 61, inline: InlineOlder, ct);
            await InsertFactAsync(connection, t1, queryId: 6, planId: 62, inline: null, ct);
            await InsertDimAsync(connection, MapNewer, gz: false, ct);
            await InsertMapAsync(connection, 62, MapNewer, ct);

            /* 7: a real older map plan, then a newer plan_id whose map row points at a digest with NO dimension
               row (the dimension GC removed it). The fact row still has inline text: it must not be read. */
            await InsertFactAsync(connection, t0, queryId: 7, planId: 71, inline: null, ct);
            await InsertDimAsync(connection, MapQ7, gz: false, ct);
            await InsertMapAsync(connection, 71, MapQ7, ct);
            await InsertFactAsync(connection, t1, queryId: 7, planId: 72, inline: InlineBehindOrphan, ct);
            await InsertMapAsync(connection, 72, OrphanDigestPlan, ct);

            /* 8: the only plan was last seen 8 days before asOf, outside the 7-day bound. */
            var asOf = new DateTime(2026, 3, 2, 12, 0, 0, DateTimeKind.Unspecified);
            await InsertFactAsync(connection, asOf.AddDays(-8), queryId: 8, planId: 81, inline: null, ct);
            await InsertDimAsync(connection, Stale, gz: false, ct);
            await InsertMapAsync(connection, 81, Stale, ct);

            /* ---- Service reader (plan_id optional). */
            Task<string?> Svc(long q, long? p) => DarlingStoredPlanReader.GetQueryStorePlanTextAsync(postgres, ServerId, Db, q, planId: p, asOf: asOf, cancellationToken: ct);
            Task<QueryStorePlanRead?> SvcRead(long q, long? p) => DarlingStoredPlanReader.ResolveQueryStorePlanAsync(postgres, ServerId, Db, q, planId: p, asOf: asOf, cancellationToken: ct);
            Assert.Equal(TextPlan, await Svc(1, null));
            Assert.Equal(TextPlan, await Svc(1, 11));
            Assert.Equal(GzPlan, await Svc(2, null));
            /* Newest plan_id is a marker: the unpinned read falls to the older plan; the pinned one has no content. */
            Assert.Equal(OlderPlan, await Svc(3, null));
            Assert.Equal(OlderPlan, await Svc(3, 31));
            Assert.Null(await Svc(3, 32));
            Assert.Equal(InlineOnly, await Svc(4, null));
            /* A map row with no content is ABSENT: the inline text behind a marker is not read. */
            Assert.Null(await Svc(5, 51));
            Assert.Null(await Svc(5, null));
            Assert.Null(await Svc(999, null));
            Assert.Null(await Svc(1, 999));
            /* The plan_id names the plan; the pinned read goes by the map's key alone. */
            Assert.Equal(TextPlan, await Svc(999, 11));

            /* Older plan inline, newer plan in the map: the map plan wins unpinned; each pinned read answers its own. */
            Assert.Equal(MapNewer, await Svc(6, null));
            Assert.Equal(MapNewer, await Svc(6, 62));
            Assert.Equal(InlineOlder, await Svc(6, 61));

            /* A map row whose digest has no dimension row is absent; the unpinned read moves to the next plan. */
            Assert.Null(await Svc(7, 72));
            Assert.Equal(MapQ7, await Svc(7, null));

            /* Last seen 8 days before asOf: the bounded pass finds nothing and the one unbounded pass does. */
            Assert.Equal(Stale, await Svc(8, null));
            Assert.Equal(Stale, await Svc(8, 81));

            /* The resolved plan_id comes back with the plan. */
            Assert.Equal(11, (await SvcRead(1, null))!.PlanId);
            Assert.Equal(31, (await SvcRead(3, null))!.PlanId);
            Assert.Equal(62, (await SvcRead(6, null))!.PlanId);
            Assert.Equal(71, (await SvcRead(7, null))!.PlanId);
            Assert.Equal(81, (await SvcRead(8, null))!.PlanId);
            Assert.Equal(61, (await SvcRead(6, 61))!.PlanId);
            Assert.Null(await SvcRead(5, null));

            /* ---- Viewer reader (plan_id required). */
            Task<string?> Vw(long q, long p) => viewer.GetQueryStorePlanTextAsync(ServerId, Db, q, p, cancellationToken: ct);
            Assert.Equal(TextPlan, await Vw(1, 11));
            Assert.Equal(GzPlan, await Vw(2, 21));
            Assert.Equal(OlderPlan, await Vw(3, 31));
            Assert.Null(await Vw(3, 32));
            Assert.Equal(InlineOnly, await Vw(4, 41));
            Assert.Null(await Vw(5, 51));
            Assert.Equal(MapNewer, await Vw(6, 62));
            Assert.Equal(InlineOlder, await Vw(6, 61));
            Assert.Null(await Vw(7, 72));
            Assert.Equal(Stale, await Vw(8, 81));
            Assert.Null(await Vw(1, 999));
            /* The map is read by plan_id alone (a plan_id belongs to one query_id), so a query_id that does not
               own the plan still gets it: the fact table is not consulted. */
            Assert.Equal(TextPlan, await Vw(999, 11));
            Assert.Null(await viewer.GetQueryStorePlanTextAsync(ServerId + 1, Db, 1, 11, cancellationToken: ct));

            /* ---- Third reader: the actual-plan resolver. */
            async Task<(string? Text, string? Plan)> Resolve(long q)
            {
                await using var command = new NpgsqlCommand(DarlingWorker.ResolveStoredQueryStoreForActualPlanSql, connection);
                command.Parameters.AddWithValue(ServerId);
                command.Parameters.AddWithValue(Db);
                command.Parameters.AddWithValue(q);
                await using var reader = await command.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct))
                {
                    return (null, null);
                }

                return (
                    reader.IsDBNull(0) ? null : reader.GetString(0),
                    PayloadDimensions.ResolveContent(
                        reader.IsDBNull(1) ? null : reader.GetString(1),
                        reader.IsDBNull(3) ? null : reader.GetFieldValue<byte[]>(3)));
            }

            Assert.Equal(("SELECT /*q1*/", TextPlan), await Resolve(1));
            Assert.Equal(("SELECT /*q2*/", GzPlan), await Resolve(2));
            Assert.Equal(OlderPlan, (await Resolve(3)).Plan);
            Assert.Equal(InlineOnly, (await Resolve(4)).Plan);
            /* A marker: the query text still answers (it is what gets re-executed); the estimated plan is absent. */
            Assert.Equal(("SELECT /*q5*/", (string?)null), await Resolve(5));
            Assert.Equal(MapNewer, (await Resolve(6)).Plan);
            Assert.Equal(MapQ7, (await Resolve(7)).Plan);
            Assert.Equal(Stale, (await Resolve(8)).Plan);
            Assert.Equal((null, null), await Resolve(999));

            /* ---- analyze_query_store_plan end to end: the analysed statement and the resolved plan_id. */
            async Task<(string Identifier, string? Statement)> Analyse(long q, long? p)
            {
                var json = await DarlingMcpPlanTools.AnalyzeQueryStorePlan(postgres, Db, q, ServerName, p, cancellationToken: ct);
                using var doc = JsonDocument.Parse(json);
                Assert.False(doc.RootElement.TryGetProperty("status", out var status) && status.GetString() == "unavailable", json);
                Assert.Equal("query_store", doc.RootElement.GetProperty("source").GetString());
                var statements = doc.RootElement.GetProperty("statements");
                return (
                    doc.RootElement.GetProperty("identifier").GetString()!,
                    statements.GetArrayLength() == 0 ? null : statements[0].GetProperty("statement_text").GetString());
            }

            Assert.Equal(($"{Db}:1:11", "SELECT 'map-text'"), await Analyse(1, null));
            Assert.Equal(($"{Db}:2:21", "SELECT 'map-gz'"), await Analyse(2, null));
            /* The newest plan is a marker, so the analysed plan is the older one, and the identifier says which. */
            Assert.Equal(($"{Db}:3:31", "SELECT 'map-older'"), await Analyse(3, null));
            Assert.Equal(($"{Db}:6:62", "SELECT 'map-newer'"), await Analyse(6, null));
            Assert.Equal(($"{Db}:6:61", "SELECT 'inline-older'"), await Analyse(6, 61));
            Assert.Equal(($"{Db}:1:11", "SELECT 'map-text'"), await Analyse(1, 11));
            var missing = await DarlingMcpPlanTools.AnalyzeQueryStorePlan(postgres, Db, 3, ServerName, 32, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(missing))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteAsync);
        }
    }

    private static IEnumerable<string> AllPlans() => new[] { TextPlan, GzPlan, OlderPlan, MapNewer, MapQ7, Stale };

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using (var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM collect.query_store_plan_map WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        foreach (var plan in AllPlans())
        {
            using var dim = new NpgsqlCommand("DELETE FROM query_plan_dim WHERE digest = $1", connection);
            dim.Parameters.AddWithValue(PayloadDimensions.Digest(plan));
            await dim.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertFactAsync(
        NpgsqlConnection connection, DateTime collectionTime, long queryId, long planId, string? inline, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_plan_text, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue((object?)inline ?? DBNull.Value);
        command.Parameters.AddWithValue($"SELECT /*q{queryId}*/");
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDimAsync(NpgsqlConnection connection, string plan, bool gz, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            gz
                ? "INSERT INTO query_plan_dim (digest, query_plan_gz, last_seen) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
                : "INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            connection);
        command.Parameters.AddWithValue(PayloadDimensions.Digest(plan));
        if (gz)
        {
            command.Parameters.AddWithValue(PayloadDimensions.CompressContent(plan));
        }
        else
        {
            command.Parameters.AddWithValue(plan);
        }

        command.Parameters.AddWithValue(new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertMapAsync(NpgsqlConnection connection, long planId, string? plan, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) VALUES ($1, $2, $3, $4, $5, $6)",
            connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(planId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bytea, Value = plan is null ? DBNull.Value : PayloadDimensions.Digest(plan) });
        command.Parameters.AddWithValue("0x5257");
        command.Parameters.AddWithValue(new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }
}
