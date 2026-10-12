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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 item 13. <c>analyze_server</c>, run through MCP, saves the families its pass could not read through
/// <see cref="CollectionCaveatStore.ApplyPassAsync"/> -- an upsert (INSERT .. ON CONFLICT DO UPDATE) and a
/// DELETE on <c>collect.analysis_collection_caveats</c> -- and the mcp role held only INSERT on
/// <c>collect.analysis_findings</c>, so the write failed 42501 on EVERY call (the DELETE runs even on a clean
/// pass). The store swallows the failure, so the caller never knew. These tests run the REAL managed role batch:
/// the mcp role now performs both statements, holds no other write on any collect table beyond the two analysis
/// targets, and a pass whose caveat write is refused says so in <c>persistence_note</c>.
///
/// <para><b>#1776 own-store</b>: every test that touches a database does so on its own <see cref="ScratchPostgres"/>
/// database. The managed roles are cluster-wide, so the class shares the <c>pg-cluster-roles</c> collection.</para>
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class McpCollectionCaveatGrantLiveTests
{
    private const string ServerName = "darling-5633-caveat-grant";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public void ComposePersistenceNote_SaysNothingWhenTheWriteLanded_AndAppendsToTheAsOfNoteWhenItDidNot()
    {
        Assert.Null(DarlingMcpTools.ComposePersistenceNote(null, caveatWriteFailed: false));
        Assert.Equal("as_of note", DarlingMcpTools.ComposePersistenceNote("as_of note", caveatWriteFailed: false));
        Assert.Equal(
            DarlingMcpTools.CollectionCaveatsNotSavedNote, DarlingMcpTools.ComposePersistenceNote(null, caveatWriteFailed: true));
        Assert.Equal(
            "as_of note " + DarlingMcpTools.CollectionCaveatsNotSavedNote,
            DarlingMcpTools.ComposePersistenceNote("as_of note", caveatWriteFailed: true));
        Assert.Contains("NOT saved", DarlingMcpTools.CollectionCaveatsNotSavedNote, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheMcpRole_RunsTheCaveatWrites_AndHoldsNoOtherCollectWrite_Gated()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        await ProvisionAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await using var mcp = NpgsqlDataSource.Create(McpConnectionString(scratch));
            var t1 = new DateTime(2026, 10, 1, 10, 0, 0, DateTimeKind.Utc);
            var t2 = t1.AddMinutes(5);

            /* The upsert, as mcp: a re-sighting keeps first_seen_utc and moves last_seen_utc + reason. */
            Assert.True(await CollectionCaveatStore.ApplyPassAsync(
                mcp, ServerId, [new CollectionCaveatStore.UnreadFamily("wait_stats", "timeout")], t1, null, ct));
            Assert.True(await CollectionCaveatStore.ApplyPassAsync(
                mcp, ServerId,
                [new CollectionCaveatStore.UnreadFamily("wait_stats", "error"), new CollectionCaveatStore.UnreadFamily("memory", "timeout")],
                t2, null, ct));

            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            var rows = await RowsAsync(owner, ct);
            Assert.Equal(2, rows.Count);
            var wait = rows.Single(r => r.Family == "wait_stats");
            Assert.Equal("error", wait.Reason);
            Assert.Equal(t1, DateTime.SpecifyKind(wait.FirstSeen, DateTimeKind.Utc));
            Assert.Equal(t2, DateTime.SpecifyKind(wait.LastSeen, DateTimeKind.Utc));

            /* The DELETE, as mcp: a pass that read memory clears it; a clean pass clears the rest. */
            Assert.True(await CollectionCaveatStore.ApplyPassAsync(
                mcp, ServerId, [new CollectionCaveatStore.UnreadFamily("wait_stats", "error")], t2, null, ct));
            Assert.Equal(["wait_stats"], (await RowsAsync(owner, ct)).Select(r => r.Family).ToArray());
            Assert.True(await CollectionCaveatStore.ApplyPassAsync(mcp, ServerId, [], t2, null, ct));
            Assert.Empty(await RowsAsync(owner, ct));

            /* NOTHING WIDER. Census over every collect relation: the only writes mcp holds are INSERT on
               analysis_findings and, on the caveat table, INSERT + DELETE + column UPDATE of exactly the two
               columns the upsert sets. Anything else (a new grant, a stray default privilege, a table-level
               UPDATE) fails here. */
            await using var mcpConnection = await mcp.OpenConnectionAsync(ct);
            var writes = new SortedSet<string>(StringComparer.Ordinal);
            await using (var census = new NpgsqlCommand(@"
SELECT c.relname, p.priv,
       CASE WHEN p.priv = 'UPDATE' THEN has_table_privilege(c.oid, 'UPDATE') OR has_any_column_privilege(c.oid, 'UPDATE')
            ELSE has_table_privilege(c.oid, p.priv) END
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'collect'
CROSS JOIN (VALUES ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE')) AS p(priv)
WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')", mcpConnection))
            await using (var reader = await census.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    if (reader.GetBoolean(2))
                    {
                        writes.Add(reader.GetString(0) + ":" + reader.GetString(1));
                    }
                }
            }

            Assert.Equal(
                ["analysis_collection_caveats:DELETE", "analysis_collection_caveats:INSERT", "analysis_collection_caveats:UPDATE", "analysis_findings:INSERT"],
                writes.ToArray());

            foreach (var column in new[] { "server_id", "family", "first_seen_utc" })
            {
                Assert.False(await ScalarBoolAsync(mcpConnection,
                    $"SELECT has_column_privilege('collect.analysis_collection_caveats', '{column}', 'UPDATE')", ct), column);
            }

            foreach (var column in new[] { "reason", "last_seen_utc" })
            {
                Assert.True(await ScalarBoolAsync(mcpConnection,
                    $"SELECT has_column_privilege('collect.analysis_collection_caveats', '{column}', 'UPDATE')", ct), column);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// The whole path: <c>analyze_server</c> as the mcp role on a server whose window is empty (the pass still
    /// reaches the collector, and the caveat bookkeeping runs on every pass, clean ones included). With the
    /// shipped grants the note is absent; with the caveat grants revoked -- the pre-fix role -- the response says
    /// the caveats were not saved instead of failing or staying silent.
    /// </summary>
    [Fact]
    public async Task AnalyzeServerAsMcp_SavesItsCaveats_AndSaysSoInPersistenceNoteWhenTheWriteIsRefused_Gated()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        await ProvisionAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                await ExecAsync(owner, $@"
INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ({ServerId}, '{ServerName}', '{ServerName}', TRUE, 15, now(), now());
INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES (1, now() - interval '8 days', {ServerId}, '{ServerName}', 'AN5633_WAIT', 50, 1000),
       (2, now() - interval '3 hours', {ServerId}, '{ServerName}', 'AN5633_WAIT', 50, 1000);", ct);
            }

            await using var mcp = NpgsqlDataSource.Create(McpConnectionString(scratch));
            var service = new DarlingAnalysisService(mcp);

            var granted = await DarlingMcpTools.AnalyzeServer(service, mcp, ServerName, 1);
            using (var doc = JsonDocument.Parse(granted))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("hints").GetProperty("persistence_note").ValueKind);
            }

            Assert.False(service.LastCollectionCaveatWriteFailed);

            /* The pre-fix role: the caveat table back to what mcp held before this change (SELECT only). */
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                await ExecAsync(owner,
                    "REVOKE INSERT, DELETE ON collect.analysis_collection_caveats FROM mcp; "
                    + "REVOKE UPDATE (reason, last_seen_utc) ON collect.analysis_collection_caveats FROM mcp;", ct);
            }

            var refused = await DarlingMcpTools.AnalyzeServer(service, mcp, ServerName, 1);
            using (var doc = JsonDocument.Parse(refused))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal(
                    DarlingMcpTools.CollectionCaveatsNotSavedNote, doc.RootElement.GetProperty("hints").GetProperty("persistence_note").GetString());
            }

            Assert.True(service.LastCollectionCaveatWriteFailed);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the mcp caveat-grant live tests.");
        return connectionString!;
    }

    /// <summary>Migrates the scratch store and runs the shipped managed-role batch whole, so the grants under
    /// test are the product's own and not a hand-copied mirror.</summary>
    private static async Task ProvisionAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);

        /* The managed roles carry fixed names and a role is cluster-wide: never adopt or drop a set that belongs
           to something else on a shared rig. */
        Assert.SkipWhen(
            await ScalarBoolAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct),
            "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");

        var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
        var batch = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
            15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
        await ExecAsync(owner, batch, ct);
    }

    private static string McpConnectionString(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Username = "mcp",
            Password = ProvisioningTestSecrets.McpPassword,
            SearchPath = "collect,config,public",
        }.ConnectionString;

    private static async Task CleanupAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await ExecAsync(cleanup,
                "DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanupCt));

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> ScalarBoolAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<List<(string Family, string Reason, DateTime FirstSeen, DateTime LastSeen)>> RowsAsync(
        NpgsqlConnection owner, CancellationToken ct)
    {
        var rows = new List<(string, string, DateTime, DateTime)>();
        await using var command = new NpgsqlCommand(
            "SELECT family, reason, first_seen_utc, last_seen_utc FROM collect.analysis_collection_caveats WHERE server_id = $1 ORDER BY family", owner);
        command.Parameters.AddWithValue(ServerId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetDateTime(2), reader.GetDateTime(3)));
        }

        return rows;
    }
}
