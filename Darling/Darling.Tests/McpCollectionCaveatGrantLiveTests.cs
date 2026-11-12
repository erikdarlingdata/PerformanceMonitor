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
/// #5633 item 13, as ruled: <c>collect.analysis_collection_caveats</c> is the SCHEDULED analysis pass's state. Only
/// the scheduled pass (the worker, as the service role) writes it; every on-demand pass (the MCP
/// <c>analyze_server</c> tool, with or without <c>as_of</c>) computes its caveats for its own response and never
/// writes the table, and the <c>mcp</c> role holds no write on it. These tests run the REAL managed role batch.
///
/// <para>An earlier shape granted <c>mcp</c> INSERT/UPDATE/DELETE on the table so on-demand passes could save their
/// caveats. That let an <c>as_of</c> run over a past window replace the standing rows, and let any MCP window choice
/// clear them, so it was withdrawn. The tests below fail on that shape: the census names the grants, and the
/// behaviour tests run with the grants put back (an operator's own grant, or the old shape) to show the pass itself
/// does not write.</para>
///
/// <para><b>#1776 own-store</b>: every test that touches a database does so on its own <see cref="ScratchPostgres"/>
/// database. The managed roles are cluster-wide, so the class shares the <c>pg-cluster-roles</c> collection.</para>
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class McpCollectionCaveatGrantLiveTests
{
    private const string ServerName = "darling-5633-caveat-grant";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string CaveatTable = "collect.analysis_collection_caveats";

    /// <summary>The mcp role holds NO write on the caveat table and, across every collect relation, exactly one
    /// write: INSERT on <c>analysis_findings</c>. The census is strict: every table privilege that is not a read
    /// (INSERT, UPDATE, DELETE, TRUNCATE, TRIGGER, REFERENCES, and MAINTAIN where the server has it), at table or
    /// column level, plus the sequence privileges that write (USAGE, UPDATE).</summary>
    [Fact]
    public async Task TheMcpRole_HoldsNoWriteOnTheCaveatTable_AndNoCollectWriteBeyondTheFindingsInsert_Gated()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        await ProvisionAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await using var mcp = NpgsqlDataSource.Create(McpConnectionString(scratch));
            await using var mcpConnection = await mcp.OpenConnectionAsync(ct);

            var hasMaintain = await ScalarBoolAsync(mcpConnection, "SELECT current_setting('server_version_num')::int >= 170000", ct);
            var privileges = "('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE'), ('TRIGGER'), ('REFERENCES')" + (hasMaintain ? ", ('MAINTAIN')" : "");

            var writes = new SortedSet<string>(StringComparer.Ordinal);
            await using (var census = new NpgsqlCommand($@"
SELECT c.relname, p.priv,
       CASE WHEN p.priv IN ('INSERT', 'UPDATE', 'REFERENCES')
            THEN has_table_privilege(c.oid, p.priv) OR has_any_column_privilege(c.oid, p.priv)
            ELSE has_table_privilege(c.oid, p.priv) END
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'collect'
CROSS JOIN (VALUES {privileges}) AS p(priv)
WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
UNION ALL
SELECT c.relname, p.priv, has_sequence_privilege(c.oid, p.priv)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'collect'
CROSS JOIN (VALUES ('USAGE'), ('UPDATE')) AS p(priv)
WHERE c.relkind = 'S'", mcpConnection))
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

            Assert.Equal(["analysis_findings:INSERT"], writes.ToArray());

            /* The read it does need stays: the web panel and get_collection_health read this table. */
            Assert.True(await ScalarBoolAsync(mcpConnection, $"SELECT has_table_privilege('{CaveatTable}', 'SELECT')", ct));
            foreach (var privilege in new[] { "INSERT", "UPDATE", "DELETE", "TRUNCATE", "TRIGGER", "REFERENCES" })
            {
                var columnLevel = privilege is "INSERT" or "UPDATE" or "REFERENCES"
                    ? $" OR has_any_column_privilege('{CaveatTable}', '{privilege}')"
                    : string.Empty;
                Assert.False(await ScalarBoolAsync(mcpConnection,
                    $"SELECT has_table_privilege('{CaveatTable}', '{privilege}'){columnLevel}", ct), privilege);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// The whole path through the MCP tool: <c>analyze_server</c> as the mcp role, ordinary and anchored
    /// (<c>as_of</c>), on a server whose pass reaches collection. The role is given the table writes the
    /// withdrawn shape granted, so what is under test is the pass and not the privilege: the seeded caveat rows
    /// (this server's and another's) must be exactly what they were. On the old shape the clean pass deletes them.
    /// </summary>
    [Fact]
    public async Task AnalyzeServerAsMcp_LeavesTheCaveatTableUnchanged_WithAndWithoutAsOf_EvenWithTheWritesGranted_Gated()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        await ProvisionAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await SeedServerAndCaveatsAsync(owner, ct);
            await ExecAsync(owner,
                $"GRANT INSERT, DELETE ON {CaveatTable} TO mcp; GRANT UPDATE (reason, last_seen_utc) ON {CaveatTable} TO mcp;", ct);
            var before = await SnapshotAsync(owner, ct);
            Assert.Equal(3, before.Count);

            await using var mcp = NpgsqlDataSource.Create(McpConnectionString(scratch));

            using (var doc = JsonDocument.Parse(await DarlingMcpTools.AnalyzeServer(new DarlingAnalysisService(mcp), mcp, ServerName, 1)))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
            }

            Assert.Equal(before, await SnapshotAsync(owner, ct));

            var asOf = DateTime.UtcNow.AddDays(-1).ToString("O", System.Globalization.CultureInfo.InvariantCulture);
            using (var doc = JsonDocument.Parse(await DarlingMcpTools.AnalyzeServer(new DarlingAnalysisService(mcp), mcp, ServerName, 1, asOf)))
            {
                Assert.NotEqual(JsonValueKind.Undefined, doc.RootElement.ValueKind);
            }

            Assert.Equal(before, await SnapshotAsync(owner, ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// The service itself, on the owner pool so no privilege is in the way: the default (on-demand) pass, anchored
    /// or not, leaves the table alone, and the pass the worker makes (<c>persistCollectionCaveats: true</c>) still
    /// writes it, here by clearing the standing rows of a server whose pass read every family.
    /// </summary>
    [Fact]
    public async Task OnlyThePassTheWorkerMakes_WritesTheCaveatTable_Gated()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        await ProvisionAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await SeedServerAndCaveatsAsync(owner, ct);
            var before = await SnapshotAsync(owner, ct);

            await using var ownerPool = NpgsqlDataSource.Create(scratch.ConnectionString);
            var service = new DarlingAnalysisService(ownerPool);

            await service.AnalyzeAsync(ServerId, ServerName, 1, ct);
            Assert.Equal(before, await SnapshotAsync(owner, ct));

            await service.AnalyzeAsync(ServerId, ServerName, 1, ct, asOfUtc: DateTime.UtcNow.AddDays(-1));
            Assert.Equal(before, await SnapshotAsync(owner, ct));

            await service.AnalyzeAsync(ServerId, ServerName, 1, ct, persistCollectionCaveats: true);
            var after = await SnapshotAsync(owner, ct);
            Assert.NotEqual(before, after);
            Assert.DoesNotContain(after, row => row.StartsWith(ServerId + "|zz_stale_family|", StringComparison.Ordinal));
            Assert.Contains(after, row => row.StartsWith((ServerId + 1) + "|", StringComparison.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>A server whose 8-day-old and 3-hour-old wait_stats pass the history gate (so a pass reaches
    /// collection), with two standing caveat rows for it and one for another server.</summary>
    private static async Task SeedServerAndCaveatsAsync(NpgsqlConnection owner, CancellationToken ct) =>
        await ExecAsync(owner, $@"
INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ({ServerId}, '{ServerName}', '{ServerName}', TRUE, 15, now(), now());
INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES (1, now() - interval '8 days', {ServerId}, '{ServerName}', 'AN5633_WAIT', 50, 1000),
       (2, now() - interval '3 hours', {ServerId}, '{ServerName}', 'AN5633_WAIT', 50, 1000);
INSERT INTO {CaveatTable} (server_id, family, reason, first_seen_utc, last_seen_utc)
VALUES ({ServerId}, 'zz_stale_family', 'timeout', '2026-10-01 10:00', '2026-10-01 10:05'),
       ({ServerId}, 'wait_stats', 'error', '2026-10-01 10:00', '2026-10-01 10:05'),
       ({ServerId + 1}, 'wait_stats', 'timeout', '2026-10-01 10:00', '2026-10-01 10:05');", ct);

    private static async Task<List<string>> SnapshotAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(
            $"SELECT server_id || '|' || family || '|' || reason || '|' || first_seen_utc || '|' || last_seen_utc FROM {CaveatTable} ORDER BY server_id, family", owner);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
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
                $"{LiveCleanupBatch.DropOwnedByExistingProductRolesSql} DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanupCt));

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
}
