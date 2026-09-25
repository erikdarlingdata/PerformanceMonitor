/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>get_store_host</c> (#4214 part 2) read through the SAME least-privilege roles it runs under in
/// production — <c>mcp</c> (<c>DarlingMcpHostService</c>'s own connection role) and <c>viewer</c> (the web
/// host's <c>/api/read</c> connection role, see <c>TryBuildViewerConnectionStringFromStoredCredential</c>)
/// — never the rig's owner/superuser every other #4214 part 2 test (including this file's siblings) connects
/// as. Proves all four verdicts (<c>matches</c>, <c>stale_after_hardware_change</c>, <c>operator_override</c>,
/// <c>not_managed</c>) are reachable end to end under EACH role, so a missing grant would surface as a failed
/// row assertion here rather than as a silent gap nobody caught before a real least-privilege deployment hit
/// it.
///
/// <para>Uses distinct <c>host_mcp_test</c>/<c>host_viewer_test</c> roles (the
/// <c>DarlingSecuritySplitLiveTests</c> pattern) rather than the literal <c>mcp</c>/<c>viewer</c> names
/// <c>DarlingManagedRoles.BuildProvisioningSql</c> renders, so this test can run against a shared rig without
/// colliding with a real provisioning run. Granted the schema-level surface that provisioning gives the real
/// roles (USAGE + SELECT on <c>collect</c>, load-bearing for <c>timescaledb_information.chunks</c>
/// visibility — a role with no SELECT on a hypertable sees none of its chunks in that view). The other three
/// reads <c>GatherStoreFactsAsync</c>/<c>GatherSettingProfilesAsync</c> make — <c>pg_extension</c>,
/// <c>pg_stat_database</c>, <c>pg_settings</c> — carry no Darling-authored GRANT anywhere in
/// <c>DarlingManagedRoles</c>; PostgreSQL's own PUBLIC defaults are what let a least-privilege role read them
/// at all, and this test is what actually proves that assumption live rather than trusting it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpStoreHostToolsLiveTests
{
    private const string McpRole = "host_mcp_test";
    private const string ViewerRole = "host_viewer_test";

    /// <summary>Round-1 review, Low 6: a hardcoded password here is public in this repository, and each of
    /// these two LOGIN roles gets SELECT on every table in <c>collect</c>. Cleanup runs in <c>finally</c>, but a
    /// cleanup failure after a failed body is swallowed (<c>LiveStoreCleanup</c>), and a killed run skips
    /// cleanup outright — generating a fresh password per run means a role that outlives this test is not a
    /// known, reusable credential. Hex output is safe to interpolate directly into the DDL string below (no
    /// quoting characters). <c>DarlingSecuritySplitLiveTests</c> uses the same hardcoded-password pattern but is
    /// explicitly out of scope here.</summary>
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    [Fact]
    public async Task GetStoreHost_AsMcpAndViewerRoles_SurfacesAllFourVerdicts_Live()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to run the get_store_host least-privilege live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        await CreateTestRolesAsync(owner, ct);

        var bodySucceeded = false;
        var root = Directory.CreateTempSubdirectory("darling-storehost-roles-");
        try
        {
            var managedDir = Path.Combine(root.FullName, "managed");
            Directory.CreateDirectory(managedDir);

            /* shared_buffers is PGC_POSTMASTER (restart-only) and max_wal_size/max_connections' LIVE value
               comes from the REAL rig server this connects to, never from this fake data directory — the fake
               conf only drives AttributeManagedSetting's FILE attribution (which marker/block a name falls
               in), not the live comparison value. So "matches" is built the one way a live value CAN be set
               for a fresh session with no restart and no ALTER SYSTEM (which would misattribute to
               operator_override via postgresql.auto.conf): work_mem is PGC_USERSET, so a per-ROLE default
               (ALTER ROLE ... SET) applies at the next session start with no reload at all. max_connections
               (PGC_POSTMASTER, TargetMaxConnections = 200 - a host-independent constant) is left at whatever
               this rig already booted with, which DarlingCliCommandsHostCheckTests already proves live is
               never 200 -> stale_after_hardware_change. maintenance_work_mem is left unassigned in the fake
               conf entirely -> ClassifyVerdict's default arm, operator_override. One conf, three verdicts. */
            var (_, _, _, memory) = DarlingStoreHostProfile.GatherHostFacts();
            var derivedWorkMemMb = DarlingManagedPostgres.DeriveMemorySettings(
                DarlingManagedPostgres.QuantizeRam(memory.EffectiveBytes)).WorkMemMb;

            File.WriteAllText(Path.Combine(managedDir, "postgresql.conf"),
                DarlingManagedPostgres.ConfMarkerV4 + "\n"
                + "work_mem = 999MB\n"
                + "max_connections = 100\n");

            var managedConfig = new PostgresConfig { Managed = true, DataDirectory = managedDir };
            var byoConfig = new PostgresConfig { Managed = false };

            foreach (var role in new[] { McpRole, ViewerRole })
            {
                /* A per-role default, applied at the NEXT session's start — must run before this role's data
                   source opens its first physical connection, which is what makes the freshly-derived value
                   visible to GetStoreHost's own live pg_settings read below. */
                await using (var setDefault = new NpgsqlCommand(
                    $"ALTER ROLE {role} SET work_mem = '{derivedWorkMemMb}MB'", owner))
                {
                    await setDefault.ExecuteNonQueryAsync(ct);
                }

                await using var dataSource = NpgsqlDataSource.Create(RoleConnectionString(connectionString!, role));

                var managedJson = await DarlingMcpStoreHostTools.GetStoreHost(dataSource, managedConfig);
                using var managed = JsonDocument.Parse(managedJson);
                AssertVerdictFor(role, managed, "work_mem", "matches");
                AssertVerdictFor(role, managed, "max_connections", "stale_after_hardware_change");
                AssertVerdictFor(role, managed, "maintenance_work_mem", "operator_override");
                Assert.True(managed.RootElement.GetProperty("any_stale").GetBoolean(),
                    $"{role}: any_stale should be true with a stale_after_hardware_change row present.");

                var byoJson = await DarlingMcpStoreHostTools.GetStoreHost(dataSource, byoConfig);
                using var byo = JsonDocument.Parse(byoJson);
                AssertVerdictFor(role, byo, "work_mem", "not_managed");
                AssertVerdictFor(role, byo, "max_connections", "not_managed");
                Assert.False(byo.RootElement.GetProperty("any_stale").GetBoolean(),
                    $"{role}: a bring-your-own gather must never report any_stale.");
            }

            bodySucceeded = true;
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DropTestRolesAsync(cleanup, cleanupCt);
            });
        }
    }

    private static void AssertVerdictFor(string role, JsonDocument doc, string settingName, string expectedVerdict)
    {
        foreach (var row in doc.RootElement.GetProperty("settings").EnumerateArray())
        {
            if (row.GetProperty("name").GetString() == settingName)
            {
                Assert.True(expectedVerdict == row.GetProperty("verdict").GetString(),
                    $"{role}/{settingName}: expected verdict '{expectedVerdict}' but get_store_host returned "
                    + $"'{row.GetProperty("verdict").GetString()}' (source: {row.GetProperty("source").GetString()}).");
                return;
            }
        }

        Assert.Fail($"{role}: get_store_host returned no '{settingName}' row.");
    }

    /// <summary>Mirrors <c>DarlingSecuritySplitLiveTests.CreateTestRolesAndGrantsAsync</c>'s shape, narrowed to
    /// what <c>get_store_host</c> actually reads: no config-schema grant, no write grant — this tool only ever
    /// SELECTs.</summary>
    private static async Task CreateTestRolesAsync(NpgsqlConnection owner, System.Threading.CancellationToken ct)
    {
        var ddl = $@"
DO $do$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{McpRole}') THEN
      CREATE ROLE {McpRole} LOGIN NOSUPERUSER PASSWORD '{RolePassword}';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ViewerRole}') THEN
      CREATE ROLE {ViewerRole} LOGIN NOSUPERUSER PASSWORD '{RolePassword}';
   END IF;
END $do$;
GRANT USAGE ON SCHEMA collect TO {McpRole}, {ViewerRole};
GRANT SELECT ON ALL TABLES IN SCHEMA collect TO {McpRole}, {ViewerRole};
ALTER DEFAULT PRIVILEGES FOR ROLE {OwnerRoleOf(owner)} IN SCHEMA collect GRANT SELECT ON TABLES TO {McpRole}, {ViewerRole};";
        await using var cmd = new NpgsqlCommand(ddl, owner);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>ALTER DEFAULT PRIVILEGES keys on the role that CREATEs the object — here the connected owner.</summary>
    private static string OwnerRoleOf(NpgsqlConnection owner)
        => new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";

    private static async Task DropTestRolesAsync(NpgsqlConnection owner, System.Threading.CancellationToken ct)
        => await new LiveCleanupBatch(owner).DropRolesAsync(
            $@"
DROP OWNED BY {McpRole}, {ViewerRole};
DROP ROLE IF EXISTS {McpRole};
DROP ROLE IF EXISTS {ViewerRole};",
            [McpRole, ViewerRole],
            ct);

    private static string RoleConnectionString(string baseConnectionString, string role)
        => new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Username = role,
            Password = RolePassword,
            SearchPath = "collect,public",
        }.ConnectionString;
}
