/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8779: the shipped <c>Darling/tools/provision-roles.sql</c>, run with the runtime's own psql under
/// <c>-v ON_ERROR_STOP=1</c> by an owner that is not a superuser (<c>LOGIN NOSUPERUSER CREATEROLE</c>, owning the
/// store database). The nine superuser-only settings used to fail and psql carried on; now each catches
/// <c>insufficient_privilege</c>, the run exits 0 and one WARNING names what a superuser still has to apply. The
/// step 1 guard still stops its own cases. Gated on <c>DARLING_TEST_PGRUNTIME</c> like
/// <see cref="ComposeStoreRolesLiveTests"/>, whose helpers these tests reuse; the cluster's PostgreSQL major version
/// is whatever that runtime ships.
/// </summary>
[Collection("darling-config-env")]
public sealed class ProvisionRolesOwnerRunLiveTests
{
    private const string OwnerPassword = "ProvOwner8779R1";
    private static readonly string[] OnErrorStop = { "-v", "ON_ERROR_STOP=1" };
    private static readonly string[] Passwords = { "AdminPw8779R1", "ViewerPw8779R1", "McpPw8779R1" };

    [Fact]
    public async Task WholeScript_OnErrorStop_AsCreateRoleOwner_ExitsZeroWithOneWarning()
    {
        await RunInOwnStoreAsync(createRole: true, async (runtimeRoot, directory, superuser, ownerLogin, ct) =>
        {
            var run = await RunOwnerScriptAsync(runtimeRoot, directory, ownerLogin, ct);

            Assert.True(run.ExitCode == 0, $"psql exited {run.ExitCode}, not 0.{Environment.NewLine}{run.Error}");
            Assert.Single(SettingsWarnings(run.Error));
            foreach (var role in new[] { "admin", "viewer", "web", "mcp" })
            {
                Assert.Equal("darling-managed", await ComposeStoreRolesLiveTests.RoleMarkerAsync(superuser, role, ct));
            }
        });
    }

    [Fact]
    public async Task WholeScript_SecondRunAfterSuperuserAppliedSettings_HasNoSettingsWarning()
    {
        await RunInOwnStoreAsync(createRole: true, async (runtimeRoot, directory, superuser, ownerLogin, ct) =>
        {
            var first = await RunOwnerScriptAsync(runtimeRoot, directory, ownerLogin, ct);
            Assert.True(first.ExitCode == 0, $"the first run exited {first.ExitCode}, not 0.{Environment.NewLine}{first.Error}");
            Assert.Single(SettingsWarnings(first.Error));

            var statements = new[] { "viewer", "web", "mcp" }.SelectMany(role => new[]
            {
                $"ALTER ROLE {role} SET temp_file_limit = '1GB'",
                $"ALTER ROLE {role} SET log_min_duration_statement = '5000ms'",
                $"ALTER ROLE {role} SET log_parameter_max_length = 0",
            });
            await ComposeStoreRolesLiveTests.ExecAsync(superuser, string.Join("; ", statements), ct);

            var second = await RunOwnerScriptAsync(runtimeRoot, directory, ownerLogin, ct);
            Assert.True(second.ExitCode == 0, $"the second run exited {second.ExitCode}, not 0.{Environment.NewLine}{second.Error}");
            Assert.Empty(SettingsWarnings(second.Error));
            Assert.DoesNotContain("superuser-only settings", second.Error, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task Guard_StillStops_OwnerWithoutCreateRole()
    {
        await RunInOwnStoreAsync(createRole: false, async (runtimeRoot, directory, superuser, ownerLogin, ct) =>
        {
            /* No role exists yet (a fresh store): the guard names the missing CREATEROLE before step 1's CREATE ROLE, not PostgreSQL's own refusal. */
            var run = await RunOwnerScriptAsync(runtimeRoot, directory, ownerLogin, ct);

            Assert.True(run.ExitCode == 3, $"psql exited {run.ExitCode}, not 3.{Environment.NewLine}{run.Error}");
            Assert.True(run.Error.Contains("does not have CREATEROLE", StringComparison.Ordinal), run.Error);
            Assert.False(await ComposeStoreRolesLiveTests.RoleExistsAsync(superuser, "admin", ct), "the stop still created a role.");
        });
    }

    [Fact]
    public async Task Guard_StillStops_TrackUtilityOnWithoutSetGrant()
    {
        await RunInOwnStoreAsync(createRole: true, async (runtimeRoot, directory, superuser, ownerLogin, ct) =>
        {
            Assert.SkipUnless(
                await ComposeStoreRolesLiveTests.OwnerScalarAsync<long>(superuser, "SELECT count(*) FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.track_utility'", ct) == 1,
                "pg_stat_statements is not in shared_preload_libraries on this runtime, so there is no utility tracking to guard against.");
            await ComposeStoreRolesLiveTests.ExecAsync(superuser, "ALTER ROLE prov_owner SET pg_stat_statements.track_utility = on", ct);

            var run = await RunOwnerScriptAsync(runtimeRoot, directory, ownerLogin, ct);

            Assert.True(run.ExitCode == 3, $"psql exited {run.ExitCode}, not 3.{Environment.NewLine}{run.Error}");
            Assert.Contains("pg_stat_statements.track_utility is on", run.Error, StringComparison.Ordinal);
            Assert.False(await ComposeStoreRolesLiveTests.RoleExistsAsync(superuser, "admin", ct), "the guard's stop still created a role.");
        });
    }

    /// <summary>The shipped script with the BEFORE RUNNING substitutions a deployment makes: the four passwords, the owner role in the
    /// <c>ALTER DEFAULT PRIVILEGES FOR ROLE darling</c> lines and the database in the four <c>ON DATABASE darling</c> lines. It runs
    /// under <c>-v ON_ERROR_STOP=1</c>, so an unsubstituted owner name would stop the run at step 4.</summary>
    private static Task<(int ExitCode, string Output, string Error)> RunOwnerScriptAsync(string runtimeRoot, string directory, string ownerLogin, CancellationToken ct)
    {
        var script = File.ReadAllText(ComposeStoreRolesLiveTests.ShippedScriptPath())
            .Replace("CHANGE_ME_ADMIN_PASSWORD", Passwords[0], StringComparison.Ordinal)
            .Replace("CHANGE_ME_VIEWER_PASSWORD", Passwords[1], StringComparison.Ordinal)
            .Replace("CHANGE_ME_MCP_PASSWORD", Passwords[2], StringComparison.Ordinal)
            .Replace("CHANGE_ME_WEB_PASSWORD", "WebPw8779R1", StringComparison.Ordinal)
            .Replace("ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA ", "ALTER DEFAULT PRIVILEGES FOR ROLE prov_owner IN SCHEMA ", StringComparison.Ordinal)
            .Replace("ON DATABASE darling ", "ON DATABASE prov_store ", StringComparison.Ordinal);
        var path = Path.Combine(directory, $"provision-{Guid.NewGuid():N}.sql");
        File.WriteAllText(path, script);
        return ComposeStoreRolesLiveTests.RunPsqlScriptAsync(runtimeRoot, ownerLogin, null, path, ct, OnErrorStop);
    }

    private static string[] SettingsWarnings(string error) =>
        Regex.Matches(error, @"(?m)^.*WARNING:.*superuser-only settings.*$").Select(match => match.Value).ToArray();

    /// <summary>Boots a cluster, creates owner <c>prov_owner</c> and database <c>prov_store</c> owned by it, migrates
    /// the store, hands the owner the schemas and every relation in them, and calls <paramref name="body"/> with the
    /// runtime root, a scratch directory, the superuser and the owner's connection strings (both on
    /// <c>prov_store</c>). When pg_stat_statements is loaded the owner's utility tracking is set off at the role level
    /// (the owner may not switch it off itself), so the run reaches the settings; the guard test sets it back on.</summary>
    internal static async Task RunInOwnStoreAsync(
        bool createRole,
        Func<string, string, string, string, CancellationToken, Task> body,
        string ownerName = "prov_owner",
        string databaseName = "prov_store")
    {
        var runtimeRoot = ComposeStoreRolesLiveTests.RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-8779-owner-");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var bootstrap = await ComposeStoreRolesLiveTests.BootMigratedAsync(cluster, ct);

            await ComposeStoreRolesLiveTests.ExecAsync(
                bootstrap, $"CREATE ROLE {ownerName} LOGIN NOSUPERUSER {(createRole ? "CREATEROLE" : "NOCREATEROLE")} PASSWORD '{OwnerPassword}'", ct);
            await ComposeStoreRolesLiveTests.ExecAsync(bootstrap, $"CREATE DATABASE {databaseName} OWNER {ownerName}", ct);
            var superuser = new NpgsqlConnectionStringBuilder(bootstrap) { Database = databaseName, Pooling = false }.ConnectionString;
            await using (var connection = await ComposeStoreRolesLiveTests.OpenAsync(superuser, ct))
            {
                await PgMigrations.MigrateAsync(connection, ct);
            }

            await ComposeStoreRolesLiveTests.ExecAsync(superuser,
                ("ALTER SCHEMA collect OWNER TO prov_owner; ALTER SCHEMA config OWNER TO prov_owner;"
                + @" DO $$
DECLARE r record;
BEGIN
   FOR r IN SELECT n.nspname, c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('collect', 'config') AND c.relkind IN ('v', 'm', 'S', 'r', 'p') ORDER BY (c.relkind = 'S'), c.relname LOOP
      BEGIN
         EXECUTE format('ALTER %s %I.%I OWNER TO prov_owner',
            CASE r.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'S' THEN 'SEQUENCE' ELSE 'TABLE' END, r.nspname, r.relname);
      EXCEPTION WHEN others THEN
         RAISE NOTICE 'could not hand % to prov_owner: %', r.relname, SQLERRM;
      END;
   END LOOP;
   FOR r IN SELECT n.nspname, p.proname, p.oid::regprocedure::text AS signature FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname IN ('collect', 'config') AND p.prokind IN ('f', 'p') LOOP
      BEGIN
         EXECUTE format('ALTER ROUTINE %s OWNER TO prov_owner', r.signature);
      EXCEPTION WHEN others THEN
         RAISE NOTICE 'could not hand % to prov_owner: %', r.signature, SQLERRM;
      END;
   END LOOP;
END $$;").Replace("prov_owner", ownerName, StringComparison.Ordinal), ct);
            if (await ComposeStoreRolesLiveTests.OwnerScalarAsync<long>(
                    superuser, "SELECT count(*) FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.track_utility'", ct) == 1)
            {
                await ComposeStoreRolesLiveTests.ExecAsync(superuser, $"ALTER ROLE {ownerName} SET pg_stat_statements.track_utility = off", ct);
            }

            var ownerLogin = ComposeStoreRolesLiveTests.Login(superuser, ownerName, OwnerPassword);
            await body(runtimeRoot, root.FullName, superuser, ownerLogin, ct);
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }
}
