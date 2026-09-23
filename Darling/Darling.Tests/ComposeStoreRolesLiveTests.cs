/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3914 on a real store: a cluster from the bundled runtime, initialized with <c>darling</c> as its bootstrap
/// superuser — exactly the compose file's store, whose image runs <c>initdb --username $POSTGRES_USER</c>.
/// <list type="bullet">
/// <item><description>The compose store: a same-named role the service did not create is left alone (its
/// password still works) and a superuser that is not the bootstrap one does not make the store the service's
/// own; then the service provisions the three roles, stamps its own marker, writes the credential files, and the
/// REAL web and MCP hosts — started as in a container, with their config read from <c>DARLING_CONFIG</c> — build
/// their store pools as <c>viewer</c> and <c>mcp</c>, with the statement_timeout backstop and the secret carve
/// in force. The mcp role reads a collect relation created after provisioning (the continuous-aggregate case), an
/// operator's own role keeps CONNECT, a second start re-keys nothing, and a lost credential file re-keys that
/// role alone.</description></item>
/// <item><description>Bring-your-own: the shipped <c>provision-roles.sql</c> runs clean, the two settings resolve
/// through the host's own entry (a literal and a <c>file:</c> reference) to <c>viewer</c> and <c>mcp</c>, an
/// unreadable reference keeps the surface down rather than handing it the owner, and running managed
/// provisioning over the script's roles changes no privilege at all — the script is the managed grant set.</description></item>
/// </list>
///
/// <para><b>#1776 own-store</b>: each test boots its own cluster, so neither races the shared store. In the
/// <c>darling-config-env</c> collection because the host test sets <c>DARLING_CONFIG</c> and
/// <c>DOTNET_RUNNING_IN_CONTAINER</c>, which are process-wide, and every test here publishes the process-wide
/// compose-store verdict.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class ComposeStoreRolesLiveTests
{
    private static readonly string[] Roles =
    {
        DarlingManagedPostgres.AdminRoleName, DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.McpRoleName,
    };

    [Fact]
    public async Task TheComposeStore_ProvisionsItsOwnRoles_AndTheRealHostsConnectAsViewerAndMcp_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-3914-compose-");
        var credentials = Path.Combine(root.FullName, "credentials");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);

        var previousConfig = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        var previousContainer = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER");
        var hosts = new List<BackgroundService>();
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            await using var ownerSource = NpgsqlDataSource.Create(owner);

            /* 1. A same-named role the service did not create — here one tools/provision-roles.sql made — is left
                  alone: nothing is created, nothing is written, and its password still logs in. */
            await ExecAsync(owner,
                "CREATE ROLE viewer LOGIN PASSWORD 'OperatorChose3914'; COMMENT ON ROLE viewer IS 'darling-managed';"
                + " CREATE ROLE reporting LOGIN PASSWORD 'Reporting3914';", ct);

            var foreign = await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, NullLogger.Instance, ct, credentials);
            Assert.False(foreign.Provisioned);
            Assert.Contains("'viewer' (created by tools/provision-roles.sql)", foreign.NotProvisionedReason, StringComparison.Ordinal);
            Assert.False(await RoleExistsAsync(owner, "admin", ct));
            Assert.False(await RoleExistsAsync(owner, "mcp", ct));
            Assert.False(Directory.Exists(credentials) && Directory.EnumerateFileSystemEntries(credentials).Any());
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", "OperatorChose3914"), ct));

            /* ...and the surfaces fall back to the owner, with the reason in the warning. */
            var fallback = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, owner, null, inContainer: true, foreign);
            Assert.Equal(DarlingStoreLogins.LoginSource.Owner, fallback.Source);
            Assert.EndsWith(foreign.NotProvisionedReason!, fallback.Warning, StringComparison.Ordinal);

            await ExecAsync(owner, "DROP ROLE viewer;", ct);

            /* 2. A superuser that is NOT the cluster's bootstrap superuser does not make the store the service's
                  own: that is someone's shared cluster, not the compose store. */
            await ExecAsync(owner, "CREATE ROLE intruder LOGIN SUPERUSER PASSWORD 'Intruder3914';", ct);
            await using (var intruder = NpgsqlDataSource.Create(Login(owner, "intruder", "Intruder3914")))
            {
                var notOwn = await DarlingStoreLogins.ProvisionComposeStoreAsync(intruder, owner, NullLogger.Instance, ct, credentials);
                Assert.False(notOwn.Provisioned);
                Assert.Contains("not the store's bootstrap superuser", notOwn.NotProvisionedReason, StringComparison.Ordinal);
            }

            Assert.False(await RoleExistsAsync(owner, "viewer", ct));

            /* 3. The compose store: all three roles, the service's own marker, the credential files. */
            var first = new CapturingTestLogger();
            var verdict = await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, first, ct, credentials);
            Assert.True(verdict.Provisioned, first.Joined);
            Assert.Equal(15, verdict.AppliedComposeStatementTimeoutSeconds);
            Assert.Contains("Role passwords: re-asserted for admin, viewer, mcp", first.Joined, StringComparison.Ordinal);

            foreach (var role in Roles)
            {
                Assert.Equal(DarlingManagedRoles.ComposeStoreRoleMarker, await RoleMarkerAsync(owner, role, ct));
                var file = Path.Combine(credentials, DarlingManagedRoles.ComposeStoreCredentialFileName(role));
                Assert.True(File.Exists(file), $"{file} was not written");
                Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(file), $"{file} is readable by ordinary users");
                Assert.Equal(role, await CurrentUserAsync(Login(owner, role, File.ReadAllText(file).Trim()), ct));
            }

            Assert.Empty(Directory.EnumerateFiles(credentials, "*.tmp"));

            /* Each surface's login, through the hosts' own decision. */
            var web = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, owner, null, inContainer: true, verdict);
            var mcp = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Mcp, owner, null, inContainer: true, verdict);
            Assert.Equal(DarlingStoreLogins.LoginSource.ComposeStoreRole, web.Source);
            Assert.Equal(DarlingStoreLogins.LoginSource.ComposeStoreRole, mcp.Source);
            await AssertLeastPrivilegeAsync(web.ConnectionString, "viewer", ct);
            await AssertLeastPrivilegeAsync(mcp.ConnectionString, "mcp", ct);

            /* The continuous-aggregate case: a collect relation created AFTER provisioning — the TimescaleDB step
               builds the aggregates later in the same start — is readable to the MCP tools without a restart. */
            await ExecAsync(owner, "CREATE VIEW collect.zz_3914_after_provisioning AS SELECT 42 AS answer;", ct);
            await using (var asMcp = await OpenAsync(mcp.ConnectionString, ct))
            {
                Assert.Equal(42, await ScalarAsync<int>(asMcp, "SELECT answer FROM collect.zz_3914_after_provisioning", ct));
            }

            /* An operator's own role on this store keeps CONNECT: the database-level PUBLIC revoke is left out. */
            Assert.Equal("reporting", await CurrentUserAsync(Login(owner, "reporting", "Reporting3914"), ct));

            /* 4. The REAL hosts, started as in a container with their config from DARLING_CONFIG, build their store
                  pools from what the worker published. */
            var configPath = Path.Combine(root.FullName, "darling.json");
            var webPort = DarlingManagedPostgresTests.FindFreeTcpPort();
            var mcpPort = DarlingManagedPostgresTests.FindFreeTcpPort();
            File.WriteAllText(configPath, JsonSerializer.Serialize(new
            {
                postgres = new { managed = false, connectionString = owner },
                servers = new[] { new { name = "sql-3914", host = "127.0.0.1,1", auth = "integrated" } },
                web = new { enabled = true, port = webPort },
                mcp = new { enabled = true, port = mcpPort },
            }));
            Environment.SetEnvironmentVariable("DARLING_CONFIG", configPath);
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", "true");

            var webLog = new CapturingTestLogger();
            var mcpLog = new CapturingTestLogger();
            var webHost = new DarlingWebHostService(
                new TypedLogger<DarlingWebHostService>(webLog), new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState());
            var mcpHost = new DarlingMcpHostService(
                new TypedLogger<DarlingMcpHostService>(mcpLog), new McpRuntimeState(), new MonitoredServerRegistryState());
            hosts.Add(webHost);
            hosts.Add(mcpHost);
            await webHost.StartAsync(ct);
            await mcpHost.StartAsync(ct);

            Assert.Equal("viewer", await HostPoolUserAsync(webHost, webPort, webLog, ct));
            Assert.Equal("mcp", await HostPoolUserAsync(mcpHost, mcpPort, mcpLog, ct));
            Assert.DoesNotContain("connects to the store as the owner login", webLog.Joined + mcpLog.Joined, StringComparison.Ordinal);

            /* 5. A second start re-keys nothing. */
            var second = new CapturingTestLogger();
            Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, second, ct, credentials)).Provisioned, second.Joined);
            Assert.Contains("Role passwords: unchanged", second.Joined, StringComparison.Ordinal);

            /* 6. A lost credential file (a container recreated without the volume) re-keys that role alone. */
            var viewerFile = Path.Combine(credentials, DarlingManagedRoles.ComposeStoreCredentialFileName("viewer"));
            var oldPassword = File.ReadAllText(viewerFile).Trim();
            File.Delete(viewerFile);
            var third = new CapturingTestLogger();
            Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, third, ct, credentials)).Provisioned, third.Joined);
            Assert.Contains("Role passwords: re-asserted for viewer (", third.Joined, StringComparison.Ordinal);
            var newPassword = File.ReadAllText(viewerFile).Trim();
            Assert.NotEqual(oldPassword, newPassword);
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", newPassword), ct));
            await Assert.ThrowsAsync<PostgresException>(() => CurrentUserAsync(Login(owner, "viewer", oldPassword), ct));
        }
        finally
        {
            foreach (var host in hosts)
            {
                await host.StopAsync(CancellationToken.None);
                host.Dispose();
            }

            Environment.SetEnvironmentVariable("DARLING_CONFIG", previousConfig);
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", previousContainer);
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    [Fact]
    public async Task TheBringYourOwnScript_GivesTheSettingsLoginsExactlyTheManagedGrants_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-3914-byo-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);

            /* The shipped script, as an operator runs it, with the three placeholders filled in. */
            var script = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql"))
                .Replace("CHANGE_ME_ADMIN_PASSWORD", "ScriptAdmin3914", StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", "ScriptViewer3914", StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", "ScriptMcp3914", StringComparison.Ordinal);
            await ExecAsync(owner, script, ct);

            /* The two settings through the host's own entry: a literal for the web dashboard, a file: reference for
               the MCP server, resolved when the host starts. Not in a container, so no verdict is consulted. */
            var mcpLoginFile = Path.Combine(root.FullName, "mcp-login");
            File.WriteAllText(mcpLoginFile, Login(owner, "mcp", "ScriptMcp3914") + "\n");
            var postgres = new PostgresConfig
            {
                ConnectionString = owner,
                WebConnectionString = Login(owner, "viewer", "ScriptViewer3914"),
                McpConnectionString = "file:" + mcpLoginFile,
            };

            var webLogin = await DarlingStoreLogins.ResolveUnmanagedAsync(DarlingStoreLogins.Surface.Web, postgres, NullLogger.Instance, ct);
            var mcpLogin = await DarlingStoreLogins.ResolveUnmanagedAsync(DarlingStoreLogins.Surface.Mcp, postgres, NullLogger.Instance, ct);
            await AssertLeastPrivilegeAsync(webLogin!, "viewer", ct);
            await AssertLeastPrivilegeAsync(mcpLogin!, "mcp", ct);

            /* Unset: the owner. An unreadable reference: the surface stays down, never the owner in its place. */
            Assert.Equal(owner, await DarlingStoreLogins.ResolveUnmanagedAsync(
                DarlingStoreLogins.Surface.Web, new PostgresConfig { ConnectionString = owner }, NullLogger.Instance, ct));
            var refused = new CapturingTestLogger();
            Assert.Null(await DarlingStoreLogins.ResolveUnmanagedAsync(
                DarlingStoreLogins.Surface.Mcp,
                new PostgresConfig { ConnectionString = owner, McpConnectionString = "file:" + Path.Combine(root.FullName, "missing") },
                refused, ct));
            Assert.Contains("postgres.mcpConnectionString", refused.Joined, StringComparison.Ordinal);

            /* The script IS the managed grant set: managed provisioning over the script's roles (same marker, so it
               adopts them) re-keys their passwords and changes no privilege, default privilege or role setting. The
               #3334 resolve function's EXECUTE is a function privilege, outside every snapshot below. */
            var before = await PrivilegeSnapshotAsync(owner, ct);
            await using (var ownerSource = NpgsqlDataSource.Create(owner))
            {
                await DarlingManagedRoles.EnsureProvisionedAsync(ownerSource, dataDirectory, NullLogger.Instance, ct);
            }

            var after = await PrivilegeSnapshotAsync(owner, ct);
            Assert.NotEmpty(before);
            Assert.True(
                before.SetEquals(after),
                $"managed provisioning changed what the script granted. Added: [{string.Join("; ", after.Except(before).Order())}]. "
                + $"Removed: [{string.Join("; ", before.Except(after).Order())}].");
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    private static string RequireRuntime()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe) to run the #3914 store-login proofs.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");
        return runtimeRoot!;
    }

    /// <summary>Boots the cluster (its <c>darling</c> login is the bootstrap superuser, as on the compose store)
    /// and migrates it, since provisioning names migrated tables.</summary>
    private static async Task<string> BootMigratedAsync(DarlingManagedPostgres cluster, CancellationToken ct)
    {
        var owner = new NpgsqlConnectionStringBuilder(await cluster.EnsureRunningAsync(ct)) { Pooling = false }.ConnectionString;
        await using var connection = await OpenAsync(owner, ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return owner;
    }

    /// <summary>The login connects as <paramref name="role"/>, carries the statement_timeout backstop, and is
    /// denied a secret column.</summary>
    private static async Task AssertLeastPrivilegeAsync(string login, string role, CancellationToken ct)
    {
        await using var connection = await OpenAsync(new NpgsqlConnectionStringBuilder(login) { Pooling = false }.ConnectionString, ct);
        Assert.Equal(role, await ScalarAsync<string>(connection, "SELECT current_user::text", ct));
        Assert.Equal("15s", await ScalarAsync<string>(connection, "SHOW statement_timeout", ct));

        var denied = await Assert.ThrowsAsync<PostgresException>(
            () => ScalarAsync<object>(connection, "SELECT smtp_encrypted_password FROM config.config_notification", ct));
        Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, denied.SqlState);
    }

    /// <summary>
    /// The login a running host built its store pool from: its <c>_appDataSource</c>, read once the listener is
    /// up, and asked who it is. Reflection on a private field is deliberate: the pool is the seam being proven,
    /// and no endpoint reports the store identity.
    /// </summary>
    private static async Task<string> HostPoolUserAsync(BackgroundService host, int port, CapturingTestLogger log, CancellationToken ct)
    {
        var field = host.GetType().GetField("_appDataSource", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.True(field is not null, $"{host.GetType().Name} has no _appDataSource field any more; point this test at its store pool.");

        var deadline = DateTime.UtcNow.AddSeconds(90);
        while (DateTime.UtcNow < deadline)
        {
            if (field!.GetValue(host) is NpgsqlDataSource pool && await IsListeningAsync(port, ct))
            {
                await using var connection = await pool.OpenConnectionAsync(ct);
                return await ScalarAsync<string>(connection, "SELECT current_user::text", ct);
            }

            await Task.Delay(250, ct);
        }

        Assert.Fail($"{host.GetType().Name} never started on port {port}: {log.Joined}");
        return "";
    }

    private static async Task<bool> IsListeningAsync(int port, CancellationToken ct)
    {
        using var client = new TcpClient();
        try
        {
            await client.ConnectAsync("127.0.0.1", port, ct);
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    /// <summary>Every table, column and default privilege, and every role setting, the three roles hold.</summary>
    private static async Task<HashSet<string>> PrivilegeSnapshotAsync(string owner, CancellationToken ct)
    {
        const string sql = @"
SELECT 'table ' || grantee || ' ' || privilege_type || ' ' || table_schema || '.' || table_name
FROM information_schema.role_table_grants
WHERE grantee IN ('admin', 'viewer', 'mcp')
UNION ALL
SELECT 'column ' || grantee || ' ' || privilege_type || ' ' || table_schema || '.' || table_name || '.' || column_name
FROM information_schema.column_privileges
WHERE grantee IN ('admin', 'viewer', 'mcp')
UNION ALL
SELECT 'default ' || d.defaclnamespace::regnamespace::text || ' ' || d.defaclobjtype::text || ' ' || a.acl::text
FROM pg_catalog.pg_default_acl AS d
CROSS JOIN LATERAL pg_catalog.unnest(d.defaclacl) AS a(acl)
UNION ALL
SELECT 'setting ' || r.rolname || ' ' || c.setting
FROM pg_catalog.pg_db_role_setting AS s
JOIN pg_catalog.pg_roles AS r ON r.oid = s.setrole
CROSS JOIN LATERAL pg_catalog.unnest(s.setconfig) AS c(setting)
WHERE r.rolname IN ('admin', 'viewer', 'mcp')
UNION ALL
SELECT 'connect ' || r.rolname || ' ' || pg_catalog.has_database_privilege(r.rolname, pg_catalog.current_database(), 'CONNECT')::text
FROM pg_catalog.pg_roles AS r
WHERE r.rolname IN ('admin', 'viewer', 'mcp')";

        var snapshot = new HashSet<string>(StringComparer.Ordinal);
        await using var connection = await OpenAsync(owner, ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            snapshot.Add(reader.GetString(0));
        }

        return snapshot;
    }

    private static string Login(string owner, string role, string password) =>
        new NpgsqlConnectionStringBuilder(owner) { Username = role, Password = password, Pooling = false }.ConnectionString;

    private static async Task<string> CurrentUserAsync(string login, CancellationToken ct)
    {
        await using var connection = await OpenAsync(login, ct);
        return await ScalarAsync<string>(connection, "SELECT current_user::text", ct);
    }

    private static async Task<bool> RoleExistsAsync(string owner, string role, CancellationToken ct)
    {
        await using var connection = await OpenAsync(owner, ct);
        await using var command = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1)", connection);
        command.Parameters.AddWithValue(role);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<string?> RoleMarkerAsync(string owner, string role, CancellationToken ct)
    {
        await using var connection = await OpenAsync(owner, ct);
        await using var command = new NpgsqlCommand(
            "SELECT pg_catalog.shobj_description(oid, 'pg_authid') FROM pg_catalog.pg_roles WHERE rolname = $1", connection);
        command.Parameters.AddWithValue(role);
        return await command.ExecuteScalarAsync(ct) as string;
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = await OpenAsync(connectionString, ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The hosts take a typed logger; this one forwards to a capture so a failed start says why.</summary>
    private sealed class TypedLogger<T> : ILogger<T>
    {
        private readonly CapturingTestLogger _inner;

        public TypedLogger(CapturingTestLogger inner) => _inner = inner;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => _inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => _inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            _inner.Log(logLevel, eventId, state, exception, formatter);
    }
}
