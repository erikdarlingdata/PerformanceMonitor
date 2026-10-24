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
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3914 on a real store: a cluster from the bundled runtime, initialized with <c>darling</c> as its bootstrap
/// superuser — exactly the compose file's store, whose image runs <c>initdb --username $POSTGRES_USER</c>.
/// <list type="bullet">
/// <item><description>The compose store: a cluster that also holds a database the service does not own is not the
/// service's own, a same-named role the service did not create is left alone (its password still works) and a
/// superuser that is not the bootstrap one does not make the store the service's
/// own; then the service provisions the three roles, stamps its own marker, writes the credential files, and the
/// REAL web and MCP hosts — started as in a container, with their config read from <c>DARLING_CONFIG</c> — build
/// their store pools as <c>viewer</c> and <c>mcp</c>, with the statement_timeout backstop and the secret carve
/// in force. The mcp role reads a collect relation created after provisioning (the continuous-aggregate case), an
/// operator's own role keeps CONNECT, a second start re-keys nothing and takes back every attribute and membership
/// someone gave the roles since, and a lost credential file re-keys that role alone.</description></item>
/// <item><description>A start that does not provision: each host stays on its role with the credential an earlier
/// start wrote, a credential the store no longer accepts keeps that surface down, only a surface with no trusted
/// credential falls back to the owner, and a credential file ordinary users can read is regenerated, never
/// read.</description></item>
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

            /* 0. A cluster that also holds a database the service does not own is not the compose store, though the
                  service logs in as its bootstrap superuser: that is an operator's own cluster (#3914 review, F2),
                  and roles are cluster-wide. Nothing is created on it and nothing is written. */
            await ExecAsync(owner, "CREATE DATABASE operator_app_3914;", ct);
            var shared = await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, NullLogger.Instance, ct, credentials);
            Assert.False(shared.Provisioned);
            Assert.Equal(
                "This cluster also holds the database 'operator_app_3914' that the service does not own, so it is not treated as the service's own and no roles were created on it.",
                shared.NotProvisionedReason);
            foreach (var role in Roles)
            {
                Assert.False(await RoleExistsAsync(owner, role, ct), $"{role} was created on a cluster that is not the service's own");
            }

            Assert.False(Directory.Exists(credentials));
            await ExecAsync(owner, "DROP DATABASE operator_app_3914;", ct);

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
            Assert.Equal(new DarlingConfig().ComposeStatementTimeoutSeconds, verdict.AppliedComposeStatementTimeoutSeconds);
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
                web = new { enabled = true, port = webPort, allowTokenlessLoopback = true },
                mcp = new { enabled = true, port = mcpPort, allowTokenlessLoopback = true },
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

            /* 5. A second start re-keys nothing, and takes back what someone gave the roles it adopts since (#3914
                  review, F5): attributes that outrank every grant, a membership that undoes the secret carve, one
                  granted by a role other than the bootstrap superuser (a plain REVOKE leaves that in place), and one
                  the member used to grant onward (a RESTRICT revoke would fail the whole batch on it). */
            await ExecAsync(owner,
                "ALTER ROLE viewer CREATEROLE CREATEDB;"
                + " GRANT pg_read_all_data TO mcp;"
                + " CREATE ROLE granter_3914 NOLOGIN; GRANT pg_monitor TO granter_3914 WITH ADMIN OPTION;"
                + " SET ROLE granter_3914; GRANT pg_monitor TO admin; RESET ROLE;"
                + " GRANT pg_signal_backend TO mcp WITH ADMIN OPTION;"
                + " SET ROLE mcp; GRANT pg_signal_backend TO reporting; RESET ROLE;", ct);
            await using (var poisoned = await OpenAsync(new NpgsqlConnectionStringBuilder(mcp.ConnectionString) { Pooling = false }.ConnectionString, ct))
            {
                /* The control: with pg_read_all_data the carve is gone, so this would have stayed that way. */
                await ScalarAsync<long>(poisoned, "SELECT count(smtp_encrypted_password) FROM config.config_notification", ct);
            }

            var second = new CapturingTestLogger();
            Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, second, ct, credentials)).Provisioned, second.Joined);
            Assert.Contains("Role passwords: unchanged", second.Joined, StringComparison.Ordinal);
            Assert.False(await OwnerScalarAsync<bool>(
                owner, "SELECT bool_or(rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls OR rolsuper) FROM pg_catalog.pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp')", ct));
            Assert.Equal(0L, await OwnerScalarAsync<long>(
                owner, "SELECT count(*) FROM pg_catalog.pg_auth_members AS a JOIN pg_catalog.pg_roles AS m ON m.oid = a.member WHERE m.rolname IN ('admin', 'viewer', 'mcp')", ct));
            Assert.False(await OwnerScalarAsync<bool>(owner, "SELECT pg_catalog.pg_has_role('reporting', 'pg_signal_backend', 'MEMBER')", ct));
            await AssertLeastPrivilegeAsync(mcp.ConnectionString, "mcp", ct);
            await AssertLeastPrivilegeAsync(web.ConnectionString, "viewer", ct);

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

    /// <summary>
    /// #3914 review, F1 and F6, on a real store. A start that does not provision the roles — refused here because the
    /// service reaches the store as a superuser that is not its bootstrap one, or a collector that stood down first —
    /// keeps each host on its role with the credential an earlier start wrote, through the hosts' own entry. A
    /// credential the store no longer accepts keeps that surface down with the store's error; only a surface with no
    /// trusted credential falls back to the owner, and says why; and a credential file ordinary users can read is
    /// regenerated by the next provisioning, never read.
    /// </summary>
    [Fact]
    public async Task AStartThatDoesNotProvision_KeepsEachSurfaceOnItsRole_WithAnEarlierStartsCredential_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-3914-earlier-");
        var credentials = Path.Combine(root.FullName, "credentials");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);

        var previousContainer = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER");
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            await using var ownerSource = NpgsqlDataSource.Create(owner);
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", "true");
            var postgres = new PostgresConfig { ConnectionString = owner };

            /* An earlier start provisioned the roles and wrote their credential files. */
            var earlier = new CapturingTestLogger();
            Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, earlier, ct, credentials)).Provisioned, earlier.Joined);

            /* 1. This start is refused. Each surface connects as its role with that credential, not as the owner. */
            await ExecAsync(owner, "CREATE ROLE intruder LOGIN SUPERUSER PASSWORD 'Intruder3914';", ct);
            await using (var intruder = NpgsqlDataSource.Create(Login(owner, "intruder", "Intruder3914")))
            {
                var refused = await DarlingStoreLogins.ProvisionComposeStoreAsync(intruder, owner, NullLogger.Instance, ct, credentials);
                Assert.False(refused.Provisioned);
                Assert.Equal(credentials, refused.CredentialDirectory);
            }

            var refusedLog = new CapturingTestLogger();
            var web = await DarlingStoreLogins.ResolveUnmanagedAsync(DarlingStoreLogins.Surface.Web, postgres, refusedLog, ct);
            var mcp = await DarlingStoreLogins.ResolveUnmanagedAsync(DarlingStoreLogins.Surface.Mcp, postgres, refusedLog, ct);
            Assert.True(web is not null && mcp is not null, refusedLog.Joined);
            await AssertLeastPrivilegeAsync(web!, "viewer", ct);
            await AssertLeastPrivilegeAsync(mcp!, "mcp", ct);
            Assert.Contains("The web dashboard connects to the store as the viewer role with the credential an earlier start provisioned", refusedLog.Joined, StringComparison.Ordinal);
            Assert.Contains("The MCP server connects to the store as the mcp role with the credential an earlier start provisioned", refusedLog.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("connects to the store as the owner login", refusedLog.Joined, StringComparison.Ordinal);

            /* 2. A collector that stood down before provisioning: the same. The verdict is the one a stand-down settles,
                  pointed at this test's directory (DarlingStoreLoginsTests pins that a real one names the shipped
                  directory). */
            DarlingStoreLogins.PublishComposeStoreVerdict(DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned(
                "The collector stopped before it could provision the store's roles (Store: connection refused).", credentials));
            var stoodDown = new CapturingTestLogger();
            var afterStandDown = await DarlingStoreLogins.ResolveUnmanagedAsync(DarlingStoreLogins.Surface.Web, postgres, stoodDown, ct);
            Assert.True(afterStandDown is not null, stoodDown.Joined);
            Assert.Equal("viewer", await CurrentUserAsync(afterStandDown!, ct));

            /* 3. The role re-keyed by hand since: the store refuses that credential, and the surface stays down with the
                  store's error. It never becomes the owner. mcp, untouched, still connects. */
            await ExecAsync(owner, "ALTER ROLE viewer PASSWORD 'ChangedByHand3914';", ct);
            var rekeyed = new CapturingTestLogger();
            Assert.Null(await DarlingStoreLogins.ResolveUnmanagedAsync(DarlingStoreLogins.Surface.Web, postgres, rekeyed, ct));
            Assert.Contains("did not accept the viewer login an earlier start provisioned", rekeyed.Joined, StringComparison.Ordinal);
            Assert.Contains(PostgresErrorCodes.InvalidPassword, rekeyed.Joined, StringComparison.Ordinal);
            Assert.NotNull(await DarlingStoreLogins.ResolveUnmanagedAsync(DarlingStoreLogins.Surface.Mcp, postgres, NullLogger.Instance, ct));

            /* 4. No trusted credential for a surface: that one falls back to the owner, with both reasons. */
            var mcpFile = Path.Combine(credentials, DarlingManagedRoles.ComposeStoreCredentialFileName("mcp"));
            File.Delete(mcpFile);
            var fallback = new CapturingTestLogger();
            Assert.Equal(owner, await DarlingStoreLogins.ResolveUnmanagedAsync(DarlingStoreLogins.Surface.Mcp, postgres, fallback, ct));
            Assert.Contains("The MCP server connects to the store as the owner login", fallback.Joined, StringComparison.Ordinal);
            Assert.Contains("The collector stopped before it could provision the store's roles", fallback.Joined, StringComparison.Ordinal);
            Assert.Contains($"No mcp credential from an earlier start can stand in: {mcpFile} does not exist.", fallback.Joined, StringComparison.Ordinal);

            /* 5. F6: a credential file ordinary local users can read is never read. The next provisioning regenerates it
                  and re-asserts the role, and the new file is owner-only from the moment it exists. */
            var viewerFile = Path.Combine(credentials, DarlingManagedRoles.ComposeStoreCredentialFileName("viewer"));
            var exposed = new FileInfo(viewerFile).GetAccessControl();
            exposed.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null), FileSystemRights.Read, AccessControlType.Allow));
            new FileInfo(viewerFile).SetAccessControl(exposed);
            var exposedPassword = File.ReadAllText(viewerFile).Trim();

            var next = new CapturingTestLogger();
            Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, next, ct, credentials)).Provisioned, next.Joined);
            Assert.Contains("'viewer' credential", next.Joined, StringComparison.Ordinal);
            Assert.Contains("is not trusted (ordinary local users can read it)", next.Joined, StringComparison.Ordinal);
            Assert.Contains("Role passwords: re-asserted for", next.Joined, StringComparison.Ordinal);
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(viewerFile), $"{viewerFile} is readable by ordinary users");
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(mcpFile), $"{mcpFile} is readable by ordinary users");
            var regenerated = File.ReadAllText(viewerFile).Trim();
            Assert.NotEqual(exposedPassword, regenerated);
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", regenerated), ct));
            Assert.Equal("mcp", await CurrentUserAsync(Login(owner, "mcp", File.ReadAllText(mcpFile).Trim()), ct));
            await Assert.ThrowsAsync<PostgresException>(() => CurrentUserAsync(Login(owner, "viewer", exposedPassword), ct));
        }
        finally
        {
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", previousContainer);
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// #4004's review, round 3 (M), on a real store. A directory planted at <c>pg-admin-credential</c>, the first name
    /// the discard tries, stopped round 2's discard before it reached the viewer and mcp passwords planted after it, and
    /// the directory went back to 0777; an operator who closed it and removed that directory got a start that re-asserted
    /// the planted passwords on their roles. Now the start that finds it open removes every planted file and keeps the
    /// directory 0700, provisions the roles with new passwords while writing none there, and the start after the
    /// directory is removed re-asserts none of the planted passwords. Driven through the Unix-mode seam.
    /// </summary>
    [Fact]
    public async Task PlantedPasswordsBehindADirectoryAtTheFirstName_AreUsedByNoLaterStart_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-4004-shielded-");
        var credentials = Path.Combine(root.FullName, "credentials");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);

        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            await using var ownerSource = NpgsqlDataSource.Create(owner);

            /* While the directory was 0777, someone put a directory at the first name and passwords at the others. */
            Directory.CreateDirectory(credentials);
            var adminPath = Path.Combine(credentials, DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.AdminRoleName));
            var planted = new[]
            {
                (Role: DarlingManagedPostgres.ViewerRoleName, Path: Path.Combine(credentials, DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.ViewerRoleName)), Password: "Planted4004ViewerPassword"),
                (Role: DarlingManagedPostgres.McpRoleName, Path: Path.Combine(credentials, DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.McpRoleName)), Password: "Planted4004McpPassword"),
            };
            Directory.CreateDirectory(Path.Combine(adminPath, "someones"));
            foreach (var file in planted)
            {
                StandInUnixModes.WriteOwnerOnly(file.Path, file.Password);
            }

            var modes = new StandInUnixModes();
            modes.Report(credentials, "777");

            /* This start: provisioning finds the directory open. */
            var first = new CapturingTestLogger();
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, first, ct, credentials)).Provisioned, first.Joined);
            }

            Assert.Equal("700", modes.Octal(credentials));
            Assert.All(planted, file => Assert.False(File.Exists(file.Path), $"the planted '{file.Role}' password outlived the start that found its directory open"));
            Assert.Contains($"could not be removed, so nothing in it is used this start: {adminPath} (", first.Joined, StringComparison.Ordinal);

            /* The operator removes what the refusal named, and restarts. */
            Directory.Delete(adminPath, recursive: true);
            var next = new CapturingTestLogger();
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, next, ct, credentials)).Provisioned, next.Joined);
            }

            foreach (var file in planted)
            {
                var written = File.ReadAllText(file.Path).Trim();
                Assert.NotEqual(file.Password, written);
                Assert.Equal(file.Role, await CurrentUserAsync(Login(owner, file.Role, written), ct));
                await Assert.ThrowsAsync<PostgresException>(() => CurrentUserAsync(Login(owner, file.Role, file.Password), ct));
            }

            var admin = File.ReadAllText(adminPath).Trim();
            Assert.Equal(DarlingManagedPostgres.AdminRoleName, await CurrentUserAsync(Login(owner, DarlingManagedPostgres.AdminRoleName, admin), ct));
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// #4004's review, round 2 (M1), on a real store. A start whose store stood down never provisions and never loads
    /// the log-hash key, but a host's earlier-credential read still sets an open credentials directory 0700. Round 1
    /// kept the "was open" verdict in memory only, so the NEXT start found the directory owner-only, re-asserted a
    /// planted admin password on the admin role and loaded a planted key. Now the host's read removes both, and the
    /// next start's provisioning and key load use neither. Driven through the Unix-mode seam (the directory is reported
    /// as 0777 at the host's read).
    /// </summary>
    [Fact]
    public async Task APlantedPasswordAndKey_InADirectoryAHostFoundOpen_AreUsedByNoLaterStart_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-4004-planted-");
        var credentials = Path.Combine(root.FullName, "credentials");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);

        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            await using var ownerSource = NpgsqlDataSource.Create(owner);

            /* An earlier start provisioned the roles and wrote their files. */
            var earlier = new CapturingTestLogger();
            Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, earlier, ct, credentials)).Provisioned, earlier.Joined);

            /* While the directory was 0777, someone replaced the admin password and planted a key. */
            var adminFile = Path.Combine(credentials, DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.AdminRoleName));
            var keyFile = Path.Combine(credentials, DarlingLogHashKeyFile.FileName);
            const string plantedAdmin = "Planted4004AdminPassword";
            File.Delete(adminFile);
            StandInUnixModes.WriteOwnerOnly(adminFile, plantedAdmin);
            var plantedKey = Convert.ToBase64String(TestLogHashKeys.FixedMaterial);
            StandInUnixModes.WriteOwnerOnly(keyFile, OperatingSystem.IsWindows() ? DarlingSecrets.Protect(plantedKey) : plantedKey);
            var modes = new StandInUnixModes();
            modes.Report(credentials, "777");

            /* This start: the store stood down, so a host read its earlier credential and nothing else ran. */
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                Assert.Null(DarlingManagedRoles.ReadEarlierComposeCredential(credentials, DarlingManagedPostgres.AdminRoleName, NullLogger.Instance).Password);
            }

            Assert.Equal("700", modes.Octal(credentials));

            /* The next start: provisioning, then the key load, on the directory this start left owner-only. */
            DarlingLogHashKeyLoad load;
            var next = new CapturingTestLogger();
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, next, ct, credentials)).Provisioned, next.Joined);
                load = DarlingLogHashKeyFile.Load(credentials, NullLogger.Instance);
            }

            var admin = File.ReadAllText(adminFile).Trim();
            Assert.NotEqual(plantedAdmin, admin);
            Assert.Equal("admin", await CurrentUserAsync(Login(owner, "admin", admin), ct));
            await Assert.ThrowsAsync<PostgresException>(() => CurrentUserAsync(Login(owner, "admin", plantedAdmin), ct));

            Assert.True(load.Generated, "the next start loaded the key planted while the directory was open");
            Assert.NotEqual(TestLogHashKeys.Fixed.RawLineHash("x"), load.Key!.RawLineHash("x"));
        }
        finally
        {
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

            /* The shipped script, as an operator runs it, with the three placeholders filled in. Npgsql sends text to
               the server and does not understand psql's meta-commands (the \set lines that save, set and restore
               ON_ERROR_STOP around the role-collision guard, #4746), so those lines are dropped and nothing else is. */
            var script = WithoutPsqlMetaCommands(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
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

    /// <summary>
    /// #4746 through psql itself. The Npgsql script test above drops the script's <c>\set</c> lines and would stop at
    /// the guard's <c>RAISE</c> without them, so it cannot tell whether the lines that make psql stop are there. This
    /// runs the shipped script with the runtime's own psql against a cluster that already has a <c>viewer</c> login the
    /// script did not create (no <c>darling-managed</c> marker). psql must exit 3, the guard's <c>DO</c> block must have
    /// left nothing behind (no <c>admin</c> role, which it would have created before it reached <c>viewer</c>), and
    /// <c>viewer</c>'s password hash must be the one it had: the <c>ALTER ROLE ... PASSWORD</c> lines after the guard
    /// never ran.
    /// </summary>
    [Fact]
    public async Task TheBringYourOwnScript_RunByPsql_StopsAtAnUnmarkedRole_AndChangesNothing_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-4746-psql-guard-");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);

            await ExecAsync(owner, "CREATE ROLE viewer LOGIN NOSUPERUSER PASSWORD 'PlantedViewer4746'", ct);
            const string HashSql = "SELECT rolpassword FROM pg_catalog.pg_authid WHERE rolname = 'viewer'";
            var before = await OwnerScalarAsync<string>(owner, HashSql, ct);
            Assert.False(string.IsNullOrEmpty(before));
            Assert.Null(await RoleMarkerAsync(owner, "viewer", ct));

            var run = await RunPsqlScriptAsync(runtimeRoot, owner, database: null, ShippedScriptPath(), ct);

            Assert.True(run.ExitCode == 3,
                $"psql exited {run.ExitCode}, not 3: the script did not stop at the unmarked viewer role.{Environment.NewLine}{run.Error}");
            Assert.Contains("Role \"viewer\" already exists and was not created by Darling", run.Error, StringComparison.Ordinal);
            Assert.False(await RoleExistsAsync(owner, "admin", ct), "the guard's DO block left the admin role behind.");
            Assert.False(await RoleExistsAsync(owner, "mcp", ct), "the script created the mcp role after the guard refused viewer.");
            Assert.Equal(before, await OwnerScalarAsync<string>(owner, HashSql, ct));
            Assert.Null(await RoleMarkerAsync(owner, "viewer", ct));
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// The caller's own <c>ON_ERROR_STOP</c> survives the guard (#4746). Run against the cluster's <c>postgres</c>
    /// database, which has no store schema, the script's grants fail one after another. Left alone, psql goes on
    /// through them and exits 0. Started with <c>-v ON_ERROR_STOP=1</c>, it must stop at the first one and exit 3:
    /// the guard puts the caller's setting back after it, instead of switching the stop off.
    /// </summary>
    [Fact]
    public async Task TheBringYourOwnScript_RunByPsql_KeepsTheCallersOnErrorStop_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-4746-psql-stop-");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);

            var goesOn = await RunPsqlScriptAsync(runtimeRoot, owner, "postgres", ShippedScriptPath(), ct);
            Assert.True(goesOn.ExitCode == 0,
                $"psql exited {goesOn.ExitCode} on a run that asked for no stop: the script's own errors are meant to be carried on past.{Environment.NewLine}{goesOn.Error}");
            Assert.Contains("ERROR:", goesOn.Error, StringComparison.Ordinal);
            Assert.True(await RoleExistsAsync(owner, "viewer", ct), "the first run did not get through the guard and create the roles.");

            var stops = await RunPsqlScriptAsync(runtimeRoot, owner, "postgres", ShippedScriptPath(), ct, "-v", "ON_ERROR_STOP=1");
            Assert.True(stops.ExitCode == 3,
                $"psql exited {stops.ExitCode}, not 3, with -v ON_ERROR_STOP=1: the script switched the caller's stop off.{Environment.NewLine}{stops.Error}");
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// When the guard stops the run, the server logs the failed statement, its whole text (<c>log_min_error_statement</c>
    /// defaults to error). The guard used to create the roles with their password literals, so that line held all three
    /// new passwords, and the documented remedy re-runs with the same ones. Now the guard carries none: the log has the
    /// guard's error and no password. The control below proves this log capture would have shown one: a failing
    /// statement with a literal in it lands in the same files.
    /// </summary>
    [Fact]
    public async Task TheBringYourOwnScript_WhenTheGuardStopsTheRun_LeavesNoPasswordInTheServerLog_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-roles-log-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            await ExecAsync(owner, "ALTER SYSTEM SET log_min_error_statement = 'error'", ct);
            await ExecAsync(owner, "SELECT pg_reload_conf()", ct);
            await ExecAsync(owner, "CREATE ROLE viewer LOGIN NOSUPERUSER PASSWORD 'PlantedViewerR1'", ct);

            var passwords = new[] { "AdminPwGuardR1x", "ViewerPwGuardR1x", "McpPwGuardR1x" };
            var run = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, passwords, ct);
            Assert.True(run.ExitCode == 3, $"psql exited {run.ExitCode}, not 3.{Environment.NewLine}{run.Error}");
            Assert.Contains("Role \"viewer\" already exists and was not created by Darling", run.Error, StringComparison.Ordinal);

            /* The control: a failing statement with a literal is written to the log with that literal. */
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(owner, "SELECT 'LogControlR1' FROM no_such_table_r1", ct));
            var log = await WaitForServerLogAsync(dataDirectory, new[] { "LogControlR1", "already exists and was not created by Darling" }, ct);

            foreach (var password in passwords)
            {
                Assert.DoesNotContain(password, log, StringComparison.Ordinal);
            }
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// An owner that is not a superuser (a login with <c>CREATEROLE</c> that owns the store), with pg_stat_statements
    /// tracking utility statements for it. Without a superuser or a SET grant, step 0 cannot turn tracking off, so
    /// pg_stat_statements would record every password statement verbatim: the script stops at the guard instead, with
    /// no role created and no password anywhere in pg_stat_statements. With the grant it runs, and a re-run with new
    /// passwords really changes them (the ALTER ROLE lines used to name NOSUPERUSER, which fails for such an owner on
    /// every version, so nothing was ever rotated while psql exited 0). A superuser's later rotation is not refused
    /// for the owner's ADMIN OPTION on the roles it created.
    /// </summary>
    [Fact]
    public async Task ANonSuperuserOwner_StopsAtTheGuardWithoutTheSetGrant_AndProvisionsAndRotatesWithIt_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-roles-owner-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            Assert.SkipUnless(await OwnerScalarAsync<long>(owner, "SELECT count(*) FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.track_utility'", ct) == 1,
                "pg_stat_statements is not loaded in this cluster, so there is no utility tracking to guard against.");
            await ExecAsync(owner, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);
            var byo = await MakeNonSuperuserOwnerAsync(owner, ct);
            await ExecAsync(owner, "SELECT pg_stat_statements_reset()", ct);

            var first = new[] { "AdminPwOwnerR1a", "ViewerPwOwnerR1a", "McpPwOwnerR1a" };
            var stopped = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, first, ct);
            Assert.True(stopped.ExitCode == 3, $"psql exited {stopped.ExitCode}, not 3, for an owner that cannot turn utility tracking off.{Environment.NewLine}{stopped.Error}");
            Assert.Contains("pg_stat_statements.track_utility is on", stopped.Error, StringComparison.Ordinal);
            Assert.Contains("GRANT SET ON PARAMETER pg_stat_statements.track_utility", stopped.Error, StringComparison.Ordinal);
            foreach (var role in Roles)
            {
                Assert.False(await RoleExistsAsync(owner, role, ct), $"the guard's stop still created {role}.");
            }

            await AssertNoPasswordInStatementStatisticsAsync(owner, first, ct);

            await ExecAsync(owner, "GRANT SET ON PARAMETER pg_stat_statements.track_utility TO byo", ct);
            var provisioned = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, first, ct);
            Assert.True(provisioned.ExitCode == 0, $"psql exited {provisioned.ExitCode} for the owner that was granted SET.{Environment.NewLine}{provisioned.Error}");
            Assert.DoesNotContain("pg_stat_statements.track_utility is on", provisioned.Error, StringComparison.Ordinal);
            const string HashSql = "SELECT rolpassword FROM pg_catalog.pg_authid WHERE rolname = 'viewer'";
            var firstHash = await OwnerScalarAsync<string>(owner, HashSql, ct);
            Assert.False(string.IsNullOrEmpty(firstHash), "the run did not set viewer's password.");
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", first[1]), ct));
            await AssertNoPasswordInStatementStatisticsAsync(owner, first, ct);

            var second = new[] { "AdminPwOwnerR1b", "ViewerPwOwnerR1b", "McpPwOwnerR1b" };
            var rotated = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, second, ct);
            Assert.True(rotated.ExitCode == 0, $"psql exited {rotated.ExitCode}.{Environment.NewLine}{rotated.Error}");
            Assert.NotEqual(firstHash, await OwnerScalarAsync<string>(owner, HashSql, ct));
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", second[1]), ct));
            await Assert.ThrowsAsync<PostgresException>(() => CurrentUserAsync(Login(owner, "viewer", first[1]), ct));
            await AssertNoPasswordInStatementStatisticsAsync(owner, second, ct);

            /* Adopted roles that picked up attributes and a membership since, run by this owner: it takes back what its own
               privileges allow (CREATEROLE, which it holds and has ADMIN OPTION to change) and says so for the rest
               (CREATEDB, which it does not hold, and a membership granted by a superuser in a role it has no ADMIN OPTION on). */
            await ExecAsync(owner, "ALTER ROLE viewer CREATEROLE CREATEDB; ALTER ROLE mcp CREATEROLE; CREATE ROLE plain_r1 NOLOGIN; GRANT plain_r1 TO viewer;", ct);
            var partial = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, second, ct);
            Assert.True(partial.ExitCode == 0, $"psql exited {partial.ExitCode}.{Environment.NewLine}{partial.Error}");
            Assert.False(await OwnerScalarAsync<bool>(owner, "SELECT bool_or(rolcreaterole) FROM pg_catalog.pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp')", ct),
                "the owner did not take back CREATEROLE, which its own privileges allow.");
            Assert.Contains("Role \"viewer\" still has the CREATEDB attribute", partial.Error, StringComparison.Ordinal);
            Assert.Contains("Role \"viewer\" is still a member of \"plain_r1\"", partial.Error, StringComparison.Ordinal);
            Assert.True(await OwnerScalarAsync<bool>(owner, "SELECT rolcreatedb FROM pg_catalog.pg_roles WHERE rolname = 'viewer'", ct));

            /* A first run by this owner, then a superuser's rotation: the owner holds ADMIN OPTION on the roles it made,
               and it is the database owner, so the guard does not refuse. The superuser's run takes back the rest. */
            var third = new[] { "AdminPwOwnerR1c", "ViewerPwOwnerR1c", "McpPwOwnerR1c" };
            var superuserRun = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, third, ct);
            Assert.True(superuserRun.ExitCode == 0, $"a superuser's rotation after the owner's first run was refused (exit {superuserRun.ExitCode}).{Environment.NewLine}{superuserRun.Error}");
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", third[1]), ct));
            Assert.False(await OwnerScalarAsync<bool>(owner, "SELECT rolcreatedb FROM pg_catalog.pg_roles WHERE rolname = 'viewer'", ct));
            Assert.Equal(0L, await OwnerScalarAsync<long>(
                owner, "SELECT count(*) FROM pg_catalog.pg_auth_members AS a JOIN pg_catalog.pg_roles AS m ON m.oid = a.member WHERE m.rolname IN ('admin', 'viewer', 'mcp')", ct));
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// The blanket <c>GRANT SELECT ON ALL TABLES IN SCHEMA config</c> and the secret-column carve are one transaction.
    /// (1) A store without <c>config.config_notification_routes</c> (below V131), run with <c>-v ON_ERROR_STOP=1</c>:
    /// the carve used to stop at the routes REVOKE and leave mcp's whole carve unapplied; now the pair is skipped, the
    /// carve commits, and neither viewer nor mcp can read a secret column afterwards. (2) A statement in the carve that
    /// fails (a column the list names is gone), with the stop on and with it off: nothing from the transaction
    /// commits, so neither role holds the blanket SELECT the carve was meant to narrow.
    /// </summary>
    [Fact]
    public async Task TheCarve_CommitsWithItsBlanketGrant_OrNotAtAll_WithOrWithoutStopOnError_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-roles-carve-");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            var passwords = new[] { "AdminPwCarveR1", "ViewerPwCarveR1", "McpPwCarveR1" };

            /* 1. Below V131, with the stop on. The run still ends at the script's later routes grant for mcp (exit 3),
                  after the carve has committed. */
            await ExecAsync(owner, "DROP TABLE config.config_notification_routes CASCADE", ct);
            var stopOn = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, passwords, ct, "-v", "ON_ERROR_STOP=1");
            Assert.True(stopOn.ExitCode == 3, $"psql exited {stopOn.ExitCode}, not 3.{Environment.NewLine}{stopOn.Error}");
            foreach (var role in new[] { "viewer", "mcp" })
            {
                await AssertCarveAppliedAsync(owner, role, skipTable: "config_notification_routes", ct);
            }

            /* 2. A carve statement that fails. */
            await ExecAsync(owner, "DROP OWNED BY admin, viewer, mcp CASCADE; DROP ROLE admin, viewer, mcp;", ct);
            await ExecAsync(owner, "ALTER TABLE config.config_command RENAME COLUMN service_instance TO service_instance_renamed", ct);
            foreach (var extra in new[] { new[] { "-v", "ON_ERROR_STOP=1" }, Array.Empty<string>() })
            {
                var failed = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, passwords, ct, extra);
                Assert.Contains("service_instance", failed.Error, StringComparison.Ordinal);
                foreach (var role in new[] { "viewer", "mcp" })
                {
                    Assert.False(await OwnerScalarAsync<bool>(owner, $"SELECT pg_catalog.has_table_privilege('{role}', 'config.config_alert_settings', 'SELECT')", ct),
                        $"{role} holds the blanket SELECT on config after a failed carve (run {(extra.Length == 0 ? "without" : "with")} stop on error).");
                    Assert.False(await OwnerScalarAsync<bool>(owner, $"SELECT pg_catalog.has_column_privilege('{role}', 'config.config_notification', 'smtp_encrypted_password', 'SELECT')", ct),
                        $"{role} can read a secret column after a failed carve.");
                    Assert.False(await OwnerScalarAsync<bool>(owner, $"SELECT pg_catalog.has_schema_privilege('{role}', 'config', 'USAGE')", ct),
                        $"{role} kept USAGE on config from a transaction that did not commit.");
                }

                await ExecAsync(owner, "DROP OWNED BY admin, viewer, mcp CASCADE; DROP ROLE admin, viewer, mcp;", ct);
            }
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// The marker is a public string, so a login with <c>CREATEROLE</c> can pre-create <c>viewer</c> with it: from
    /// PostgreSQL 16 that login holds ADMIN OPTION on the role it created, and kept it after the operator's run re-keyed
    /// and granted the role. The script now refuses a marked role another non-superuser holds ADMIN OPTION on, before it
    /// re-keys or grants anything. A plain membership (<c>GRANT viewer TO some_analyst</c>) is not refused. And an
    /// adopted role, which keeps whatever it was given since, loses what managed provisioning takes back:
    /// <c>CREATEROLE</c>, <c>CREATEDB</c>, <c>REPLICATION</c>, <c>BYPASSRLS</c> and every membership, including one
    /// another role granted and one the member granted onward.
    /// </summary>
    [Fact]
    public async Task AMarkedRoleAdministeredElsewhere_IsRefused_APlainMembershipIsNot_AndAnAdoptedRoleLosesWhatManagedTakesBack_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-roles-marked-");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            Assert.SkipUnless(await OwnerScalarAsync<int>(owner, "SELECT current_setting('server_version_num')::int", ct) >= 160000,
                "A CREATEROLE login holds ADMIN OPTION on the roles it creates from PostgreSQL 16.");
            const string HashSql = "SELECT coalesce(rolpassword, '') FROM pg_catalog.pg_authid WHERE rolname = 'viewer'";

            /* 1. Another login creates viewer with the marker. */
            await ExecAsync(owner, "CREATE ROLE other_holder_r1 LOGIN CREATEROLE NOSUPERUSER PASSWORD 'OtherHolderPwR1'", ct);
            await ExecAsync(Login(owner, "other_holder_r1", "OtherHolderPwR1"),
                "CREATE ROLE viewer LOGIN; COMMENT ON ROLE viewer IS 'darling-managed';", ct);
            Assert.True(await OwnerScalarAsync<bool>(owner,
                "SELECT bool_or(a.admin_option) FROM pg_catalog.pg_auth_members a JOIN pg_catalog.pg_roles m ON m.oid = a.member JOIN pg_catalog.pg_roles g ON g.oid = a.roleid WHERE m.rolname = 'other_holder_r1' AND g.rolname = 'viewer'", ct),
                "the other login does not hold ADMIN OPTION on the role it created: this server does not give it one, so the scenario proves nothing.");
            var claimed = await OwnerScalarAsync<string>(owner, HashSql, ct);

            var stopped = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, new[] { "AdminPwMarkedR1", "ViewerPwMarkedR1", "McpPwMarkedR1" }, ct);
            Assert.True(stopped.ExitCode == 3, $"psql exited {stopped.ExitCode}, not 3: the script did not refuse the marked role.{Environment.NewLine}{stopped.Error}");
            Assert.Contains("Role \"viewer\" carries the 'darling-managed' marker", stopped.Error, StringComparison.Ordinal);
            Assert.Contains("other_holder_r1", stopped.Error, StringComparison.Ordinal);
            Assert.Equal(claimed, await OwnerScalarAsync<string>(owner, HashSql, ct));
            Assert.False(await RoleExistsAsync(owner, "admin", ct), "the refusal left the admin role behind.");
            Assert.False(await OwnerScalarAsync<bool>(owner, "SELECT pg_catalog.has_schema_privilege('viewer', 'config', 'USAGE')", ct), "the refused role was granted privileges.");

            /* 2. Not refused: the same run once nobody else administers the roles, and a plain membership afterwards. */
            await ExecAsync(owner, "DROP ROLE viewer; DROP ROLE other_holder_r1;", ct);
            var provisioned = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, new[] { "AdminPwMarkedR1b", "ViewerPwMarkedR1b", "McpPwMarkedR1b" }, ct);
            Assert.True(provisioned.ExitCode == 0, $"psql exited {provisioned.ExitCode}.{Environment.NewLine}{provisioned.Error}");
            var provisionedHash = await OwnerScalarAsync<string>(owner, HashSql, ct);
            await ExecAsync(owner, "CREATE ROLE some_analyst_r1 NOLOGIN; GRANT viewer TO some_analyst_r1;", ct);
            var rerun = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, new[] { "AdminPwMarkedR1c", "ViewerPwMarkedR1c", "McpPwMarkedR1c" }, ct);
            Assert.True(rerun.ExitCode == 0, $"a plain membership in viewer made the script refuse it (exit {rerun.ExitCode}).{Environment.NewLine}{rerun.Error}");
            Assert.NotEqual(provisionedHash, await OwnerScalarAsync<string>(owner, HashSql, ct));

            /* 3. An adopted role loses what managed provisioning takes back (the compose test's own plant). */
            await ExecAsync(owner,
                "ALTER ROLE viewer CREATEROLE CREATEDB REPLICATION BYPASSRLS;"
                + " ALTER ROLE mcp CREATEROLE;"
                + " GRANT pg_read_all_data TO mcp;"
                + " CREATE ROLE granter_r1 NOLOGIN; GRANT pg_monitor TO granter_r1 WITH ADMIN OPTION;"
                + " SET ROLE granter_r1; GRANT pg_monitor TO admin; RESET ROLE;"
                + " GRANT pg_signal_backend TO mcp WITH ADMIN OPTION;"
                + " CREATE ROLE reporting_r1 NOLOGIN;"
                + " SET ROLE mcp; GRANT pg_signal_backend TO reporting_r1; RESET ROLE;", ct);
            Assert.True(await OwnerScalarAsync<bool>(owner, "SELECT bool_or(rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls) FROM pg_catalog.pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp')", ct));
            var adopted = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, new[] { "AdminPwMarkedR1d", "ViewerPwMarkedR1d", "McpPwMarkedR1d" }, ct);
            Assert.True(adopted.ExitCode == 0, $"psql exited {adopted.ExitCode}.{Environment.NewLine}{adopted.Error}");
            Assert.False(await OwnerScalarAsync<bool>(
                owner, "SELECT bool_or(rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls OR rolsuper) FROM pg_catalog.pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp')", ct),
                "an adopted role kept an attribute managed provisioning takes back.");
            Assert.Equal(0L, await OwnerScalarAsync<long>(
                owner, "SELECT count(*) FROM pg_catalog.pg_auth_members AS a JOIN pg_catalog.pg_roles AS m ON m.oid = a.member WHERE m.rolname IN ('admin', 'viewer', 'mcp')", ct));
            Assert.False(await OwnerScalarAsync<bool>(owner, "SELECT pg_catalog.pg_has_role('reporting_r1', 'pg_signal_backend', 'MEMBER')", ct));
            Assert.True(await OwnerScalarAsync<bool>(owner, "SELECT pg_catalog.pg_has_role('some_analyst_r1', 'viewer', 'MEMBER')", ct),
                "the strip took back a membership IN a script role, which the twin leaves alone.");
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// A superuser does the first run, then the owner (a login with <c>CREATEROLE</c> that is not a superuser and owns the
    /// store) runs the script with new passwords. From PostgreSQL 16 the owner holds no ADMIN OPTION on roles a superuser
    /// made, so each <c>ALTER ROLE ... PASSWORD</c> line failed, the server wrote the failed statement with its password to
    /// its log, no password changed, and psql exited 0. The guard now stops the run first: exit 3, the roles and the GRANT
    /// statements named in the message, no password sent and none in the server log. The statements the message prints
    /// (run by a superuser) let the same run through, and it changes the passwords. An owner that re-runs the script over
    /// roles it created itself holds ADMIN OPTION on them and is not stopped.
    /// </summary>
    [Fact]
    public async Task ANonSuperuserOwner_OverRolesASuperuserMade_StopsAtTheGuard_LogsNoPassword_AndPassesWithAdminOption_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-roles-admin-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            Assert.SkipUnless(await OwnerScalarAsync<int>(owner, "SELECT current_setting('server_version_num')::int", ct) >= 160000,
                "A CREATEROLE login holds ADMIN OPTION only on the roles it creates, and needs it to change another role, from PostgreSQL 16.");
            await ExecAsync(owner, "ALTER SYSTEM SET log_min_error_statement = 'error'", ct);
            await ExecAsync(owner, "SELECT pg_reload_conf()", ct);
            const string HashSql = "SELECT coalesce(rolpassword, '') FROM pg_catalog.pg_authid WHERE rolname = 'viewer'";

            /* 1. A superuser's first run makes the roles, and the store then belongs to the non-superuser owner. */
            var first = new[] { "AdminPwNoAdminR1a", "ViewerPwNoAdminR1a", "McpPwNoAdminR1a" };
            var superuserRun = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, first, ct);
            Assert.True(superuserRun.ExitCode == 0, $"psql exited {superuserRun.ExitCode} for the superuser's first run.{Environment.NewLine}{superuserRun.Error}");
            var firstHash = await OwnerScalarAsync<string>(owner, HashSql, ct);
            Assert.NotEqual(string.Empty, firstHash);
            var byo = await MakeNonSuperuserOwnerAsync(owner, ct);
            await ExecAsync(owner, "ALTER ROLE byo SET pg_stat_statements.track_utility = off", ct);
            Assert.False(await OwnerScalarAsync<bool>(owner,
                "SELECT pg_catalog.pg_has_role('byo', 'viewer', 'MEMBER WITH ADMIN OPTION')", ct),
                "the owner already holds ADMIN OPTION on a role a superuser made, so the scenario proves nothing.");

            /* 2. The owner's run with new passwords stops at the guard, sends no password and logs none. */
            var second = new[] { "AdminPwNoAdminR1b", "ViewerPwNoAdminR1b", "McpPwNoAdminR1b" };
            var stopped = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, second, ct);
            Assert.True(stopped.ExitCode == 3, $"psql exited {stopped.ExitCode}, not 3: the owner's run got past the guard.{Environment.NewLine}{stopped.Error}");
            Assert.Contains("does not hold ADMIN OPTION on these roles: admin, mcp, viewer", stopped.Error, StringComparison.Ordinal);
            Assert.DoesNotContain("permission denied to alter role", stopped.Error, StringComparison.Ordinal);
            Assert.Equal(firstHash, await OwnerScalarAsync<string>(owner, HashSql, ct));
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", first[1]), ct));

            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(owner, "SELECT 'LogControlNoAdminR1' FROM no_such_table_r1", ct));
            var log = await WaitForServerLogAsync(dataDirectory, new[] { "LogControlNoAdminR1", "does not hold ADMIN OPTION on these roles" }, ct);
            foreach (var password in second)
            {
                Assert.DoesNotContain(password, log, StringComparison.Ordinal);
            }

            /* 3. The statements the message prints, run by a superuser, are the remedy. */
            var grants = System.Text.RegularExpressions.Regex.Match(stopped.Error, @"run this script again: (?<sql>GRANT [^\r\n]*)");
            Assert.True(grants.Success, $"the stop does not print the GRANT statements.{Environment.NewLine}{stopped.Error}");
            Assert.Contains("GRANT viewer TO byo WITH ADMIN OPTION;", grants.Groups["sql"].Value, StringComparison.Ordinal);
            await ExecAsync(owner, grants.Groups["sql"].Value, ct);
            var granted = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, second, ct);
            Assert.True(granted.ExitCode == 0, $"psql exited {granted.ExitCode} after the superuser granted ADMIN OPTION.{Environment.NewLine}{granted.Error}");
            Assert.NotEqual(firstHash, await OwnerScalarAsync<string>(owner, HashSql, ct));
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", second[1]), ct));
            await Assert.ThrowsAsync<PostgresException>(() => CurrentUserAsync(Login(owner, "viewer", first[1]), ct));

            /* 4. Roles the owner made itself: it holds ADMIN OPTION on them, so its re-run is not stopped and rotates them. */
            await ExecAsync(owner, "DROP OWNED BY admin, viewer, mcp CASCADE; DROP ROLE admin, viewer, mcp;", ct);
            var third = new[] { "AdminPwNoAdminR1c", "ViewerPwNoAdminR1c", "McpPwNoAdminR1c" };
            var created = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, third, ct);
            Assert.True(created.ExitCode == 0, $"psql exited {created.ExitCode} for the owner's own first run.{Environment.NewLine}{created.Error}");
            var thirdHash = await OwnerScalarAsync<string>(owner, HashSql, ct);
            Assert.NotEqual(string.Empty, thirdHash);
            Assert.True(await OwnerScalarAsync<bool>(owner, "SELECT pg_catalog.pg_has_role('byo', 'viewer', 'MEMBER WITH ADMIN OPTION')", ct),
                "the owner does not hold ADMIN OPTION on the role it created.");
            var fourth = new[] { "AdminPwNoAdminR1d", "ViewerPwNoAdminR1d", "McpPwNoAdminR1d" };
            var rerun = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, fourth, ct);
            Assert.True(rerun.ExitCode == 0, $"the owner's re-run over roles it created was stopped (exit {rerun.ExitCode}).{Environment.NewLine}{rerun.Error}");
            Assert.DoesNotContain("ADMIN OPTION on these roles", rerun.Error, StringComparison.Ordinal);
            Assert.NotEqual(thirdHash, await OwnerScalarAsync<string>(owner, HashSql, ct));
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", fourth[1]), ct));
            Assert.Equal("mcp", await CurrentUserAsync(Login(owner, "mcp", fourth[2]), ct));
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// From PostgreSQL 16 a revoke removes only the membership row recorded with the grantor it names, and warns, without an
    /// error, when it finds none. The owner holds ADMIN OPTION on a role it created, but a membership in that role that a
    /// superuser granted is recorded with the superuser as grantor, so a plain revoke by the owner left it in place and a
    /// role that reads every table survived the run. The guard now also asks that the owner can act as the recorded grantor.
    /// (1) The owner cannot: exit 3 at the guard, no password sent, the membership still there. (2) The owner can (it is a
    /// member of the grantor): the run revokes the membership GRANTED BY that grantor, and the roles keep none.
    /// </summary>
    [Fact]
    public async Task ARunnerWithAdminOption_ButNotTheRecordedGrantor_StopsAtTheGuard_AndRevokesOnceItCanActAsTheGrantor_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-roles-grantor-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            Assert.SkipUnless(await OwnerScalarAsync<int>(owner, "SELECT current_setting('server_version_num')::int", ct) >= 160000,
                "Before PostgreSQL 16 a revoke ignores the grantor, so a membership cannot outlast it.");
            const string HashSql = "SELECT coalesce(rolpassword, '') FROM pg_catalog.pg_authid WHERE rolname = 'viewer'";
            const string MembershipSql = "SELECT count(*) FROM pg_catalog.pg_auth_members AS a JOIN pg_catalog.pg_roles m ON m.oid = a.member WHERE m.rolname IN ('admin', 'viewer', 'mcp')";

            /* 1. The owner makes the three roles (so it holds ADMIN OPTION on them) and a role of its own, G. A superuser
                  then puts G in pg_read_all_data and viewer in G: both rows are recorded with the superuser as grantor. */
            var byo = await MakeNonSuperuserOwnerAsync(owner, ct);
            await ExecAsync(owner, "ALTER ROLE byo SET pg_stat_statements.track_utility = off", ct);
            var first = new[] { "AdminPwGrantorR2a", "ViewerPwGrantorR2a", "McpPwGrantorR2a" };
            var created = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, first, ct);
            Assert.True(created.ExitCode == 0, $"psql exited {created.ExitCode} for the owner's first run.{Environment.NewLine}{created.Error}");
            var firstHash = await OwnerScalarAsync<string>(owner, HashSql, ct);
            await ExecAsync(byo, "CREATE ROLE g_grantor_r2 NOLOGIN", ct);
            await ExecAsync(owner, "GRANT pg_read_all_data TO g_grantor_r2; GRANT g_grantor_r2 TO viewer;", ct);
            Assert.True(await OwnerScalarAsync<bool>(owner, "SELECT pg_catalog.pg_has_role('byo', 'g_grantor_r2', 'MEMBER WITH ADMIN OPTION')", ct),
                "the owner does not hold ADMIN OPTION on the role it created, so the scenario proves nothing.");
            Assert.Equal("darling", await OwnerScalarAsync<string>(owner,
                "SELECT b.rolname FROM pg_catalog.pg_auth_members a JOIN pg_catalog.pg_roles g ON g.oid = a.roleid JOIN pg_catalog.pg_roles m ON m.oid = a.member JOIN pg_catalog.pg_roles b ON b.oid = a.grantor WHERE g.rolname = 'g_grantor_r2' AND m.rolname = 'viewer'", ct));
            Assert.False(await OwnerScalarAsync<bool>(owner, "SELECT pg_catalog.pg_has_role('byo', 'darling', 'USAGE')", ct));
            Assert.True(await OwnerScalarAsync<bool>(owner, "SELECT pg_catalog.pg_has_role('viewer', 'pg_read_all_data', 'MEMBER')", ct));

            /* 2. The owner cannot act as the grantor: the run stops at the guard, sends no password, and the membership stays. */
            var second = new[] { "AdminPwGrantorR2b", "ViewerPwGrantorR2b", "McpPwGrantorR2b" };
            var stopped = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, second, ct);
            Assert.True(stopped.ExitCode == 3, $"psql exited {stopped.ExitCode}, not 3: the run got past a membership it cannot revoke.{Environment.NewLine}{stopped.Error}");
            Assert.Contains("These roles are members of roles whose privileges outrank the grants below: viewer in g_grantor_r2", stopped.Error, StringComparison.Ordinal);
            Assert.Equal(firstHash, await OwnerScalarAsync<string>(owner, HashSql, ct));
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", first[1]), ct));
            Assert.Equal(1L, await OwnerScalarAsync<long>(owner, MembershipSql, ct));

            /* 3. Once the owner can act as the grantor, the guard passes, the revoke names the grantor, and no membership is left. */
            await ExecAsync(owner, "GRANT darling TO byo", ct);
            var third = new[] { "AdminPwGrantorR2c", "ViewerPwGrantorR2c", "McpPwGrantorR2c" };
            var revoked = await RunFilledScriptAsync(runtimeRoot, root.FullName, byo, null, third, ct);
            Assert.True(revoked.ExitCode == 0, $"psql exited {revoked.ExitCode} for an owner that can act as the grantor.{Environment.NewLine}{revoked.Error}");
            Assert.NotEqual(firstHash, await OwnerScalarAsync<string>(owner, HashSql, ct));
            Assert.Equal(0L, await OwnerScalarAsync<long>(owner, MembershipSql, ct));
            Assert.False(await OwnerScalarAsync<bool>(owner, "SELECT pg_catalog.pg_has_role('viewer', 'pg_read_all_data', 'MEMBER')", ct));
            Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", third[1]), ct));
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// The other reasons a role that is not a superuser cannot change the roles, on any PostgreSQL version: it has no
    /// <c>CREATEROLE</c>, or one of the three roles is a superuser. Each used to fail at the <c>ALTER ROLE ... PASSWORD</c>
    /// lines and write the passwords to the server log while psql exited 0. Each now stops at the guard (exit 3), changes
    /// nothing and logs no password. Before PostgreSQL 16 a role that has REPLICATION stops the run the same way.
    /// </summary>
    [Fact]
    public async Task ARunnerThatCannotChangeTheRoles_StopsAtTheGuard_AndLogsNoPassword_ForEachReason_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-roles-cannot-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            var version = await OwnerScalarAsync<int>(owner, "SELECT current_setting('server_version_num')::int", ct);
            await ExecAsync(owner, "ALTER SYSTEM SET log_min_error_statement = 'error'", ct);
            await ExecAsync(owner, "SELECT pg_reload_conf()", ct);
            const string HashSql = "SELECT coalesce(rolpassword, '') FROM pg_catalog.pg_authid WHERE rolname = 'viewer'";

            var first = new[] { "AdminPwCannotR1a", "ViewerPwCannotR1a", "McpPwCannotR1a" };
            var superuserRun = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, first, ct);
            Assert.True(superuserRun.ExitCode == 0, $"psql exited {superuserRun.ExitCode} for the superuser's first run.{Environment.NewLine}{superuserRun.Error}");
            var firstHash = await OwnerScalarAsync<string>(owner, HashSql, ct);
            var byo = await MakeNonSuperuserOwnerAsync(owner, ct);
            var database = new NpgsqlConnectionStringBuilder(owner).Database!;
            await ExecAsync(owner,
                "ALTER ROLE byo SET pg_stat_statements.track_utility = off;"
                + " CREATE ROLE plain_r1 LOGIN NOSUPERUSER PASSWORD 'PlainPwR1';"
                + " ALTER ROLE plain_r1 SET pg_stat_statements.track_utility = off;"
                + $" GRANT CONNECT ON DATABASE \"{database}\" TO plain_r1;", ct);
            var plain = Login(owner, "plain_r1", "PlainPwR1");
            var passwords = new List<string>();

            async Task AssertStopAsync(string login, string reason, string expected, string tag)
            {
                var words = new[] { $"AdminPw{tag}R1", $"ViewerPw{tag}R1", $"McpPw{tag}R1" };
                passwords.AddRange(words);
                var run = await RunFilledScriptAsync(runtimeRoot, root.FullName, login, null, words, ct);
                Assert.True(run.ExitCode == 3, $"psql exited {run.ExitCode}, not 3, for {reason}.{Environment.NewLine}{run.Error}");
                Assert.Contains(expected, run.Error, StringComparison.Ordinal);
                Assert.DoesNotContain("permission denied to alter role", run.Error, StringComparison.Ordinal);
                Assert.Equal(firstHash, await OwnerScalarAsync<string>(owner, HashSql, ct));
            }

            /* 1. No CREATEROLE. (Held over roles the owner has no ADMIN OPTION on, so no other check answers first.) */
            await AssertStopAsync(plain, "a role without CREATEROLE",
                "is not a superuser and does not have CREATEROLE", "NoCreate");

            /* 2. A role that is a superuser. The owner has CREATEROLE, so the superuser check is the one that stops it. */
            await ExecAsync(owner, "ALTER ROLE viewer SUPERUSER", ct);
            await AssertStopAsync(byo, "a role that is a superuser", "These roles are superusers: viewer", "Super");
            await ExecAsync(owner, "ALTER ROLE viewer NOSUPERUSER", ct);

            /* 3. Before PostgreSQL 16, a role that has REPLICATION. From 16 the owner may change it (with ADMIN OPTION). */
            if (version < 160000)
            {
                await ExecAsync(owner, "ALTER ROLE mcp REPLICATION", ct);
                await AssertStopAsync(byo, "a role that has REPLICATION before PostgreSQL 16", "These roles have the REPLICATION attribute: mcp", "Repl");
                await ExecAsync(owner, "ALTER ROLE mcp NOREPLICATION", ct);
            }

            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(owner, "SELECT 'LogControlCannotR1' FROM no_such_table_r1", ct));
            var log = await WaitForServerLogAsync(dataDirectory,
                new[] { "LogControlCannotR1", "is not a superuser and does not have CREATEROLE", "These roles are superusers: viewer" }, ct);
            foreach (var password in passwords)
            {
                Assert.DoesNotContain(password, log, StringComparison.Ordinal);
            }
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// A password literal reaches the server log on a run that succeeds, too, when a logging setting records statement text.
    /// For a superuser, step 0 turns each of them off for the session, so the log holds no password. Each setting is put on the
    /// login as its default, so it applies from the session's start (no reload to wait for). The control for each is a
    /// statement from a session of its own that the same setting logs, with its literal: it shows the log would have held a
    /// password too.
    /// </summary>
    [Fact]
    public async Task ASuperuserRun_WithStatementLoggingOn_LeavesNoPasswordInTheServerLog_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-roles-logging-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            var superuser = new NpgsqlConnectionStringBuilder(owner).Username!;
            var login = $"\"{superuser.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

            var phases = new[]
            {
                (Tag: "Ddl", Settings: "SET log_statement = 'ddl'", Control: "CREATE ROLE control_ddl_r1 NOLOGIN PASSWORD 'LogControlDdlR1'", Logged: "LogControlDdlR1"),
                (Tag: "Dur", Settings: "SET log_min_duration_statement = 0", Control: "SELECT 'LogControlDurR1'", Logged: "LogControlDurR1"),
                (Tag: "Smp", Settings: "SET log_min_duration_sample = 0", Control: "SELECT 'LogControlSmpR1'", Logged: "LogControlSmpR1"),
                (Tag: "Trn", Settings: "SET log_transaction_sample_rate = 1", Control: "SELECT 'LogControlTrnR1'", Logged: "LogControlTrnR1"),
            };
            var passwords = new List<string>();
            var expected = new List<string>();
            foreach (var phase in phases)
            {
                await ExecAsync(owner, $"ALTER ROLE {login} RESET ALL", ct);
                await ExecAsync(owner, $"ALTER ROLE {login} {phase.Settings}", ct);
                if (phase.Tag == "Smp")
                {
                    await ExecAsync(owner, $"ALTER ROLE {login} SET log_statement_sample_rate = 1", ct);
                }

                var controlPath = Path.Combine(root.FullName, $"control-{phase.Tag}.sql");
                File.WriteAllText(controlPath, phase.Control + ";" + Environment.NewLine);
                var control = await RunPsqlScriptAsync(runtimeRoot, owner, null, controlPath, ct);
                Assert.True(control.ExitCode == 0, $"the {phase.Tag} control statement failed.{Environment.NewLine}{control.Error}");
                expected.Add(phase.Logged);

                var words = new[] { $"AdminPwLog{phase.Tag}R1", $"ViewerPwLog{phase.Tag}R1", $"McpPwLog{phase.Tag}R1" };
                passwords.AddRange(words);
                var run = await RunFilledScriptAsync(runtimeRoot, root.FullName, owner, null, words, ct);
                Assert.True(run.ExitCode == 0, $"psql exited {run.ExitCode} in the {phase.Tag} run.{Environment.NewLine}{run.Error}");
                Assert.Equal("viewer", await CurrentUserAsync(Login(owner, "viewer", words[1]), ct));
                await ExecAsync(owner, $"ALTER ROLE {login} RESET ALL", ct);
            }

            var log = await WaitForServerLogAsync(dataDirectory, expected.ToArray(), ct);
            foreach (var password in passwords)
            {
                Assert.DoesNotContain(password, log, StringComparison.Ordinal);
            }
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>Writes the shipped script with its three password placeholders replaced (in a file under
    /// <paramref name="directory"/>) and runs it with the runtime's own psql.</summary>
    private static async Task<(int ExitCode, string Output, string Error)> RunFilledScriptAsync(
        string runtimeRoot, string directory, string login, string? database, string[] passwords, CancellationToken ct, params string[] extraArguments)
    {
        var script = File.ReadAllText(ShippedScriptPath())
            .Replace("CHANGE_ME_ADMIN_PASSWORD", passwords[0], StringComparison.Ordinal)
            .Replace("CHANGE_ME_VIEWER_PASSWORD", passwords[1], StringComparison.Ordinal)
            .Replace("CHANGE_ME_MCP_PASSWORD", passwords[2], StringComparison.Ordinal);
        var path = Path.Combine(directory, $"provision-{Guid.NewGuid():N}.sql");
        File.WriteAllText(path, script);
        return await RunPsqlScriptAsync(runtimeRoot, login, database, path, ct, extraArguments);
    }

    /// <summary>A login with <c>CREATEROLE</c> that is not a superuser and owns the store (the database, its schemas and
    /// every table, view and sequence in them), the shape the script's header calls "the database owner". pg_stat_statements
    /// tracks utility statements for it whatever the cluster says. Returns its connection string.</summary>
    private static async Task<string> MakeNonSuperuserOwnerAsync(string superuser, CancellationToken ct)
    {
        var database = new NpgsqlConnectionStringBuilder(superuser).Database!;
        await ExecAsync(superuser,
            "CREATE ROLE byo LOGIN CREATEROLE NOSUPERUSER PASSWORD 'ByoOwnerPwR1';"
            + $" ALTER DATABASE \"{database}\" OWNER TO byo;"
            + " ALTER SCHEMA collect OWNER TO byo; ALTER SCHEMA config OWNER TO byo;"
            + @" DO $$
DECLARE r record;
BEGIN
   FOR r IN SELECT n.nspname, c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('collect', 'config') AND c.relkind IN ('v', 'm', 'S', 'r', 'p') ORDER BY (c.relkind = 'S'), c.relname LOOP
      BEGIN
         EXECUTE format('ALTER %s %I.%I OWNER TO byo',
            CASE r.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'S' THEN 'SEQUENCE' ELSE 'TABLE' END, r.nspname, r.relname);
      EXCEPTION WHEN others THEN
         NULL;
      END;
   END LOOP;
END $$;
ALTER ROLE byo SET pg_stat_statements.track_utility = on;", ct);
        return Login(superuser, "byo", "ByoOwnerPwR1");
    }

    /// <summary>No statement pg_stat_statements holds names one of the passwords.</summary>
    private static async Task AssertNoPasswordInStatementStatisticsAsync(string superuser, string[] passwords, CancellationToken ct)
    {
        foreach (var password in passwords)
        {
            Assert.Equal(0L, await OwnerScalarAsync<long>(superuser, $"SELECT count(*) FROM pg_stat_statements WHERE query LIKE '%{password}%'", ct));
        }
    }

    /// <summary><paramref name="role"/> reads no secret column of any carved config table and reads a non-secret one
    /// (the carve is applied, and the blanket grant it narrows is not what is left). <paramref name="skipTable"/> is
    /// a carved table this store does not have.</summary>
    private static async Task AssertCarveAppliedAsync(string owner, string role, string skipTable, CancellationToken ct)
    {
        foreach (var acl in DarlingManagedRoles.ViewerRestrictedConfigTables.Where(a => a.Table != skipTable))
        {
            foreach (var secret in acl.SecretColumns)
            {
                Assert.False(await OwnerScalarAsync<bool>(owner,
                    $"SELECT pg_catalog.has_column_privilege('{role}', 'config.{acl.Table}', '{secret}', 'SELECT')", ct),
                    $"{role} can read config.{acl.Table}.{secret} after the script ran.");
            }

            /* #5750: EVERY non-secret column, not the first, so a column a later rung adds (the route scope columns, V181) is
               proven readable by the viewer and mcp roles after the migration pass and the provisioning that follows it. */
            /* #6230: the mcp role is not granted the columns DarlingManagedRoles.McpWithheldColumns names (result_json reaches it only
               through config.read_mcp_command_result). */
            var withheld = string.Equals(role, "mcp", StringComparison.Ordinal)
                && DarlingManagedRoles.McpWithheldColumns.TryGetValue(acl.Table, out var withheldColumns)
                    ? withheldColumns
                    : Array.Empty<string>();
            foreach (var column in acl.NonSecretColumns.Where(c => !withheld.Contains(c, StringComparer.Ordinal)))
            {
                Assert.True(await OwnerScalarAsync<bool>(owner,
                    $"SELECT pg_catalog.has_column_privilege('{role}', 'config.{acl.Table}', '{column}', 'SELECT')", ct),
                    $"{role} cannot read config.{acl.Table}.{column}: the carve was not applied.");
            }
        }
    }

    /// <summary>The whole text of every log the cluster writes (pg_ctl's start log next to the data directory and the
    /// logging collector's files), once every one of <paramref name="expected"/> is in it or 30 seconds have passed. The
    /// collector writes on its own schedule, so a text the server has already logged can take a moment to reach the file.</summary>
    private static async Task<string> WaitForServerLogAsync(string dataDirectory, string[] expected, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        string text;
        do
        {
            var builder = new System.Text.StringBuilder();
            var files = new List<string> { Path.Combine(Path.GetDirectoryName(dataDirectory) ?? dataDirectory, DarlingManagedPostgres.ServerLogFileName) };
            var collector = Path.Combine(dataDirectory, "log");
            if (Directory.Exists(collector))
            {
                files.AddRange(Directory.EnumerateFiles(collector));
            }

            foreach (var file in files)
            {
                try
                {
                    using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                    using var reader = new StreamReader(stream);
                    builder.AppendLine(await reader.ReadToEndAsync(ct));
                }
                catch (IOException)
                {
                    /* A file the server is rotating or has not created: read what is there on the next pass. */
                }
            }

            text = builder.ToString();
            if (expected.All(e => text.Contains(e, StringComparison.Ordinal)) || DateTime.UtcNow >= deadline)
            {
                break;
            }

            await Task.Delay(500, ct);
        }
        while (true);

        foreach (var e in expected)
        {
            Assert.True(text.Contains(e, StringComparison.Ordinal), $"the server log never showed '{e}', so a missing password there would prove nothing.");
        }

        return text;
    }

    /// <summary>The script as the build copies it next to the tests.</summary>
    private static string ShippedScriptPath() => Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql");

    /// <summary>Runs the runtime's own <c>psql</c> (<c>-X</c>: no psqlrc) on a script file against the cluster the
    /// owner connection string names, and returns its exit code and output. <paramref name="database"/> overrides the
    /// owner's database. The child is ours: it is killed if the run is cancelled.</summary>
    private static async Task<(int ExitCode, string Output, string Error)> RunPsqlScriptAsync(
        string runtimeRoot, string owner, string? database, string script, CancellationToken ct, params string[] extraArguments)
    {
        var psql = Path.Combine(runtimeRoot, "pgsql", "bin", "psql.exe");
        Assert.True(File.Exists(psql), $"{psql} is missing from the runtime.");
        var connection = new NpgsqlConnectionStringBuilder(owner);
        var start = new System.Diagnostics.ProcessStartInfo(psql)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (var argument in new[]
        {
            "-X", "-w",
            "-h", connection.Host!,
            "-p", connection.Port.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "-U", connection.Username!,
            "-d", database ?? connection.Database!,
        }.Concat(extraArguments).Concat(new[] { "-f", script }))
        {
            start.ArgumentList.Add(argument);
        }

        if (!string.IsNullOrEmpty(connection.Password))
        {
            start.Environment["PGPASSWORD"] = connection.Password;
        }

        using var process = System.Diagnostics.Process.Start(start)!;
        try
        {
            var output = process.StandardOutput.ReadToEndAsync(ct);
            var error = process.StandardError.ReadToEndAsync(ct);
            await process.WaitForExitAsync(ct);
            return (process.ExitCode, await output, await error);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
    }

    /// <summary>Answers <see cref="IAlertDeliverer"/> without sending anything (#3580 requires both members
    /// answered by hand): the tests below read fired state back from <see cref="CustomAlertStateStore"/> instead
    /// of capturing a delivery.</summary>
    private sealed class NoopDeliverer : IAlertDeliverer
    {
        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            await DeliverAsync(outcome, cancellationToken);
            return null;
        }
    }

    /// <summary>
    /// #3970: the custom-alert evaluator admits this start's compose verdict beside the managed DPAPI gate, and
    /// the resulting VIEWER pool — carrying the statement_timeout backstop and denied the secret column, exactly
    /// like the web dashboard's own login (<see cref="AssertLeastPrivilegeAsync"/>) — is enough to read a
    /// composed metric and fire a rule, the one thing <see cref="CustomAlertEvaluator"/> ever asks of it (rules,
    /// state and history all stay on the owner pool, as the real worker builds them).
    /// </summary>
    [Fact]
    public async Task TheComposeStore_CustomAlertEvaluator_ConnectsAsViewer_AndEvaluatesARule_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-3970-evaluator-");
        var credentials = Path.Combine(root.FullName, "credentials");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            await using var ownerSource = NpgsqlDataSource.Create(owner);

            var provisioning = new CapturingTestLogger();
            var verdict = await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, provisioning, ct, credentials);
            Assert.True(verdict.Provisioned, provisioning.Joined);

            var viewerLogin = await DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync(owner, NullLogger.Instance, ct);
            Assert.Equal(verdict.ViewerConnectionString, viewerLogin);
            await AssertLeastPrivilegeAsync(viewerLogin!, "viewer", ct);

            const int serverId = 993970;
            var storage = "car_3970_srv_" + Guid.NewGuid().ToString("N");
            var ruleName = "car_3970_" + Guid.NewGuid().ToString("N");
            // Gauge metric (avg over 15m), one seeded row, fire on the first breach -- the same deterministic
            // shape CustomAlertSeverityEscalationLiveTests uses -- scoped to exactly this server.
            var definition =
                "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"hours\":0.25}," +
                "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25}," +
                "\"hysteresis\":{\"breachSamples\":1,\"clearSamples\":1}," +
                "\"scope\":{\"mode\":\"servers\",\"servers\":[\"" + storage + "\"]}}";

            await using (var register = ownerSource.CreateCommand(
                "INSERT INTO servers (server_id, server_name, display_name) VALUES ($1, $2, $2)"))
            {
                register.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                register.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storage });
                await register.ExecuteNonQueryAsync(ct);
            }

            await using (var seed = ownerSource.CreateCommand(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, (now() AT TIME ZONE 'UTC'), $2, $3, (now() AT TIME ZONE 'UTC'), 60, 0)"))
            {
                seed.Parameters.Add(new NpgsqlParameter<long> { TypedValue = DateTime.UtcNow.Ticks });
                seed.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                seed.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storage });
                await seed.ExecuteNonQueryAsync(ct);
            }

            var rules = new CustomAlertRuleStore(ownerSource, StorageCommandDeadlines.McpReadSeconds);
            var state = new CustomAlertStateStore(ownerSource, StorageCommandDeadlines.McpReadSeconds);
            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(ruleName, null, definition, true, "test", ct));
            var ruleId = created.Rule!.Id;
            var version = created.Rule!.Version;

            /* The real construction (DarlingWorker): rules/state/history on the OWNER pool, only the metric READ
               on the resolved compose viewer pool. */
            await using var viewerSource = NpgsqlDataSource.Create(viewerLogin!);
            var evaluator = new CustomAlertEvaluator(
                rules, state, viewerSource, new NoopDeliverer(), isAlertMuted: null, alertsEnabled: static () => true,
                new PgAlertHistoryStore(ownerSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds), defaultIntervalSeconds: 0, cacheTtl: TimeSpan.Zero, NullLogger.Instance);

            await evaluator.EvaluateServerAsync(serverId, storage, storage, ct);

            var fired = await state.LoadAsync(ruleId, serverId, version, ct);
            Assert.True(fired.Persistence.Firing, "the rule did not fire through the compose store's viewer pool");
            Assert.Equal("Warning", fired.FiredSeverity);
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            await cluster.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// #3970: for a start that did NOT provision the compose store's roles, the evaluator tries a trusted
    /// credential an earlier start left (F1) exactly as a host does, accepted before use -- but stops there. When
    /// the store refuses it (re-keyed by hand) or none exists at all, a host falls back to the owner login
    /// (<see cref="DarlingStoreLogins.ResolveUnmanagedAsync"/>); the evaluator must not, in either case, since a
    /// rule's compose metric running with owner rights is exactly what the viewer-role pool exists to prevent.
    /// </summary>
    [Fact]
    public async Task TheComposeStore_CustomAlertEvaluator_TrustsAnEarlierCredential_ButNeverFallsBackToTheOwner_Gated()
    {
        var runtimeRoot = RequireRuntime();
        var root = Directory.CreateTempSubdirectory("darling-3970-earlier-");
        var credentials = Path.Combine(root.FullName, "credentials");
        var cluster = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = Path.Combine(root.FullName, "pg") },
            NullLogger.Instance, runtimeRoot);

        var previousContainer = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER");
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var ct = timeout.Token;
            var owner = await BootMigratedAsync(cluster, ct);
            await using var ownerSource = NpgsqlDataSource.Create(owner);

            /* An earlier start provisioned the roles and wrote their credential files. */
            var earlier = new CapturingTestLogger();
            Assert.True((await DarlingStoreLogins.ProvisionComposeStoreAsync(ownerSource, owner, earlier, ct, credentials)).Provisioned, earlier.Joined);

            /* This start did not provision (a stand-down, say) -- the verdict a real one settles, pointed at this
               test's directory. */
            DarlingStoreLogins.PublishComposeStoreVerdict(DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned(
                "The collector stopped before it could provision the store's roles (Store: connection refused).", credentials));
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", "true");

            /* 1. The role still has the password an earlier start gave it: the evaluator uses it, exactly as a
                  host would (F1). */
            var trusted = await DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync(owner, NullLogger.Instance, ct);
            Assert.NotNull(trusted);
            await AssertLeastPrivilegeAsync(trusted!, "viewer", ct);

            /* 2. The role re-keyed by hand since: the store refuses that credential, so the evaluator gets
                  nothing -- never the owner in its place. */
            await ExecAsync(owner, "ALTER ROLE viewer PASSWORD 'ChangedByHand3970';", ct);
            var rekeyed = new CapturingTestLogger();
            Assert.Null(await DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync(owner, rekeyed, ct));
            Assert.Contains(
                "Custom alert evaluator not started this attempt: the store did not accept the viewer login an earlier start provisioned",
                rekeyed.Joined, StringComparison.Ordinal);

            /* 3. No trusted credential at all: a host falls back to the owner here (#3914's ruling); the
                  evaluator must not, which is the divergence this test exists to pin. */
            File.Delete(Path.Combine(credentials, DarlingManagedRoles.ComposeStoreCredentialFileName("viewer")));
            Assert.Null(await DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync(owner, NullLogger.Instance, ct));

            var hostFallback = await DarlingStoreLogins.ResolveUnmanagedAsync(
                DarlingStoreLogins.Surface.Web, new PostgresConfig { ConnectionString = owner }, NullLogger.Instance, ct);
            Assert.Equal(owner, hostFallback);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", previousContainer);
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
    /// and migrates it, since provisioning names migrated tables. A boot failure carries the cluster's own
    /// server log tail (#4352): the CI failure artifact does not collect it, and a bare exception message
    /// ("connection open timed out", "CREATE DATABASE timed out") does not say what the server was doing.</summary>
    private static async Task<string> BootMigratedAsync(DarlingManagedPostgres cluster, CancellationToken ct)
    {
        string connectionString;
        try
        {
            connectionString = await cluster.EnsureRunningAsync(ct);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"The test cluster at {cluster.DataDirectory} did not boot: {ex.Message}{Environment.NewLine}{ServerLogTail(cluster.DataDirectory)}",
                ex);
        }

        var owner = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString;
        await using var connection = await OpenAsync(owner, ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return owner;
    }

    /// <summary>The last ~200 lines of every log the cluster writes: pg_ctl's start log (one file, next to the
    /// data directory) and the logging collector's output (one or more files under <c>&lt;data directory&gt;/log</c>).
    /// A missing or unreadable file adds a one-line note instead of throwing, since this runs from a catch block.</summary>
    private static string ServerLogTail(string dataDirectory)
    {
        const int TailLines = 200;
        var builder = new System.Text.StringBuilder();

        var startLog = Path.Combine(Path.GetDirectoryName(dataDirectory) ?? dataDirectory, DarlingManagedPostgres.ServerLogFileName);
        AppendLogTail(builder, startLog, TailLines);

        var collectorDirectory = Path.Combine(dataDirectory, "log");
        try
        {
            if (Directory.Exists(collectorDirectory))
            {
                foreach (var path in Directory.EnumerateFiles(collectorDirectory).OrderBy(p => p, StringComparer.Ordinal))
                {
                    AppendLogTail(builder, path, TailLines);
                }
            }
            else
            {
                builder.AppendLine($"--- {collectorDirectory} ---").AppendLine("(no logging-collector directory)");
            }
        }
        catch (Exception ex)
        {
            builder.AppendLine($"--- {collectorDirectory} ---").AppendLine($"(could not list: {ex.Message})");
        }

        return builder.ToString();
    }

    private static void AppendLogTail(System.Text.StringBuilder builder, string path, int tailLines)
    {
        builder.AppendLine($"--- {path} ---");
        try
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using var reader = new StreamReader(stream);
            var lines = new List<string>();
            string? line;
            while ((line = reader.ReadLine()) is not null)
            {
                lines.Add(line);
                if (lines.Count > tailLines)
                {
                    lines.RemoveAt(0);
                }
            }

            foreach (var kept in lines)
            {
                builder.AppendLine(kept);
            }
        }
        catch (Exception ex)
        {
            builder.AppendLine($"(could not read: {ex.Message})");
        }
    }

    /// <summary>The login connects as <paramref name="role"/>, carries the statement_timeout backstop, and is
    /// denied a secret column.</summary>
    private static async Task AssertLeastPrivilegeAsync(string login, string role, CancellationToken ct)
    {
        await using var connection = await OpenAsync(new NpgsqlConnectionStringBuilder(login) { Pooling = false }.ConnectionString, ct);
        Assert.Equal(role, await ScalarAsync<string>(connection, "SELECT current_user::text", ct));
        await AssertStatementTimeoutSecondsAsync(connection, new DarlingConfig().ComposeStatementTimeoutSeconds, ct);

        var denied = await Assert.ThrowsAsync<PostgresException>(
            () => ScalarAsync<object>(connection, "SELECT smtp_encrypted_password FROM config.config_notification", ct));
        Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, denied.SqlState);
    }

    /// <summary>Asserts the current session's live <c>statement_timeout</c> equals <paramref name="expectedSeconds"/>,
    /// comparing by value (milliseconds from <c>pg_settings.setting</c>) rather than PostgreSQL's display text —
    /// which normalizes 60 s to <c>1min</c>, not <c>60s</c>.</summary>
    private static async Task AssertStatementTimeoutSecondsAsync(NpgsqlConnection connection, int expectedSeconds, CancellationToken ct)
    {
        Assert.Equal(
            expectedSeconds * 1000,
            await ScalarAsync<int>(connection, "SELECT setting::int FROM pg_settings WHERE name = 'statement_timeout'", ct));
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

    private static async Task<T> OwnerScalarAsync<T>(string owner, string sql, CancellationToken ct)
    {
        await using var connection = await OpenAsync(owner, ct);
        return await ScalarAsync<T>(connection, sql, ct);
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

    /// <summary>The script text without its psql meta-commands: the lines that start with a backslash (#4746's
    /// three <c>\set</c> lines that save, set and restore <c>ON_ERROR_STOP</c>), which psql runs itself and a server
    /// would reject as a syntax error. Every other line, comments and blank lines included, is kept as it was.</summary>
    internal static string WithoutPsqlMetaCommands(string script) =>
        string.Join("\n", script.Split('\n').Where(line => !line.StartsWith('\\')));

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

    /// <summary>The hosts take a typed logger; this one records its lines so a failed start says why.</summary>
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
