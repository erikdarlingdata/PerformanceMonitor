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

            var rules = new CustomAlertRuleStore(ownerSource);
            var state = new CustomAlertStateStore(ownerSource);
            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(ruleName, null, definition, true, "test", ct));
            var ruleId = created.Rule!.Id;
            var version = created.Rule!.Version;

            /* The real construction (DarlingWorker): rules/state/history on the OWNER pool, only the metric READ
               on the resolved compose viewer pool. */
            await using var viewerSource = NpgsqlDataSource.Create(viewerLogin!);
            var evaluator = new CustomAlertEvaluator(
                rules, state, viewerSource, new NoopDeliverer(), isAlertMuted: null, alertsEnabled: static () => true,
                new PgAlertHistoryStore(ownerSource), defaultIntervalSeconds: 0, cacheTtl: TimeSpan.Zero, NullLogger.Instance);

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
