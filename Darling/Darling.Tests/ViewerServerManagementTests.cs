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
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The control-plane Stage-3a viewer writes to <c>config.config_monitored_servers</c> — the write partial's
/// SQL shape (parameterization, ON CONFLICT semantics, the V17 column parity with the service's
/// <see cref="StoreConfigProvider"/>) and the shared <c>server_id</c> identity, pinned with no live Postgres
/// exactly like the mute-rule + command pins.
/// </summary>
public sealed class ViewerMonitoredServerSqlTests
{
    private static readonly string[] StoreColumns =
    {
        "server_id", "name", "host", "database", "auth", "username", "encrypted_password", "encrypt_mode",
        "trust_server_certificate", "read_only_intent", "multi_subnet_failover", "excluded_databases",
        "monthly_cost_usd", "capture_plans", "is_enabled", "alert_delivery_mode_override",
    };

    [Fact]
    public void UpsertSql_WritesEveryStoreColumn_AsBoundParameters_OnConflictUpdate()
    {
        var sql = ViewerDataService.MonitoredServerUpsertSql;

        Assert.Contains("INSERT INTO config_monitored_servers", sql, StringComparison.Ordinal);
        foreach (var column in StoreColumns)
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }

        /* Every value is a $N parameter (16 columns incl. the V18 alert_delivery_mode_override), never inlined. */
        for (var i = 1; i <= 16; i++)
        {
            Assert.Contains("$" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), sql, StringComparison.Ordinal);
        }

        /* Upsert on the shared identity; created_at is server-side, never clobbered on update. */
        Assert.Contains("ON CONFLICT (server_id) DO UPDATE", sql, StringComparison.Ordinal);
        Assert.Contains("modified_at = (now() AT TIME ZONE 'UTC')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void InsertIfAbsentSql_DoesNothingOnConflict_SoTheMigrateNeverDoubleSeeds()
    {
        var sql = ViewerDataService.MonitoredServerInsertIfAbsentSql;
        Assert.Contains("INSERT INTO config_monitored_servers", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id) DO NOTHING", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeleteAndTargetedUpdates_KeyOnServerId()
    {
        Assert.Contains("DELETE FROM config_monitored_servers WHERE server_id = $1", ViewerDataService.MonitoredServerDeleteSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.MonitoredServerSetEnabledSql, StringComparison.Ordinal);
        Assert.Contains("is_enabled = $2", ViewerDataService.MonitoredServerSetEnabledSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.MonitoredServerSetExcludedDatabasesSql, StringComparison.Ordinal);
        Assert.Contains("excluded_databases = $2", ViewerDataService.MonitoredServerSetExcludedDatabasesSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ManagedServersSql_IsConfigAuthoritative_EnrichedByCollectServers()
    {
        var sql = ViewerDataService.ManagedServersSql;
        /* Membership from the DESIRED-state config table, facts LEFT JOINed from the observed registry. */
        Assert.Contains("FROM config_monitored_servers c", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN servers s ON s.server_id = c.server_id", sql, StringComparison.Ordinal);
        Assert.Contains("c.is_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("c.monthly_cost_usd", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SelectSql_ReadsCreatedAt_ForTheManageServersGrid()
    {
        Assert.Contains("created_at", ViewerDataService.MonitoredServersSelectSql, StringComparison.Ordinal);
        Assert.Contains("created_at", ViewerDataService.MonitoredServerByIdSql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("SQL2022", null, false)]
    [InlineData("myazure.database.windows.net", "AdventureWorks", false)]
    [InlineData("ag-listener", null, true)]
    [InlineData("host", "db", true)]
    public void ComputeServerId_MatchesTheSharedHelper(string host, string? database, bool readOnlyIntent)
    {
        var expected = ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(host, database, readOnlyIntent));
        Assert.Equal(expected, ViewerDataService.ComputeServerId(host, database, readOnlyIntent));
    }

    [Fact]
    public void ComputeServerId_MatchesTheServiceMonitoredServerIdentity()
    {
        /* The service seeds server_id from its MonitoredServer.StorageName; the viewer must derive the SAME id
           for the same host/database/read-only-intent so a viewer-written row JOINs collected data and the
           service's reconcile matches it. */
        var svc = new MonitoredServer { Host = "SQL2022", Database = "tpcc", ReadOnlyIntent = true };
        Assert.Equal(
            ServerIdHelper.GetDeterministicHashCode(svc.StorageName),
            ViewerDataService.ComputeServerId("SQL2022", "tpcc", true));
    }
}

/// <summary>
/// The viewer half of the command plane — the enqueue/poll SQL shape, the terminal-state helper, and the
/// <c>test_connect</c> args_json that the service's <see cref="DarlingCommandExecutor"/> deserializes as a
/// <see cref="MonitoredServer"/> (proving the two ends agree, and that the password rides as the DPAPI blob,
/// never plaintext, into Postgres).
/// </summary>
public sealed class ViewerCommandSqlTests
{
    private static readonly JsonSerializerOptions s_caseInsensitive = new() { PropertyNameCaseInsensitive = true };

    [Fact]
    public void EnqueueSql_InsertsPendingAndReturnsCommandId()
    {
        var sql = ViewerDataService.CommandEnqueueSql;
        Assert.Contains("INSERT INTO config_command", sql, StringComparison.Ordinal);
        Assert.Contains("'pending'", sql, StringComparison.Ordinal);
        Assert.Contains("RETURNING command_id", sql, StringComparison.Ordinal);
        /* requested_by, command_type, target_server_id, args_json bound as $1..$4. */
        Assert.Contains("$1, $2, $3, $4", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PollSql_ReadsStatusAndResult_ByCommandId()
    {
        var sql = ViewerDataService.CommandPollSql;
        Assert.Contains("SELECT status, result_status, result_json FROM config_command", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE command_id = $1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeleteSql_RemovesTheCredentialBearingRow_ByCommandId()
    {
        /* The viewer cleans up its own terminal test_connect command so the DPAPI blob in args_json does not
           linger in the store. */
        Assert.Equal("DELETE FROM config_command WHERE command_id = $1", ViewerDataService.CommandDeleteSql);
    }

    [Theory]
    [InlineData("succeeded", true)]
    [InlineData("failed", true)]
    [InlineData("pending", false)]
    [InlineData("in_progress", false)]
    [InlineData(null, false)]
    public void IsTerminal_OnlyForSucceededOrFailed(string? status, bool expected)
    {
        Assert.Equal(expected, ViewerDataService.IsTerminal(status));
    }

    [Fact]
    public void BuildTestConnectArgs_DeserializesIntoTheServiceMonitoredServer_WithNoPlaintextPassword()
    {
        var args = new TestConnectServer
        {
            Name = "Prod",
            Host = "SQL2022",
            Database = "tpcc",
            Auth = "sql",
            Username = "monitor",
            EncryptedPassword = "DPAPI-BLOB==",
            ReadOnlyIntent = true,
            TrustServerCertificate = true,
            EncryptMode = "Strict",
            MultiSubnetFailover = true,
        };

        var json = ViewerDataService.BuildTestConnectArgs(args);

        /* The service uses PropertyNameCaseInsensitive; the viewer emits the MonitoredServer's [JsonPropertyName]
           (camelCase) names, so the round-trip reconstructs the server the probe connects with. */
        var server = JsonSerializer.Deserialize<MonitoredServer>(json, s_caseInsensitive);
        Assert.NotNull(server);
        Assert.Equal("SQL2022", server!.Host);
        Assert.Equal("tpcc", server.Database);
        Assert.Equal("sql", server.Auth);
        Assert.Equal("monitor", server.Username);
        Assert.Equal("DPAPI-BLOB==", server.EncryptedPassword);
        Assert.True(server.ReadOnlyIntent);
        Assert.True(server.TrustServerCertificate);
        Assert.Equal("Strict", server.EncryptMode);
        Assert.True(server.MultiSubnetFailover);

        /* The plaintext password field is NEVER emitted — the secret only ever travels as the DPAPI blob. */
        Assert.Null(server.Password);
        Assert.DoesNotContain("\"password\"", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BuildTestConnectArgs_IntegratedAuth_OmitsUsernameAndSecret()
    {
        var json = ViewerDataService.BuildTestConnectArgs(new TestConnectServer { Host = "SQL2022", Auth = "integrated" });
        var server = JsonSerializer.Deserialize<MonitoredServer>(json, s_caseInsensitive);
        Assert.NotNull(server);
        Assert.Equal("integrated", server!.Auth);
        Assert.Null(server.Username);
        Assert.Null(server.EncryptedPassword);
    }

    /// <summary>
    /// #3244 — every <c>test_connect</c> request the viewer builds names its engine explicitly. The field is
    /// non-null with the SQL Server default, so the dialogs (which author SQL Server targets) need no call-site
    /// change and the args can never be engine-silent; the service reads the same token contract as
    /// <c>MonitoredServer.Engine</c> (omitted or unrecognized = SQL Server), so an older service is unaffected.
    /// </summary>
    [Fact]
    public void BuildTestConnectArgs_AlwaysNamesTheEngine_DefaultSqlServer()
    {
        var json = ViewerDataService.BuildTestConnectArgs(new TestConnectServer { Host = "SQL2022", Auth = "integrated" });

        Assert.Contains("\"engine\":\"sqlserver\"", json, StringComparison.Ordinal);

        var server = JsonSerializer.Deserialize<MonitoredServer>(json, s_caseInsensitive);
        Assert.NotNull(server);
        Assert.Equal(CollectorTargetEngine.SqlServer, server!.TargetEngine);
    }

    /// <summary>
    /// The other arm of the #3244 pin: a PostgreSQL request round-trips into a server the probe will actually
    /// connect to as PostgreSQL. This is the latch that was missing — before the field existed, a viewer-built
    /// probe could only ever reach the SQL Server arm, so the engine-aware reply the dialogs parse (#3149) was
    /// unreachable with a PostgreSQL payload.
    /// </summary>
    [Fact]
    public void BuildTestConnectArgs_PostgresEngine_ReachesTheServiceAsAPostgresTarget()
    {
        var json = ViewerDataService.BuildTestConnectArgs(
            new TestConnectServer { Host = "pg18.example.test", Auth = "sql", Engine = "postgres" });

        Assert.Contains("\"engine\":\"postgres\"", json, StringComparison.Ordinal);

        var server = JsonSerializer.Deserialize<MonitoredServer>(json, s_caseInsensitive);
        Assert.NotNull(server);
        Assert.Equal(CollectorTargetEngine.PostgreSql, server!.TargetEngine);
    }
}

/// <summary>The pure auth mapping — the service honors integrated + SQL only; every Azure/Entra mode blocks,
/// including any added later, because the mapping is a whitelist with a null default.</summary>
public sealed class ServerStoreCredentialTests
{
    [Theory]
    [InlineData(AuthenticationTypes.Windows, "integrated")]
    [InlineData(AuthenticationTypes.SqlServer, "sql")]
    public void MapAuth_SupportedModes_MapToStoreAuth(string authType, string expected)
    {
        Assert.Equal(expected, ServerStoreCredential.MapAuth(authType));
        Assert.True(ServerStoreCredential.IsSupported(authType));
    }

    [Theory]
    [InlineData(AuthenticationTypes.EntraMFA)]
    [InlineData(AuthenticationTypes.ServicePrincipal)]
    [InlineData(AuthenticationTypes.ManagedIdentity)]
    [InlineData(AuthenticationTypes.EntraDefaultCredential)]
    public void MapAuth_AzureModes_AreUnsupported(string authType)
    {
        Assert.Null(ServerStoreCredential.MapAuth(authType));
        Assert.False(ServerStoreCredential.IsSupported(authType));
    }

    [Fact]
    public void RequiresSecret_OnlyForSql()
    {
        Assert.True(ServerStoreCredential.RequiresSecret(AuthenticationTypes.SqlServer));
        Assert.False(ServerStoreCredential.RequiresSecret(AuthenticationTypes.Windows));
        Assert.False(ServerStoreCredential.RequiresSecret(AuthenticationTypes.ManagedIdentity));
    }
}

/// <summary>
/// The DPAPI blob the viewer writes into <c>encrypted_password</c> must be readable by the SERVICE, and vice
/// versa — a cross-project round trip against <see cref="DarlingSecrets"/> proving the entropy + scope match
/// (Windows-only; DPAPI is unavailable elsewhere).
/// </summary>
public sealed class ViewerServerSecretTests
{
    [Fact]
    public void ViewerProtect_IsReadableByTheService()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");
        var blob = ViewerServerSecret.Protect("s3cr3t!");
        Assert.Equal("s3cr3t!", DarlingSecrets.Unprotect(blob));
    }

    [Fact]
    public void ServiceProtect_IsReadableByTheViewer()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");
        var blob = DarlingSecrets.Protect("another-pw");
        Assert.Equal("another-pw", ViewerServerSecret.TryUnprotect(blob));
    }

    [Fact]
    public void TryUnprotect_ReturnsNullOnGarbage_NeverThrows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");
        Assert.Null(ViewerServerSecret.TryUnprotect(null));
        Assert.Null(ViewerServerSecret.TryUnprotect(""));
        Assert.Null(ViewerServerSecret.TryUnprotect("not-base64!!"));
        Assert.Null(ViewerServerSecret.TryUnprotect(Convert.ToBase64String(new byte[] { 1, 2, 3, 4 })));
    }
}

/// <summary>
/// The one-time viewer-servers.json → config migrate-in: the per-entry projection (auth mapping + secret
/// resolution + the shared server_id) and the once-marker guard. Uses an in-memory secret fake + temp files,
/// never the real vault or a live store.
/// </summary>
public sealed class ViewerServerMigrationTests
{
    [Fact]
    public void Projection_IntegratedEntry_ImportsWithNoSecret_OnTheSharedIdentity()
    {
        using var fixture = new Fixture();
        fixture.ServerStore.AddServer(
            new ViewerServerEntry { ServerName = "SQL2022", DisplayName = "Prod", AuthenticationType = AuthenticationTypes.Windows },
            null, null);

        var (row, reason) = fixture.Migration.TryProjectEntry(fixture.ServerStore.GetAllServers()[0]);

        Assert.Null(reason);
        Assert.NotNull(row);
        Assert.Equal("integrated", row!.Auth);
        Assert.Null(row.EncryptedPassword);
        Assert.Equal("SQL2022", row.Host);
        Assert.Equal("Prod", row.Name);
        Assert.Equal(ViewerDataService.ComputeServerId("SQL2022", null, false), row.ServerId);
    }

    [Fact]
    public void Projection_SqlEntry_SealsTheSecretAsAServiceReadableBlob()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        using var fixture = new Fixture();
        var entry = new ViewerServerEntry { ServerName = "SQL2019", DisplayName = "Dev", AuthenticationType = AuthenticationTypes.SqlServer };
        fixture.ServerStore.AddServer(entry, "monitor", "p@ss");

        var (row, reason) = fixture.Migration.TryProjectEntry(fixture.ServerStore.GetByServerName("SQL2019")!);

        Assert.Null(reason);
        Assert.NotNull(row);
        Assert.Equal("sql", row!.Auth);
        Assert.Equal("monitor", row.Username);
        Assert.NotNull(row.EncryptedPassword);
        /* The service must be able to read what the migrate wrote. */
        Assert.Equal("p@ss", DarlingSecrets.Unprotect(row.EncryptedPassword!));
    }

    [Fact]
    public void Projection_SqlEntry_WithNoStoredSecret_IsSkipped()
    {
        using var fixture = new Fixture();
        /* SQL auth but no secret saved (username/password null) — an un-connectable row; skip, don't import broken. */
        fixture.ServerStore.AddServer(
            new ViewerServerEntry { ServerName = "SQL2016", DisplayName = "Old", AuthenticationType = AuthenticationTypes.SqlServer },
            null, null);

        var (row, reason) = fixture.Migration.TryProjectEntry(fixture.ServerStore.GetAllServers()[0]);

        Assert.Null(row);
        Assert.Contains("no stored password", reason, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(AuthenticationTypes.EntraMFA)]
    [InlineData(AuthenticationTypes.ServicePrincipal)]
    [InlineData(AuthenticationTypes.ManagedIdentity)]
    [InlineData(AuthenticationTypes.EntraDefaultCredential)]
    public void Projection_AzureAuth_IsSkipped(string authType)
    {
        using var fixture = new Fixture();
        fixture.ServerStore.AddServer(
            new ViewerServerEntry { ServerName = "azure", DisplayName = "Azure", AuthenticationType = authType },
            null, null);

        var (row, reason) = fixture.Migration.TryProjectEntry(fixture.ServerStore.GetAllServers()[0]);

        Assert.Null(row);
        Assert.Contains("not supported", reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Projection_CarriesConnectionOptionsAndExcludedDatabases()
    {
        using var fixture = new Fixture();
        fixture.ServerStore.AddServer(new ViewerServerEntry
        {
            ServerName = "ag",
            DisplayName = "AG",
            AuthenticationType = AuthenticationTypes.Windows,
            ReadOnlyIntent = true,
            MultiSubnetFailover = true,
            TrustServerCertificate = true,
            EncryptMode = "Strict",
            MonthlyCostUsd = 250m,
            IsEnabled = false,
            ExcludedDatabases = new List<string> { "ReportServer", "ReportServerTempDB" },
        }, null, null);

        var (row, _) = fixture.Migration.TryProjectEntry(fixture.ServerStore.GetAllServers()[0]);

        Assert.NotNull(row);
        Assert.True(row!.ReadOnlyIntent);
        Assert.True(row.MultiSubnetFailover);
        Assert.True(row.TrustServerCertificate);
        Assert.Equal("Strict", row.EncryptMode);
        Assert.Equal(250m, row.MonthlyCostUsd);
        Assert.False(row.IsEnabled);
        Assert.Equal(new[] { "ReportServer", "ReportServerTempDB" }, row.ExcludedDatabases);
        /* Read-only intent participates in the identity. */
        Assert.Equal(ViewerDataService.ComputeServerId("ag", null, true), row.ServerId);
    }

    [Fact]
    public void Marker_GatesTheOnceGuard()
    {
        using var fixture = new Fixture();
        Assert.False(fixture.Migration.AlreadyMigrated);

        File.WriteAllText(fixture.MarkerPath, "done");
        Assert.True(new ViewerServerMigration(fixture.ServerStore, fixture.ProfileStore, fixture.MarkerPath).AlreadyMigrated);
    }

    /// <summary>Temp files + an in-memory secret fake so the migration never touches the real vault or store.</summary>
    private sealed class Fixture : IDisposable
    {
        private readonly string _serversPath = Path.Combine(Path.GetTempPath(), $"vs-{Guid.NewGuid():N}.json");
        private readonly string _profilesPath = Path.Combine(Path.GetTempPath(), $"vp-{Guid.NewGuid():N}.json");

        public string MarkerPath { get; } = Path.Combine(Path.GetTempPath(), $"vm-{Guid.NewGuid():N}.marker");
        public ViewerServerStore ServerStore { get; }
        public ViewerProfileStore ProfileStore { get; }
        public ViewerServerMigration Migration { get; }

        public Fixture()
        {
            var secrets = new FakeSecretStore();
            ServerStore = new ViewerServerStore(_serversPath, secrets);
            ProfileStore = new ViewerProfileStore(ServerStore, _profilesPath, secrets);
            Migration = new ViewerServerMigration(ServerStore, ProfileStore, MarkerPath);
        }

        public void Dispose()
        {
            foreach (var path in new[] { _serversPath, _profilesPath, MarkerPath })
            {
                try { File.Delete(path); } catch { /* best-effort */ }
            }
        }
    }

    private sealed class FakeSecretStore : IViewerServerSecretStore
    {
        private readonly Dictionary<string, (string Username, string Password)> _map = new();
        public void Save(string id, string username, string password) => _map[id] = (username, password);
        public (string Username, string Password)? Find(string id) => _map.TryGetValue(id, out var v) ? v : null;
        public void Delete(string id) => _map.Remove(id);
    }
}

/// <summary>
/// The read-only degradation: a viewer connected read-only cannot write server config or enqueue commands.
/// The proactive gating hides the affordances; these pin that the write paths still fail safe (the reactive
/// 42501 → <see cref="ViewerReadOnlyException"/> backstop is shared with the mute-rule path, tested there).
/// </summary>
public sealed class ViewerServerManagementReadOnlyTests
{
    [Fact]
    public async System.Threading.Tasks.Task MigrateAsync_Disconnected_IsANoOp()
    {
        using var serverDir = new TempDir();
        var secrets = new NoopSecretStore();
        var serverStore = new ViewerServerStore(Path.Combine(serverDir.Path, "s.json"), secrets);
        var profileStore = new ViewerProfileStore(serverStore, Path.Combine(serverDir.Path, "p.json"), secrets);
        var migration = new ViewerServerMigration(serverStore, profileStore, Path.Combine(serverDir.Path, "m.marker"));

        /* No store → nothing imported, and the once-marker is NOT written (so a later connected run still runs). */
        var imported = await migration.MigrateAsync(dataService: null);
        Assert.Equal(0, imported);
        Assert.False(migration.AlreadyMigrated);
    }

    private sealed class TempDir : IDisposable
    {
        public string Path { get; } = Directory.CreateTempSubdirectory("darling-cp-").FullName;
        public void Dispose() { try { Directory.Delete(Path, recursive: true); } catch { /* best-effort */ } }
    }

    private sealed class NoopSecretStore : IViewerServerSecretStore
    {
        public void Save(string id, string username, string password) { }
        public (string Username, string Password)? Find(string id) => null;
        public void Delete(string id) { }
    }
}

/// <summary>
/// The Darling "Add Multiple Servers" pure mapping + gate helpers (WPF-free statics on
/// <see cref="AddMultipleServersDialog"/>): the row → <see cref="MonitoredServerRow"/> / → <see cref="TestConnectServer"/>
/// projection, the service-honors-only-Windows/SQL belt (mirroring the single dialog's MapAuth block), the
/// blank-db → NULL trap, the single-Protect blob reused per row, and the ratified OrdinalIgnoreCase dedupe gate
/// over the shared <see cref="ServerIdHelper"/> storage name (mirrors Lite's B7 pins).
/// </summary>
public sealed class BulkServerOnboardingMappingTests
{
    private static BulkServerParseLine Line(string server, string? display = null, string? database = null) =>
        new(1, server, server, display, database);

    [Fact]
    public void BuildMonitoredServerRow_WindowsAuth_MapsToIntegrated_NoSecret()
    {
        var (row, error) = AddMultipleServersDialog.BuildMonitoredServerRow(
            Line("SQL2022"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows });

        Assert.Null(error);
        Assert.NotNull(row);
        Assert.Equal("integrated", row!.Auth);
        Assert.Null(row.EncryptedPassword);
        Assert.True(row.IsEnabled);
        Assert.False(row.ReadOnlyIntent);
    }

    [Fact]
    public void BuildMonitoredServerRow_SqlAuth_MapsToSql_CarriesBlobAndUsername()
    {
        var (row, _) = AddMultipleServersDialog.BuildMonitoredServerRow(
            Line("SQL2022"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.SqlServer, Username = "monitor", EncryptedPassword = "DPAPI-BLOB==" });

        Assert.Equal("sql", row!.Auth);
        Assert.Equal("monitor", row.Username);
        Assert.Equal("DPAPI-BLOB==", row.EncryptedPassword);
    }

    [Theory]
    [InlineData(AuthenticationTypes.EntraMFA)]
    [InlineData(AuthenticationTypes.ServicePrincipal)]
    [InlineData(AuthenticationTypes.ManagedIdentity)]
    [InlineData(AuthenticationTypes.EntraDefaultCredential)]
    public void BuildMonitoredServerRow_AzureAuth_IsRejected_TheBelt(string authType)
    {
        // The trimmed radios never offer these, but a picked profile could resolve to one — the mapping helper
        // still rejects (mirrors the single dialog's MapAuth-null block).
        var (row, error) = AddMultipleServersDialog.BuildMonitoredServerRow(
            Line("azure.database.windows.net"),
            new BulkSharedSettings { AuthType = authType });

        Assert.Null(row);
        Assert.False(string.IsNullOrEmpty(error));
    }

    [Fact]
    public void BuildMonitoredServerRow_ServerId_MatchesComputeServerId()
    {
        var (row, _) = AddMultipleServersDialog.BuildMonitoredServerRow(
            Line("myazure.database.windows.net", null, "AdventureWorks"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows });

        Assert.Equal(ViewerDataService.ComputeServerId("myazure.database.windows.net", "AdventureWorks", false), row!.ServerId);
    }

    [Fact]
    public void BuildMonitoredServerRow_BlankDatabase_IsNull_NeverMaster()
    {
        // The builder trap: a blank/whitespace database must map to NULL, not "master". Coercing would derive a
        // MISMATCHED server_id (master's), breaking dedupe against single-added rows and splitting collected data.
        var (row, _) = AddMultipleServersDialog.BuildMonitoredServerRow(
            Line("SQL2022", null, "   "),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows });

        Assert.Null(row!.Database);
        Assert.Equal(ViewerDataService.ComputeServerId("SQL2022", null, false), row.ServerId);
    }

    [Fact]
    public void BuildMonitoredServerRow_CarriesEncryptTrustDatabase_AndDisplayFallback()
    {
        var (row, _) = AddMultipleServersDialog.BuildMonitoredServerRow(
            Line("SQL2022", null, "  tpcc  "),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows, EncryptMode = "Strict", TrustServerCertificate = true });

        Assert.Equal("Strict", row!.EncryptMode);
        Assert.True(row.TrustServerCertificate);
        Assert.Equal("tpcc", row.Database);
        Assert.Equal("SQL2022", row.Name); // display name falls back to host
    }

    [Fact]
    public void SharedBlob_IsReusedAcrossRows_ProtectNotCalledPerRow()
    {
        // The shared settings carry ONE pre-Protected blob (Protect is called once at resolve time); the mapping
        // helper reuses it verbatim on every row rather than re-encrypting per server.
        var shared = new BulkSharedSettings { AuthType = AuthenticationTypes.SqlServer, Username = "u", EncryptedPassword = "ONE-BLOB==" };
        var (a, _) = AddMultipleServersDialog.BuildMonitoredServerRow(Line("SQL2022"), shared);
        var (b, _) = AddMultipleServersDialog.BuildMonitoredServerRow(Line("SQL2019"), shared);

        Assert.Equal("ONE-BLOB==", a!.EncryptedPassword);
        Assert.Same(a.EncryptedPassword, b!.EncryptedPassword);
    }

    [Fact]
    public void BuildTestConnectServer_ProjectsTheSameFields()
    {
        var (test, error) = AddMultipleServersDialog.BuildTestConnectServer(
            Line("SQL2022", "Prod", "tpcc"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.SqlServer, Username = "monitor", EncryptedPassword = "BLOB==", EncryptMode = "Strict", TrustServerCertificate = true });

        Assert.Null(error);
        Assert.NotNull(test);
        Assert.Equal("SQL2022", test!.Host);
        Assert.Equal("Prod", test.Name);
        Assert.Equal("tpcc", test.Database);
        Assert.Equal("sql", test.Auth);
        Assert.Equal("monitor", test.Username);
        Assert.Equal("BLOB==", test.EncryptedPassword);
        Assert.Equal("Strict", test.EncryptMode);
        Assert.True(test.TrustServerCertificate);
    }

    [Fact]
    public void BuildTestConnectServer_AzureAuth_IsRejected()
    {
        var (test, error) = AddMultipleServersDialog.BuildTestConnectServer(
            Line("azure"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.ServicePrincipal });

        Assert.Null(test);
        Assert.False(string.IsNullOrEmpty(error));
    }

    [Fact]
    public void DedupeGate_CaseVariant_IsSkipped_EvenThoughStoredHashesDiffer()
    {
        // Hostnames are case-insensitive in reality, so an existing SQL01 must block a bulk 'sql01' even though
        // the stored server_id differs by case. Pins against a "simplify back to the raw hash" regression.
        Assert.NotEqual(
            ViewerDataService.ComputeServerId("SQL01", null, false),
            ViewerDataService.ComputeServerId("sql01", null, false));

        var seen = AddMultipleServersDialog.SeedGate(new[] { Row("SQL01", null, false) });
        Assert.False(seen.Add(AddMultipleServersDialog.GateKey(Row("sql01", null, false))));
    }

    [Fact]
    public void DedupeGate_ReadOnlyIntent_SeedDoesNotBlockReadWrite_ButReadWriteDoes()
    {
        // An existing read-only SQL01:RO does NOT block a bulk read-write SQL01 (bulk rows are read-write)…
        var seenRo = AddMultipleServersDialog.SeedGate(new[] { Row("SQL01", null, true) });
        Assert.True(seenRo.Add(AddMultipleServersDialog.GateKey(Row("SQL01", null, false))));

        // …but an existing read-write SQL01 DOES.
        var seenRw = AddMultipleServersDialog.SeedGate(new[] { Row("SQL01", null, false) });
        Assert.False(seenRw.Add(AddMultipleServersDialog.GateKey(Row("SQL01", null, false))));
    }

    [Fact]
    public void DedupeGate_DifferentDatabases_StayDistinct()
    {
        var seen = AddMultipleServersDialog.SeedGate(new[] { Row("host", "db1", false) });
        Assert.True(seen.Add(AddMultipleServersDialog.GateKey(Row("host", "db2", false))));
    }

    private static MonitoredServerRow Row(string host, string? database, bool readOnlyIntent) => new()
    {
        ServerId = ViewerDataService.ComputeServerId(host, database, readOnlyIntent),
        Host = host,
        Database = database,
        ReadOnlyIntent = readOnlyIntent,
    };
}
