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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// M2 slice D: the collection loop's dispatch must cover every collector in the shared catalog
/// and every scheduled default — a new definition that isn't wired into the worker fails here,
/// not silently in production.
/// </summary>
public sealed class DarlingWorkerTests
{
    [Fact]
    public void Dispatch_CoversEveryCatalogCollector_AndEveryScheduleDefault()
    {
        var dispatched = DarlingWorker.DispatchedCollectorNames.ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var definition in CollectorCatalog.All)
        {
            Assert.True(dispatched.Contains(definition.Name), $"worker dispatch missing catalog collector '{definition.Name}'");
        }

        foreach (var name in CollectorScheduleDefaults.All.Keys)
        {
            Assert.True(dispatched.Contains(name), $"worker dispatch missing scheduled collector '{name}'");
        }

        Assert.Equal(CollectorCatalog.All.Count, dispatched.Count);
    }

    /// <summary>
    /// A bring-your-own store connection string that omits the search path must get the canonical
    /// collect/config/public one injected BEFORE the data source is created — otherwise the Npgsql
    /// pool's first physical connections keep a pre-migration session search_path and a fresh BYO
    /// store silently collects nothing (42P01) until the service restarts. The injected path pins
    /// against the managed constant so both modes carry the same schemas in the same order.
    /// </summary>
    [Fact]
    public void EnsureStoreSearchPath_InjectsCanonicalPath_WhenByoStringOmitsIt()
    {
        const string byo = "Host=localhost;Port=5432;Username=darling;Database=darling";
        Assert.Empty(new NpgsqlConnectionStringBuilder(byo).SearchPath ?? string.Empty);

        var result = DarlingWorker.EnsureStoreSearchPath(byo);

        var parsed = new NpgsqlConnectionStringBuilder(result);
        Assert.Equal("collect,config,public", parsed.SearchPath);
        Assert.Equal(DarlingManagedPostgres.SearchPath, parsed.SearchPath);

        /* The rest of the connection string is preserved. */
        Assert.Equal("localhost", parsed.Host);
        Assert.Equal(5432, parsed.Port);
        Assert.Equal("darling", parsed.Username);
        Assert.Equal("darling", parsed.Database);
    }

    /// <summary>
    /// The Stage 2 pause gate: the collection sweep runs only when NOT paused. This pins the gate the loop
    /// keys off (config_service.paused -> _paused -> skip collection/alert/analysis/purge) so a future edit
    /// can't silently invert or drop it; the command loop keeps running while paused so a resume is honored.
    /// </summary>
    [Fact]
    public void ShouldRunCollection_SkipsOnlyWhenPaused()
    {
        Assert.True(DarlingWorker.ShouldRunCollection(paused: false));
        Assert.False(DarlingWorker.ShouldRunCollection(paused: true));
    }

    /// <summary>
    /// A connection string that ALREADY specifies a Search Path (managed mode carries it, and a BYO
    /// operator may set their own) is returned untouched — no double-set, and a non-default choice
    /// is respected.
    /// </summary>
    [Fact]
    public void EnsureStoreSearchPath_LeavesExistingSearchPathUnchanged()
    {
        /* Managed-mode shape: already carries collect,config,public. */
        var managed = DarlingManagedPostgres.BuildConnectionString(5641, "pw123");
        Assert.Equal(managed, DarlingWorker.EnsureStoreSearchPath(managed));

        /* A BYO operator's own non-default choice is respected, not overwritten. */
        const string custom = "Host=pg.example.com;Database=metrics;Search Path=collect,config,reporting,public";
        var result = DarlingWorker.EnsureStoreSearchPath(custom);
        Assert.Equal("collect,config,reporting,public", new NpgsqlConnectionStringBuilder(result).SearchPath);
    }

    /// <summary>
    /// Win 1: a cost-only edit must NOT count as a server-definition change, so ReconcileServers does not tear
    /// down and re-establish the connection for it. The reload's
    /// <see cref="DarlingObservability.SyncServerEnabledStatesAsync"/> mirrors the new MonthlyCostUsd straight
    /// onto collect.servers (which the FinOps display reads) with no connection churn.
    /// </summary>
    [Fact]
    public void ServerDefinitionEquals_CostOnlyDelta_IsEqual_SoNoReconnect()
    {
        var original = SampleServer();
        var costChanged = SampleServer();
        costChanged.MonthlyCostUsd = original.MonthlyCostUsd + 250m;

        Assert.True(DarlingWorker.ServerDefinitionEquals(original, costChanged),
            "a cost-only change must compare equal (no reconnect) — cost is synced onto collect.servers without connection churn");
    }

    /// <summary>
    /// #1236: a per-server alert-delivery override edit is likewise NON-connection — it must compare equal so
    /// ReconcileServers does not tear down the connection. The deliverer reads the override live off the
    /// refreshed state.Config, so the change still takes effect on the reload without connection churn.
    /// </summary>
    [Fact]
    public void ServerDefinitionEquals_DeliveryOverrideDelta_IsEqual_SoNoReconnect()
    {
        var original = SampleServer();
        original.AlertDeliveryModeOverride = null;

        var overrideSet = SampleServer();
        overrideSet.AlertDeliveryModeOverride = AlertNotificationMode.PerEvent;

        Assert.True(DarlingWorker.ServerDefinitionEquals(original, overrideSet),
            "an alert-delivery-override change must compare equal (no reconnect) — it is read live off state.Config, not a connection field");
    }

    /// <summary>
    /// The flip side: a genuine connection- or collection-affecting change (host, excluded databases) still
    /// compares NOT equal, so ReconcileServers reconnects the server with the new definition.
    /// </summary>
    [Fact]
    public void ServerDefinitionEquals_ConnectionOrDbDelta_IsNotEqual_SoReconnects()
    {
        var original = SampleServer();

        var hostChanged = SampleServer();
        hostChanged.Host = "other-host";
        Assert.False(DarlingWorker.ServerDefinitionEquals(original, hostChanged), "a host change must reconnect");

        var excludedChanged = SampleServer();
        excludedChanged.ExcludedDatabases = new List<string> { "tempdb", "reporting" };
        Assert.False(DarlingWorker.ServerDefinitionEquals(original, excludedChanged), "an excluded-databases change must reconnect");

        /* Sanity: two identical definitions compare equal. */
        Assert.True(DarlingWorker.ServerDefinitionEquals(original, SampleServer()));
    }

    /// <summary>
    /// D-BYO: managed=false with any network.* set warns per section that the fields are ignored (the
    /// operator's own PostgreSQL governs exposure). Asserted against the pure function's returned strings,
    /// NOT a live logger and NOT Validate().
    /// </summary>
    [Fact]
    public void GetNetworkStartupWarnings_ByoWithNetwork_WarnsBothSectionsIgnored()
    {
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig
            {
                Managed = false,
                ConnectionString = "Host=pg;Database=darling",
                Network = new PostgresNetworkConfig { Listen = "192.168.1.205" },
            },
            Mcp = new McpConfig { Network = new McpNetworkConfig { Listen = "192.168.1.205" } },
        };

        var warnings = DarlingWorker.GetNetworkStartupWarnings(config);
        Assert.Contains(warnings, w => w.Contains("postgres.network", StringComparison.Ordinal) && w.Contains("bring-your-own", StringComparison.Ordinal));
        /* #1804: the mcp notice names the container path now — in a container the block is HONORED, so
           the notice is suppressed there (see the bind ladder's container gate); outside one (this test
           process) it still fires. The postgres.network notice is container-independent: the bundled
           store never runs in BYO mode. */
        Assert.Contains(warnings, w => w.Contains("mcp.network", StringComparison.Ordinal) && w.Contains("managed-mode (or container, #1804) only", StringComparison.Ordinal));
        /* BYO never emits the admin-pivot warning — the network config is ignored anyway. */
        Assert.DoesNotContain(warnings, w => w.Contains("pivot", StringComparison.OrdinalIgnoreCase) || w.Contains("config_command", StringComparison.Ordinal));
    }

    /// <summary>
    /// D7: managed mode + an EXPOSED store whose network role is admin warns about the config-write /
    /// service-credential pivot a remote admin connection can reach.
    /// </summary>
    [Fact]
    public void GetNetworkStartupWarnings_ManagedExposedAdminRole_WarnsPivot()
    {
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig
            {
                Managed = true,
                Network = new PostgresNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Role = "admin" },
            },
        };

        var warnings = DarlingWorker.GetNetworkStartupWarnings(config);
        Assert.Contains(warnings, w => w.Contains("network.role admits 'admin'", StringComparison.Ordinal) && w.Contains("config_command", StringComparison.Ordinal));
        /* Managed mode does not emit the BYO "ignored" notices. */
        Assert.DoesNotContain(warnings, w => w.Contains("IGNORED", StringComparison.Ordinal));
    }

    /// <summary>
    /// #2665: the pivot warning follows the ROLE, not the spelling of the field. A value admitting both roles
    /// makes a remote admin connection just as reachable as <c>"admin"</c> alone does, so the D7 warning has to
    /// fire for every list form — the one that would silently stop warning is the one an operator reaches for
    /// precisely because they were thinking about the read-only half.
    /// </summary>
    [Theory]
    [InlineData("admin,viewer")]
    [InlineData("viewer,admin")]
    [InlineData("admin+viewer")]
    [InlineData("VIEWER, Admin")]
    public void GetNetworkStartupWarnings_ManagedExposedRoleListIncludingAdmin_StillWarnsPivot(string role)
    {
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig
            {
                Managed = true,
                Network = new PostgresNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Role = role },
            },
        };

        Assert.Contains(
            DarlingWorker.GetNetworkStartupWarnings(config),
            w => w.Contains("network.role admits 'admin'", StringComparison.Ordinal) && w.Contains("config_command", StringComparison.Ordinal));
    }

    /// <summary>The secure defaults are silent: managed + viewer role, managed + no network, and managed +
    /// admin role that is NOT exposed (loopback listen) all produce zero warnings.</summary>
    [Fact]
    public void GetNetworkStartupWarnings_ManagedViewerOrLoopback_IsSilent()
    {
        var viewerExposed = new DarlingConfig
        {
            Postgres = new PostgresConfig
            {
                Managed = true,
                Network = new PostgresNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Role = "viewer" },
            },
        };
        Assert.Empty(DarlingWorker.GetNetworkStartupWarnings(viewerExposed));

        Assert.Empty(DarlingWorker.GetNetworkStartupWarnings(new DarlingConfig { Postgres = new PostgresConfig { Managed = true } }));

        /* admin role but bound to loopback = not exposed => no pivot warning (nothing is reachable). */
        var adminButLoopback = new DarlingConfig
        {
            Postgres = new PostgresConfig
            {
                Managed = true,
                Network = new PostgresNetworkConfig { Listen = "127.0.0.1", AllowFrom = "192.168.1.0/24", Role = "admin" },
            },
        };
        Assert.Empty(DarlingWorker.GetNetworkStartupWarnings(adminButLoopback));
    }

    /// <summary>A fully-populated server definition the equality tests perturb a single field of.</summary>
    private static MonitoredServer SampleServer() => new()
    {
        Name = "prod-01",
        Host = "prod-host",
        Database = "AppDb",
        Auth = "sql",
        Username = "monitor",
        EncryptedPassword = "AQAAblob==",
        Password = null,
        EncryptMode = "Mandatory",
        TrustServerCertificate = true,
        ReadOnlyIntent = false,
        MultiSubnetFailover = false,
        MonthlyCostUsd = 500m,
        ExcludedDatabases = new List<string> { "tempdb" },
    };
}
