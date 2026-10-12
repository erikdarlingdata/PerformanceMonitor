/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The first alert sweep after a start, for an Azure SQL Database master target. No connection check has read the
/// edition yet, so the status is blank. The snapshot is built the way CheckPerformanceAlerts builds it: from the
/// configured server, with the edition stored under that server's storage id. Blocking and deadlocks in the databases
/// monitored as their own targets are not counted. On the live edition alone, the master was unscoped in this sweep
/// and fired on them.
/// </summary>
public partial class LiteAlertForwardingTests
{
    private const string AzureHost = "example-sql.database.windows.net";

    private static List<ServerConnection> AzureFleet(out ServerConnection master)
    {
        master = new ServerConnection { Id = "cfg-master", ServerName = AzureHost, DatabaseName = "master" };
        return new List<ServerConnection>
        {
            master,
            new() { Id = "cfg-gp", ServerName = AzureHost, DatabaseName = "GP" },
            new() { Id = "cfg-hs", ServerName = AzureHost, DatabaseName = "HS" },
        };
    }

    /// <summary>The store's answer at startup: the master's newest server_properties row says Azure SQL Database.</summary>
    private static KnownEngineEditions SeededFromTheStore(ServerConnection master)
    {
        var editions = new KnownEngineEditions();
        editions.Seed(new Dictionary<int, int>
        {
            [KnownEngineEditions.StorageId(master)] = CollectorEngineCapability.AzureSqlDatabaseEngineEdition,
        });
        return editions;
    }

    /// <summary>The two scope fields as CheckPerformanceAlerts fills them for a configured server with a blank status.</summary>
    private static AlertServerSnapshot FirstSweepSnapshot(
        KnownEngineEditions editions, ServerConnection server, List<ServerConnection> servers)
    {
        const int blankStatusEdition = CollectorEngineCapability.UnknownEngineEdition;
        return Harness.Snapshot(isOnline: false, isAzureSqlDb: editions.IsAzureSqlDatabase(server, blankStatusEdition)) with
        {
            SeparatelyMonitoredDatabases = editions.SeparatelyMonitoredDatabases(server, blankStatusEdition, servers),
        };
    }

    private static BlockedProcessAlertRow BlockingRowIn(string database, int blockedSpid)
    {
        var row = BlockingRow(blockedSpid);
        row.DatabaseName = database;
        return row;
    }

    private static DeadlockAlertRow DeadlockRowIn(string database)
    {
        var row = DeadlockRow();
        row.DeadlockGraphXml = row.DeadlockGraphXml.Replace("currentdbname=\"StackOverflow\"", $"currentdbname=\"{database}\"");
        return row;
    }

    [Fact]
    public async Task AzureMaster_FirstSweep_StoredEdition_SiblingBlockingAndDeadlocksDoNotFire()
    {
        DisableAllChecks();
        App.AlertBlockingEnabled = true;
        App.AlertDeadlockEnabled = true;
        var servers = AzureFleet(out var master);
        var h = new Harness();
        h.Adapter.Blocking.Add(BlockingRowIn("GP", 51));
        h.Adapter.Blocking.Add(BlockingRowIn("HS", 52));
        h.Adapter.Deadlocks.Add(DeadlockRowIn("GP"));
        h.Adapter.Deadlocks.Add(DeadlockRowIn("HS"));

        var snapshot = FirstSweepSnapshot(SeededFromTheStore(master), master, servers);
        await h.Build().EvaluateServerAsync(snapshot);

        Assert.True(snapshot.IsAzureSqlDb);
        Assert.Equal(new[] { "GP", "HS" }, snapshot.SeparatelyMonitoredDatabases);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AzureMaster_FirstSweep_StoredEdition_CountsOnlyItsOwnBlockingAndDeadlocks()
    {
        DisableAllChecks();
        App.AlertBlockingEnabled = true;
        App.AlertDeadlockEnabled = true;
        var servers = AzureFleet(out var master);
        var h = new Harness();
        h.Adapter.Blocking.Add(BlockingRowIn("GP", 51));
        h.Adapter.Blocking.Add(BlockingRowIn("HS", 52));
        h.Adapter.Blocking.Add(BlockingRowIn("master", 53));
        h.Adapter.Deadlocks.Add(DeadlockRowIn("GP"));
        h.Adapter.Deadlocks.Add(DeadlockRowIn("master"));

        await h.Build().EvaluateServerAsync(FirstSweepSnapshot(SeededFromTheStore(master), master, servers));

        Assert.Equal("1", Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Detected").CurrentValue);
        Assert.Equal("1", Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Deadlocks Detected").CurrentValue);
    }
}
