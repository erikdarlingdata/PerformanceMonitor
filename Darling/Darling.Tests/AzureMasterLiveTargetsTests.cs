/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>Pins the Darling wiring of the Azure master duplicate skip: the list comes from the live store set, by store id.</summary>
public sealed class AzureMasterLiveTargetsTests
{
    private const string Host = "srv.database.windows.net";

    private static MonitoredServer S(int id, string name, string db) =>
        new() { Name = name, Host = Host, Database = db, StoredServerId = id };

    private static IReadOnlyList<string> Run(int self, IReadOnlyList<MonitoredServer>? live) =>
        AzureMasterScope.SeparatelyMonitoredDatabases(
            true, self.ToString(CultureInfo.InvariantCulture), Host, "master", DarlingWorker.LiveAlertTargets(live));

    [Fact]
    public void StoreDisabledTarget_AbsentFromLiveSet_DoesNotCount() =>
        Assert.Equal(new[] { "A" }, Run(1, new[] { S(1, "m", "master"), S(2, "a", "A") }));

    [Fact]
    public void StoreOnlyTarget_InLiveSet_Counts() =>
        Assert.Contains("Added", Run(1, new[] { S(1, "m", "master"), S(9, "added", "Added") }));

    [Fact]
    public void NullRegistry_GivesEmptyList()
    {
        Assert.Empty(DarlingWorker.LiveAlertTargets(null));
        Assert.Empty(Run(1, null));
    }

    [Fact]
    public void Self_IsExcludedByStoreId_EvenWhenAnotherTargetSharesTheName() =>
        Assert.Equal(new[] { "B" }, Run(1, new[] { S(1, "same", "master"), S(2, "same", "B") }));
}
