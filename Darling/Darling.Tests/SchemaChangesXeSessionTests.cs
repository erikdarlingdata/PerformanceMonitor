/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643: the statements of the schema-changes session. The session records committed table, view, index and trigger DDL so the
/// schema pass reads only the objects that changed. It carries no statement text, so nothing a user typed leaves the monitored server.
/// </summary>
public sealed class SchemaChangesXeSessionTests
{
    private const string Id = "0a1b2c3d";
    private const string Name = "PerformanceMonitor_Darling_0a1b2c3d_SchemaChanges";

    [Fact]
    public void NameFor_IsPrefixDarlingIdSuffix()
    {
        Assert.Equal(Name, SchemaChangesXeSession.NameFor(Id));
        Assert.Equal(Name, SchemaChangesXeSession.TryNameFor(Id));
        Assert.True(SchemaChangesXeSession.IsInstallSessionName(Name));
        Assert.Null(SchemaChangesXeSession.TryNameFor("not-an-id"));
        Assert.Throws<ArgumentException>(() => SchemaChangesXeSession.NameFor("not-an-id"));
        Assert.False(SchemaChangesXeSession.IsInstallSessionName("PerformanceMonitor_Darling_0a1b2c3d_Deadlock"));
        Assert.False(SchemaChangesXeSession.IsInstallSessionName(null));
    }

    [Fact]
    public void CleanupPlan_DropsTheSchemaSession_ForThisInstall()
    {
        Assert.Contains(Name, DarlingXeSessionCleanup.InstallSessionNames(Id));
        Assert.Contains(Name, DarlingXeSessionCleanup.NamesToDrop(Id));
        Assert.False(DarlingXeSessionCleanup.IsOtherInstallSessionName(Name, Id));
        Assert.True(DarlingXeSessionCleanup.IsOtherInstallSessionName("PerformanceMonitor_Darling_ffeeddcc_SchemaChanges", Id));
        Assert.Contains("SchemaChanges", DarlingXeSessionCleanup.GuardedDropScript(), StringComparison.Ordinal);
        Assert.Contains("SchemaChanges", DarlingXeSessionCleanup.ComposeFindOthersSql(XeSessionScope.Server), StringComparison.Ordinal);

        var stop = DarlingXeSessionCleanup.ComposeStopStatement(Name);
        Assert.Contains(Name, stop, StringComparison.Ordinal);
        Assert.Contains("STATE = STOP", stop, StringComparison.Ordinal);
        Assert.Equal(
            "DROP EVENT SESSION [" + Name + "] ON SERVER;",
            DarlingXeSessionCleanup.DropStatement(Name, XeSessionScope.Server, Id));
    }
}
