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
    public void BuildCreateSql_ServerScope_HasEventsPredicatesActionsAndTarget()
    {
        var sql = SchemaChangesXeSession.BuildCreateSql(Name, databaseScoped: false);

        Assert.Contains("ON SERVER", sql, StringComparison.Ordinal);
        foreach (var evt in new[] { "sqlserver.object_created", "sqlserver.object_altered", "sqlserver.object_deleted", "sqlserver.module_end" })
        {
            Assert.Contains(evt, sql, StringComparison.Ordinal);
        }

        Assert.Contains("ddl_phase = 1", sql, StringComparison.Ordinal);
        Assert.Contains("object_type <> 21587", sql, StringComparison.Ordinal);
        Assert.Contains("object_name = N'sp_rename'", sql, StringComparison.Ordinal);
        foreach (var action in new[] { "sqlserver.database_name", "sqlserver.server_principal_name", "sqlserver.client_app_name", "sqlserver.client_hostname", "package0.event_sequence" })
        {
            Assert.Contains(action, sql, StringComparison.Ordinal);
        }

        Assert.Contains("package0.ring_buffer", sql, StringComparison.Ordinal);
        Assert.Contains("MAX_EVENTS_LIMIT = 10000", sql, StringComparison.Ordinal);
        Assert.Contains("max_memory = 4096", sql, StringComparison.Ordinal);
        Assert.Contains("STARTUP_STATE = OFF", sql, StringComparison.Ordinal);
        Assert.Contains("MEMORY_PARTITION_MODE = NONE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sql_text", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildCreateSql_DatabaseScope_UsesOnDatabase_UsernameAction_NoPartitionMode()
    {
        var sql = SchemaChangesXeSession.BuildCreateSql(Name, databaseScoped: true);

        Assert.Contains("ON DATABASE", sql, StringComparison.Ordinal);
        Assert.Contains("sqlserver.username", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_principal_name", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("MEMORY_PARTITION_MODE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sql_text", sql, StringComparison.Ordinal);
        Assert.Contains("MAX_EVENTS_LIMIT = 10000", sql, StringComparison.Ordinal);
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

    [Fact]
    public void BuildStateProbeSql_ReadsTheCatalogOfItsScope()
    {
        Assert.Contains("sys.server_event_sessions", SchemaChangesXeSession.BuildStateProbeSql(databaseScoped: false), StringComparison.Ordinal);
        var azure = SchemaChangesXeSession.BuildStateProbeSql(databaseScoped: true);
        Assert.Contains("sys.database_event_sessions", azure, StringComparison.Ordinal);
        Assert.Contains("sys.dm_xe_database_sessions", azure, StringComparison.Ordinal);
        Assert.Contains("@session_name", azure, StringComparison.Ordinal);
    }
}
