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
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The client-timeout capture (#5640): a third always-on Extended Events session, <c>sqlserver.attention</c> counted by an
/// event_counter target and sampled by a ring buffer. These tests pin the shared builder's statements for both scopes, the
/// kind switches (a kind with no arm throws, never reads as a deadlock session), the cleanup and removal lists, and the
/// Darling ensure's shape. The live behavior (a real timeout, the counter, the ring buffer) was checked on SQL Server 2019 by hand.
/// </summary>
public sealed class AttentionXeSessionTests
{
    private const string InstallIdValue = "0a1b2c3d";
    private const AlwaysOnXeSessionKind Attention = AlwaysOnXeSessionKind.Attention;

    [Fact]
    public void TheSharedNameIsPerformanceMonitorAttention_AndTheOwnNameCarriesTheProductAndTheId()
    {
        Assert.Equal("PerformanceMonitor_Attention", AlwaysOnXeSessions.SharedNameFor(Attention));
        Assert.Equal("PerformanceMonitor_Attention", AlwaysOnXeSessions.AttentionXeSessionName);
        Assert.Equal(
            "PerformanceMonitor_Darling_" + InstallIdValue + "_Attention",
            AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue, Attention));
    }

    [Fact]
    public void AnOwnNameBelongsToOneKindOnly()
    {
        foreach (var kind in Enum.GetValues<AlwaysOnXeSessionKind>())
        {
            var own = AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue, kind);
            foreach (var other in Enum.GetValues<AlwaysOnXeSessionKind>())
            {
                Assert.Equal(kind == other, AlwaysOnXeSessions.IsOwnName(own, other));
            }
        }
    }

    [Fact]
    public void TheServerScopedStatement_IsServerScoped_WithTheTwoTargetsTheFiveActionsAndNoHash()
    {
        var sql = AlwaysOnXeSessions.BuildAttentionServerCreateSql();

        Assert.Contains("CREATE EVENT SESSION [PerformanceMonitor_Attention]", sql, StringComparison.Ordinal);
        Assert.Contains("ON SERVER", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON DATABASE", sql, StringComparison.Ordinal);
        Assert.Contains("ADD EVENT sqlserver.attention", sql, StringComparison.Ordinal);
        foreach (var action in new[] { "client_app_name", "client_hostname", "database_name", "session_id", "sql_text" })
        {
            Assert.Contains("sqlserver." + action, sql, StringComparison.Ordinal);
        }

        /* query_hash reads 0 on every attention (observed on SQL Server 2019), so it is not captured. */
        Assert.DoesNotContain("query_hash", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_handle", sql, StringComparison.Ordinal);
        Assert.Contains("ADD TARGET package0.event_counter", sql, StringComparison.Ordinal);
        Assert.Contains("ADD TARGET package0.ring_buffer", sql, StringComparison.Ordinal);
        Assert.Contains("SET max_memory = 1024", sql, StringComparison.Ordinal);
        Assert.Contains("MEMORY_PARTITION_MODE = NONE", sql, StringComparison.Ordinal);
        Assert.Contains("STARTUP_STATE = ON", sql, StringComparison.Ordinal);
        Assert.Contains("EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS", sql, StringComparison.Ordinal);
        Assert.EndsWith("ALTER EVENT SESSION [PerformanceMonitor_Attention] ON SERVER STATE = START;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAzureStatement_IsDatabaseScoped_WithTheSameTargetsAndNoServerOnlyOption()
    {
        var shared = AlwaysOnXeSessions.BuildAzureCreateSql(Attention, AlwaysOnXeSessions.SharedNameFor(Attention));
        var own = AlwaysOnXeSessions.BuildAzureCreateSql(
            Attention, AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue, Attention));

        foreach (var sql in new[] { shared, own })
        {
            Assert.Contains("ON DATABASE", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("ON SERVER", sql, StringComparison.Ordinal);
            Assert.Contains("ADD EVENT sqlserver.attention", sql, StringComparison.Ordinal);
            Assert.Contains("ADD TARGET package0.event_counter", sql, StringComparison.Ordinal);
            Assert.Contains("SET max_memory = 1024", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("MEMORY_PARTITION_MODE", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("query_hash", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("xml_deadlock_report", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("blocked_process_report", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("max_memory = 4096", sql, StringComparison.Ordinal);
        }

        Assert.Contains("STARTUP_STATE = ON", shared, StringComparison.Ordinal);
        Assert.Contains("STARTUP_STATE = OFF", own, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAzureStatementCarriesEveryActionNotOnTheClosedRejectedList()
    {
        var sql = AlwaysOnXeSessions.BuildAzureCreateDefinitionSql(Attention, AlwaysOnXeSessions.SharedNameFor(Attention));

        foreach (var action in new[] { "client_app_name", "client_hostname", "database_name", "session_id", "sql_text" })
        {
            var rejected = AlwaysOnXeSessions.AttentionAzureRejectedActions.Contains("sqlserver." + action);
            Assert.Equal(!rejected, sql.Contains("sqlserver." + action, StringComparison.Ordinal));
        }
    }

    [Fact]
    public void EveryKindHasAnArmInEverySwitch_AndAKindWithNoArmThrows()
    {
        var names = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        foreach (var kind in Enum.GetValues<AlwaysOnXeSessionKind>())
        {
            var shared = AlwaysOnXeSessions.SharedNameFor(kind);
            Assert.True(names.Add(shared), "two kinds share the name " + shared);
            Assert.Contains("[" + shared + "]", AlwaysOnXeSessions.BuildAzureCreateSql(kind, shared), StringComparison.Ordinal);
            Assert.Contains("@session_name", AlwaysOnXeSessions.BuildAzureCatalogProbeSql(kind, "test"), StringComparison.Ordinal);
            Assert.False(string.IsNullOrWhiteSpace(AlwaysOnXeSessions.ReadOnlyDatabaseMessage(kind)));
        }

        var unknown = (AlwaysOnXeSessionKind)99;
        Assert.Throws<ArgumentOutOfRangeException>(() => AlwaysOnXeSessions.SharedNameFor(unknown));
        Assert.Throws<ArgumentOutOfRangeException>(() => AlwaysOnXeSessions.ReadOnlyDatabaseMessage(unknown));
        Assert.Throws<ArgumentOutOfRangeException>(() => AlwaysOnXeSessions.BuildAzureCatalogProbeSql(unknown, "test"));
        Assert.ThrowsAny<ArgumentException>(() => AlwaysOnXeSessions.BuildAzureCreateSql(unknown, "PerformanceMonitor_Attention"));
    }

    [Fact]
    public void TheReadOnlyMessageNamesTheClientTimeoutSession_NotTheDeadlockOne()
    {
        var message = AlwaysOnXeSessions.ReadOnlyDatabaseMessage(Attention);

        Assert.StartsWith("The client timeout Extended Events session could not be created", message, StringComparison.Ordinal);
        Assert.DoesNotContain("deadlock", message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCatalogProbeForAttention_CountsTheSessionPresentByNameAlone()
    {
        var sql = AlwaysOnXeSessions.BuildAzureCatalogProbeSql(Attention, "PerformanceMonitorDarling");

        Assert.Contains("sys.database_event_sessions", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("database_xml_deadlock_report", sql, StringComparison.Ordinal);
        Assert.Contains("THEN 2", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServerProbe_AsksForTheSharedSessionByParameter()
    {
        var sql = AlwaysOnXeSessions.BuildAttentionServerProbeSql("PerformanceMonitorDarling");

        Assert.Contains("/* PerformanceMonitorDarling */", sql, StringComparison.Ordinal);
        Assert.Contains("sys.server_event_sessions", sql, StringComparison.Ordinal);
        Assert.Contains("ses.name = @session_name", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCleanupVerbDropsTheSharedSession_AndThisInstallsOwnOne_AndListsOtherInstallsOnly()
    {
        var ownName = AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue, Attention);
        var otherLite = AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.LiteProduct, "ffffffff", Attention);

        Assert.Contains("PerformanceMonitor_Attention", DarlingXeSessionCleanup.SessionNames);
        Assert.Contains(ownName, DarlingXeSessionCleanup.InstallSessionNames(InstallIdValue));
        Assert.Contains(ownName, DarlingXeSessionCleanup.NamesToDrop(InstallIdValue));
        Assert.DoesNotContain(otherLite, DarlingXeSessionCleanup.NamesToDrop(InstallIdValue));
        Assert.True(DarlingXeSessionCleanup.IsOtherInstallSessionName(otherLite, InstallIdValue));
        Assert.False(DarlingXeSessionCleanup.IsOtherInstallSessionName(ownName, InstallIdValue));
        Assert.Contains("PerformanceMonitor[_]%[_]Attention", DarlingXeSessionCleanup.ComposeFindOthersSql(XeSessionScope.Server), StringComparison.Ordinal);
        Assert.Contains("PerformanceMonitor[_]%[_]Attention", DarlingXeSessionCleanup.ComposeFindOthersSql(XeSessionScope.Database), StringComparison.Ordinal);
    }

    [Fact]
    public void TheOwnSessionIsStoppedByTheBuilderBeforeADrop_AndTheCaptureNoteNamesClientTimeouts()
    {
        var ownName = AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue, Attention);

        Assert.Equal(AlwaysOnXeSessions.BuildAzureStopSql(Attention, ownName), DarlingXeSessionCleanup.ComposeStopStatement(ownName));

        var note = DarlingXeSessionCleanup.CaptureStopsNote(new[]
        {
            new ExistingXeSession("PerformanceMonitor_Attention", XeSessionScope.Server),
            new ExistingXeSession("PerformanceMonitor_Deadlock", XeSessionScope.Server),
        });
        Assert.NotNull(note);
        Assert.Contains("deadlocks and client timeouts", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServerEnsure_IsAThirdTryBlockThroughTheSharedBuilder_AndHoldsNoCopyOfTheStatement()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingXeSessions.cs");

        Assert.Contains("await EnsureAttentionOnPremAsync(connection, server, logger, cancellationToken);", source, StringComparison.Ordinal);
        Assert.Contains("AlwaysOnXeSessions.BuildAttentionServerCreateSql()", source, StringComparison.Ordinal);
        Assert.Contains("AlwaysOnXeSessionKind.Attention, \"client timeout\"", source, StringComparison.Ordinal);
        Assert.DoesNotContain("sqlserver.attention", source, StringComparison.Ordinal);

        /* Three captures, three separate try blocks: one capture's failure never skips another. */
        var ensureAll = source.Substring(source.IndexOf("public static async Task EnsureAllAsync", StringComparison.Ordinal));
        ensureAll = ensureAll.Substring(0, ensureAll.IndexOf("private static async Task EnsureDatabaseScopedAsync", StringComparison.Ordinal));
        Assert.Equal(3, ensureAll.Split("catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))").Length - 1);
    }

    [Fact]
    public void TheRemovalOfAServer_DropsTheOwnAttentionSessionToo()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingRemovedServerSessions.cs");

        Assert.Contains("AlwaysOnXeSessionKind.Attention", source, StringComparison.Ordinal);
    }
}
