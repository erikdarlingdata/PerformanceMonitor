/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's side of the client-timeout capture (#5640). The session is built by the shared builder, so Lite holds no copy of its
/// statement; Lite ensures it before the read, gated on the attention collector's schedule, and raises
/// <c>XeSessionEnsureException</c> on a refusal like the deadlock ensure. The statement text is pinned in the Darling twin
/// (<c>AttentionXeSessionTests</c>) because both products read the same builder; these tests pin Lite's use of it.
/// </summary>
public sealed class AttentionXeSessionLiteTests
{
    private static string ReadLite(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative)).Replace("\r\n", "\n", StringComparison.Ordinal);
    }

    [Fact]
    public void TheBuilderMakesTheSameStatementBothProductsSend()
    {
        var sql = AlwaysOnXeSessions.BuildAttentionServerCreateSql();

        Assert.Contains("ON SERVER", sql, StringComparison.Ordinal);
        Assert.Contains("ADD EVENT sqlserver.attention", sql, StringComparison.Ordinal);
        Assert.Contains("ADD TARGET package0.event_counter", sql, StringComparison.Ordinal);
        Assert.Contains("MEMORY_PARTITION_MODE = NONE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_hash", sql, StringComparison.Ordinal);

        var lite = AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.LiteProduct, "0a1b2c3d", AlwaysOnXeSessionKind.Attention);
        Assert.Equal("PerformanceMonitor_Lite_0a1b2c3d_Attention", lite);
        Assert.True(AlwaysOnXeSessions.IsOwnName(lite, AlwaysOnXeSessionKind.Attention));
        Assert.False(AlwaysOnXeSessions.IsOwnName(lite, AlwaysOnXeSessionKind.Deadlock));
    }

    [Fact]
    public void LiteHoldsNoCopyOfTheAttentionStatement_AndEnsuresThroughTheBuilder()
    {
        var ensure = ReadLite("Lite/Services/RemoteCollectorService.AttentionXeSession.cs");

        Assert.DoesNotContain("sqlserver.attention", ensure, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE EVENT SESSION", ensure, StringComparison.Ordinal);
        Assert.Contains("AlwaysOnXeSessions.BuildAttentionServerCreateSql()", ensure, StringComparison.Ordinal);
        Assert.Contains("AlwaysOnXeSessions.BuildAttentionServerProbeSql(", ensure, StringComparison.Ordinal);
        Assert.Contains("AlwaysOnXeSessionKind.Attention", ensure, StringComparison.Ordinal);
    }

    [Fact]
    public void TheEnsureIsGatedOnTheAttentionSchedule_AndRunsBeforeTheRead()
    {
        var ensure = ReadLite("Lite/Services/RemoteCollectorService.AttentionXeSession.cs");
        Assert.Contains("GetScheduleForServer(server.Id, \"attention\")", ensure, StringComparison.Ordinal);
        Assert.Contains("!schedule.Enabled", ensure, StringComparison.Ordinal);

        var runner = ReadLite("Lite/Services/RemoteCollectorService.cs");
        var call = runner.IndexOf("await EnsureAttentionXeSessionAsync(server, engineEdition, cancellationToken);", StringComparison.Ordinal);
        var read = runner.IndexOf("rowsCollected = collectorName switch", StringComparison.Ordinal);
        Assert.True(call > 0, "the run calls the attention ensure");
        Assert.True(call < read, "the ensure runs before the collector's read, so a refusal throws before a zero-row read");
    }

    [Fact]
    public void ARefusedServerScopedEnsure_RaisesTheEnsureException_ThroughTheSharedServerArm()
    {
        var ensure = ReadLite("Lite/Services/RemoteCollectorService.AttentionXeSession.cs");

        Assert.Contains("EnsureServerScopedXeSessionAsync(", ensure, StringComparison.Ordinal);
        Assert.Contains("EnsureDatabaseScopedXeSessionsAsync(", ensure, StringComparison.Ordinal);

        /* The shared server arm is what raises XeSessionEnsureException for the deadlock and blocked-process ensures too. */
        var arm = ReadLite("Lite/Services/RemoteCollectorService.BlockedProcessReport.cs");
        Assert.Contains("throw new XeSessionEnsureException(captureName, ex)", arm, StringComparison.Ordinal);
    }

    [Fact]
    public void RemovingAServer_ForgetsTheOwnAttentionSessionToo()
    {
        var removal = ReadLite("Lite/Services/RemoteCollectorService.LongQueryCompletions.cs");

        Assert.Contains("AlwaysOnXeSessionKind.BlockedProcess, AlwaysOnXeSessionKind.Attention", removal, StringComparison.Ordinal);
    }
}
