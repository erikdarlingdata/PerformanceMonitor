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
/// Lite's ensure of the client-timeout session and its two siblings (#5640, review round 1): the inner ensures no longer log
/// their own failures (L1), and the client-timeout ensure replaces a session of its name with another definition (L4).
/// The outer ensure's log-once rule (#4964) is driven in <c>DeadlockAndBlockedProcessEnsureLogLevelLiteTests</c>, which now
/// covers the client-timeout session as a third session.
/// </summary>
public sealed class AttentionXeSessionEnsureLiteTests
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

    /// <summary>
    /// L1: the inner ensure's catches logged Warn or Error and rethrew, so a failure that repeats on every 1-minute cycle logged
    /// at full level every minute, round the log-once rule the outer ensure applies. A failure now propagates to the outer
    /// catch, the one place that logs it.
    /// </summary>
    [Theory]
    [InlineData("Lite/Services/RemoteCollectorService.AttentionXeSession.cs")]
    [InlineData("Lite/Services/RemoteCollectorService.BlockedProcessReport.cs")]
    [InlineData("Lite/Services/RemoteCollectorService.Deadlocks.cs")]
    public void TheInnerServerScopedEnsures_DoNotLogTheirOwnFailure(string file)
    {
        var source = ReadLite(file);
        var start = source.IndexOf("SessionOnPremAsync(SqlConnection connection", StringComparison.Ordinal);
        Assert.True(start > 0, "the inner on-prem ensure is in " + file);
        var inner = source[start..];
        var next = inner.IndexOf("\n    /// <summary>", 1, StringComparison.Ordinal);
        if (next > 0)
        {
            inner = inner[..next];
        }

        Assert.DoesNotContain("Failed to start", inner, StringComparison.Ordinal);
        Assert.DoesNotContain("Failed to create", inner, StringComparison.Ordinal);
        Assert.DoesNotContain("AppLogger.Error", inner, StringComparison.Ordinal);
    }

    /// <summary>The outer ensure still owns the failure line, with the log-once rule, so dropping the inner lines loses none.</summary>
    [Fact]
    public void TheOuterServerScopedEnsure_StillLogsTheFailureOnce_ThenAtDebug()
    {
        var source = ReadLite("Lite/Services/RemoteCollectorService.BlockedProcessReport.cs");

        Assert.Contains("var repeatsAtDebug = _databaseScopedXeSessionEnsureWarned.ContainsKey(warnedKey);", source, StringComparison.Ordinal);
        Assert.Contains("AppLogger.Debug(\"XeSession\", failure);", source, StringComparison.Ordinal);
        Assert.Contains("throw new XeSessionEnsureException(captureName, ex) { RepeatsAtDebug = repeatsAtDebug };", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// L4: the probe answers 2 for another definition; the ensure drops that session through the builder's statement and falls
    /// through to the create, once per run of the app and server.
    /// </summary>
    [Fact]
    public void TheAttentionEnsure_ReplacesASessionWithAnotherDefinition_OncePerServer()
    {
        var source = ReadLite("Lite/Services/RemoteCollectorService.AttentionXeSession.cs");

        Assert.Contains("mismatch == AlwaysOnXeSessions.AttentionDefinitionMismatch", source, StringComparison.Ordinal);
        Assert.Contains("AlwaysOnXeSessions.BuildAttentionServerDropSql()", source, StringComparison.Ordinal);
        Assert.Contains("_attentionDefinitionReplaced.TryAdd(server.Id, true)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("DROP EVENT SESSION", source, StringComparison.Ordinal);
        Assert.Equal("DROP EVENT SESSION [PerformanceMonitor_Attention] ON SERVER;", AlwaysOnXeSessions.BuildAttentionServerDropSql());
    }

    /// <summary>L6: the Azure create the engine refuses for an unproved action is retried by the shared driver; Lite words the note.</summary>
    [Fact]
    public void TheAzureEnsureNote_ForACreateWithoutStatementText_IsLoggedAsAWarning()
    {
        var source = ReadLite("Lite/Services/RemoteCollectorService.AlwaysOnXeSessions.cs");

        Assert.Contains("case AlwaysOnXeChange.CreatedWithoutStatementText:", source, StringComparison.Ordinal);
        Assert.Contains("error 25744", source, StringComparison.Ordinal);
    }
}
