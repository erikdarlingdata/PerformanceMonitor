/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A denied XE session logs at Warn, not Error.
///
/// <para><b>Why this is worth pinning.</b> Declining to grant <c>ALTER ANY EVENT SESSION</c> is a
/// least-privilege choice a customer is entitled to make (#1823), and the collector already treats it as
/// one: it classifies the outcome as <c>PERMISSIONS</c> and flags the collector so the scheduler stops
/// retrying it for the session. The logging did not agree. A field log from #2594 carried three consecutive
/// <c>[ERROR]</c> lines — two from the XE layer, one from the collector — for a login that was simply not
/// granted the permission, while every other permission denial in the same method logs at Warn. Someone
/// reading that log reasonably concludes something is broken.</para>
///
/// <para>Anchored on the logger call adjacent to the permission test rather than on the message text, so a
/// reworded message does not fail this and a level change does.</para>
/// </summary>
public class XeSessionPermissionLogLevelTests
{
    /* #4964 moved the logging of a failed create out of each session's own routine and into ONE shared ensure
       (EnsureServerScopedXeSessionAsync, in the blocked-process file), so the deadlock, blocked-process and attention
       (client timeout, #5640) sessions cannot drift apart. Each routine lets a failure propagate to that ensure. */
    private const string SharedEnsureFile = "RemoteCollectorService.BlockedProcessReport.cs";

    /* The file of each session's ensure, and the constant naming the session it hands to the shared ensure. */
    private static readonly (string File, string SessionConstant)[] SessionEnsures =
    {
        ("RemoteCollectorService.Deadlocks.cs", "DeadlockXeSessionName"),
        ("RemoteCollectorService.BlockedProcessReport.cs", "BlockedProcessXeSessionName"),
        ("RemoteCollectorService.AttentionXeSession.cs", "AttentionXeSessionName"),
    };

    [Fact]
    public void ADeniedXeSession_LogsAtWarn_NotError()
    {
        var source = ReadServiceSource(SharedEnsureFile);

        var guards = Regex.Matches(
            source,
            @"if \(SqlServerPermissionErrors\.IsPermissionDenied\(ex\.Number\)\)\s*\{\s*AppLogger\.(\w+)\(",
            RegexOptions.Singleline);

        Assert.True(
            guards.Count > 0,
            $"{SharedEnsureFile} no longer routes a denied XE session through IsPermissionDenied, so a least-privilege "
            + "login is back to logging as a fault.");

        foreach (Match guard in guards)
        {
            Assert.Equal("Warn", guard.Groups[1].Value);
        }

        AssertEverySessionRoutesThroughTheSharedEnsure();
    }

    /// <summary>
    /// The Warn and Error arms only cover a session whose routine hands its failure to the shared ensure: each
    /// session's file must call it with the session's own name, and must not catch and log the failure a second time
    /// (a line there would repeat at full level every cycle, which the shared ensure's log-once rule ends).
    /// </summary>
    private static void AssertEverySessionRoutesThroughTheSharedEnsure()
    {
        foreach (var (file, sessionConstant) in SessionEnsures)
        {
            var source = ReadServiceSource(file);

            Assert.True(
                Regex.IsMatch(source, @"EnsureServerScopedXeSessionAsync\(\s*server,\s*""[^""]+"",\s*" + sessionConstant + @"\b"),
                $"{file} no longer hands the {sessionConstant} session to EnsureServerScopedXeSessionAsync, so its failure "
                + "skips the Warn-when-denied, Error-otherwise rule.");

            if (file != SharedEnsureFile)
            {
                Assert.DoesNotContain("AppLogger.Error(\"XeSession\"", source, StringComparison.Ordinal);
                Assert.DoesNotContain("catch (SqlException", source, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>
    /// The Error arm must survive. Downgrading everything would hide a genuine XE failure — a session that
    /// cannot start for a reason that is not permissions is exactly what #1086 made loud.
    /// </summary>
    [Fact]
    public void AnXeFailureThatIsNotPermissions_StillLogsAtError()
    {
        var source = ReadServiceSource(SharedEnsureFile);

        Assert.Contains("AppLogger.Error(\"XeSession\"", source, StringComparison.Ordinal);

        AssertEverySessionRoutesThroughTheSharedEnsure();
    }

    private static string ReadServiceSource(string fileName)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);

        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "Lite", "Services")))
        {
            dir = dir.Parent;
        }

        Assert.NotNull(dir);

        var path = Path.Combine(dir!.FullName, "Lite", "Services", fileName);
        Assert.True(File.Exists(path), $"could not locate {fileName}");

        return File.ReadAllText(path);
    }
}
