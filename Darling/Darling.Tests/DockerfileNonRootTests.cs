/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The container runs as the aspnet image's built-in non-root user, <c>$APP_UID</c> (#8141), and that user owns the
/// credentials and log folders the service writes to.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DockerfileNonRootTests
{
    private static string[] RuntimeStageLines()
    {
        var lines = ReadRepoFile("Darling", "Dockerfile").Split('\n').Select(l => l.TrimEnd('\r')).ToArray();
        var lastFrom = Array.FindLastIndex(lines, l => l.StartsWith("FROM ", StringComparison.Ordinal));
        Assert.True(lastFrom >= 0, "Darling/Dockerfile has no FROM line.");
        return lines.Skip(lastFrom).ToArray();
    }

    [Fact]
    public void RuntimeStage_SwitchesToAppUid_AfterEveryRun()
    {
        var stage = RuntimeStageLines();
        var user = Array.IndexOf(stage, "USER $APP_UID");
        Assert.True(user >= 0, "The runtime stage has no `USER $APP_UID` line.");

        var lastRun = Array.FindLastIndex(stage, l => l.StartsWith("RUN ", StringComparison.Ordinal));
        Assert.True(user > lastRun, "`USER $APP_UID` must come after every RUN, which still needs root to install and chown.");
    }

    [Fact]
    public void RuntimeStage_GivesTheCredentialsAndLogFoldersToAppUid()
    {
        var stage = string.Join("\n", RuntimeStageLines());
        Assert.Contains("mkdir -p /var/lib/darling/credentials /usr/share/PerformanceMonitorDarling/logs", stage);
        Assert.Contains("chown -R $APP_UID:$APP_UID /var/lib/darling /usr/share/PerformanceMonitorDarling", stage);
        Assert.Contains("chmod 700 /var/lib/darling/credentials", stage);
    }

    [Fact]
    public void RuntimeStage_NeverNamesTheUserLiterally()
    {
        var stage = string.Join("\n", RuntimeStageLines());
        Assert.DoesNotContain("USER app", stage);
        Assert.DoesNotContain("1654", stage);
    }
}
