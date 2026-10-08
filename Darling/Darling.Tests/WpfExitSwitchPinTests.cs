/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5596: both test projects start WPF STA threads, so both declare the WPF switch that keeps the process exit short
/// (see <see cref="WpfExitSwitchTests"/>). A project that drops it, or a new WPF test project that never had it, goes back
/// to paying about 0.3 seconds per STA test thread after the run returns, and the runner then fails a green run with
/// "Foreground threads were left running". Each project's own runtime configuration is checked by
/// <see cref="WpfExitSwitchTests"/> inside its own process; this reads the project files so the failure names the project
/// before any shard runs.
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class WpfExitSwitchPinTests
{
    [Theory]
    [InlineData("Darling/Darling.Tests/Darling.Tests.csproj")]
    [InlineData("Lite.Tests/Lite.Tests.csproj")]
    public void Each_test_project_that_starts_WPF_STA_threads_sets_the_weak_event_table_exit_switch(string project)
    {
        var path = Path.Combine(RepoRoot(), project.Replace('/', Path.DirectorySeparatorChar));
        var text = File.ReadAllText(path);
        Assert.Contains(
            "<RuntimeHostConfigurationOption Include=\"" + WpfExitSwitchTests.SwitchName + "\" Value=\"true\"",
            text);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "Directory.Build.props")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, "could not locate the repo root from the test source path");
        return dir!;
    }
}
