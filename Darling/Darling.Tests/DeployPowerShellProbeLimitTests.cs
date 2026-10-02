/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The deploy-script tests run each Windows PowerShell probe as one <c>powershell.exe</c> call. A cold
/// start or a busy runner has outlasted a 60 s wait with no output, so both helpers wait on a named
/// 180 s limit. These pins read the helpers' source: the probes themselves only run on Windows.
/// </summary>
public sealed class DeployPowerShellProbeLimitTests
{
    [Theory]
    [InlineData("DarlingDeployStaleFileTests.cs")]
    [InlineData("DarlingDeployRollbackRetentionTests.cs")]
    public void TheProbeHelper_WaitsOnTheNamedOneHundredEightySecondLimit(string file)
    {
        var source = RepoFile.ReadRepoFile("Darling", "Darling.Tests", file);

        Assert.Contains("PowerShellExitLimit = TimeSpan.FromSeconds(180);", source, StringComparison.Ordinal);
        Assert.Contains("process.WaitForExit(PowerShellExitLimit)", source, StringComparison.Ordinal);
        Assert.Contains("{PowerShellExitLimit.TotalSeconds:0} seconds", source, StringComparison.Ordinal);

        var oldLiteral = "WaitForExit(" + "60_000)";
        Assert.DoesNotContain(oldLiteral, source, StringComparison.Ordinal);
    }
}
