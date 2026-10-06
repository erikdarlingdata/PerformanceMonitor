/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The service's start creates the store's password rules on a store it does not provision itself. The start is one large
/// method on a live worker, so no unit seam reaches it; this reads the worker's source, comments removed, and fails when
/// either call is gone from the provisioning chain or moves out of its branch.
/// </summary>
public sealed class SelfManagedStoreRulesStartPinTests
{
    private static string ProvisioningChain()
    {
        var source = Regex.Replace(
            ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"),
            @"/\*.*?\*/|//[^\n]*", "", RegexOptions.Singleline);

        var begin = source.IndexOf("DarlingManagedRoles.EnsureProvisionedAsync(", StringComparison.Ordinal);
        Assert.True(begin > 0, "the managed provisioning call was not found");

        /* The chain ends at the TimescaleDB block's first call. */
        var end = source.IndexOf("TimescaleSupport.TryEnableAsync(", begin, StringComparison.Ordinal);
        Assert.True(end > begin, "the end of the provisioning chain was not found");
        return source[begin..end];
    }

    [Fact]
    public void ASelfManagedStoreStart_CreatesThePasswordRules_InBothBranchesThatDoNotProvisionTheStore()
    {
        var chain = ProvisioningChain();

        /* A container store whose compose provisioning did not provision it. */
        Assert.Matches(
            new Regex(
                @"ProvisionComposeStoreAsync\(.*?if \(verdict\.Provisioned\)\s*\{.*?\}\s*else\s*\{\s*await DarlingManagedRoles\.EnsureServerPasswordRulesAsync\(postgres, _logger, stoppingToken\);\s*\}",
                RegexOptions.Singleline),
            chain);

        /* Any other store that is not managed. */
        Assert.Matches(
            new Regex(
                @"else if \(!config\.Postgres\.Managed\)\s*\{\s*await DarlingManagedRoles\.EnsureServerPasswordRulesAsync\(postgres, _logger, stoppingToken\);\s*\}",
                RegexOptions.Singleline),
            chain);

        Assert.Equal(2, Regex.Matches(chain, @"EnsureServerPasswordRulesAsync\(").Count);
    }
}
