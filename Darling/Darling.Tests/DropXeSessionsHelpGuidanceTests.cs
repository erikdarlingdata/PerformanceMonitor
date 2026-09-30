/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4732: the named form of <c>--drop-xe-sessions</c> finds only a server this service still monitors (the store's list of
/// enabled servers), and dropping the sessions on such a server stops its deadlock and blocked-process capture until this
/// service reconnects to it. So the help says to run it just before the server is removed, and to use <c>--print-sql</c>
/// after. It must not describe the named form's target as a server that is no longer monitored, which is the one target it
/// cannot reach.
/// </summary>
public sealed class DropXeSessionsHelpGuidanceTests
{
    private const string BeforeRemoval = "just before you remove the server";

    [Fact]
    public void TheHelpLineForTheNamedForm_NamesAServerStillMonitored_AndSaysToRunItBeforeRemoval()
    {
        var line = DarlingCliCommands.UsageText()
            .Split(Environment.NewLine)
            .Single(l => l.Contains("--drop-xe-sessions <server-name>", StringComparison.Ordinal));

        Assert.DoesNotContain("no longer monitored", line, StringComparison.Ordinal);
        Assert.Contains("still monitors", line, StringComparison.Ordinal);
        Assert.Contains(BeforeRemoval, line, StringComparison.Ordinal);
    }

    [Fact]
    public void TheVerbsOwnUsage_SaysToRunTheNamedFormBeforeRemoval_AndPrintSqlAfterIt()
    {
        var usage = DarlingCliCommands.DropXeSessionsUsageText();

        Assert.DoesNotContain("no longer monitored", usage, StringComparison.Ordinal);
        Assert.Contains(BeforeRemoval, usage, StringComparison.Ordinal);
        Assert.Contains("after the removal, use --print-sql", usage, StringComparison.Ordinal);
        Assert.All(usage, ch => Assert.True(ch < 128, $"usage text must be ASCII; found U+{(int)ch:X4}"));
    }
}
