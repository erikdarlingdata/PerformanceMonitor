/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The 60 s server ceiling (<see cref="ComposeLimits.StatementTimeout"/>, mirrored in
/// <see cref="DarlingConfig.ComposeStatementTimeoutSeconds"/>'s default) is what fires FIRST on a managed
/// store; every client-side read deadline paired with it must sit strictly above, so the store's own
/// <c>57014</c> cancellation is the error a user sees rather than a client-side stream fault (#2826, #4442).
/// This pin reads the constants directly rather than a copied list of numbers, so it catches a future edit
/// to any one of them without itself being edited.
/// </summary>
public sealed class PairedClientDeadlinesTests
{
    /// <summary>
    /// The server ceiling itself, read from both places it is stated: the compile-time doc constant
    /// (<see cref="ComposeLimits.StatementTimeout"/>, a Postgres interval literal like <c>"60s"</c>) and the
    /// file-seeded default a fresh store's <c>config_service</c> row starts at
    /// (<see cref="DarlingConfig.ComposeStatementTimeoutSeconds"/>'s property default). Both are the number
    /// each paired deadline below has to clear.
    /// </summary>
    private static int ServerCeilingSeconds()
    {
        var literal = ComposeLimits.StatementTimeout;
        Assert.EndsWith("s", literal, System.StringComparison.Ordinal);
        var fromDoc = int.Parse(literal[..^1], System.Globalization.CultureInfo.InvariantCulture);

        var fromConfigDefault = new DarlingConfig().ComposeStatementTimeoutSeconds;

        Assert.Equal(fromDoc, fromConfigDefault);
        return fromDoc;
    }

    [Fact]
    public void PairedClientDeadlines_AreAllStrictlyAboveTheServerCeiling()
    {
        var ceiling = ServerCeilingSeconds();

        Assert.True(
            McpCommandDeadlines.ReadSeconds > ceiling,
            $"McpCommandDeadlines.ReadSeconds ({McpCommandDeadlines.ReadSeconds}s) must sit strictly above " +
            $"the {ceiling}s server ceiling, or the mcp role's own 57014 never gets a chance to fire first.");

        Assert.True(
            StorageCommandDeadlines.McpReadSeconds > ceiling,
            $"StorageCommandDeadlines.McpReadSeconds ({StorageCommandDeadlines.McpReadSeconds}s) must sit " +
            $"strictly above the {ceiling}s server ceiling, for the same reason as the sibling reader above.");

        Assert.True(
            ViewerCommandDeadlines.InteractiveReadSeconds > ceiling,
            $"ViewerCommandDeadlines.InteractiveReadSeconds ({ViewerCommandDeadlines.InteractiveReadSeconds}s) " +
            $"must sit strictly above the {ceiling}s server ceiling; the WPF viewer runs under the same " +
            "viewer role GUC on a locked-down seat.");

        /* ComposedQueryFallbackSeconds is the one deliberate exception: its own doc comment says it mirrors
           the shipped server default rather than sitting above it ("a store that cannot answer behaves like
           a store nobody has tuned"), so it is pinned at EQUALITY, not "above" — a fallback used only when
           the store's own row cannot be read, standing in for the ceiling rather than bounding it. */
        Assert.Equal(ceiling, McpCommandDeadlines.ComposedQueryFallbackSeconds);
    }
}
