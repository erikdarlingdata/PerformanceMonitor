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
/// store. On the web/MCP composed read path, the client-side deadline paired with it must sit strictly
/// above, so the store's own <c>57014</c> cancellation is the error a user sees rather than a client-side
/// stream fault (#2826, #4442). This pin reads the constants directly rather than a copied list of numbers,
/// so it catches a future edit to any one of them without itself being edited.
///
/// <para><b>Scope: the web/MCP composed reads only, not every client deadline in the project.</b> The WPF
/// Viewer's <see cref="ViewerCommandDeadlines.InteractiveReadSeconds"/> and the storage-MCP family's
/// <see cref="StorageCommandDeadlines.McpReadSeconds"/> are deliberate exceptions, pinned separately below:
/// both stay UNDER the server ceiling, on purpose, because #3004 measured a tighter bound for each (a
/// scarce ten-connection pool for the viewer, a hung-read cost for the storage reader) that this issue's
/// ceiling change did not re-measure. A user on either surface still sees a client-side timeout, not the
/// store's <c>57014</c>, until a follow-up re-derives those two bands with fresh latency data.</para>
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

        /* ComposedQueryFallbackSeconds is the other deliberate exception on this same surface: its own doc
           comment says it mirrors the shipped server default rather than sitting above it ("a store that
           cannot answer behaves like a store nobody has tuned"), so it is pinned at EQUALITY, not "above" —
           a fallback used only when the store's own row cannot be read, standing in for the ceiling rather
           than bounding it. */
        Assert.Equal(ceiling, McpCommandDeadlines.ComposedQueryFallbackSeconds);
    }

    /// <summary>
    /// The two client deadlines that stay DELIBERATELY under the server ceiling, named apart from the
    /// test above so a future raise of the ceiling has to notice and revisit them rather than silently
    /// dragging them along.
    ///
    /// <para>Both are #3004's measured bands: the WPF Viewer's <see cref="ViewerCommandDeadlines"/>
    /// (a scarce ten-connection pool one control can hold entirely) and the storage-MCP family's
    /// <see cref="StorageCommandDeadlines"/> (a hung read with no enclosing budget). Neither band was
    /// re-measured against the new 60 s ceiling this issue raised, so both stay under it: a user on either
    /// surface still sees a client-side timeout, not the store's <c>57014</c>, until a follow-up re-derives
    /// them with fresh latency data.</para>
    /// </summary>
    [Fact]
    public void TheViewerAndStorageMcpDeadlines_StayDeliberatelyUnderTheServerCeiling()
    {
        var ceiling = ServerCeilingSeconds();

        Assert.True(
            ViewerCommandDeadlines.InteractiveReadSeconds < ceiling,
            $"ViewerCommandDeadlines.InteractiveReadSeconds ({ViewerCommandDeadlines.InteractiveReadSeconds}s) " +
            $"must stay strictly under the {ceiling}s server ceiling — #3004's measured pool-contention band " +
            "for the WPF viewer, deliberately not re-derived by this issue.");

        Assert.True(
            StorageCommandDeadlines.McpReadSeconds < ceiling,
            $"StorageCommandDeadlines.McpReadSeconds ({StorageCommandDeadlines.McpReadSeconds}s) must stay " +
            $"strictly under the {ceiling}s server ceiling — #3004's measured hung-read band for the " +
            "storage-MCP reader family, deliberately not re-derived by this issue.");
    }
}
