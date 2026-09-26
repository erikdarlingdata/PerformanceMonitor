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
/// #4427: <c>purge_now</c>'s raw-table step (<see cref="PerformanceMonitor.Darling.Service.DarlingWorker.RunPurgeNowAsync"/>)
/// had no pin at all before this — <see cref="RawTablesLeaveCatalogSweepLiveTests"/> and
/// <see cref="RawPurgeTriggerLiveTests"/> drive <c>TriggerRawPurgeCoreAsync</c> and
/// <c>BuildRawTablePurgeNowReportAsync</c> directly, so deleting the block inside
/// <c>RunPurgeNowAsync</c> that calls both of them (and that assigns the result to <c>rawTables</c> in the
/// response JSON) would pass every existing test.
///
/// <para>A source-shape pin over the method body, matching the class of pin
/// <c>StragglerCommandTimeoutTests</c>/<c>AlertMasterSwitchSurfaceTests</c> already use for wiring that a full
/// host is otherwise needed to exercise behaviourally. <see cref="RepoFile.ReadRepoFileLf"/> normalises CRLF
/// (this checkout's line ending) to LF so the anchors below do not have to embed <c>\r</c>.</para>
/// </summary>
public sealed class RawPurgeNowWiringTests
{
    private static string RunPurgeNowAsyncBody()
    {
        var text = RepoFile.ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        const string signature = "private async Task<CommandOutcome> RunPurgeNowAsync(";
        var start = text.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, "RunPurgeNowAsync not found — this pin's anchor has moved or the method was renamed/removed");

        /* The next method-level closing brace after the opening one, tracked by depth so a nested block
           (the `if (_timescaleAvailable)` raw step, its own try/catch) cannot end the scan early. */
        var openBrace = text.IndexOf('{', start);
        Assert.True(openBrace >= 0, "RunPurgeNowAsync has no opening brace — source shape is not what this pin expects");

        var depth = 0;
        var i = openBrace;
        for (; i < text.Length; i++)
        {
            if (text[i] == '{')
            {
                depth++;
            }
            else if (text[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    break;
                }
            }
        }

        Assert.True(depth == 0, "RunPurgeNowAsync's braces never balanced — source shape is not what this pin expects");

        return text.Substring(start, i - start + 1);
    }

    /// <summary>
    /// THE PIN: <c>RunPurgeNowAsync</c> calls the gated trigger AND the report builder, and the response it
    /// serializes carries a <c>rawTables</c> field. Deleting the #4427 raw-reporting block (mutation) removes
    /// one or more of these three anchors and fails the test.
    /// </summary>
    [Fact]
    public void RunPurgeNowAsync_CallsTheGatedTriggerAndTheReportBuilder_AndCarriesRawTablesInTheResponse()
    {
        var body = RunPurgeNowAsyncBody();

        Assert.Contains("TriggerRawPurgeCoreAsync(", body, StringComparison.Ordinal);
        Assert.Contains("BuildRawTablePurgeNowReportAsync(", body, StringComparison.Ordinal);
        Assert.Contains("rawTables", body, StringComparison.Ordinal);
    }
}
