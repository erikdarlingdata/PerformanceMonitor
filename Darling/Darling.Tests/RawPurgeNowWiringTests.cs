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
/// #4427: <c>purge_now</c>'s raw-table step (<see cref="PerformanceMonitor.Darling.Service.DarlingWorker.RunPurgeNowBackgroundAsync"/>)
/// had no pin at all before this — <see cref="RawTablesLeaveCatalogSweepLiveTests"/> and
/// <see cref="RawPurgeTriggerLiveTests"/> drive <c>TriggerRawPurgeCoreAsync</c> and
/// <c>BuildRawTablePurgeNowReportAsync</c> directly, so deleting the block inside
/// <c>RunPurgeNowBackgroundAsync</c> that calls both of them (and that turns the result into the second
/// collection-log run record) would pass every existing test.
///
/// <para>#4825: the purge no longer runs inline on the command loop. The command starts it in the daily purge's
/// own slot and answers at once, so the raw-table decisions that used to ride the command's result JSON go to a
/// run record the background work writes after the raw step. These pins hold that shape: the background body
/// is paced, labelled as a manual purge, and writes the raw record; the inline handler is gone; and the host
/// hands the purge the SERVICE's stopping token, never the per-command one that ends when the command does.</para>
///
/// <para>A source-shape pin over the method body, matching the class of pin
/// <c>StragglerCommandTimeoutTests</c>/<c>AlertMasterSwitchSurfaceTests</c> already use for wiring that a full
/// host is otherwise needed to exercise behaviourally. <see cref="RepoFile.ReadRepoFileLf"/> normalises CRLF
/// (this checkout's line ending) to LF so the anchors below do not have to embed <c>\r</c>.</para>
/// </summary>
public sealed class RawPurgeNowWiringTests
{
    private static string WorkerText() => RepoFile.ReadRepoFileLf(
        "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

    /// <summary>The text of the method whose declaration begins with <paramref name="signature"/>, up to its
    /// closing brace.</summary>
    private static string MethodBody(string text, string signature)
    {
        var start = text.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{signature} not found — this pin's anchor has moved or the method was renamed/removed");

        /* The next method-level closing brace after the opening one, tracked by depth so a nested block
           (the `if (timescaleAvailable)` raw step, its own try/catch) cannot end the scan early. */
        var openBrace = text.IndexOf('{', start);
        Assert.True(openBrace >= 0, $"{signature} has no opening brace — source shape is not what this pin expects");

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

        Assert.True(depth == 0, $"{signature}'s braces never balanced — source shape is not what this pin expects");

        return text.Substring(start, i - start + 1);
    }

    private static string BackgroundBody() => MethodBody(WorkerText(), "internal async Task RunPurgeNowBackgroundAsync(");

    /// <summary>
    /// THE PIN: the background purge calls the gated trigger AND the report builder, and turns the report into
    /// the second run record it writes. Deleting the #4427 raw-reporting block (mutation) removes one or more of
    /// these anchors and fails the test.
    /// </summary>
    [Fact]
    public void RunPurgeNowBackgroundAsync_CallsTheGatedTriggerAndTheReportBuilder_AndWritesTheRawRunRecord()
    {
        var body = BackgroundBody();

        Assert.Contains("TriggerRawPurgeCoreAsync(", body, StringComparison.Ordinal);
        Assert.Contains("BuildRawTablePurgeNowReportAsync(", body, StringComparison.Ordinal);
        Assert.Contains("BuildRawPurgeNowRunRecord(", body, StringComparison.Ordinal);
        Assert.Contains("LogRetentionRunAsync(", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The background purge is paced like the daily one and says in its run record that it was manual.
    /// </summary>
    [Fact]
    public void RunPurgeNowBackgroundAsync_PacesItsWal_AndLabelsTheRunRecordAsManual()
    {
        var body = BackgroundBody();
        var callAt = body.IndexOf("DarlingRetention.PurgeAsync(", StringComparison.Ordinal);
        Assert.True(callAt >= 0, "the PurgeAsync call moved");
        var callEnd = body.IndexOf(");", callAt, StringComparison.Ordinal) + 2;
        var call = body.Substring(callAt, callEnd - callAt);

        Assert.Contains("paceWal: true", call, StringComparison.Ordinal);
        Assert.Contains("runLabel:", call, StringComparison.Ordinal);
        Assert.Contains("BuildManualPurgeLabel(", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The inline handler is gone: nothing on the command loop awaits the purge any more, and the host adapter
    /// starts it in the shared slot with the service's stopping token — not the token the command loop hands the
    /// command, which is a per-command one.
    /// </summary>
    [Fact]
    public void PurgeNow_StartsInTheBackground_OnTheServicesStoppingToken()
    {
        var worker = WorkerText();

        Assert.DoesNotContain("private async Task<CommandOutcome> RunPurgeNowAsync(", worker, StringComparison.Ordinal);

        var host = MethodBody(worker, "public Task<CommandOutcome> PurgeNowAsync(");
        var hostBody = host.Substring(host.IndexOf('{'));
        Assert.Contains("_stoppingToken", hostBody, StringComparison.Ordinal);
        Assert.DoesNotContain("cancellationToken", hostBody, StringComparison.Ordinal);

        var start = MethodBody(worker, "private CommandOutcome StartPurgeNow(");
        Assert.Contains("TryStartPurgeNow(", start, StringComparison.Ordinal);
        Assert.Contains("RunPurgeNowBackgroundAsync(", start, StringComparison.Ordinal);
    }
}
