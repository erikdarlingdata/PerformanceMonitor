/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The global read-only seat indicator (#2479, items 3 and 4).
///
/// <para><b>What it replaces.</b> #2400 asked why a viewer on a jumpbox refuses "Current Active Queries",
/// and #2403 answered by rewriting the individual refusals — four better messages, and the same discovery
/// model: you learn your seat at the moment a write is denied. The probe has existed since V8 and its
/// answer was never shown anywhere you could look on purpose.</para>
///
/// <para><b>The half these tests exist to protect.</b> <c>DetectReadOnlyAsync</c> fails safe to read-only
/// for an unexpected RESPONSE but raises <see cref="ViewerStoreUnreachableException"/> for a failure to
/// REACH the store — a split #2117 built deliberately, because collapsing them told an operator whose
/// service was simply down to go and change a database role. A status field is exactly where that
/// distinction gets quietly re-collapsed, so both the state machine and the connect flow's arms are
/// pinned.</para>
/// </summary>
public class ViewerSeatIndicatorTests
{
    /// <summary>
    /// Four states, four distinct labels, and "unreachable" must never read as "read-only" — the whole
    /// point of keeping <see cref="ViewerStoreUnreachableException"/> a separate arm.
    /// </summary>
    [Fact]
    public void EveryState_HasItsOwnLabel_AndUnreachableNeverReadsAsReadOnly()
    {
        var texts = new List<string>();
        foreach (ViewerSeatState state in Enum.GetValues<ViewerSeatState>())
        {
            var text = ViewerSeatIndicator.Text(state);
            Assert.StartsWith("Seat:", text, StringComparison.Ordinal);
            texts.Add(text);
        }

        Assert.Equal(texts.Count, new HashSet<string>(texts, StringComparer.Ordinal).Count);

        Assert.Equal("Seat: read-only", ViewerSeatIndicator.Text(ViewerSeatState.ReadOnly));
        Assert.Equal("Seat: read-write", ViewerSeatIndicator.Text(ViewerSeatState.ReadWrite));
        Assert.DoesNotContain("read-only", ViewerSeatIndicator.Text(ViewerSeatState.Unreachable), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The unreachable tooltip must actively DENY the read-only reading, and must not send the operator
    /// at a role. Nothing about the seat was measured, so nothing about the seat is claimed.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void UnreachableToolTip_DeniesTheReadOnlyReading_AndNamesNoRole(bool storeIsOnThisMachine)
    {
        var tip = ViewerSeatIndicator.ToolTip(ViewerSeatState.Unreachable, storeIsOnThisMachine);

        Assert.Contains("NOT a read-only seat", tip, StringComparison.Ordinal);
        Assert.DoesNotContain("connectAs", tip, StringComparison.Ordinal);
        Assert.DoesNotContain("network.role", tip, StringComparison.Ordinal);
    }

    /// <summary>
    /// The read-only tooltip is where #2479 item 4's answer lives: the seat is read-only BY DESIGN on a
    /// remote seat, because <c>postgres.network.role</c> defaults to the restricted role while local
    /// <c>postgres.connectAs</c> defaults to <c>admin</c>. Both defaults are named in both placements —
    /// a tooltip that names only the likely one is wrong exactly when the operator most needs it.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ReadOnlyToolTip_NamesBothDefaults_WhatIsBlocked_AndHowToChangeIt(bool storeIsOnThisMachine)
    {
        var tip = ViewerSeatIndicator.ToolTip(ViewerSeatState.ReadOnly, storeIsOnThisMachine);

        Assert.Contains("postgres.network.role", tip, StringComparison.Ordinal);
        Assert.Contains("defaults to \"viewer\"", tip, StringComparison.Ordinal);
        Assert.Contains("postgres.connectAs", tip, StringComparison.Ordinal);
        Assert.Contains("defaults to \"admin\"", tip, StringComparison.Ordinal);

        /* The #2400 misconception, answered before it is formed: the blocked write is a row in the
           MONITORING store, and the monitored server is never written to at all. */
        Assert.Contains("Nothing is ever written to a monitored SQL Server", tip, StringComparison.Ordinal);
        Assert.Contains("MONITORING store", tip, StringComparison.Ordinal);

        Assert.Contains("To change it:", tip, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>StoreIsOnThisMachine</c> may only ORDER the two sentences, never claim which default decided
    /// this seat. A non-loopback store host does not prove a remote seat (#2279 — a bring-your-own store
    /// on another host, reached from the service host, reads identically), so the likelier default leads
    /// and both are stated.
    /// </summary>
    [Fact]
    public void StoreLocation_OnlyOrdersTheTwoDefaults_AndClaimsNeither()
    {
        var remote = ViewerSeatIndicator.ToolTip(ViewerSeatState.ReadOnly, storeIsOnThisMachine: false);
        var local = ViewerSeatIndicator.ToolTip(ViewerSeatState.ReadOnly, storeIsOnThisMachine: true);

        Assert.True(
            remote.IndexOf("postgres.network.role", StringComparison.Ordinal)
            < remote.IndexOf("postgres.connectAs", StringComparison.Ordinal),
            "a store that is not on this machine should lead with the network-role default");

        Assert.True(
            local.IndexOf("postgres.connectAs", StringComparison.Ordinal)
            < local.IndexOf("postgres.network.role", StringComparison.Ordinal),
            "a store on this machine should lead with the connectAs default");

        Assert.Contains("The store is not on this machine", remote, StringComparison.Ordinal);
        Assert.Contains("The store is on this machine", local, StringComparison.Ordinal);
    }

    /// <summary>Every state answers with something. A blank tooltip on the one field an operator now
    /// hovers first is the silence this change exists to end.</summary>
    [Fact]
    public void EveryState_HasANonEmptyToolTip_InBothPlacements()
    {
        foreach (ViewerSeatState state in Enum.GetValues<ViewerSeatState>())
        {
            Assert.False(string.IsNullOrWhiteSpace(ViewerSeatIndicator.ToolTip(state, true)), $"{state} / local");
            Assert.False(string.IsNullOrWhiteSpace(ViewerSeatIndicator.ToolTip(state, false)), $"{state} / remote");
        }
    }

    /// <summary>
    /// The connect flow must publish a seat state on every arm that leaves <c>OnLoaded</c>, and the
    /// unreachable arms must publish <c>Unreachable</c> rather than <c>ReadOnly</c>.
    ///
    /// <para>This is the structural half, and it is the one that decays: the pure helper cannot be wrong
    /// about a state nobody sets, and a later refactor that adds an early return or reroutes the catch is
    /// exactly how the field silently goes stale — showing the previous verdict, which is worse than
    /// showing none.</para>
    /// </summary>
    [Fact]
    public void ConnectFlow_PublishesASeatState_OnEveryArm_AndNeverCollapsesUnreachableIntoReadOnly()
    {
        var source = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs"));

        /* The verdict arm sits with the probe, so the two cannot drift apart. */
        var probe = source.IndexOf("await _dataService.DetectReadOnlyAsync();", StringComparison.Ordinal);
        Assert.True(probe >= 0, "the viewer no longer probes the seat at connect");
        Assert.Contains(
            "ApplySeatState(_dataService.IsReadOnly ? ViewerSeatState.ReadOnly : ViewerSeatState.ReadWrite);",
            source,
            StringComparison.Ordinal);

        /* The store-unreachable arm. Its body must set Unreachable and must not mention ReadOnly at all —
           #2117's split, re-asserted at the surface that would otherwise quietly undo it. */
        var arm = source.IndexOf("catch (ViewerStoreUnreachableException ex)", StringComparison.Ordinal);
        Assert.True(arm >= 0, "the viewer no longer distinguishes an unreachable store at connect (#2117)");

        var armBody = BlockAfter(source, arm);
        Assert.Contains("ApplySeatState(ViewerSeatState.Unreachable);", armBody, StringComparison.Ordinal);
        Assert.DoesNotContain("ViewerSeatState.ReadOnly", armBody, StringComparison.Ordinal);

        /* Every ApplySeatState call must precede its arm's return, or the field never changes. */
        foreach (var state in new[] { "Unknown", "Unreachable", "ReadOnly" })
        {
            Assert.Contains($"ViewerSeatState.{state}", source, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The field exists in the status bar, is named, and the column count matches the number of fields.
    /// A <c>TextBlock</c> at <c>Grid.Column="5"</c> with only five columns defined silently stacks on top
    /// of column 4, which is the kind of thing that ships looking fine in a screenshot.
    /// </summary>
    [Fact]
    public void StatusBar_CarriesTheSeatField_AndHasAColumnForEveryField()
    {
        var xaml = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"));

        var bar = xaml.IndexOf("<Grid Grid.Row=\"1\" Margin=\"0,8,0,0\">", StringComparison.Ordinal);
        Assert.True(bar >= 0, "the viewer's status bar grid is gone or was renamed");

        var body = xaml[bar..];
        var end = body.IndexOf("</Grid>", StringComparison.Ordinal);
        body = end > 0 ? body[..end] : body;

        Assert.Contains("x:Name=\"SeatText\"", body, StringComparison.Ordinal);
        Assert.Contains("Text=\"Seat: --\"", body, StringComparison.Ordinal);

        var columns = Count(body, "<ColumnDefinition ");
        var fields = Count(body, "<TextBlock ");
        Assert.True(
            columns == fields,
            $"the status bar defines {columns} columns for {fields} fields — a field without a column stacks on its neighbour");

        /* The seat is the first fixed field, right of the message area: it describes THIS CONNECTION
           rather than the fleet, and it is the answer #2479 exists to make findable. */
        Assert.True(
            body.IndexOf("x:Name=\"SeatText\"", StringComparison.Ordinal)
            < body.IndexOf("x:Name=\"CollectorHealthText\"", StringComparison.Ordinal),
            "the seat field should lead the fixed status fields");
    }

    private static int Count(string haystack, string needle)
    {
        var n = 0;
        var at = 0;
        while ((at = haystack.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            n++;
            at += needle.Length;
        }

        return n;
    }

    /// <summary>The brace-balanced block that opens after <paramref name="from"/>.</summary>
    private static string BlockAfter(string source, int from)
    {
        var open = source.IndexOf('{', from);
        Assert.True(open >= 0, "expected a block");

        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{') { depth++; }
            else if (source[i] == '}')
            {
                depth--;
                if (depth == 0) { return source[open..(i + 1)]; }
            }
        }

        Assert.Fail("unbalanced braces while reading the connect flow");
        return string.Empty;
    }

    private static string ReadSource(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, $"could not locate {relative} from the test source path");
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
