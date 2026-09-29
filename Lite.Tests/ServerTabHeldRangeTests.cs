/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the Server tab holds its custom range as two UTC instants (<see cref="CustomRangeState"/>) and the pickers
/// on the toolbar only show it. The window a refresh reads is the pair as held, whatever zone the pickers are drawn
/// in; a switch of display mode draws the same pair in the new zone and parses nothing back.
///
/// <para>A range whose start is 06:30 UTC on 1 November is the case that needs this. On a US Eastern server that
/// instant is the SECOND 01:30 of the day (the clocks fall back at 06:00 UTC), and the picker text "01:30" names the
/// first (05:30 UTC). Read the text back after a redraw and the range moves an hour, so the redraw must not be read
/// as the user typing. The tab is a WPF control this suite does not instantiate: the window and the zone are pure
/// static methods tested here directly, and the wiring around them is a source pin.</para>
/// </summary>
public sealed class ServerTabHeldRangeTests
{
    private static ServerClock Eastern() => ServerClock.Resolve("Eastern Standard Time", -300);

    private static DateTime At(int day, int hour, int minute) =>
        new(2026, 11, day, hour, minute, 0, DateTimeKind.Unspecified);

    /* A range from the second 01:30 to the second 02:30 of the change day, as the tab holds it. */
    private static CustomRangeState Held()
    {
        var held = new CustomRangeState();
        held.Set(At(1, 6, 30), At(1, 7, 30));
        return held;
    }

    // ── The window a refresh reads ──

    [Theory]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    [InlineData(TimeDisplayMode.ServerTime)]
    public void AHeldRange_IsTheWindow_WhateverZoneThePickersAreDrawnIn(TimeDisplayMode mode)
    {
        var held = Held();
        var zone = ServerTab.PickerZone(mode, Eastern());

        /* The pickers are drawn in this zone. Drawing them reads the pair and writes nothing. */
        Assert.NotNull(held.Render(zone));

        var (hoursBack, fromUtc, toUtc) = ServerTab.CurrentWindowUtc(4, customSelected: true, held);

        Assert.Equal(4, hoursBack);
        Assert.Equal(At(1, 6, 30), fromUtc);
        Assert.Equal(At(1, 7, 30), toUtc);
    }

    [Fact]
    public void APresetOrAnEmptyHold_HasNoBounds()
    {
        var held = Held();
        Assert.Equal((4, (DateTime?)null, (DateTime?)null), ServerTab.CurrentWindowUtc(4, customSelected: false, held));

        var nothingHeldYet = new CustomRangeState();
        Assert.Equal((24, (DateTime?)null, (DateTime?)null), ServerTab.CurrentWindowUtc(24, customSelected: true, nothingHeldYet));
    }

    [Fact]
    public void ThePickerZone_IsUtc_ThisMachinesZone_OrTheTabsOwnServerClock()
    {
        var clock = Eastern();

        Assert.Equal(TimeZoneInfo.Utc, ServerTab.PickerZone(TimeDisplayMode.UTC, clock));
        Assert.Equal(TimeZoneInfo.Local, ServerTab.PickerZone(TimeDisplayMode.LocalTime, clock));

        /* The zone of the clock the tab was handed, so a tab that is not the selected one keeps its own server's zone. */
        var server = ServerTab.PickerZone(TimeDisplayMode.ServerTime, clock);
        Assert.Equal(TimeSpan.FromHours(-4), server.GetUtcOffset(DateTime.SpecifyKind(At(1, 5, 30), DateTimeKind.Utc)));
        Assert.Equal(TimeSpan.FromHours(-5), server.GetUtcOffset(DateTime.SpecifyKind(At(1, 6, 30), DateTimeKind.Utc)));
    }

    [Fact]
    public void ADisplayModeRoundTrip_ServerThenUtcThenServer_LeavesTheHeldInstantsAlone()
    {
        var held = Held();
        var clock = Eastern();
        var serverZone = ServerTab.PickerZone(TimeDisplayMode.ServerTime, clock);
        var utcZone = ServerTab.PickerZone(TimeDisplayMode.UTC, clock);

        var inServer = held.Render(serverZone);
        var inUtc = held.Render(utcZone);
        var inServerAgain = held.Render(serverZone);

        /* 06:30 UTC is 01:30 on the server's clock, and 07:30 UTC is 02:30 there. */
        Assert.Equal((At(1, 1, 30), At(1, 2, 30)), inServer);
        Assert.Equal((At(1, 6, 30), At(1, 7, 30)), inUtc);
        Assert.Equal(inServer, inServerAgain);

        Assert.Equal(At(1, 6, 30), held.FromUtc);
        Assert.Equal(At(1, 7, 30), held.ToUtc);
        Assert.Equal(At(1, 6, 30), ServerTab.CurrentWindowUtc(4, customSelected: true, held).fromUtc);
    }

    /// <summary>
    /// The fact that makes the redraw guard necessary. The pickers show the second 01:30 as the text "01:30". Read
    /// back as a typed edit, "01:30" as the start of a range is the FIRST 01:30, 05:30 UTC: an hour earlier than the
    /// range that was held. So a picker write made by the tab itself must never reach the edit path.
    /// </summary>
    [Fact]
    public void ReadingTheRenderedServerTextBackAsAnEdit_MovesA0630ZStartTo0530Z()
    {
        var held = Held();
        var serverZone = ServerTab.PickerZone(TimeDisplayMode.ServerTime, Eastern());
        var shown = held.Render(serverZone)!.Value;
        Assert.Equal(At(1, 1, 30), shown.From);

        var edited = held.ApplyEdit(shown.From, BoundSide.From, serverZone);

        Assert.Equal(At(1, 5, 30), edited);
        Assert.Equal(At(1, 5, 30), held.FromUtc);
        Assert.Equal(At(1, 7, 30), held.ToUtc);
    }

    // ── The wiring: source pins on Lite/Controls/ServerTab.TimeRange.cs ──

    /// <summary>
    /// The display-mode switch draws the held range in the new zone and calls nothing that reads the pickers'
    /// text: not the parse itself and not the capture that wraps it. Comments are stripped, so a sentence that names
    /// them cannot fail the pin.
    /// </summary>
    [Fact]
    public void TheDisplayModeSwitch_DrawsTheHeldRange_AndParsesNothing()
    {
        var body = MethodBody(CodeOnly(ReadTimeRangeSource()), "private async void TimeDisplayMode_SelectionChanged(");

        Assert.Contains("RenderCustomRange();", body, StringComparison.Ordinal);
        Assert.DoesNotContain("GetDateTimeFromPickers", body, StringComparison.Ordinal);
        Assert.DoesNotContain("CaptureCustomRangeEdit", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>RenderCustomRange</c> raises <c>_renderingCustomRange</c> before its first picker write and lowers it in a
    /// <c>finally</c>, and both picker handlers return on it before they reach <c>CaptureCustomRangeEdit</c>, so the
    /// tab's own writes are never read as an edit.
    /// </summary>
    [Fact]
    public void TheTabsOwnPickerWrites_AreMarked_AndBothPickerHandlersIgnoreThem()
    {
        var source = CodeOnly(ReadTimeRangeSource());

        var render = MethodBody(source, "private void RenderCustomRange(");
        var raised = render.IndexOf("_renderingCustomRange = true;", StringComparison.Ordinal);
        var firstWrite = render.IndexOf("FromDatePicker.SelectedDate", StringComparison.Ordinal);
        var lowered = render.IndexOf("_renderingCustomRange = false;", StringComparison.Ordinal);
        var finallyBlock = render.IndexOf("finally", StringComparison.Ordinal);
        Assert.True(raised >= 0 && raised < firstWrite, "RenderCustomRange has to raise _renderingCustomRange before it writes a picker.");
        Assert.True(finallyBlock > firstWrite && lowered > finallyBlock, "RenderCustomRange has to lower _renderingCustomRange in a finally.");

        foreach (var signature in new[] { "private async void CustomDateRange_Changed(", "private async void CustomTimeCombo_Changed(" })
        {
            var handler = MethodBody(source, signature);
            var guard = Regex.Match(handler, @"\|\|\s*_renderingCustomRange\s*\)\s*return;");
            var capture = handler.IndexOf("CaptureCustomRangeEdit(", StringComparison.Ordinal);

            Assert.True(guard.Success, $"{signature} has to return while _renderingCustomRange is set.");
            Assert.True(capture > guard.Index, $"{signature} has to return on _renderingCustomRange before CaptureCustomRangeEdit.");
        }
    }

    /* The text of the method that starts at the signature, up to its closing brace: these files indent members by four
       spaces, so the first "\n    }\n" after the signature is the end of that member. */
    private static string MethodBody(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' is no longer in the source; update this pin.");
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the end of '{signature}' was not found.");
        return source[start..end];
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ReadTimeRangeSource([CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", "ServerTab.TimeRange.cs")));
}
