/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a custom range is held as a pair of UTC instants, and the pickers on screen only show it. The window a
/// read gets is the pair as held; the picker text is the pair in the display zone; a switch of display mode
/// re-renders the same instants and parses nothing. Only a typed edit reads text back.
/// </summary>
public sealed class CustomRangeStateTests
{
    [Fact]
    public void APickerBoundAt0630Z_ReachesTheWindowAs0630Z()
    {
        var state = new CustomRangeState();
        state.Set(DisplayZoneFixtures.At(2026, 11, 1, 6, 30), DisplayZoneFixtures.At(2026, 11, 1, 7, 30));

        /* The window a read gets is the pair, untouched. */
        Assert.True(state.IsCustom);
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 6, 30), state.FromUtc);
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 7, 30), state.ToUtc);

        /* The pickers' text, in each display mode. */
        Assert.Equal((DisplayZoneFixtures.At(2026, 11, 1, 6, 30), DisplayZoneFixtures.At(2026, 11, 1, 7, 30)), state.Render(TimeZoneInfo.Utc));
        Assert.Equal((DisplayZoneFixtures.At(2026, 11, 1, 1, 30), DisplayZoneFixtures.At(2026, 11, 1, 2, 30)), state.Render(DisplayZoneFixtures.Eastern));
        Assert.Equal((DisplayZoneFixtures.At(2026, 10, 31, 23, 30), DisplayZoneFixtures.At(2026, 11, 1, 0, 30)), state.Render(DisplayZoneFixtures.Pacific));
    }

    [Fact]
    public void AModeSwitchRoundTrip_KeepsTheHeldInstants()
    {
        var state = new CustomRangeState();
        state.Set(DisplayZoneFixtures.At(2026, 11, 1, 6, 30), DisplayZoneFixtures.At(2026, 11, 1, 7, 30));

        /* UTC to Server and back, then Local to Server and back: rendering reads the state and writes nothing. */
        foreach (var zone in new[] { TimeZoneInfo.Utc, DisplayZoneFixtures.Eastern, TimeZoneInfo.Utc, DisplayZoneFixtures.Pacific, DisplayZoneFixtures.Eastern, DisplayZoneFixtures.Pacific })
        {
            Assert.NotNull(state.Render(zone));
            Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 6, 30), state.FromUtc);
            Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 7, 30), state.ToUtc);
        }
    }

    [Fact]
    public void APreset_HoldsNothing_AndClearReturnsToOne()
    {
        var state = new CustomRangeState();

        Assert.False(state.IsCustom);
        Assert.Null(state.FromUtc);
        Assert.Null(state.ToUtc);
        Assert.Null(state.Render(TimeZoneInfo.Utc));

        state.Set(DisplayZoneFixtures.At(2026, 3, 1, 0), DisplayZoneFixtures.At(2026, 3, 2, 0));
        Assert.True(state.IsCustom);

        state.Clear();
        Assert.False(state.IsCustom);
        Assert.Null(state.FromUtc);
        Assert.Null(state.Render(DisplayZoneFixtures.Eastern));
    }

    [Fact]
    public void ATypedEdit_ReplacesOnlyItsOwnSide_ThroughTheRangeRule()
    {
        var state = new CustomRangeState();
        state.Set(DisplayZoneFixtures.At(2026, 11, 1, 3, 0), DisplayZoneFixtures.At(2026, 11, 1, 9, 0));

        /* FROM typed as 01:00 on the server's clock is the first 01:00. TO typed as 01:00 is the second. */
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 5, 0), state.ApplyEdit(DisplayZoneFixtures.At(2026, 11, 1, 1, 0), BoundSide.From, DisplayZoneFixtures.Eastern));
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 9, 0), state.ToUtc);

        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 6, 0), state.ApplyEdit(DisplayZoneFixtures.At(2026, 11, 1, 1, 0), BoundSide.To, DisplayZoneFixtures.Eastern));
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 5, 0), state.FromUtc);
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 6, 0), state.ToUtc);
    }

    [Fact]
    public void ATypedEdit_WithAPresetInForce_ConvertsWithoutHoldingAHalfRange()
    {
        var state = new CustomRangeState();

        var bound = state.ApplyEdit(DisplayZoneFixtures.At(2026, 3, 8, 2, 30), BoundSide.From, DisplayZoneFixtures.Eastern);

        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 7, 0), bound);
        Assert.False(state.IsCustom);
        Assert.Null(state.FromUtc);
    }
}
