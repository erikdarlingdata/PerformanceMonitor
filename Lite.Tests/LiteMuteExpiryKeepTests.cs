/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7173: editing a mute rule used to round its expiry to 1 hour, 24 hours or 7 days from now and save now plus that preset, so a
/// Reason-only edit stretched a window by days and brought an expired rule back for an hour. Entry 0 now keeps the stored expiry.
/// </summary>
public sealed class LiteMuteExpiryKeepTests
{
    private static readonly DateTime Now = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void KeepEntry_ReturnsTheStoredExpiry_NotNowPlusAPreset()
    {
        var stored = Now.AddDays(3);
        Assert.Equal(stored, MuteRuleDialog.ExpiryFromSelection(0, stored, Now));
    }

    [Fact]
    public void KeepEntry_OnAnExpiredRule_StaysExpired()
    {
        var stored = Now.AddHours(-5);
        Assert.Equal(stored, MuteRuleDialog.ExpiryFromSelection(0, stored, Now));
    }

    [Fact]
    public void PresetsAfterTheKeepEntry_AreStillTheFourPresets()
    {
        var stored = Now.AddDays(3);
        Assert.Equal(Now.AddHours(1), MuteRuleDialog.ExpiryFromSelection(1, stored, Now));
        Assert.Equal(Now.AddHours(24), MuteRuleDialog.ExpiryFromSelection(2, stored, Now));
        Assert.Equal(Now.AddDays(7), MuteRuleDialog.ExpiryFromSelection(3, stored, Now));
        Assert.Null(MuteRuleDialog.ExpiryFromSelection(4, stored, Now));
    }

    [Fact]
    public void WithoutAStoredExpiry_TheComboIsTheFourPresets()
    {
        Assert.Equal(Now.AddHours(1), MuteRuleDialog.ExpiryFromSelection(0, null, Now));
        Assert.Equal(Now.AddHours(24), MuteRuleDialog.ExpiryFromSelection(1, null, Now));
        Assert.Equal(Now.AddDays(7), MuteRuleDialog.ExpiryFromSelection(2, null, Now));
        Assert.Null(MuteRuleDialog.ExpiryFromSelection(3, null, Now));
    }

    [Fact]
    public void KeepLabel_MarksAnExpiredRule_AndLeavesALiveOneUnmarked()
    {
        Assert.EndsWith("(expired)", MuteRuleDialog.KeepExpiryLabel(Now.AddMinutes(-1), Now));
        Assert.DoesNotContain("expired", MuteRuleDialog.KeepExpiryLabel(Now.AddMinutes(1), Now));
        Assert.StartsWith("Keep: ", MuteRuleDialog.KeepExpiryLabel(Now.AddMinutes(1), Now));
    }
}
