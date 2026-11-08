/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8146: <c>xe_ring_buffer_max_events</c> in settings.json, read by <see cref="App.LoadAlertSettings(string, Func{string, string}, Action{string, string})"/>.
/// Shares the statics collection of the other tests that load settings, because they all write App statics.
/// </summary>
[Collection("app-alert-statics")]
public sealed class XeRingBufferMaxEventsSettingTests
{
    private static int LoadWith(string? settingsJson)
    {
        var configDir = Path.Combine(Path.GetTempPath(), $"pmlite_xering_{Guid.NewGuid():N}");
        Directory.CreateDirectory(configDir);
        var before = App.XeRingBufferMaxEvents;
        try
        {
            App.XeRingBufferMaxEvents = RingBufferTarget.DefaultMaxEvents;
            if (settingsJson is not null)
            {
                File.WriteAllText(Path.Combine(configDir, "settings.json"), settingsJson);
            }

            App.LoadAlertSettings(configDir, _ => string.Empty, (key, _) => Assert.Fail($"wrote credential '{key}'"));
            return App.XeRingBufferMaxEvents;
        }
        finally
        {
            App.XeRingBufferMaxEvents = before;
            Directory.Delete(configDir, recursive: true);
        }
    }

    [Fact]
    public void XeRingBufferMaxEvents_Default()
    {
        Assert.Equal(RingBufferTarget.DefaultMaxEvents, LoadWith("{ }"));
        Assert.Equal(RingBufferTarget.DefaultMaxEvents, LoadWith(null));
    }

    [Theory]
    [InlineData("4321", 4321)]
    [InlineData("50", 1000)]
    [InlineData("20000", 10000)]
    /* #8146: a value too large for an int still lands on the nearest bound, as the guide says, instead of falling back. */
    [InlineData("20000000000", 10000)]
    [InlineData("-5", 1000)]
    [InlineData("0", 1000)]
    [InlineData("\"abc\"", 1000)]
    /* #8146: what lite-guide.md says of the edges: a fraction is dropped, a number past Int64 still clamps, and only a number
       past double range is unusable and reads as the default. */
    [InlineData("4321.7", 4321)]
    [InlineData("99999999999999999999", 10000)]
    [InlineData("1e999", 1000)]
    public void XeRingBufferMaxEvents_ReadsAndClamps(string written, int expected)
    {
        Assert.Equal(expected, LoadWith("{ \"xe_ring_buffer_max_events\": " + written + " }"));
    }
}
