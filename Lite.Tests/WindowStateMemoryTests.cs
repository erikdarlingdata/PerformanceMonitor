/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9735: restoring a minimized window from the tray (or by a second launch) used to force Normal, so a window
/// that was maximized came back at its declared 1200x700 size, past the edge of a small screen. The restore
/// target is a pure function, so these need no STA thread or Application.
/// </summary>
public sealed class WindowStateMemoryTests
{
    [Fact]
    public void Minimized_window_that_was_maximized_restores_maximized()
    {
        Assert.Equal(WindowState.Maximized, WindowStateMemory.RestoreTarget(WindowState.Minimized, WindowState.Maximized));
    }

    [Fact]
    public void Minimized_window_that_was_normal_restores_normal()
    {
        Assert.Equal(WindowState.Normal, WindowStateMemory.RestoreTarget(WindowState.Minimized, WindowState.Normal));
    }

    [Fact]
    public void Minimized_window_with_nothing_recorded_restores_maximized()
    {
        Assert.Equal(WindowState.Maximized, WindowStateMemory.RestoreTarget(WindowState.Minimized, null));
    }

    [Theory]
    [InlineData(WindowState.Maximized, WindowState.Normal)]
    [InlineData(WindowState.Normal, WindowState.Maximized)]
    [InlineData(WindowState.Maximized, null)]
    [InlineData(WindowState.Normal, null)]
    public void Window_that_is_not_minimized_keeps_its_state(WindowState current, WindowState? lastNonMinimized)
    {
        Assert.Equal(current, WindowStateMemory.RestoreTarget(current, lastNonMinimized));
    }

    [Fact]
    public void Recording_minimized_does_not_overwrite_the_remembered_state()
    {
        var memory = new WindowStateMemory();
        memory.Record(WindowState.Normal);
        memory.Record(WindowState.Minimized);

        Assert.Equal(WindowState.Normal, memory.RestoreTarget(WindowState.Minimized));
    }

    [Fact]
    public void Latest_non_minimized_state_wins()
    {
        var memory = new WindowStateMemory();
        memory.Record(WindowState.Normal);
        memory.Record(WindowState.Maximized);
        memory.Record(WindowState.Minimized);

        Assert.Equal(WindowState.Maximized, memory.RestoreTarget(WindowState.Minimized));
    }

    [Fact]
    public void Nothing_recorded_restores_maximized()
    {
        Assert.Equal(WindowState.Maximized, new WindowStateMemory().RestoreTarget(WindowState.Minimized));
    }
}
