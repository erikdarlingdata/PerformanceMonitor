/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The in-app message after its owner has gone. A Settings window's "test sent" confirmation arrives after an await, and the person
/// may have closed Settings meanwhile: the message then shows on its own, centred on the screen, instead of throwing when it is
/// given a closed window as its owner. A live owner is kept.
/// </summary>
public sealed class InAppMessageClosedOwnerTests
{
    [Fact]
    public void AMessageWhoseOwnerHasClosed_ShowsUnowned_InsteadOfThrowing()
    {
        var (owner, location, shown) = StaTestThread.Run(() =>
        {
            var settings = new Window { Width = 300, Height = 200, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(settings);
            settings.Close();

            var message = InAppMessage.Build(settings, "Test Email", "Test email sent successfully!");
            /* Read before the show: showing a window off screen places it by hand, which rewrites its startup location. */
            var location = message.WindowStartupLocation;
            StaTestThread.ShowOffScreen(message);
            try
            {
                return (message.Owner, location, message.IsVisible);
            }
            finally
            {
                message.Close();
            }
        });

        Assert.Null(owner);
        Assert.Equal(WindowStartupLocation.CenterScreen, location);
        Assert.True(shown, "the message window did not show");
    }

    [Fact]
    public void AMessageWhoseOwnerIsStillShown_IsOwnedByIt()
    {
        var (owned, location) = StaTestThread.Run(() =>
        {
            var settings = new Window { Width = 300, Height = 200, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(settings);
            try
            {
                var message = InAppMessage.Build(settings, "Settings", "Settings saved.");
                return (ReferenceEquals(message.Owner, settings), message.WindowStartupLocation);
            }
            finally
            {
                settings.Close();
            }
        });

        Assert.True(owned, "a shown owner was not taken");
        Assert.Equal(WindowStartupLocation.CenterOwner, location);
    }
}
