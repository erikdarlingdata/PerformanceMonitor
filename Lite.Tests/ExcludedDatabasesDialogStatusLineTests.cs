/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows.Controls;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Excluded Databases and XE Databases dialog's status line is where a failed database load is explained, so a sentence
/// long enough to name the server and say what to do must fit the dialog at its own width instead of being cut at the right edge.
/// The dialog is laid out unshown: shown, its Loaded handler would connect to the server named in it.
/// </summary>
[Collection("credential-manager")]
public sealed class ExcludedDatabasesDialogStatusLineTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheStatusLine_ShowsAWholeUnreachableServerSentence_InsideTheDialog(bool xeSessionsMode)
    {
        var dir = DialogWalkTestHost.ScratchDir("ExcludedDbStatus");
        try
        {
            var clipped = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "sql-reporting-02.corp.example", DisplayName = "Reporting replica", IsEnabled = true };
                var dialog = new ExcludedDatabasesDialog(new ServerManager(dir), server, xeSessionsMode);
                var root = DialogWalkTestHost.LayOutUnshown(dialog);
                var status = (TextBlock)dialog.FindName("StatusText");
                status.Text = "Reporting replica can't be reached, so its database list can't load. Check that the server is up and that this computer can connect to it, then open this window again.";
                root.UpdateLayout();
                return ClippedTextProbe.Clipped(root, root).Where(c => c.Contains("can't be reached", StringComparison.Ordinal)).ToList();
            }));

            Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
        }
        finally
        {
            DialogWalkTestHost.Delete(dir);
        }
    }
}
