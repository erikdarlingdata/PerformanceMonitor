/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Excluded Databases and XE Databases dialog's status line is where a failed database load is explained, so a sentence
/// long enough to name the server and say what to do must fit the dialog at its own width instead of being cut at the right edge.
/// The dialog's content is shown in a host of the dialog's size; the dialog window stays unshown, since shown it would connect to the server named in it.
/// </summary>
[InMemoryCredentials]
[Collection("application-theme")]
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
                /* The dialog's own content, shown in a host of the dialog's size: the dialog window itself stays unshown, so its
                   Loaded handler never reaches for the server, while the status line is on screen for the probe. */
                var root = (FrameworkElement)dialog.Content;
                dialog.Content = null;
                var host = new Border { Width = dialog.Width - 16, Height = dialog.Height - 39, Child = root, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
                var window = new Window { Content = host, Width = dialog.Width + 40, Height = dialog.Height + 40 };
                DialogWalkTestHost.Show(window);
                try
                {
                    var status = (TextBlock)root.FindName("StatusText") ?? DialogWalkTestHost.TextBlocks(root).Single(t => t.FontStyle == FontStyles.Italic && t.FontSize == 10);
                    status.Text = "Reporting replica can't be reached, so its database list can't load. Check that the server is up and that this computer can connect to it, then open this window again.";
                    ClippedTextProbe.Settle(window);
                    return ClippedTextProbe.Clipped(host, host).Where(c => c.Contains("can't be reached", StringComparison.Ordinal)).ToList();
                }
                finally
                {
                    window.Close();
                }
            }));

            Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
        }
        finally
        {
            DialogWalkTestHost.Delete(dir);
        }
    }
}
