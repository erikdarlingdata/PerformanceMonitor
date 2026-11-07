/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8112: an Overview card is 300 px wide and its metric grid is [dot] [label] [value] [detail], the first three columns Auto. A card
/// that shows an Identity capacity row ("Identity capacity:") and a Protection row ("4 items on 4 databases") must still give the
/// CPU, Threads, Memory, Blocking, Deadlocks and Collectors detail cells room to read, trim them with an ellipsis rather than wrap
/// or overflow the card, and carry each one's full text as a tooltip. The real card template is realized on the shared STA thread
/// with the app's theme, as ServerTabToolbarWrapTests does for Lite's toolbars. Lite's own card is a two-column grid with no detail
/// cell, so it has no twin here.
/// </summary>
public sealed class ViewerOverviewCardDetailColumnTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    /// <summary>The card's own width, fixed in its template; the window around it does not change what a detail cell is given.</summary>
    private const double CardWidth = 300;

    /// <summary>
    /// Room to read "Other: 3%", "Available: 584/704" or "BP 96.0, QMG 2.0 GB" at 11 px. A detail column squeezed by a long optional
    /// row is left about 37 px, which shows three or four characters and an ellipsis.
    /// </summary>
    private const double ReadableDetailWidth = 80;

    [Fact]
    public void ACardWithIdentityAndProtectionRows_KeepsEveryCoreDetailReadable_TrimmedWithATooltip()
    {
        var item = CardWithOptionalRows();
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var host = new ContentControl
            {
                Content = item,
                ContentTemplate = OverviewCardTemplate(),
                HorizontalAlignment = HorizontalAlignment.Left,
                VerticalAlignment = VerticalAlignment.Top,
            };
            var window = new Window { Content = host, Width = 1920, Height = 1080, Left = 0, Top = 0 };
            window.Resources["BoolToVisibility"] = new BooleanToVisibilityConverter();
            window.Show();
            try
            {
                ClippedTextProbe.Settle(window);
                var card = ClippedTextProbe.Descendants(host).OfType<Border>().First(b => b.Width == CardWidth);
                var blocks = ClippedTextProbe.Descendants(card).OfType<TextBlock>().ToList();
                var found = new List<string>();
                foreach (var label in new[] { "Identity capacity:", "Protection:" })
                {
                    if (!blocks.Any(b => b.Text == label && b.IsVisible))
                    {
                        found.Add(label + " row is not showing, so the card is not the one under test");
                    }
                }

                foreach (var (name, text) in new[]
                {
                    ("CPU", item.CpuDetail), ("Threads", item.ThreadsDetail), ("Memory", item.MemoryDetail),
                    ("Blocking", item.BlockingDetail), ("Deadlocks", item.DeadlockDetail), ("Collectors", item.CollectorDetail),
                })
                {
                    if (string.IsNullOrEmpty(text))
                    {
                        found.Add(name + ": the card computed no detail text, so nothing was measured");
                        continue;
                    }

                    var cell = blocks.FirstOrDefault(b => b.Text == text);
                    if (cell is null)
                    {
                        found.Add(name + ": no cell on the card shows \"" + text + "\"");
                        continue;
                    }

                    var bounds = cell.TransformToAncestor(card).TransformBounds(new Rect(cell.RenderSize));
                    if (cell.ActualWidth < ReadableDetailWidth)
                    {
                        found.Add($"{name}: \"{text}\" is given {cell.ActualWidth:F0} px, under the {ReadableDetailWidth} px it needs to be read");
                    }

                    if (bounds.Right > card.ActualWidth + 0.5)
                    {
                        found.Add($"{name}: the detail cell ends at {bounds.Right:F0} px, past the {card.ActualWidth:F0} px card");
                    }

                    if (cell.TextTrimming != TextTrimming.CharacterEllipsis || cell.TextWrapping != TextWrapping.NoWrap)
                    {
                        found.Add($"{name}: TextTrimming is {cell.TextTrimming} and TextWrapping is {cell.TextWrapping}, so a long detail wraps or is cut without a mark");
                    }

                    if (cell.ActualHeight > cell.FontSize * 2)
                    {
                        found.Add($"{name}: the detail cell is {cell.ActualHeight:F0} px tall, more than one line of {cell.FontSize} px text");
                    }

                    if (!Equals(cell.ToolTip, text))
                    {
                        var tooltip = cell.ToolTip is null ? "unset" : "\"" + cell.ToolTip + "\"";
                        found.Add($"{name}: the tooltip is {tooltip}, so a trimmed detail cannot be read on hover");
                    }
                }

                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(findings.Count == 0, "the Overview card at " + CardWidth + " px:" + Environment.NewLine + string.Join(Environment.NewLine, findings));
    }

    /// <summary>An online SQL Server card whose six core details all have text, one of them long, with both optional rows showing.</summary>
    private static ServerSummaryItem CardWithOptionalRows() => new()
    {
        ServerId = 1,
        ServerName = "card-detail-test",
        DisplayName = "card-detail-test",
        IsOnline = true,
        CpuPercent = 42,
        OtherProcessCpuPercent = 3,
        TotalThreads = 704,
        CurrentWorkers = 120,
        MemoryMb = 131072,
        BufferPoolMb = 98304,
        GrantedMemoryMb = 2048,
        BlockingCount = 3,
        MaxBlockingWaitMs = 12000,
        LastBlockingMinutesAgo = 14,
        DeadlockCount = 2,
        LastDeadlockMinutesAgo = 30,
        CollectorCount = 50,
        HealthyCollectorCount = 44,
        WarningCollectorCount = 3,
        NotCollectedCollectorCount = 2,
        NoPermissionCollectorCount = 1,
        IdentityCapacity = new IdentityCapacityCard("Warning", 2, 0),
        DatabaseProtectionChipValue = "4 items on 4 databases",
        DatabaseProtectionOverdueCount = 4,
        DatabaseProtectionChipDetail = "CHECKDB overdue on 4 databases, the oldest 31 days ago",
        LastCollectionTime = DateTime.UtcNow.AddMinutes(-2),
    };

    /// <summary>
    /// The card template lives inline in MainWindow.xaml, so the shell is built (never shown) to read it. Its constructor seeds the
    /// process-wide time display mode from the app settings; that is put back so the other classes see what they set.
    /// </summary>
    private static DataTemplate OverviewCardTemplate()
    {
        var mode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            var shell = new MainWindow();
            return ((ItemsControl)shell.FindName("OverviewItemsControl")).ItemTemplate;
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
        }
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("BackgroundLightBrush"))
        {
            added = new ResourceDictionary { Source = new Uri(ThemeUri) };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}
