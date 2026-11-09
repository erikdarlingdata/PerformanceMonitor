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
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8459: when the clipboard cannot be opened within the retry budget (another app holds it), Ctrl+V in the standalone
/// plan viewer tells the user so, with the same notice the "Paste XML" button gives. Lite and the Darling Viewer both
/// forward Ctrl+V to <see cref="StandalonePlanViewerController"/>, so one class covers both apps.
///
/// <para>Shares the <c>clipboard-text-seams</c> collection with the retry-budget tests because it swaps
/// <c>ClipboardText.ReadOnce</c> for a read that always fails to open the clipboard, and the backoff for a completed
/// task, and restores both in a <c>finally</c>. The notices are recorded through the controller's own notice seam, so no
/// modal dialog opens.</para>
/// </summary>
[Collection("clipboard-text-seams")]
public sealed class StandalonePlanViewerPasteNoticeTests
{
    // CLIPBRD_E_CANT_OPEN: what Clipboard.GetText throws while another process holds the clipboard.
    private const int ClipboardCannotOpen = unchecked((int)0x800401D0);

    [Fact]
    public void CtrlV_WhenTheClipboardCannotBeRead_GivesTheSameNoticeAsThePasteButton()
    {
        StaTestThread.Run(async () =>
        {
            var originalRead = ClipboardText.ReadOnce;
            var originalDelay = ClipboardText.DelayBackoff;
            ClipboardText.ReadOnce = () => throw new COMException("the clipboard is held by another app", ClipboardCannotOpen);
            ClipboardText.DelayBackoff = _ => Task.CompletedTask;
            try
            {
                // The sub-tab's empty state resolves its brushes and the close button's style from the host's resources.
                _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
                var tabs = new TabControl();
                var host = new Border { Child = tabs };
                host.Resources.MergedDictionaries.Add(new ResourceDictionary
                {
                    Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
                });

                var controller = new StandalonePlanViewerController(tabs);
                var notices = new List<(string Text, string Caption)>();
                controller.ShowNotice = (text, caption, _, _) =>
                {
                    notices.Add((text, caption));
                    return MessageBoxResult.OK;
                };
                controller.EnsureInitialized();
                var tab = controller.AddNewEmptyPlanSubTab();
                try
                {
                    StaTestThread.Step("Ctrl+V with a clipboard that cannot be opened");
                    await controller.PasteIntoActivePlanSubTabAsync();
                    var fromKey = Assert.Single(notices);
                    Assert.Contains("clipboard", fromKey.Text, StringComparison.OrdinalIgnoreCase);

                    StaTestThread.Step("the Paste XML button with the same clipboard");
                    notices.Clear();
                    PasteButton(tab).RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                    var fromButton = Assert.Single(notices);

                    Assert.Equal(fromButton, fromKey);
                }
                finally
                {
                    controller.Reset();
                }
            }
            finally
            {
                ClipboardText.ReadOnce = originalRead;
                ClipboardText.DelayBackoff = originalDelay;
            }
        });
    }

    /// <summary>The "Paste XML" button in the sub-tab's empty state.</summary>
    private static Button PasteButton(TabItem tab) =>
        Buttons((DependencyObject)tab.Content).Single(b => b.Content is "Paste XML");

    private static IEnumerable<Button> Buttons(DependencyObject root)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            if (child is Button b)
                yield return b;
            foreach (var inner in Buttons(child))
                yield return inner;
        }
    }
}
