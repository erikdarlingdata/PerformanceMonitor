/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8273: a test that shows a server tab and selects a sub-tab fires the tab's <c>SelectionChanged</c> handlers the way a click does. Those
/// handlers are <c>async void</c>: they await a store read and then redraw the time range picker, and in the app the code after the await
/// runs on the UI thread because the UI thread has a dispatcher synchronization context. A test body that selects the sub-tab with no such
/// context sends that continuation to the thread pool, where the redraw touches the picker from the wrong thread, the exception escapes
/// the handler, and the run prints an unhandled thread error that fails no test. The shared tab-revealing helper installs the context, so a
/// handler the selection fires resumes on the tab's own thread, where the test's next pump runs it.
/// </summary>
public sealed class ViewerServerTabSubTabChangeTests
{
    [Fact]
    public void RevealingASubTab_ResumesItsSelectionChangedHandlersOnTheTabsThread()
    {
        var resumedOnTabThread = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "sub-tab-change", "sub-tab-change", true, null));
            var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = 1280, Height = 800, Left = 0, Top = 0 };
            window.Show();
            try
            {
                /* The heatmap is the last Queries sub-tab, so revealing it is a real change from the one shown at start. */
                var chart = (WpfPlot)tab.FindName("QueryHeatmapChart");
                var path = ChartLayoutProbe.TabPath(chart);
                var subTabs = (TabControl)ItemsControl.ItemsControlFromItemContainer(path[^1]);
                Assert.NotSame(path[^1], subTabs.SelectedItem);

                /* A handler shaped like the tab's own: it awaits, then touches the control. The thread it resumes on is the thread every
                   handler of this event resumes on, because they all capture the context the selection ran under. */
                var resumed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                subTabs.SelectionChanged += async (_, e) =>
                {
                    if (!ReferenceEquals(e.OriginalSource, subTabs))
                    {
                        return;
                    }

                    await Task.Yield();
                    resumed.TrySetResult(subTabs.CheckAccess());
                };

                StaTestThread.Step("revealing the Query Heatmap sub-tab");
                ChartLayoutProbe.Reveal(path);
                Assert.Same(path[^1], subTabs.SelectedItem);

                /* A continuation posted to the tab's dispatcher runs in this pump; one sent to the thread pool has already run, or runs at once. */
                StaTestThread.WaitForIdle(tab.Dispatcher, StaTestThread.IdleLimit);
                Assert.True(resumed.Task.Wait(TimeSpan.FromSeconds(10)), "the handler's continuation never ran");
                return resumed.Task.Result;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(resumedOnTabThread, "the sub-tab's SelectionChanged handler resumed off the tab's thread after its first await: the selection ran with no dispatcher synchronization context");
    }

    /// <summary>Runs <paramref name="body"/> with the Viewer's dark theme merged into the application resources, as the rendered classes do, and removes it after.</summary>
    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary
            {
                Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
            };
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
