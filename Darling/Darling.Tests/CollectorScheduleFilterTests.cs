/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Windows;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6768: the Collector Schedules window's search box and "Disabled only" box. The rule is a pure method; the window tests build the
/// real window (never shown, so no store read happens) and check that the filter only narrows the grid's view: the list Save,
/// Reset, the presets and Copy from read still holds every row, and the filter survives a rebind of the grid.
/// </summary>
public sealed class CollectorScheduleFilterTests
{
    [Theory]
    [InlineData("query_store", true, "", false, true)]          // empty text, box off: everything
    [InlineData("query_store", true, "   ", false, true)]       // blank text counts as empty
    [InlineData("query_store", true, "QUERY", false, true)]     // any letter case
    [InlineData("query_store", true, "ry_st", false, true)]     // substring, not prefix
    [InlineData("query_store", true, "wait", false, false)]     // no match
    [InlineData("query_store", true, "", true, false)]          // disabled only hides an enabled row
    [InlineData("query_store", false, "", true, true)]          // ... and shows a disabled one
    [InlineData("query_store", false, "query", true, true)]     // both: match and disabled
    [InlineData("query_store", true, "query", true, false)]     // both: match but enabled
    [InlineData("query_store", false, "wait", true, false)]     // both: disabled but no match
    public void Matches_IsSubstringAnyCase_AndedWithDisabledOnly(string name, bool enabled, string text, bool disabledOnly, bool expected) =>
        Assert.Equal(expected, CollectorScheduleFilter.Matches(name, enabled, text, disabledOnly));

    private static void OnWindow(System.Action<CollectorScheduleEditorWindow> body) =>
        StaTestThread.Run(() =>
        {
            _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            /* Npgsql opens no connection until a query runs; the window is not shown, so Loaded never fires and nothing reads the store. */
            var viewer = new ViewerDataService("Host=127.0.0.1;Port=1;Username=u;Database=d");
            var window = new CollectorScheduleEditorWindow(viewer, new List<DarlingServer>());
            try
            {
                body(window);
            }
            finally
            {
                window.Close();
            }
        });

    private static List<CollectorScheduleEditItem> Editing(CollectorScheduleEditorWindow window) =>
        Assert.IsType<List<CollectorScheduleEditItem>>(window.ScheduleGrid.ItemsSource);

    private static List<CollectorScheduleEditItem> Shown(CollectorScheduleEditorWindow window) =>
        window.ScheduleGrid.Items.Cast<CollectorScheduleEditItem>().ToList();

    [Fact]
    public void Filter_HidesRowsInTheView_ButNotInTheListSaveWrites_AndHiddenRowsKeepTheirEdits()
    {
        OnWindow(window =>
        {
            window.LoadScopeSchedule();
            var all = Editing(window);
            var total = all.Count;
            Assert.True(total > 20);
            Assert.Equal(total, Shown(window).Count);

            window.FilterBox.Text = "query_store";
            var shown = Shown(window);
            Assert.InRange(shown.Count, 1, total - 1);
            Assert.All(shown, i => Assert.Contains("query_store", i.Name));

            /* The list Save, Reset, the presets and Copy from read still holds every row, and an edit to a hidden row is in it. */
            var hidden = all.First(i => !i.Name.Contains("query_store"));
            hidden.FrequencyMinutes = 7;
            Assert.Equal(total, Editing(window).Count);
            Assert.Equal(7, Editing(window).Single(i => i.Name == hidden.Name).FrequencyMinutes);

            window.FilterBox.Text = "";
            Assert.Equal(total, Shown(window).Count);
            Assert.Equal(Visibility.Collapsed, window.NoMatchText.Visibility);
        });
    }

    [Fact]
    public void DisabledOnly_AndSearch_Combine_AndAStatusLineShowsWhenNothingMatches()
    {
        OnWindow(window =>
        {
            window.LoadScopeSchedule();
            var all = Editing(window);
            var target = all.First(i => i.Name.Contains("query_store"));
            target.Enabled = false;

            window.DisabledOnlyCheckBox.IsChecked = true;
            Assert.Equal(all.Count(i => !i.Enabled), Shown(window).Count);
            Assert.Contains(target, Shown(window));

            window.FilterBox.Text = target.Name;
            Assert.Equal(target, Assert.Single(Shown(window)));

            window.FilterBox.Text = "no_such_collector";
            Assert.Empty(Shown(window));
            Assert.Equal(Visibility.Visible, window.NoMatchText.Visibility);

            window.FilterBox.Text = "";
            window.DisabledOnlyCheckBox.IsChecked = false;
            Assert.Equal(all.Count, Shown(window).Count);
            Assert.Equal(Visibility.Collapsed, window.NoMatchText.Visibility);
        });
    }

    [Fact]
    public void Filter_SurvivesARebindOfTheGrid_AndARowTickedUnderDisabledOnlyStaysInPlace()
    {
        OnWindow(window =>
        {
            window.LoadScopeSchedule();
            window.FilterBox.Text = "query_store";
            var shown = Shown(window).Select(i => i.Name).ToList();
            Assert.InRange(shown.Count, 1, Editing(window).Count - 1);

            /* A scope change rebinds the grid to a new list; the filter is still on and still hides the same rows. */
            window.LoadScopeSchedule();
            Assert.Equal(shown, Shown(window).Select(i => i.Name).ToList());

            /* Unticking a row, then re-ticking it, under "Disabled only": the row stays shown until the filter changes. */
            window.FilterBox.Text = "";
            var target = Editing(window).First(i => i.Name.Contains("query_store"));
            target.Enabled = false;
            window.DisabledOnlyCheckBox.IsChecked = true;
            Assert.Contains(target, Shown(window));
            target.Enabled = true;
            window.ScheduleGrid.Items.Refresh();
            Assert.Contains(target, Shown(window));

            window.DisabledOnlyCheckBox.IsChecked = false;
            window.DisabledOnlyCheckBox.IsChecked = true;
            Assert.DoesNotContain(target, Shown(window));
        });
    }
}
