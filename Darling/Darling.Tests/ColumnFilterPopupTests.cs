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
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The column filter popup's value list (#5565) as the controls show it: the list section appears only for a column
/// with a catalog, search narrows it, Select All and the entries' ticks reach the applied filter, the notes, and
/// "Clear all filters" shows only when the grid has a filter and raises its own event.
/// </summary>
public sealed class ColumnFilterPopupTests
{
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        using var staGate = WpfStaGate.Enter();
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();
        if (error is not null)
            throw error;
        return result;
    }

    private static T Find<T>(DependencyObject root, Func<T, bool> where) where T : DependencyObject
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            if (child is T hit && where(hit))
                return hit;
            try { return Find(child, where); } catch (InvalidOperationException) { }
        }
        throw new InvalidOperationException("not found");
    }

    private static Button ButtonNamed(ColumnFilterPopup popup, string content) =>
        Find<Button>(popup, b => b.Content as string == content);

    private static void Click(Button button) => button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));

    private static ColumnValueCatalog Catalog(params string?[] cells) => ColumnValueCatalog.Build(cells);

    [Fact]
    public void The_list_section_shows_only_for_a_column_with_a_catalog()
    {
        OnStaThread(() =>
        {
            var popup = new ColumnFilterPopup();
            var panel = Find<StackPanel>(popup, p => p.Name == "ValueListPanel");

            popup.Initialize("LoginName", null, Catalog("sa", "app"));
            Assert.Equal(Visibility.Visible, panel.Visibility);
            Assert.NotNull(popup.ListModel);

            popup.Initialize("QueryText", null, null);
            Assert.Equal(Visibility.Collapsed, panel.Visibility);
            Assert.Null(popup.ListModel);
            return true;
        });
    }

    [Fact]
    public void The_search_box_narrows_the_list_and_Select_All_covers_what_it_shows()
    {
        OnStaThread(() =>
        {
            var popup = new ColumnFilterPopup();
            popup.Initialize("LoginName", null, Catalog("sa", "app", "job_svc", null));
            var search = Find<TextBox>(popup, t => t.Name == "SearchTextBox");
            var selectAll = Find<CheckBox>(popup, c => c.Name == "SelectAllCheckBox");

            Assert.Equal(new[] { "(Blanks)", "app", "job_svc", "sa" }, popup.ListModel!.Entries.Select(e => e.Label));

            search.Text = "JOB";
            Assert.Equal(new[] { "job_svc" }, popup.ListModel.Entries.Select(e => e.Label));

            selectAll.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent)); // everything shown is ticked, so this unticks it
            Assert.False(selectAll.IsChecked);

            FilterAppliedEventArgs? applied = null;
            popup.FilterApplied += (_, e) => applied = e;
            Click(ButtonNamed(popup, "Apply"));

            Assert.NotNull(applied);
            Assert.Equal(ColumnValueMode.Hide, applied!.FilterState.ValueMode);
            Assert.Equal(new[] { "job_svc" }, applied.FilterState.Values);
            return true;
        });
    }

    [Fact]
    public void Nothing_ticked_shows_the_note_and_the_cap_note_shows_for_a_long_list()
    {
        OnStaThread(() =>
        {
            var popup = new ColumnFilterPopup();
            popup.Initialize("LoginName", null, Catalog("a", "b"));
            var note = Find<TextBlock>(popup, t => t.Name == "NoteText");
            Assert.Equal(Visibility.Collapsed, note.Visibility);

            foreach (var entry in popup.ListModel!.Entries)
                entry.IsTicked = false;
            Assert.Equal(Visibility.Visible, note.Visibility);
            Assert.Equal("No values are ticked, so no rows show.", note.Text);

            popup.Initialize("LoginName", null, ColumnValueCatalog.Build(Enumerable.Range(0, 1200).Select(i => (string?)("u" + i))));
            Assert.Equal("Showing 1,000 of 1,200 values. Search to find the rest.", note.Text);
            return true;
        });
    }

    [Fact]
    public void An_existing_value_filter_comes_back_as_the_ticks_and_the_text_match_beside_it()
    {
        OnStaThread(() =>
        {
            var existing = new ColumnFilterState { ColumnName = "LoginName", ValueMode = ColumnValueMode.Hide, Operator = FilterOperator.StartsWith, Value = "s" };
            existing.Values.Add("job_svc");
            var popup = new ColumnFilterPopup();
            popup.Initialize("LoginName", existing, Catalog("sa", "job_svc", "app"), gridHasFilters: true);

            Assert.False(popup.ListModel!.Entries.Single(e => e.Value == "job_svc").IsTicked);
            Assert.True(popup.ListModel.Entries.Single(e => e.Value == "sa").IsTicked);

            FilterAppliedEventArgs? applied = null;
            popup.FilterApplied += (_, e) => applied = e;
            Click(ButtonNamed(popup, "Apply"));

            Assert.Equal(ColumnValueMode.Hide, applied!.FilterState.ValueMode);
            Assert.Equal(FilterOperator.StartsWith, applied.FilterState.Operator);
            Assert.Equal("s", applied.FilterState.Value);
            return true;
        });
    }

    [Fact]
    public void A_column_without_a_list_keeps_a_stored_value_filter_when_the_text_match_is_applied()
    {
        OnStaThread(() =>
        {
            var existing = new ColumnFilterState { ColumnName = "LoginName", ValueMode = ColumnValueMode.ShowOnly };
            existing.Values.Add("sa");
            var popup = new ColumnFilterPopup();
            popup.Initialize("LoginName", existing, null);

            FilterAppliedEventArgs? applied = null;
            popup.FilterApplied += (_, e) => applied = e;
            Click(ButtonNamed(popup, "Apply"));

            Assert.Equal(ColumnValueMode.ShowOnly, applied!.FilterState.ValueMode);
            Assert.Equal(new[] { "sa" }, applied.FilterState.Values);
            return true;
        });
    }

    [Fact]
    public void Clear_all_filters_shows_only_when_the_grid_has_a_filter_and_raises_its_event()
    {
        OnStaThread(() =>
        {
            var popup = new ColumnFilterPopup();
            var button = ButtonNamed(popup, "Clear all filters");

            popup.Initialize("LoginName", null, Catalog("a"), gridHasFilters: false);
            Assert.Equal(Visibility.Collapsed, button.Visibility);
            popup.Initialize("LoginName", null, Catalog("a"), gridHasFilters: true);
            Assert.Equal(Visibility.Visible, button.Visibility);

            var raised = 0;
            popup.ClearAllRequested += (_, _) => raised++;
            Click(button);
            Assert.Equal(1, raised);
            return true;
        });
    }

    [Fact]
    public void Clear_applies_an_empty_filter_including_the_value_part()
    {
        OnStaThread(() =>
        {
            var existing = new ColumnFilterState { ColumnName = "LoginName", ValueMode = ColumnValueMode.Hide };
            existing.Values.Add("a");
            var popup = new ColumnFilterPopup();
            popup.Initialize("LoginName", existing, Catalog("a", "b"));

            FilterAppliedEventArgs? applied = null;
            popup.FilterApplied += (_, e) => applied = e;
            Click(ButtonNamed(popup, "Clear"));

            Assert.False(applied!.FilterState.IsActive);
            return true;
        });
    }

    [Fact]
    public void Esc_closes_the_popup_from_anywhere_in_it_and_the_list_is_virtualised_and_tab_stops_once()
    {
        OnStaThread(() =>
        {
            var popup = new ColumnFilterPopup();
            popup.Initialize("LoginName", null, Catalog("a", "b"));
            var cleared = 0;
            popup.FilterCleared += (_, _) => cleared++;

            var search = Find<TextBox>(popup, t => t.Name == "SearchTextBox");
            var keyEvent = new System.Windows.Input.KeyEventArgs(
                System.Windows.Input.Keyboard.PrimaryDevice,
                new System.Windows.Interop.HwndSource(0, 0, 0, 0, 0, "t", IntPtr.Zero),
                0, System.Windows.Input.Key.Escape)
            { RoutedEvent = System.Windows.UIElement.PreviewKeyDownEvent };
            search.RaiseEvent(keyEvent);
            Assert.Equal(1, cleared);

            var list = Find<ListBox>(popup, l => l.Name == "ValuesListBox");
            Assert.True(VirtualizingPanel.GetIsVirtualizing(list));
            Assert.Equal(System.Windows.Input.KeyboardNavigationMode.Once, System.Windows.Input.KeyboardNavigation.GetTabNavigation(list));
            return true;
        });
    }
}
