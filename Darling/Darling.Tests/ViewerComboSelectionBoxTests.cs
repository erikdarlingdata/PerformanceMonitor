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
using System.Windows.Media;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6452: the CLOSED box of a ComboBox shows what the open list shows. The Viewer's own ComboBox template
/// (ViewerDarkTheme.xaml) left the item-template selector out of its selection box, so a combo bound to a record with a
/// DisplayMemberPath showed the record's type name ("ServerChoice { ServerId = ..."), and it had no PART_EditableTextBox, so an
/// IsEditable combo (the Metric box in Add Known Pattern) was blank. The tests build the real dialog, and then every ComboBox
/// template the Viewer ships (the three app themes and the dialog dictionary), and read the text the closed box actually shows.
/// </summary>
public sealed class ViewerComboSelectionBoxTests
{
    private const string Base = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/";

    private sealed record Choice(int Id, string Display);

    /* Runs on the shared STA thread with the app's Dark theme merged, as App.xaml supplies it, and takes it back out afterwards
       (process-wide state; see ViewerPostgresGridNumericSortTests.OnTab). */
    private static void WithAppTheme(Action body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var added = new ResourceDictionary { Source = new Uri(Base + "Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
            try
            {
                body();
            }
            finally
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        });

    private static IEnumerable<T> Descendants<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var nested in Descendants<T>(child))
            {
                yield return nested;
            }
        }
    }

    /// <summary>The text the closed box shows: the editable box's Text when the combo is editable, otherwise the text of the selection-box presenter.</summary>
    private static string ClosedBoxText(ComboBox combo)
    {
        if (combo.IsEditable)
        {
            var editable = Descendants<TextBox>(combo).FirstOrDefault(t => t.Name == "PART_EditableTextBox");
            Assert.NotNull(editable);
            Assert.True(editable!.IsVisible, "the editable box must be on screen, not hidden behind the template");
            return editable.Text;
        }

        var site = Descendants<ContentPresenter>(combo).First(p => p.Name == "ContentSite");
        Assert.True(site.IsVisible);
        return string.Concat(Descendants<TextBlock>(site).Select(t => t.Text));
    }

    public static IEnumerable<object[]> TemplateSources() => new[]
    {
        new object[] { "Themes/DarkTheme.xaml" },
        new object[] { "Themes/LightTheme.xaml" },
        new object[] { "Themes/CoolBreezeTheme.xaml" },
        new object[] { "ViewerDarkTheme.xaml" },
    };

    private static void Host(ComboBox combo, string themeSource)
    {
        combo.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri(Base + themeSource) });
        var window = new Window { Content = combo, Width = 400, Height = 200, ShowInTaskbar = false };
        StaTestThread.Settle(window);
    }

    [Theory]
    [MemberData(nameof(TemplateSources))]
    public void ClosedBox_ShowsTheDisplayMember_NotTheRecordTypeName(string themeSource)
    {
        WithAppTheme(() =>
        {
            var combo = new ComboBox
            {
                DisplayMemberPath = nameof(Choice.Display),
                ItemsSource = new List<Choice> { new(1, "(any server)"), new(790649233, "sql-prod-01") },
            };
            Host(combo, themeSource);

            combo.SelectedIndex = 1;
            StaTestThread.WaitForIdle(combo.Dispatcher, TimeSpan.FromSeconds(10));

            Assert.Equal("sql-prod-01", ClosedBoxText(combo));
        });
    }

    [Theory]
    [MemberData(nameof(TemplateSources))]
    public void ClosedBox_OfAnEditableCombo_ShowsTheChosenText(string themeSource)
    {
        WithAppTheme(() =>
        {
            var combo = new ComboBox { IsEditable = true, ItemsSource = KnownPatternsEditorModel.MetricChoices };
            Host(combo, themeSource);

            combo.SelectedItem = "High CPU";
            StaTestThread.WaitForIdle(combo.Dispatcher, TimeSpan.FromSeconds(10));

            Assert.Equal("High CPU", ClosedBoxText(combo));
        });
    }

    [Theory]
    [MemberData(nameof(TemplateSources))]
    public void ClosedBox_OfAPlainCombo_StillShowsItsContent(string themeSource)
    {
        WithAppTheme(() =>
        {
            var combo = new ComboBox { ItemsSource = new[] { "Optional", "Mandatory" }, SelectedIndex = 1 };
            Host(combo, themeSource);

            Assert.Equal("Mandatory", ClosedBoxText(combo));
        });
    }

    [Fact]
    public void KnownPatternDialog_NewEntry_ShowsTheServerName_AndAnEditEntryShowsServerAndMetric()
    {
        WithAppTheme(() =>
        {
            var servers = new List<(int ServerId, string Name)> { (790649233, "sql-prod-01"), (5, "sql-dev-02") };

            var add = new KnownPatternEditDialog(servers);
            StaTestThread.Settle(add);
            Assert.Equal("(any server)", ClosedBoxText(Combo(add, "ServerCombo")));
            add.Close();

            var existing = new KnownPattern(1, 790649233, "High CPU", new TimeOnly(2, 10), "UTC", 10, "nightly job", "erik (viewer)", DateTime.UtcNow, null);
            var edit = new KnownPatternEditDialog(servers, existing);
            StaTestThread.Settle(edit);
            Assert.Equal("sql-prod-01", ClosedBoxText(Combo(edit, "ServerCombo")));
            Assert.Equal("High CPU", ClosedBoxText(Combo(edit, "MetricCombo")));
            edit.Close();
        });
    }

    private static ComboBox Combo(Window window, string name) => (ComboBox)window.FindName(name);
}
