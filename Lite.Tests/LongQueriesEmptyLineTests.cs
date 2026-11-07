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
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Long Queries grid's empty text says one thing once. The banner above the grid already explains that the completion
/// trace is opt-in, where its switch is and which box to tick, so the sentence over the empty rows only says why the rows are
/// empty: as one sentence, short enough to sit on one line in the shared empty-hint style at the width that style allows.
/// </summary>
public sealed class LongQueriesEmptyLineTests
{
    private static readonly Uri LiteTheme = new("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml");
    private static readonly Uri Shared = new("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml");

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheGridsEmptyText_IsOneSentence_ThatDoesNotRepeatTheBanner(bool traceEnabled)
    {
        var text = LongQueriesEmptyText.Text(traceEnabled);

        Assert.EndsWith(".", text);
        Assert.Single(text.Split(new[] { ". ", "! ", "? " }, StringSplitOptions.RemoveEmptyEntries));
        Assert.DoesNotContain('\n', text);
        Assert.DoesNotContain("Turn it on", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("long_query_completions", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Settings →", text, StringComparison.Ordinal);
        if (!traceEnabled)
        {
            Assert.Contains("trace is off", text, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheGridsEmptyText_SitsOnOneLine_InTheEmptyHintStyle(bool traceEnabled)
    {
        var text = LongQueriesEmptyText.Text(traceEnabled);

        var (lines, width) = StaTestThread.Run(() => WithStyles(() =>
        {
            var style = (Style)Application.Current.FindResource("EmptyHint");
            var sentence = new TextBlock { Style = style, Text = text, Visibility = Visibility.Visible };
            var oneLine = new TextBlock { Style = style, Text = "x", Visibility = Visibility.Visible };
            var host = new Grid { Children = { sentence, oneLine } };
            var window = new Window { Content = host, Width = 900, Height = 600, Left = 0, Top = 0 };
            window.Show();
            try
            {
                ClippedTextProbe.Settle(window);
                return (Math.Round(sentence.ActualHeight / oneLine.ActualHeight), sentence.MaxWidth);
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(lines == 1, $"the text wraps to {lines} lines at the {width:F0} px the empty-hint style allows: \"{text}\"");
    }

    private static T WithStyles<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new List<ResourceDictionary>();
        if (!app.Resources.Contains("EmptyHint"))
        {
            added.Add(new ResourceDictionary { Source = LiteTheme });
            added.Add(new ResourceDictionary { Source = Shared });
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }

        try
        {
            return body();
        }
        finally
        {
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Remove(dictionary);
            }
        }
    }
}
