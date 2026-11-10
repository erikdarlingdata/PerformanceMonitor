/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using Darling.Tests;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8533: the status bar's collection field is repainted by two signals, the field's own one-second tick and the collection loop's
/// cycle signal. The window facts build the real main window on the shared STA thread and a real collection loop, start the field
/// over that loop as the window's load does, and read the field the window draws. The tick is given no length, so it is due at
/// once and each drain of the dispatcher runs exactly one. A cycle is one collector pass whose work the test holds open. The clock
/// is the loop's own last time plus an age the test sets. No fact waits.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class CollectionStatusCadenceTests
{
    /// <summary>
    /// The tick is stopped, so only the loop's signal can repaint the field: Running while the pass is held open, and the age from
    /// the new time once it ends.
    /// </summary>
    [Fact]
    public void ACycleStartingAndEnding_RepaintsTheWindowsField_WithItsTickHeld()
    {
        var problems = OnMainWindow((window, field) =>
        {
            var found = new List<string>();
            var loop = new CollectionBackgroundService(null!);
            window.StartCollectionStatus(loop, () => loop.LastCollectionTime ?? default);
            window.CollectionAgeTimer.Stop();

            var held = new TaskCompletionSource();
            var pass = loop.RunCollectorPassAsync(() => held.Task);
            Drain();
            if (field.Text != "Collection: Running")
            {
                found.Add($"with a cycle running and the tick held, the field reads \"{field.Text}\"");
            }

            held.SetResult();
            Drain();
            if (!pass.IsCompletedSuccessfully)
            {
                found.Add("the collector pass did not end when its work did");
            }

            if (field.Text != "Collection: 0s ago")
            {
                found.Add($"with the cycle ended and the tick held, the field reads \"{field.Text}\"");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// Starting the field starts its tick, one second apart, and each tick repaints the age from the clock. The age is moved with no
    /// cycle signal in between, so only a tick can have painted it.
    /// </summary>
    [Fact]
    public void TheFieldsOwnTick_IsStartedWithTheField_AndRepaintsTheAge()
    {
        var problems = OnMainWindow((window, field) =>
        {
            var found = new List<string>();
            var loop = new CollectionBackgroundService(null!);
            var age = 0;
            var timer = window.CollectionAgeTimer;
            if (timer.Interval != CollectionStatusLabel.AgeTick)
            {
                found.Add($"the field's tick is {timer.Interval} apart, and the label's is {CollectionStatusLabel.AgeTick}");
            }

            if (timer.IsEnabled)
            {
                found.Add("the field's tick runs before the field is started");
            }

            timer.Interval = TimeSpan.Zero;
            window.StartCollectionStatus(loop, () => (loop.LastCollectionTime ?? default).AddSeconds(age));
            if (!timer.IsEnabled)
            {
                found.Add("starting the field did not start its tick");
            }

            Drain();
            if (field.Text != "Collection: Starting...")
            {
                found.Add($"before any cycle, one tick leaves the field reading \"{field.Text}\"");
            }

            var pass = loop.RunCollectorPassAsync(() => Task.CompletedTask);
            Drain();
            if (!pass.IsCompletedSuccessfully || field.Text != "Collection: 0s ago")
            {
                found.Add($"after one cycle the field reads \"{field.Text}\"");
            }

            foreach (var seconds in new[] { 9, 10, 38 })
            {
                age = seconds;
                Drain();
                if (field.Text != $"Collection: {seconds}s ago")
                {
                    found.Add($"{seconds} seconds after the cycle, one tick leaves the field reading \"{field.Text}\"");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The loop raises its signal when a pass starts and when it ends, and the end is raised with the cycle's time already set, so a
    /// repaint from it counts from the cycle that just ended.
    /// </summary>
    [Fact]
    public async Task TheLoop_SignalsAPassStartingAndEnding_AndTheEndAlreadyHasTheNewTime()
    {
        var loop = new CollectionBackgroundService(null!);
        var seen = new List<(bool Collecting, DateTime? Last)>();
        loop.CycleStateChanged += () => seen.Add((loop.IsCollecting, loop.LastCollectionTime));
        var held = new TaskCompletionSource();

        var pass = loop.RunCollectorPassAsync(() => held.Task);

        var start = Assert.Single(seen);
        Assert.True(start.Collecting, "the pass's first signal was raised with the loop not collecting");
        Assert.Null(start.Last);

        held.SetResult();
        await pass;

        Assert.Equal(2, seen.Count);
        Assert.False(seen[1].Collecting, "the pass's last signal was raised with the loop still collecting");
        Assert.True(seen[1].Last is not null, "the pass's last signal was raised before the cycle's time was set");
        Assert.Equal(loop.LastCollectionTime, seen[1].Last);
    }

    /// <summary>A pass whose collectors fail still signals its end, and the last time stays where the last finished cycle left it.</summary>
    [Fact]
    public async Task APassWhoseCollectorsFail_SignalsItsEnd_AndLeavesTheLastTimeAsItWas()
    {
        var loop = new CollectionBackgroundService(null!);
        var seen = new List<(bool Collecting, DateTime? Last)>();
        loop.CycleStateChanged += () => seen.Add((loop.IsCollecting, loop.LastCollectionTime));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => loop.RunCollectorPassAsync(() => Task.FromException(new InvalidOperationException("the collectors failed"))));

        Assert.Equal(2, seen.Count);
        Assert.True(seen[0].Collecting, "the pass's first signal was raised with the loop not collecting");
        Assert.False(seen[1].Collecting, "the pass's last signal was raised with the loop still collecting");
        Assert.Null(seen[1].Last);
        Assert.Null(loop.LastCollectionTime);
    }

    /// <summary>
    /// Runs what the dispatcher has queued down to the timers' own priority: a repaint the loop's signal posted, then a tick that is
    /// due. The empty call is queued behind both and ends the drain.
    /// </summary>
    private static void Drain() => Dispatcher.CurrentDispatcher.Invoke(() => { }, DispatcherPriority.Background);

    /// <summary>
    /// Builds the real main window with the Dark theme in a folder of its own, hosts its content at 900x600 and hands the body the
    /// window and its collection field. The field's tick is stopped afterwards, so it cannot fire in a later test's pump.
    /// </summary>
    private static List<string> OnMainWindow(Func<MainWindow, TextBlock, List<string>> body) =>
        InOwnFolder(dir => StaTestThread.Run(() => WithTheme(() =>
        {
            /* The window reads the app's three store paths when it is built and writes servers.json beside them; it gets a folder of its own (#8434). */
            using var paths = new AppPathsScope(dir);
            var window = new MainWindow();
            var root = (UIElement)window.Content;
            window.Content = null;
            var host = new Window { Content = root, Width = 900, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            StaTestThread.ShowOffScreen(host);
            try
            {
                ClippedTextProbe.Settle(host);
                return body(window, (TextBlock)window.FindName("CollectionStatusText"));
            }
            finally
            {
                window.CollectionAgeTimer.Stop();
                host.Close();
            }
        })));

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") });
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var d in saved)
            {
                app.Resources.MergedDictionaries.Add(d);
            }
        }
    }

    /// <summary>Runs <paramref name="body"/> with a scratch folder of the test's own, deleted afterwards.</summary>
    private static T InOwnFolder<T>(Func<string, T> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "CollectionStatusCadence_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            return body(dir);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
