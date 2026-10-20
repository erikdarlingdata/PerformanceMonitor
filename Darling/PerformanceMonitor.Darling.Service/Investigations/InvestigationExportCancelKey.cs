/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// Turns Ctrl+C into a cancellation for the command-line export (#6236): while a subscription is open the key press is
/// swallowed (<c>Cancel = true</c>) and the subscriber's action runs, so the export unwinds and deletes its partial file instead of
/// the process ending where it stands. <see cref="Press"/> is the same path without a console, for a test.
/// </summary>
internal static class InvestigationExportCancelKey
{
    private static readonly object s_lock = new();
    private static readonly List<Action> s_subscribers = [];

    /// <summary>Runs <paramref name="onPress"/> on Ctrl+C until the returned subscription is disposed.</summary>
    internal static IDisposable Subscribe(Action onPress)
    {
        lock (s_lock)
        {
            if (s_subscribers.Count == 0)
            {
                Console.CancelKeyPress += OnCancelKey;
            }

            s_subscribers.Add(onPress);
        }

        return new Subscription(onPress);
    }

    /// <summary>The key press itself: every open subscription's action runs.</summary>
    internal static void Press()
    {
        Action[] run;
        lock (s_lock)
        {
            run = [.. s_subscribers];
        }

        foreach (var action in run)
        {
            action();
        }
    }

    private static void OnCancelKey(object? sender, ConsoleCancelEventArgs e)
    {
        e.Cancel = true;
        Press();
    }

    private sealed class Subscription(Action onPress) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            lock (s_lock)
            {
                s_subscribers.Remove(onPress);
                if (s_subscribers.Count == 0)
                {
                    Console.CancelKeyPress -= OnCancelKey;
                }
            }
        }
    }
}
