/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Defers and coalesces a picker list refresh out of the checkbox toggle event. A picker checkbox sits
/// INSIDE the list its handler re-sorts, so rebuilding that list's ItemsSource in the event tears down the
/// container that raised it while WPF is still walking the visual tree ("Cannot modify the Visual children
/// for this node because a tree walk is in progress"). <see cref="Request"/> posts ONE pending callback
/// however many toggles arrive before it runs; the guard clears when the callback starts, so a toggle during
/// the refresh posts a follow-up. The refresh reads the checkbox state when it RUNS. The post delegate
/// carries the dispatcher, which keeps this type free of WPF.
/// </summary>
internal sealed class PickerRefreshCoalescer
{
    private readonly Action<Action> _post;
    private readonly Action _refresh;
    private bool _pending;

    internal PickerRefreshCoalescer(Action<Action> post, Action refresh)
    {
        _post = post ?? throw new ArgumentNullException(nameof(post));
        _refresh = refresh ?? throw new ArgumentNullException(nameof(refresh));
    }

    /// <summary>Asks for a refresh after the current event; a request while one is already posted is absorbed.</summary>
    internal void Request()
    {
        if (_pending)
        {
            return;
        }

        _pending = true;
        _post(Run);
    }

    private void Run()
    {
        _pending = false;
        _refresh();
    }
}
