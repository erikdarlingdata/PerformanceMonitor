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

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Defers and coalesces a picker list refresh out of the checkbox toggle event. A picker checkbox sits
/// INSIDE the list its handler re-sorts, so rebuilding that list's ItemsSource in the event tears down the
/// container that raised it while WPF is still walking the visual tree ("Cannot modify the Visual children
/// for this node because a tree walk is in progress"). <see cref="Request"/> posts ONE pending callback
/// however many toggles arrive before it runs; the guard clears when the callback starts, so a toggle during
/// the refresh posts a follow-up. The refresh reads the checkbox state when it RUNS. The post delegate
/// carries the dispatcher, which keeps this type free of WPF.
/// <para>The optional selection signature makes the callback idempotent: it applies only when the signature
/// differs from the one it last applied. A regenerated (or recycled) checkbox container can raise
/// <c>Checked</c> when its binding first sets <c>IsChecked</c>, and that would request another refresh that
/// regenerates the containers again; with the gate the follow-up pass finds nothing changed and stops.</para>
/// </summary>
internal sealed class PickerRefreshCoalescer
{
    private readonly Action<Action> _post;
    private readonly Action _refresh;
    private readonly Func<string>? _selectionSignature;
    private string? _appliedSignature;
    private bool _pending;

    internal PickerRefreshCoalescer(Action<Action> post, Action refresh, Func<string>? selectionSignature = null)
    {
        _post = post ?? throw new ArgumentNullException(nameof(post));
        _refresh = refresh ?? throw new ArgumentNullException(nameof(refresh));
        _selectionSignature = selectionSignature;
    }

    /// <summary>Asks for a refresh after the current event; a request while one is already posted is absorbed.</summary>
    internal void Request()
    {
        if (_pending)
        {
            return;
        }

        _pending = true;
        try
        {
            _post(Run);
        }
        catch
        {
            _pending = false;
            throw;
        }
    }

    /// <summary>
    /// Forgets the applied signature. A direct refresh (Select All, Clear All, the initial population) changes
    /// the selection without going through the gate, so the next coalesced pass must apply even if its
    /// signature matches an older one.
    /// </summary>
    internal void Invalidate()
    {
        _appliedSignature = null;
    }

    private void Run()
    {
        _pending = false;
        if (_selectionSignature is not null)
        {
            var signature = _selectionSignature();
            if (string.Equals(signature, _appliedSignature, StringComparison.Ordinal))
            {
                return;
            }

            _appliedSignature = signature;
        }

        _refresh();
    }

    /// <summary>The selection signature of a picker: the selected names, ordinal-sorted and joined.</summary>
    internal static string SignatureOf(IEnumerable<string> selectedNames)
    {
        return string.Join("\u001f", selectedNames.OrderBy(n => n, StringComparer.Ordinal));
    }
}
