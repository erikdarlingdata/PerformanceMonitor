/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Windows;

namespace PerformanceMonitor.Ui;

/// <summary>
/// A value-changed subscription on one dependency property of a source element, held only while the control that owns it is loaded
/// (#8566). <see cref="DependencyPropertyDescriptor.AddValueChanged"/> records the source and the handler in a table that lives as
/// long as the process, so a handler that reaches its control keeps that control, its visual tree and the rows bound to it reachable
/// until <see cref="DependencyPropertyDescriptor.RemoveValueChanged"/> is called. Here the subscription is made when the owner loads
/// and removed when it unloads, so a control that has left its window is not held by that table. A TabControl unloads the tab it
/// switches away from and loads it again when that tab is selected, and the subscription follows it both ways.
/// </summary>
public sealed class LoadedValueWatch
{
    private readonly DependencyPropertyDescriptor? _descriptor;
    private readonly DependencyObject _source;
    private readonly EventHandler _changed;
    private bool _subscribed;

    private LoadedValueWatch(DependencyPropertyDescriptor? descriptor, DependencyObject source, Action changed)
    {
        _descriptor = descriptor;
        _source = source;
        _changed = (_, _) => changed();
    }

    /// <summary>
    /// Runs <paramref name="changed"/> whenever <paramref name="property"/> changes on <paramref name="source"/> while
    /// <paramref name="owner"/> is loaded.
    /// </summary>
    /// <param name="owner">The control whose loaded state the subscription follows.</param>
    /// <param name="source">The element whose property is watched.</param>
    /// <param name="property">The watched dependency property.</param>
    /// <param name="sourceType">The type of <paramref name="source"/> the property is read through.</param>
    /// <param name="changed">What to run on a change.</param>
    /// <param name="runAtLoad">
    /// True to run <paramref name="changed"/> at each load as well. A change made while the owner is unloaded is not seen, so a
    /// handler that derives state from the property's current value asks for this and is brought up to date when the owner loads.
    /// </param>
    public static LoadedValueWatch Attach(
        FrameworkElement owner,
        DependencyObject source,
        DependencyProperty property,
        Type sourceType,
        Action changed,
        bool runAtLoad = false)
    {
        ArgumentNullException.ThrowIfNull(owner);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(changed);

        var watch = new LoadedValueWatch(DependencyPropertyDescriptor.FromProperty(property, sourceType), source, changed);

        owner.Loaded += (_, _) =>
        {
            watch.Subscribe();
            if (runAtLoad)
                changed();
        };
        owner.Unloaded += (_, _) => watch.Release();

        if (owner.IsLoaded)
            watch.Subscribe();

        return watch;
    }

    /// <summary>
    /// Removes the subscription now. The owner calls this where it is closed, so the subscription ends there and does not wait for
    /// the unload. An owner that loads again afterwards is subscribed again.
    /// </summary>
    public void Release()
    {
        if (!_subscribed)
            return;

        _subscribed = false;
        _descriptor?.RemoveValueChanged(_source, _changed);
    }

    /* Loaded can be raised more than once without an Unloaded between, so a second call adds nothing. */
    private void Subscribe()
    {
        if (_subscribed || _descriptor is null)
            return;

        _subscribed = true;
        _descriptor.AddValueChanged(_source, _changed);
    }
}
