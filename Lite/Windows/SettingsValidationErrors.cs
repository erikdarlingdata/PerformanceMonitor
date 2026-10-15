/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Windows;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// The invalid values a Settings Save found, each with the control it came from (#5846). The window is split into
/// tabs, so a message alone no longer says where to look: Save opens the first tab holding one of these controls.
/// </summary>
internal sealed class SettingsValidationErrors
{
    private readonly List<(string Message, DependencyObject Source)> _items = new();

    public int Count => _items.Count;

    /// <summary>The messages, in the order Save found them.</summary>
    public IEnumerable<string> Messages => _items.Select(i => i.Message);

    /// <summary>The controls holding the invalid values, in the same order as <see cref="Messages"/>.</summary>
    public IEnumerable<DependencyObject> Sources => _items.Select(i => i.Source);

    public void Add(string message, DependencyObject source) => _items.Add((message, source));

    /// <summary>Adds each of <paramref name="messages"/> against the one control they came from.</summary>
    public void AddRange(IEnumerable<string> messages, DependencyObject source)
    {
        foreach (var message in messages)
        {
            Add(message, source);
        }
    }
}
