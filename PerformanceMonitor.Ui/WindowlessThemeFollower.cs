/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Keeps a control that is shown outside every window on the application theme. <see cref="ThemeManager.Apply"/> replaces the
    /// dictionaries in <c>Application.Current.Resources</c>, and WPF re-resolves the <c>{DynamicResource}</c> references under each open
    /// Window; a tray balloon sits in a Popup with no window above it, so its brushes keep the values they resolved when it was built.
    /// <see cref="Attach"/> subscribes the element to <see cref="ThemeManager.ThemeChanged"/> until it is unloaded; on a change, the new
    /// theme's dictionary goes into the element's own resources, which re-resolves every brush and implicit style under it.
    /// </summary>
    public static class WindowlessThemeFollower
    {
        public static void Attach(FrameworkElement element)
        {
            ArgumentNullException.ThrowIfNull(element);
            ResourceDictionary? applied = null;
            void OnThemeChanged(string theme)
            {
                var merged = element.Resources.MergedDictionaries;
                if (applied != null) merged.Remove(applied);
                applied = ThemeManager.BuildDictionary(theme);
                merged.Add(applied);
            }

            ThemeManager.ThemeChanged += OnThemeChanged;
            element.Unloaded += (_, _) => ThemeManager.ThemeChanged -= OnThemeChanged;
        }
    }
}
