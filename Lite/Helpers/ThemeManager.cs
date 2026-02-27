/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;

namespace PerformanceMonitorLite.Helpers
{
    /// <summary>
    /// Manages the application color theme (Dark / Light) by swapping the merged resource dictionary at runtime.
    /// </summary>
    public static class ThemeManager
    {
        /// <summary>The currently active theme name ("Dark" or "Light").</summary>
        public static string CurrentTheme { get; private set; } = "Dark";

        /// <summary>Returns true when the Light theme is active.</summary>
        public static bool IsLight => CurrentTheme == "Light";

        /// <summary>Fired after the theme dictionary has been swapped.</summary>
        public static event Action<string>? ThemeChanged;

        /// <summary>
        /// Applies the specified theme by replacing the top-level merged resource dictionary.
        /// All DynamicResource bindings in XAML update automatically.
        /// </summary>
        public static void Apply(string theme)
        {
            CurrentTheme = theme;
            var uri = theme == "Light"
                ? new Uri("pack://application:,,,/Themes/LightTheme.xaml")
                : new Uri("pack://application:,,,/Themes/DarkTheme.xaml");

            var dictionaries = Application.Current.Resources.MergedDictionaries;
            dictionaries.Clear();
            dictionaries.Add(new ResourceDictionary { Source = uri });

            ThemeChanged?.Invoke(theme);
        }
    }
}
