/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows.Media.Imaging;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Maps plan-operator icon names to their PNG resources, shared by Lite and Dashboard.
/// The PlanIcons PNGs now live in (and are compiled as Resource into) this assembly, so the
/// pack URI is component-qualified to PerformanceMonitor.Ui rather than the host application.
/// </summary>
public static class PlanIconMapper
{
    private static readonly Dictionary<string, BitmapImage> _iconCache = new();

    public static BitmapImage? GetIcon(string iconName)
    {
        if (_iconCache.TryGetValue(iconName, out var cached))
            return cached;

        try
        {
            var uri = new Uri($"pack://application:,,,/PerformanceMonitor.Ui;component/Resources/PlanIcons/{iconName}.png", UriKind.Absolute);
            var bitmap = new BitmapImage();
            bitmap.BeginInit();
            bitmap.UriSource = uri;
            bitmap.CacheOption = BitmapCacheOption.OnLoad;
            bitmap.DecodePixelWidth = 32;
            bitmap.EndInit();
            bitmap.Freeze();
            _iconCache[iconName] = bitmap;
            return bitmap;
        }
        catch
        {
            // Try fallback icon
            if (iconName != "iterator_catch_all")
                return GetIcon("iterator_catch_all");
            return null;
        }
    }
}
