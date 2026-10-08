/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The server a grid's column filters belong to, for keeping them across a restart (#5565). An inherited attached
/// property: set it once on a server tab's root and every grid below it (the filtered ones are each named in XAML)
/// reads it, so the filter managers need no one-by-one wiring. A grid with no scope above it, or no Name, keeps its
/// filters for the session only.
/// </summary>
public static class ColumnFilterScope
{
    public static readonly DependencyProperty ServerProperty = DependencyProperty.RegisterAttached(
        "Server",
        typeof(string),
        typeof(ColumnFilterScope),
        new FrameworkPropertyMetadata(null, FrameworkPropertyMetadataOptions.Inherits));

    public static string? GetServer(DependencyObject element) => (string?)element.GetValue(ServerProperty);

    public static void SetServer(DependencyObject element, string? value) => element.SetValue(ServerProperty, value);
}
