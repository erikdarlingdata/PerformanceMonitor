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
/// #7385: the XAML switch for charts that are shown together. Set <c>ui:ChartGroup.IsAligned="True"</c> on the container
/// that holds them (a Grid, a StackPanel) and its charts share one plot rectangle (<see cref="ChartGroupAligner"/>). A
/// <see cref="ChartGridPanel"/> is a group without it.
/// </summary>
public static class ChartGroup
{
    public static readonly DependencyProperty IsAlignedProperty =
        DependencyProperty.RegisterAttached("IsAligned", typeof(bool), typeof(ChartGroup), new PropertyMetadata(false));

    public static bool GetIsAligned(DependencyObject element) => (bool)element.GetValue(IsAlignedProperty);

    public static void SetIsAligned(DependencyObject element, bool value) => element.SetValue(IsAlignedProperty, value);
}
