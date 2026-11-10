/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Input;
using System.Windows.Media;
using ScottPlot.WPF;

namespace Darling.Tests;

/// <summary>
/// Points WPF's mouse at one drawn cell of a heatmap in a <see cref="WpfPlot"/> and raises the move the chart's own handler listens
/// for. The position reaches WPF's mouse device as an input report on the window's own source, the path a real move takes, so
/// <c>MouseEventArgs.GetPosition</c> reads it back wherever the machine's cursor is. Nothing here waits: the report and the event
/// are processed before each call returns.
/// </summary>
internal static class HeatmapHoverProbe
{
    private const int Activate = 0x02;
    private const int Deactivate = 0x04;
    private const int AbsoluteMove = 0x10;

    /// <summary>The middle, in the chart's own units, of the cell the chart drew at <paramref name="column"/> and
    /// <paramref name="rowFromBottom"/>, found by asking the drawn heatmap which cell each point of the chart falls in.</summary>
    public static Point CellPoint(WpfPlot chart, int column, int rowFromBottom)
    {
        var heatmap = chart.Plot.GetPlottables<ScottPlot.Plottables.Heatmap>().Single();
        var dpi = VisualTreeHelper.GetDpi(chart);
        double sumX = 0, sumY = 0;
        var points = 0;
        for (double y = 0; y < chart.ActualHeight; y += 2)
        {
            for (double x = 0; x < chart.ActualWidth; x += 2)
            {
                var coordinates = chart.Plot.GetCoordinates(new ScottPlot.Pixel((float)(x * dpi.DpiScaleX), (float)(y * dpi.DpiScaleY)));
                var (cellColumn, cellRow) = heatmap.GetIndexes(coordinates);
                if (cellColumn == column && cellRow == rowFromBottom)
                {
                    sumX += x;
                    sumY += y;
                    points++;
                }
            }
        }

        if (points == 0)
        {
            throw new Xunit.Sdk.XunitException($"the chart drew no cell at column {column}, row {rowFromBottom}");
        }

        return new Point(sumX / points, sumY / points);
    }

    /// <summary>Moves WPF's mouse to <paramref name="point"/> of <paramref name="chart"/> and raises the chart's mouse move.</summary>
    public static void MoveTo(Window window, WpfPlot chart, Point point)
    {
        var source = PresentationSource.FromVisual(window) ?? throw new Xunit.Sdk.XunitException("the window has no presentation source");
        var inWindow = chart.TranslatePoint(point, (UIElement)source.RootVisual);
        var inDevice = source.CompositionTarget.TransformToDevice.Transform(inWindow);
        Report(source, Activate | AbsoluteMove, (int)Math.Round(inDevice.X), (int)Math.Round(inDevice.Y));

        var seen = Mouse.PrimaryDevice.GetPosition(chart);
        if (Math.Abs(seen.X - point.X) > 1.5 || Math.Abs(seen.Y - point.Y) > 1.5)
        {
            throw new Xunit.Sdk.XunitException($"the mouse was sent to {point} of the chart and WPF reads it at {seen}");
        }

        chart.RaiseEvent(new MouseEventArgs(Mouse.PrimaryDevice, 0) { RoutedEvent = Mouse.MouseMoveEvent });
    }

    /// <summary>Takes WPF's mouse off the window, so the device holds no source once the window closes.</summary>
    public static void Leave(Window window)
    {
        if (PresentationSource.FromVisual(window) is { } source)
        {
            Report(source, Deactivate, 0, 0);
        }
    }

    private static void Report(PresentationSource source, int actions, int x, int y)
    {
        const BindingFlags Any = BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;
        var core = typeof(InputManager).Assembly;
        var actionsType = core.GetType("System.Windows.Input.RawMouseActions", throwOnError: true)!;
        var reportType = core.GetType("System.Windows.Input.RawMouseInputReport", throwOnError: true)!;
        var argsType = core.GetType("System.Windows.Input.InputReportEventArgs", throwOnError: true)!;
        var report = Activator.CreateInstance(reportType, Any, null,
            [InputMode.Foreground, 0, source, Enum.ToObject(actionsType, actions), x, y, 0, IntPtr.Zero], null)!;
        var args = (InputEventArgs)Activator.CreateInstance(argsType, Any, null, [null, report], null)!;
        args.RoutedEvent = (RoutedEvent)typeof(InputManager).GetField("PreviewInputReportEvent", Any)!.GetValue(null)!;
        InputManager.Current.ProcessInput(args);
    }
}
