/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Media.Imaging;

namespace Darling.Tests;

/// <summary>
/// #8223: reads a grid as it renders at a given width: each column's header (its text, whether it carries a filter button, where its text
/// ends) and the first row's cell (where its value ends, whether the text is trimmed with an ellipsis and what the tooltip holds). Every
/// figure comes from the realized controls, never from the XAML. Shared by Lite.Tests (by <c>Compile Include</c>) and Darling.Tests,
/// because the two apps carry the same grids.
/// </summary>
internal static class GridPolishProbe
{
    /// <summary>What one column looks like once the grid has rendered.</summary>
    internal sealed class Col
    {
        internal string Header = "";
        internal bool Filter;
        /// <summary>Left edge of the header, from the left edge of the grid.</summary>
        internal double Left;
        internal double Width;
        /// <summary>Gap between the end of the header's content and the header's right edge.</summary>
        internal double HeaderGapRight;
        /// <summary>Gap between the end of the first row's value and its cell's right edge; null when the cell shows no text.</summary>
        internal double? ValueGapRight;
        internal bool Trimmed;
        internal string? Tooltip;
        internal string? Value;
        internal double ValueNaturalWidth;
        internal double ValueBoxWidth;
        internal double CellWidth;
        /// <summary>How far the grid scrolls sideways (the same for every column of one grid): a column ending past the viewport is reachable when it ends within this much more.</summary>
        internal double Scrollable;

        public override string ToString() => $"'{Header}' left {Left:0} width {Width:0} filter {Filter}";
    }

    /// <summary>Shows <paramref name="grid"/> alone at <paramref name="width"/> px with <paramref name="rows"/> bound and reads its columns.</summary>
    internal static List<Col> Measure(DataGrid grid, IList rows, double width, string? png = null) =>
        Layout(grid, rows, width, Read, png);

    /// <summary>The grid's trailing filler header (the blank header cell right of the last column, when the columns do not fill the grid): its width and the right border it draws.</summary>
    internal sealed record Filler(double Width, double BorderRight);

    /// <summary>Shows the grid, binds <paramref name="rows"/>, and reads the filler header; null when the grid has none (its columns fill it).</summary>
    internal static Filler? MeasureFiller(DataGrid grid, IList rows, double width, string? png = null) =>
        Layout(grid, rows, width, ReadFiller, png);

    private static Filler? ReadFiller(DataGrid grid)
    {
        var header = ClippedTextProbe.Descendants(grid).OfType<DataGridColumnHeader>().FirstOrDefault(h => h.Column is null);
        if (header is null || header.ActualWidth < 1)
        {
            return null;
        }

        var border = ClippedTextProbe.Descendants(header).OfType<Border>().FirstOrDefault();
        return new Filler(header.ActualWidth, border?.BorderThickness.Right ?? 0);
    }

    private static T Layout<T>(DataGrid grid, IList rows, double width, Func<DataGrid, T> read, string? png)
    {
        var window = new Window { Content = grid, Width = width, Height = 600, Left = 0, Top = 0 };
        window.Show();
        try
        {
            ClippedTextProbe.Settle(window);
            grid.ItemsSource = rows;
            ClippedTextProbe.Settle(window);
            var found = read(grid);
            if (png is not null)
            {
                Png(grid, png);
            }

            return found;
        }
        finally
        {
            window.Content = null;
            window.Close();
        }
    }

    private static bool IsInside(DependencyObject element, DependencyObject ancestor)
    {
        for (var parent = VisualTreeHelper.GetParent(element); parent is not null; parent = VisualTreeHelper.GetParent(parent))
        {
            if (parent == ancestor)
            {
                return true;
            }
        }

        return false;
    }

    private static List<Col> Read(DataGrid grid)
    {
        var found = new List<Col>();
        var scrollable = (grid.Template?.FindName("DG_ScrollViewer", grid) as ScrollViewer)?.ScrollableWidth ?? 0;
        var all = ClippedTextProbe.Descendants(grid).ToList();
        var row = all.OfType<DataGridRow>().OrderBy(r => r.GetIndex()).FirstOrDefault();
        var cells = row is null ? new List<DataGridCell>() : ClippedTextProbe.Descendants(row).OfType<DataGridCell>().ToList();
        foreach (var header in all.OfType<DataGridColumnHeader>().Where(h => h.Column is not null).OrderBy(h => h.Column.DisplayIndex))
        {
            var buttons = ClippedTextProbe.Descendants(header).OfType<Button>().ToList();
            var texts = ClippedTextProbe.Descendants(header).OfType<TextBlock>().Where(b => b.Text.Length > 0 && !buttons.Any(x => IsInside(b, x))).ToList();
            var col = new Col
            {
                Header = texts.FirstOrDefault()?.Text ?? header.Content?.ToString() ?? "",
                Filter = ClippedTextProbe.Descendants(header).OfType<Button>().Any(b => b.Tag is string),
                Left = header.TransformToAncestor(grid).Transform(new Point(0, 0)).X,
                Width = header.ActualWidth,
                Scrollable = scrollable,
            };

            var contentRight = 0.0;
            foreach (var block in texts)
            {
                contentRight = Math.Max(contentRight, block.TransformToAncestor(header).TransformBounds(new Rect(0, 0, block.ActualWidth, block.ActualHeight)).Right);
            }

            foreach (var button in ClippedTextProbe.Descendants(header).OfType<Button>())
            {
                contentRight = Math.Max(contentRight, button.TransformToAncestor(header).TransformBounds(new Rect(0, 0, button.ActualWidth, button.ActualHeight)).Right);
            }

            col.HeaderGapRight = header.ActualWidth - contentRight;

            var cell = cells.FirstOrDefault(c => c.Column == header.Column);
            var block1 = cell is null ? null : ClippedTextProbe.Descendants(cell).OfType<TextBlock>().FirstOrDefault(b => b.Text.Length > 0);
            if (cell is not null && block1 is not null)
            {
                col.Value = block1.Text;
                col.ValueNaturalWidth = ClippedTextProbe.NaturalWidth(block1);
                col.ValueBoxWidth = block1.ActualWidth;
                col.CellWidth = cell.ActualWidth;
                var rightAligned = block1.TextAlignment == TextAlignment.Right || block1.HorizontalAlignment == HorizontalAlignment.Right;
                var box = block1.TransformToAncestor(cell).TransformBounds(new Rect(0, 0, block1.ActualWidth, block1.ActualHeight));
                var end = rightAligned ? box.Right : box.Left + Math.Min(col.ValueNaturalWidth, box.Width);
                col.ValueGapRight = cell.ActualWidth - end;
                col.Trimmed = block1.TextTrimming != TextTrimming.None;
                col.Tooltip = (block1.ToolTip ?? cell.ToolTip) as string ?? ((block1.ToolTip ?? cell.ToolTip) as ToolTip)?.Content as string;
            }

            found.Add(col);
        }

        return found;
    }

    /// <summary>The visible width of the grid's cells: columns whose left edge is at or past this are off screen until the grid scrolls.</summary>
    internal static double Viewport(DataGrid grid) =>
        (grid.Template?.FindName("DG_ScrollViewer", grid) as ScrollViewer)?.ViewportWidth ?? grid.ActualWidth;

    /// <summary>Sample rows for a row type: a string property reads as its own name, a number grows by row, so a column's cells hold realistic text.</summary>
    internal static List<object> Rows(Type type, int count, IDictionary<string, object?>? values = null)
    {
        var rows = new List<object>();
        for (var i = 0; i < count; i++)
        {
            rows.Add(Make(type, i, values));
        }

        return rows;
    }

    /// <summary>
    /// A sample instance of <paramref name="type"/>: through its parameterless constructor with every writable property set, or through its widest
    /// constructor (a record's, or a row class over a record) with each argument sampled by its own name and type.
    /// </summary>
    private static object Make(Type type, int i, IDictionary<string, object?>? values)
    {
        var ctor = type.GetConstructor(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance, Type.EmptyTypes)
            ?? type.GetConstructors().OrderByDescending(c => c.GetParameters().Length).First();
        var args = ctor.GetParameters().Select(p => Sample(p.ParameterType, p.Name ?? "", i, values, constructorArgument: true)).ToArray();
        var instance = ctor.Invoke(args);
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanWrite && p.GetIndexParameters().Length == 0))
        {
            var value = Sample(property.PropertyType, property.Name, i, values, constructorArgument: false);
            if (value is not null)
            {
                property.SetValue(instance, value);
            }
        }

        return instance;
    }

    private static object? Sample(Type propertyType, string name, int i, IDictionary<string, object?>? values, bool constructorArgument)
    {
        var target = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        if (values is not null)
        {
            var key = values.Keys.FirstOrDefault(k => string.Equals(k, name, StringComparison.OrdinalIgnoreCase));
            if (key is not null)
            {
                var fixedValue = values[key];
                return fixedValue is null || fixedValue.GetType() == target ? fixedValue : Convert.ChangeType(fixedValue, target, CultureInfo.InvariantCulture);
            }
        }

        if (target == typeof(string))
        {
            return $"{name} {i + 1}";
        }

        if (target == typeof(int))
        {
            return 1234 * (i + 1);
        }

        if (target == typeof(long))
        {
            return 1234567L * (i + 1);
        }

        if (target == typeof(double))
        {
            return 12.3456 * (i + 1);
        }

        if (target == typeof(decimal))
        {
            return 12.3456m * (i + 1);
        }

        if (target == typeof(bool))
        {
            return true;
        }

        if (target == typeof(DateTime))
        {
            return new DateTime(2026, 11, 7, 9, 30, 0, DateTimeKind.Utc).AddMinutes(-i);
        }

        if (target.IsEnum)
        {
            return Enum.GetValues(target).GetValue(0);
        }

        /* A constructor argument that is itself a class (the record a row class is built over) is sampled the same way; a property that is none of the types above stays as it is. */
        return constructorArgument && target.IsClass && target.Namespace is not null && !target.Namespace.StartsWith("System", StringComparison.Ordinal)
            ? Make(target, i, values)
            : null;
    }

    /// <summary>Writes the grid as a PNG, for a person to look at.</summary>
    internal static void Png(FrameworkElement element, string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var dpi = VisualTreeHelper.GetDpi(element);
        var bitmap = new RenderTargetBitmap(
            Math.Max(1, (int)Math.Ceiling(element.ActualWidth * dpi.DpiScaleX)), Math.Max(1, (int)Math.Ceiling(element.ActualHeight * dpi.DpiScaleY)),
            dpi.PixelsPerInchX, dpi.PixelsPerInchY, PixelFormats.Pbgra32);
        bitmap.Render(element);
        var encoder = new PngBitmapEncoder();
        encoder.Frames.Add(BitmapFrame.Create(bitmap));
        using var stream = File.Create(path);
        encoder.Save(stream);
    }

    /// <summary>The folder PNGs go to when a person asks for them (an environment variable), otherwise none are written.</summary>
    internal static string? PngPath(string tab, string grid, string theme, double width)
    {
        var dir = Environment.GetEnvironmentVariable("GRID_POLISH_PNG_DIR");
        return string.IsNullOrEmpty(dir) ? null : Path.Combine(dir, $"{tab}-{grid}-{theme}-{width.ToString("0", CultureInfo.InvariantCulture)}.png");
    }
}
