/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text;
using System.Windows.Data;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Shows query or statement text on one line in a grid cell (#7382). A <c>TextBlock</c> with
    /// <c>TextTrimming</c> still breaks at every line break in its text, so one multi-line statement made its
    /// row as tall as the statement and pushed the other columns out of view. This collapses every run of
    /// whitespace (line breaks, tabs, indentation) to one space and drops leading and trailing whitespace, so
    /// the cell shows the first line's worth of the statement and trims with an ellipsis.
    /// <para>
    /// Use it on the cell's <c>Text</c> binding and keep the binding path on the full text property. The full
    /// text then stays in the tooltip, and Copy Cell, Copy Row and the exports (which read the bound property
    /// off the row, not the converted value) keep the original line breaks.
    /// </para>
    /// </summary>
    public sealed class OneLineTextConverter : IValueConverter
    {
        /// <summary>Singleton — the converter is stateless, so one instance serves every cell in both apps.</summary>
        public static readonly OneLineTextConverter Instance = new();

        /// <summary>
        /// Longest one-line form produced. A cell is a few hundred pixels wide and trims with an ellipsis, so
        /// text past this never shows; stopping here keeps a megabyte batch from being copied for every cell
        /// the grid draws.
        /// </summary>
        public const int MaxLength = 2000;

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture) =>
            OneLine(value as string);

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) =>
            throw new NotSupportedException();

        /// <summary>
        /// <paramref name="text"/> with each run of whitespace collapsed to one space and the ends trimmed, cut at
        /// <see cref="MaxLength"/> characters. Null gives an empty string.
        /// </summary>
        public static string OneLine(string? text)
        {
            if (string.IsNullOrEmpty(text))
                return "";

            var sb = new StringBuilder(Math.Min(text.Length, MaxLength));
            var pendingSpace = false;
            foreach (var c in text)
            {
                if (char.IsWhiteSpace(c))
                {
                    pendingSpace = sb.Length > 0;
                    continue;
                }

                if (pendingSpace)
                {
                    sb.Append(' ');
                    pendingSpace = false;
                }

                sb.Append(c);
                if (sb.Length >= MaxLength)
                    break;
            }

            return sb.ToString();
        }
    }
}
