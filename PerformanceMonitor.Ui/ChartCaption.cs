/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #6832: a chart caption set as the plot title is drawn as one centred line, so at 1366x768 a long one ("Source: raw
    /// (one point per collection). Data begins ...") is cut at both ends. <see cref="Wrap"/> breaks it at spaces so each line
    /// fits the width the title is centred over.
    /// </summary>
    public static class ChartCaption
    {
        /// <summary>Average glyph width as a fraction of the font size; the same figure <see cref="ChartAxisTitle"/> uses.</summary>
        private const double GlyphWidth = 0.6;

        /// <summary>What the y-axis title, its tick labels and the plot's padding take from the control's width; the caption is centred over what is left.</summary>
        public const double AxisMargin = 80;

        /// <summary>The fewest characters a line is allowed, so a narrow chart does not turn a caption into a column of words.</summary>
        private const int MinLineChars = 24;

        /// <summary>
        /// The caption with a line break at the last space that keeps each line inside <paramref name="chartWidth"/> less the axis margin.
        /// A word longer than a line stays whole on its own line. Text that already fits is returned as it came.
        /// </summary>
        public static string Wrap(string caption, double chartWidth, double fontSize)
        {
            if (string.IsNullOrEmpty(caption) || fontSize <= 0)
            {
                return caption;
            }

            var maxChars = Math.Max(MinLineChars, (int)((chartWidth - AxisMargin) / (GlyphWidth * fontSize)));
            if (caption.Length <= maxChars)
            {
                return caption;
            }

            var lines = new List<string>();
            var line = new StringBuilder();
            foreach (var word in caption.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            {
                if (line.Length > 0 && line.Length + 1 + word.Length > maxChars)
                {
                    lines.Add(line.ToString());
                    line.Clear();
                }

                if (line.Length > 0)
                {
                    line.Append(' ');
                }

                line.Append(word);
            }

            if (line.Length > 0)
            {
                lines.Add(line.ToString());
            }

            return string.Join("\n", lines);
        }
    }
}
