/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// One (object, day) size sample feeding the FinOps object-growth heatmap. <see cref="ReservedMb"/>
    /// is the absolute reserved footprint for that object on that day (already summed across its indexes
    /// by the read layer), NOT a per-day delta — see #1138 plan §3A.
    /// </summary>
    public readonly record struct FinOpsObjectDaySample(string ObjectKey, DateTime Day, double ReservedMb);

    /// <summary>
    /// A long-form series pivoted into the dense matrix the heatmap renderer consumes. Row index 0 is the
    /// BOTTOM row (matching the query-heatmap renderer's <c>FlipVertically = true</c>), so callers pass row
    /// keys bottom-to-top. Columns are the distinct sample days, ascending left-to-right.
    /// </summary>
    public sealed class FinOpsHeatmapMatrix
    {
        public double[,] Intensities { get; init; } = new double[0, 0];
        public string[] RowLabels { get; init; } = Array.Empty<string>();
        public DateTime[] Days { get; init; } = Array.Empty<DateTime>();

        public bool IsEmpty => RowLabels.Length == 0 || Days.Length == 0;
    }

    /// <summary>
    /// Pure, app-agnostic shaping helpers for the FinOps heatmaps (#1138). Lives in Common (no ScottPlot /
    /// WPF dependency) so Dashboard and Lite share ONE implementation of the top-N ranking, the long→matrix
    /// pivot, and the per-column color-scale math — the parity-critical logic — and both test projects can
    /// exercise it directly without a database.
    /// </summary>
    public static class FinOpsHeatmapBuilder
    {
        /// <summary>
        /// Ranks objects by growth descending (biggest grower first) and returns the top-N keys. Ties break
        /// on the key (ordinal) so the result is deterministic. Used both to choose WHICH objects appear and
        /// to order the companion grid; the heatmap reverses this for bottom-to-top row order.
        /// </summary>
        public static List<string> RankTopGrowers(IEnumerable<(string Key, double Growth)> growthByKey, int topN)
        {
            if (growthByKey == null) throw new ArgumentNullException(nameof(growthByKey));
            if (topN < 0) topN = 0;

            return growthByKey
                .OrderByDescending(x => x.Growth)
                .ThenBy(x => x.Key, StringComparer.Ordinal)
                .Take(topN)
                .Select(x => x.Key)
                .ToList();
        }

        /// <summary>
        /// Pivots the long-form (object, day, reserved) samples into a dense [rows, cols] matrix. Rows follow
        /// <paramref name="rowKeysBottomToTop"/> exactly (index 0 = bottom). Columns are the distinct days
        /// present in <paramref name="samples"/>, ascending. Cells with no sample stay 0.0, which the renderer
        /// maps to a blank (NaN) cell — the desired "object absent / truncated that day" rendering (#1138 M2).
        /// Samples whose key is not in <paramref name="rowKeysBottomToTop"/> are ignored; duplicate
        /// (key, day) pairs are summed defensively.
        /// </summary>
        public static FinOpsHeatmapMatrix BuildMatrix(
            IReadOnlyList<string> rowKeysBottomToTop,
            IEnumerable<FinOpsObjectDaySample> samples)
        {
            if (rowKeysBottomToTop == null) throw new ArgumentNullException(nameof(rowKeysBottomToTop));
            if (samples == null) throw new ArgumentNullException(nameof(samples));

            var sampleList = samples as IReadOnlyList<FinOpsObjectDaySample> ?? samples.ToList();

            var rowIndex = new Dictionary<string, int>(StringComparer.Ordinal);
            for (int i = 0; i < rowKeysBottomToTop.Count; i++)
                rowIndex[rowKeysBottomToTop[i]] = i; // last write wins on accidental dupes

            var days = sampleList
                .Select(s => s.Day)
                .Distinct()
                .OrderBy(d => d)
                .ToArray();

            var colIndex = new Dictionary<DateTime, int>();
            for (int c = 0; c < days.Length; c++)
                colIndex[days[c]] = c;

            int rows = rowKeysBottomToTop.Count;
            var intensities = new double[rows, days.Length];

            foreach (var s in sampleList)
            {
                if (!rowIndex.TryGetValue(s.ObjectKey, out int r)) continue;
                if (!colIndex.TryGetValue(s.Day, out int c)) continue;
                intensities[r, c] += s.ReservedMb; // sum guards accidental same-(key,day) duplicates
            }

            return new FinOpsHeatmapMatrix
            {
                Intensities = intensities,
                RowLabels = rowKeysBottomToTop.ToArray(),
                Days = days
            };
        }

        /// <summary>
        /// Puts every cell of a heatmap matrix into one of <paramref name="bandCount"/> discrete bands, the discrete form of
        /// the desktop's single colour scale. Each cell above 0 is placed by <c>log1p(value)</c>, normalised across the
        /// WHOLE matrix from <c>log1p</c> of the smallest positive cell to <c>log1p</c> of the largest:
        /// <c>floor(t * bandCount)</c>, clamped to <c>bandCount - 1</c>, so the smallest positive cell is band 0 and the
        /// largest is the top band. A cell of 0, or with no sample, has no band (null). When every positive cell is
        /// equal, or there is only one, there is no range to place them in, and they all take band 0 (the darkest colour, where
        /// the desktop's scale paints position 0.0). A non-finite cell (NaN, +Infinity or -Infinity) has no band (null) and
        /// is left out of the range. <paramref name="bandCount"/> below 1 throws <see cref="ArgumentOutOfRangeException"/>
        /// and a null <paramref name="matrix"/> throws <see cref="ArgumentNullException"/>. The result has the matrix's shape.
        /// </summary>
        public static int?[,] MatrixLogBands(FinOpsHeatmapMatrix matrix, int bandCount)
        {
            if (matrix == null) throw new ArgumentNullException(nameof(matrix));
            ArgumentOutOfRangeException.ThrowIfLessThan(bandCount, 1);

            int rows = matrix.Intensities.GetLength(0);
            int cols = matrix.Intensities.GetLength(1);
            var bands = new int?[rows, cols];

            double smallest = double.PositiveInfinity;
            double largest = double.NegativeInfinity;
            for (int r = 0; r < rows; r++)
            {
                for (int c = 0; c < cols; c++)
                {
                    double v = matrix.Intensities[r, c];
                    if (!(v > 0) || double.IsInfinity(v)) continue;
                    if (v < smallest) smallest = v;
                    if (v > largest) largest = v;
                }
            }
            if (double.IsPositiveInfinity(smallest)) return bands;

            double low = Math.Log(1 + smallest);
            double span = Math.Log(1 + largest) - low;
            for (int r = 0; r < rows; r++)
            {
                for (int c = 0; c < cols; c++)
                {
                    double v = matrix.Intensities[r, c];
                    if (!(v > 0) || double.IsInfinity(v)) continue;
                    if (!(span > 0))
                    {
                        bands[r, c] = 0;
                        continue;
                    }
                    int band = (int)Math.Floor((Math.Log(1 + v) - low) / span * bandCount);
                    bands[r, c] = Math.Clamp(band, 0, bandCount - 1);
                }
            }
            return bands;
        }

        /// <summary>
        /// Computes per-column color intensities (0..1) on a log1p scale, normalized to the column max. A
        /// column whose max is 0 yields all-zero intensities (the max=0 divide-by-zero guard, #1138 §3B).
        /// Negative values clamp to 0. This is the math behind the Locking grid's per-column cell shading;
        /// each wait-type column is scaled independently so a quiet column never washes out under a loud one.
        /// </summary>
        public static double[] ColumnLogIntensities(IReadOnlyList<long> values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));

            var result = new double[values.Count];
            long max = 0;
            for (int i = 0; i < values.Count; i++)
                if (values[i] > max) max = values[i];

            if (max <= 0) return result; // all zero — guard against divide-by-zero

            double denom = Math.Log(1 + max);
            for (int i = 0; i < values.Count; i++)
            {
                long v = values[i];
                result[i] = v <= 0 ? 0.0 : Math.Log(1 + v) / denom;
            }
            return result;
        }
    }
}
