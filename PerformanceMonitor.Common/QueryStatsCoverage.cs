/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// What a read of the stored query_stats rows (or a rollup built from them) can and cannot add up (#7419). A collection run stores a statement
    /// only when it is in the top 200 of that run by interval CPU, elapsed time, executions or logical reads, so a sum over the table is a sum over
    /// the stored rows, not over every statement the plan cache held. The reads that present a total of the server, a share of it or a count of
    /// statements say so with these words, on both products.
    /// </summary>
    public static class QueryStatsCoverage
    {
        /// <summary>The sentence for a read that sums or counts statements.</summary>
        public const string StoredRowsNote =
            "The plan cache collector stores a statement in a run only when it is in the top 200 of that run by CPU, elapsed time, executions or logical reads for the interval. "
            + "Totals, counts and shares computed from those rows cover the stored statements and are lower than the whole server's work, "
            + "most for executions and for a statement that ranks in some runs and not in others. The heaviest statements are stored in every run, so the top of a ranking is not affected.";
    }
}
