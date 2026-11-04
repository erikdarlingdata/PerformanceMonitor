/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// The plan-cache single-use bloat verdict (#7445), one rule for the Darling MCP tool and web tile, the Darling
    /// Viewer badge, and Lite's badge and MCP tool. It used to be three copies of a bare ratio over every cache
    /// type, so an idle 49-plan, 11 MB cache (38 of those entries view parse trees) read MEDIUM and was told to
    /// consider Forced Parameterization, a database-wide change.
    ///
    /// <para>Two rules fix that. The ratio is over COMPILED plans only: parse trees and extended procedures are
    /// not plans a query compiled, and a single-use parse tree is not ad-hoc plan bloat. And single-use plans
    /// that together take under <see cref="SingleUseFloorMb"/> are not bloat at all, whatever the ratio.</para>
    /// </summary>
    public static class PlanCacheBloatRule
    {
        /// <summary>
        /// Compiled single-use plans smaller than this in total are not bloat, whatever the ratio. The value is a
        /// judgment, not a measurement: no benchmark says where single-use plans start to cost a server. It is the
        /// floor the analysis engine already applies before it scores a PLAN_CACHE_BLOAT card, so the live badge
        /// and the scored recommendation agree on where a cache is too small to judge.
        /// </summary>
        public const long SingleUseFloorMb = 100;

        /// <summary>The cacheobjtype values that are not compiled plans: view and statement parse trees, and
        /// extended procedures.</summary>
        public const string NonCompiledCacheTypesSql = "'Parse Tree', 'Extended Proc'";

        /// <summary>A SQL boolean over a <c>cacheobjtype</c> column that is true for a compiled plan, for the
        /// stores to filter on. A NULL type counts as compiled, as <see cref="IsCompiled"/> does.</summary>
        public const string CompiledPredicateSql = "COALESCE(cacheobjtype, '') NOT IN (" + NonCompiledCacheTypesSql + ")";

        /// <summary>True when the cache type is a compiled plan rather than a parse tree or an extended
        /// procedure.</summary>
        public static bool IsCompiled(string? cacheObjType) =>
            !string.Equals(cacheObjType, "Parse Tree", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(cacheObjType, "Extended Proc", StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Banded level and the paired recommendation. <paramref name="totalPlans"/> and
        /// <paramref name="totalSizeMb"/> are the whole cache, used only to say how small it is. The other three
        /// are over compiled plans only. Above the floor: &gt; 50% CRITICAL, &gt; 30% HIGH, &gt; 20% MEDIUM, else
        /// NORMAL, and the recommendation names the low-risk first step, <c>optimize for ad hoc workloads</c>,
        /// before the database-wide Forced Parameterization.
        /// </summary>
        public static (string Level, string Recommendation) Classify(
            long totalPlans, long totalSizeMb, long compiledPlans, long compiledSingleUsePlans, long compiledSingleUseSizeMb)
        {
            if (compiledSingleUseSizeMb < SingleUseFloorMb)
            {
                return ("NORMAL", string.Format(
                    CultureInfo.InvariantCulture,
                    "Plan cache is too small to judge: {0} plans using {1} MB, with {2} MB in single-use compiled plans (the floor is {3} MB)",
                    totalPlans, totalSizeMb, compiledSingleUseSizeMb, SingleUseFloorMb));
            }

            double singleUsePercent = compiledPlans > 0 ? compiledSingleUsePlans * 100.0 / compiledPlans : 0.0;

            var level =
                singleUsePercent > 50 ? "CRITICAL" :
                singleUsePercent > 30 ? "HIGH" :
                singleUsePercent > 20 ? "MEDIUM" :
                "NORMAL";

            var recommendation = singleUsePercent > 20
                ? "Optimize for ad hoc workloads is the low-risk first step: the server then caches only a small stub for a plan's first execution. "
                  + "If single-use plans stay high after that, check for unparameterized queries and consider Forced Parameterization, which is a database-wide change"
                : "Plan cache composition is healthy";

            return (level, recommendation);
        }
    }
}
