/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace Darling.Tests;

/// <summary>
/// Reads an <c>EXPLAIN (ANALYZE, FORMAT JSON)</c> plan for how many rows a query resolved against one relation
/// — the read-shape measure the drill-down text tests (#3902, #3959) assert, shared so the two cannot count
/// differently.
/// </summary>
internal static class ExplainJoinProbe
{
    /// <summary>
    /// At every join with <paramref name="relation"/> on one side, the actual rows (across loops) arriving from
    /// the OTHER side, summed over the plan. Counted at the join rather than at the relation's scan because the
    /// join METHOD is the planner's call — a hash join over a small dimension reads all of it for five probe
    /// rows, and a nested loop over a large one reads one row per probe — while the probe side is the property
    /// the read shape decides. SubPlans are walked too, and their loops count.
    /// </summary>
    public static double RowsResolvedAgainst(string explainJson, string relation)
    {
        using var document = JsonDocument.Parse(explainJson);

        var resolved = 0.0;
        Walk(document.RootElement[0].GetProperty("Plan"));
        return resolved;

        void Walk(JsonElement node)
        {
            /* Join inputs only: an InitPlan or SubPlan child is not a side of the join. */
            var inputs = Children(node)
                .Where(c => c.GetProperty("Parent Relationship").GetString() is "Outer" or "Inner")
                .ToList();

            if (inputs.Count == 2 && inputs.Count(Reads) == 1)
            {
                var probe = inputs.Single(c => !Reads(c));
                resolved += probe.GetProperty("Actual Rows").GetDouble() * probe.GetProperty("Actual Loops").GetDouble();
            }

            foreach (var child in Children(node))
            {
                Walk(child);
            }
        }

        bool Reads(JsonElement node) =>
            (node.TryGetProperty("Relation Name", out var name) && name.GetString() == relation)
            || Children(node).Any(Reads);

        static IEnumerable<JsonElement> Children(JsonElement node) =>
            node.TryGetProperty("Plans", out var children) ? children.EnumerateArray() : [];
    }
}
