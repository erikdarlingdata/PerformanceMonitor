// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #7565: the one definition of when <c>perfmon_value_per_second</c> is a true rate. The measure compiles to
/// SUM(delta) / SUM(interval seconds). Every instance row of a counter, and every server in the bucket, repeats the same
/// sweep interval, so the rate is exact only for ONE counter on ONE instance on ONE server per series, and reads
/// (instance count x server count) times too low otherwise (a median of 12 and up to 17 rows per collection for
/// Transactions/sec, <c>DarlingTrendReader</c>). Custom Views and notebooks (<see cref="CustomViewValidation"/>) and custom
/// alert rules (<see cref="CustomAlertRuleDefinition.TryParse"/>) both call <see cref="Error"/>, so the two cannot drift.
/// </summary>
internal static class PerfmonRate
{
    private const string RateKey = "perfmon_value_per_second";

    /// <summary>
    /// Returns the error naming what to add, or null when the panel is fine. A dimension is pinned by an <c>eq</c> filter with
    /// one literal (or a variable, which holds one value).
    /// <para>View or notebook panel (<paramref name="forAlert"/> false): groupBy also pins a dimension (one series per
    /// value), and the server is pinned by a server filter, a groupBy on server, or a declared <c>server</c> variable. The
    /// validator cannot see the run's server picker, so the variable is the one place a saved view says it is one server. A
    /// variable has no multi-select mode, so a declared <c>server</c> variable always counts, even though a run that picks
    /// All for it is not caught (documented, #7565).</para>
    /// <para>Alert metric (<paramref name="forAlert"/> true): evaluation is per server and the evaluator scopes the compile
    /// to that one server, so the server is always pinned. An alert metric is a Scalar panel and the compose parser refuses a
    /// groupBy on a Scalar panel ("a single-value panel cannot group"), so for an alert only an <c>eq</c> filter can pin
    /// counter_name and instance_name; the groupBy test below is never true there.</para>
    /// </summary>
    internal static string? Error(PanelPlan plan, IReadOnlyList<ComposeVariable> variables, string where, bool forAlert = false)
    {
        var role = string.Equals(plan.Measure.Key, RateKey, StringComparison.Ordinal) ? "measure"
            : plan.Overlay is { } overlay && string.Equals(overlay.Measure.Key, RateKey, StringComparison.Ordinal) ? "overlay measure"
            : null;
        if (role is null)
        {
            return null;
        }

        bool Pinned(string dimension) =>
            plan.GroupBy.Any(d => string.Equals(d.Name, dimension, StringComparison.Ordinal))
            || plan.Filters.Any(f => string.Equals(f.Dimension.Name, dimension, StringComparison.Ordinal)
                && f.Op == ComposeFilterOp.Eq
                && (f.Value.IsVariable || f.Value.Literals is { Count: 1 }));

        var fixes = new List<string>();
        if (!Pinned("counter_name"))
        {
            fixes.Add("filter counter_name eq 'Transactions/sec'" + (forAlert ? string.Empty : " (or group by counter_name)"));
        }

        if (!Pinned("instance_name"))
        {
            fixes.Add("filter instance_name eq '_Total'" + (forAlert ? string.Empty : " (or group by instance_name)"));
        }

        if (!forAlert
            && !Pinned(MeasureCatalog.ServerDimensionName)
            && !variables.Any(v => string.Equals(v.Dimension, ComposeVariable.ServerDimension, StringComparison.Ordinal)))
        {
            fixes.Add("filter server eq '<server name>' (or group by server, or declare a server variable)");
        }

        if (fixes.Count == 0)
        {
            return null;
        }

        var scope = forAlert ? "one counter and one instance (the alert is already one server)" : "one counter, one instance and one server per series";
        var cause = forAlert ? "Every instance in a sample repeats the sample interval" : "Every instance and server in a bucket repeats the sample interval";
        return $"{where}: the {role} 'perfmon_value_per_second' is a rate only for {scope}. " +
               $"{cause}, so the rate would read that many times too low. Add: " +
               string.Join("; ", fixes) + ".";
    }
}
