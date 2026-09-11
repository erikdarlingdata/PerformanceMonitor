/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// The single, app-agnostic source of truth for classifying an alert-history row by its metric
    /// NAME. The row-styling predicates (<see cref="IsResolution"/> / <see cref="IsCritical"/> /
    /// <see cref="IsWarning"/>) are used by ALL THREE Alert History grids — Lite's, the Darling
    /// Viewer's and the deprecated Dashboard's — and by the Dashboard sidebar's Alert badge count.
    ///
    /// Alert classification across this codebase is metric-name based — there is no structural "kind"
    /// field on a row — so this centralizes a string convention that was previously duplicated inline
    /// in Dashboard's AlertsHistoryContent and Lite's AlertHistoryRow, and had drifted: both copies
    /// recognized only the "Cleared"/"Resolved" resolution suffixes and missed "Restored", even though
    /// the UI legend documents "Server Restored" as a resolved/green state (#1225).
    ///
    /// <para><b>The VALUE members below are a narrower set — read this before "fixing" the Dashboard to
    /// use them (#1913).</b> <see cref="IsStateOnly"/>, <see cref="StateOnlyDisplay"/> and
    /// <see cref="FormatHistoryValue"/> apply ONLY to the two stores that persist an alert's value as a
    /// <c>double</c>: Lite's DuckDB <c>config_alert_log</c> and Darling's PostgreSQL one. The em dash
    /// exists because those columns are NOT NULL doubles, so a metric whose value is a state
    /// ("DISCONNECTED", "Blocking and Deadlock") has nowhere to put it and stores a 0 that would render
    /// as "0.00".</para>
    ///
    /// <para>The deprecated Dashboard is <b>structurally immune and deliberately excluded</b>. Its store
    /// (<c>JsonAlertHistoryStore</c>) keeps <c>CurrentValue</c>/<c>ThresholdValue</c> as STRINGS and
    /// discards <c>AlertHistoryRecord</c>'s numeric slots entirely, so its grid renders the producer's
    /// display text verbatim — "Blocking and Deadlock", not a dash. It never coerces, so it never had
    /// the defect, and routing it through <see cref="FormatHistoryValue"/> would mean inventing a
    /// numeric column in order to REPLACE readable text with an em dash. That is a regression, not a
    /// parity fix. The dash is damage control for a lossy store, not the better rendering.</para>
    /// </summary>
    public static class AlertMetricClassifier
    {
        /// <summary>
        /// True when the metric name denotes a resolution / good-news notice — a condition that
        /// previously alerted has cleared — rather than an actionable alert. Recognizes every
        /// resolution suffix the alert engines emit: "&#8230; Cleared", "&#8230; Resolved",
        /// "&#8230; Restored" (e.g. Blocking Cleared, CPU Resolved, Capture Restored, Server Restored),
        /// plus "&#8230; Resumed", "&#8230; Restarted", "&#8230; Recovered" and "&#8230; Reconnected".
        ///
        /// Those last four were the same #1225 drift one layer down: Darling's self-alert recoveries have
        /// been emitting "Collection Resumed", "Agent Restarted" and "Compression Job Recovered" — genuine
        /// resolution rows, written by the very same <c>RecordResolutionAsync</c> path as the recognized
        /// "Capture Restored" — and every one of them was landing in the history grids styled as a live
        /// actionable alert, because the suffix list had never caught up with the alerts. The AG family
        /// (#991) adds "AG Replica Reconnected", "AG Sync Recovered" and "AG Data Movement Resumed", so
        /// the list is completed here rather than adding a fifth unrecognized suffix.
        ///
        /// No actionable metric name in either app contains any of these words, so widening the match
        /// cannot turn a real alert green.
        /// </summary>
        public static bool IsResolution(string? metricName)
        {
            if (string.IsNullOrEmpty(metricName))
                return false;

            return metricName.Contains("Cleared", StringComparison.Ordinal)
                || metricName.Contains("Resolved", StringComparison.Ordinal)
                || metricName.Contains("Restored", StringComparison.Ordinal)
                || metricName.Contains("Resumed", StringComparison.Ordinal)
                || metricName.Contains("Restarted", StringComparison.Ordinal)
                || metricName.Contains("Recovered", StringComparison.Ordinal)
                || metricName.Contains("Reconnected", StringComparison.Ordinal);
        }

        /// <summary>
        /// True when the metric name denotes a critical-severity alert (deadlock or poison wait),
        /// used for row emphasis in the history grids. Mirrors the long-standing inline convention.
        /// </summary>
        public static bool IsCritical(string? metricName)
        {
            if (string.IsNullOrEmpty(metricName))
                return false;

            return metricName.Contains("Deadlock", StringComparison.Ordinal)
                || metricName.Contains("Poison", StringComparison.Ordinal);
        }

        /// <summary>
        /// True for an ordinary (warning-severity) alert: actionable, neither a resolution notice
        /// nor critical.
        /// </summary>
        public static bool IsWarning(string? metricName) =>
            !IsResolution(metricName) && !IsCritical(metricName);

        /// <summary>
        /// What the history grids render in place of a number for a <see cref="IsStateOnly"/> metric
        /// that stored the 0 sentinel (#1846). An em dash — the typographic "no value here", and
        /// distinct from a hyphen, which reads as a minus sign in a numeric column.
        /// </summary>
        public const string StateOnlyDisplay = "—";

        /// <summary>
        /// Renders one stored alert-history double with the unit and precision that match its metric
        /// (#1134), for the Value and Threshold columns of the two grids whose stores are double-typed:
        /// Lite's and the Darling Viewer's. The deprecated Dashboard's is string-typed and does not call
        /// this — see the class summary for why that is correct rather than a gap.
        ///
        /// <para>This lived as two copies — Lite's <c>AlertHistoryRow.FormatValue</c> and the Darling
        /// Viewer's, the second one carrying the comment "Copied from Lite's ... so both grids read
        /// identically". They had stayed identical, but by hand: every metric added since had to be
        /// added twice, and the #1846 dash arm below is now load-bearing rather than cosmetic, because
        /// #1881 made the producers guarantee the 0 sentinel it keys on. Two copies of a rule that
        /// decides whether a full disk shows "0.00" or "no value" is one copy too many.</para>
        ///
        /// <para>The fallback is <c>:F2</c> and never <c>:G</c>, so an unmapped metric — an
        /// "Analysis: &lt;category&gt; [&lt;hash&gt;]" finding severity, say — cannot render as a raw
        /// full-precision float (the reported Volume Free Space 0.9746057751382348).</para>
        /// </summary>
        public static string FormatHistoryValue(string? metricName, double value) => metricName switch
        {
            /* Percent metrics — CPU/tempdb usage, free space, and the job's "% of average".
               "TempDB Space" is the PRE-RENAME spelling of "tempdb Space" (the tempdb token was
               lowercased across both apps' UI in c0109f34, which changed the metric_name KEY). That
               commit accepted "historical alert-history rows keep the old name", so archived rows still
               carry it — and matching here is ordinal, so those rows were falling through to the bare
               :F2 default and rendering a percentage with no unit. Nothing writes the old name any
               more; it is kept solely so already-stored rows format like the new ones. */
            "High CPU" or "tempdb Space" or "TempDB Space" or "Volume Free Space" or "Long-Running Job" => $"{value:F1}%",

            /* Poison wait carries an average ms/wait; long-running query carries elapsed minutes. */
            "Poison Wait" => $"{value:F0} ms",
            "Long-Running Query" => $"{value:F0} m",

            /* #1839 total blocked wait — seconds, whole (the numeric is already seconds, not ms). */
            "Blocking Wait Time" => $"{value:F0} s",

            /* Count metrics — whole-number event counts. "Custom Alert Rules Unhealthy" (#3304) is the
               fleet-level self-alert whose value is the COUNT of custom rules that are broken or never-firing;
               a whole number like its siblings, not a state, so it renders here rather than joining
               IsStateOnly (its "Custom Alert Rules Recovered" resolution is state-only via IsResolution). */
            "Blocking Detected" or "Deadlocks Detected" or "Failed Agent Job"
                or "Custom Alert Rules Unhealthy" => $"{value:F0}",

            /* #1846: a state-only metric never had a number — its display value is a role, a connection
               state, a version or the literal "resolved", and the stored double is the 0 sentinel the
               NOT NULL column demands. Render the dash rather than inventing "0.00".

               Gated on value == 0, which is what keeps "Store Disk Pressure" — deliberately NOT
               state-only — rendering its percent-free as a number, INCLUDING a genuine 0 meaning a full
               volume. The gate also lets any metric here graduate to a real measurement later without
               touching the classification: a producer that starts passing a numeric simply shows it. */
            _ when value == 0 && IsStateOnly(metricName) => StateOnlyDisplay,

            _ => $"{value:F2}",
        };

        /// <summary>
        /// True when the metric's "current value" is a STATE, not a measurement — so the double stored
        /// in the NOT NULL <c>current_value</c>/<c>threshold_value</c> columns is a sentinel rather than
        /// data, and rendering it as "0.00" invents a number the alert never had (#1846).
        ///
        /// <para>Callers apply it ONLY to a stored 0 — a nonzero value on one of these metrics means some
        /// future producer started passing a real numeric, and that number is shown rather than hidden
        /// behind a dash.</para>
        ///
        /// <para><b>The producers now guarantee the 0 rather than happening to store one (#1881).</b>
        /// #1846 shipped this list read-side only and left the write side to luck: producers hand the
        /// stores a display STRING, and <c>AlertValueParser.ParseOrDefault</c> returns 0 only when that
        /// string carries no digit at all. That parser scans to the first digit ANYWHERE rather than
        /// requiring a leading number, so the guarantee was never real — "AG Sync Fell Behind" spells the
        /// lag seconds into its prose, "Server Unreachable" carries whatever number the driver put in its
        /// error message, and ANY of them can pick a digit out of an object name ("SQL01", "Sales2024").
        /// Every metric named below now passes an explicit 0 at its fire site, so membership here and a
        /// stored 0 mean the same thing instead of merely correlating. The 0-gate stays as the read-side
        /// half of the contract: it is what lets a metric graduate to a real measurement later without
        /// touching this list.</para>
        ///
        /// <para>Two families, both verified at their fire sites rather than assumed:</para>
        /// <list type="bullet">
        /// <item><b>Every resolution notice</b>, via <see cref="IsResolution"/>. Darling's
        /// <c>BuildResolutionRecord</c> hardcodes <c>CurrentValueText: "resolved"</c> and
        /// <c>ThresholdValueText: ""</c> with both numerics null, for BOTH its own self-alert recoveries
        /// (Collection Resumed, Capture Restored, Agent Restarted, Store Disk Pressure Resolved,
        /// Compression Job Recovered) and the shared engine's resolution callback (CPU Resolved, Blocking
        /// Cleared, Blocking Wait Cleared, Deadlocks Cleared, Poison Waits Cleared, Long-Running Queries
        /// Cleared, tempdb Space Resolved, Volume Free Space Resolved, Long-Running Jobs Cleared). Reusing
        /// the classifier instead of listing those fourteen is deliberate: a fifteenth resolution metric
        /// gets the right rendering for free, which is the drift this class exists to stop. It also
        /// absorbs four of the AG/connection metrics below — "Server Restored", "AG Replica Reconnected",
        /// "AG Sync Recovered" and "AG Data Movement Resumed" are resolutions by name.</item>
        /// <item><b>The actionable state metrics</b> enumerated here, whose value is a role, a connection
        /// state, a suspend reason, a version, or a prose explanation.</item>
        /// </list>
        ///
        /// <para><b>"Store Disk Pressure" is the one metric that must NEVER be added.</b> Its value is
        /// percent-free, which is a genuine measurement, and a genuine 0 there means a FULL volume — the
        /// single reading an operator most needs to see as a number. It is the only self-alert whose
        /// threshold text is a real bound ("10% free") rather than an English phrase, which is the tell
        /// that separates it from everything listed below; it passes its percent-free explicitly as of
        /// #1881, so it no longer depends on the parser finding it either.</para>
        /// </summary>
        public static bool IsStateOnly(string? metricName)
        {
            if (string.IsNullOrEmpty(metricName))
                return false;

            if (IsResolution(metricName))
                return true;

            return metricName switch
            {
                /* Role desc ("PRIMARY"/"SECONDARY"), connected state ("DISCONNECTED"), the
                   suspend_reason_desc, and JudgeSync's prose reason ("Availability Group '…': database …"),
                   respectively — none of them a measurement. */
                "AG Failover"
                    or "AG Replica Disconnected"
                    or "AG Sync Fell Behind"
                    or "AG Database Suspended"
                    /* The connection-loss reason string ("Login timeout", "Network error", …). */
                    or "Server Unreachable"

                    /* #1846 classified the AG/connection family and stopped there; these two are the same
                       shape and were simply missed. "Capture Down" reports WHICH capture is missing
                       ("Blocking", "Deadlock", "Blocking and Deadlock") against the threshold phrase
                       "session running"; "Agent Not Running" is literally "Stopped" against "Running".
                       Both stored the 0 sentinel from the day they shipped and rendered it as "0.00". */
                    or "Capture Down"
                    or "Agent Not Running"

                    /* #1881, the metrics that stored a NONZERO number that was not a measurement:

                       "Collection Stopped" — a prose diagnosis against the threshold phrase "collecting".
                       It fires from two structurally different rules and used to store whichever number
                       their sentences happened to lead with: the failure-streak branch's run count, or the
                       staleness branch's minutes. One column, two units. The run count was never data
                       either — the read is LIMITed to the threshold constant and the rule requires the
                       full window, so it is always exactly ConsecutiveFailureThreshold, i.e. the threshold
                       restated. The minutes ARE a measurement, but only the staleness branch has them, and
                       a column that means minutes on one row and nothing on the next is the defect itself.

                       "Compression Job Stuck" — the same split: TimescaleDB's stuck reason is elapsed
                       minutes when a run hung, and a scheduler state with no duration at all when
                       next_start is -infinity.

                       "Store Runtime Upgrade" — the PostgreSQL MAJOR VERSION ("PostgreSQL 18"), on both
                       sides: 18 as the "current value" and 17 as the "threshold". A version is an
                       identity, not a quantity, and nothing about the store's upgrade is measured. */
                    or "Collection Stopped"
                    or "Compression Job Stuck"
                    or "Store Runtime Upgrade" => true,
                _ => false,
            };
        }
    }
}
