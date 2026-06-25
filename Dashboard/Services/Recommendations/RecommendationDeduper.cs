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

namespace PerformanceMonitorDashboard.Services.Recommendations
{
    /// <summary>
    /// The PURE, database-free core of the Recommendations surface (C3): canonical severity
    /// mapping + cross-store de-dupe. Every method here is a static function over already-
    /// mapped <see cref="RecommendationItem"/> lists (or scalars), so the merge/collision
    /// rules can be unit-tested with synthesized rows — no DB, no UI. The
    /// <see cref="RecommendationsReader"/> reads the two stores, maps each row to a
    /// <see cref="RecommendationItem"/>, then hands the two lists to <see cref="Merge"/>.
    /// </summary>
    public static class RecommendationDeduper
    {
        /// <summary>
        /// Maps an engine finding's <c>double</c> severity (0–~2.0) onto the canonical band.
        /// Cutoffs (D1): <c>&gt;= 1.5</c> → Critical, <c>&gt;= 0.75</c> → Warning, else Info.
        /// </summary>
        public static CanonicalSeverity FromEngineSeverity(double severity)
        {
            if (severity >= 1.5)
                return CanonicalSeverity.Critical;
            if (severity >= 0.75)
                return CanonicalSeverity.Warning;
            return CanonicalSeverity.Info;
        }

        /// <summary>
        /// Maps the legacy <c>config.critical_issues</c> text severity onto the canonical
        /// band. CRITICAL → Critical, WARNING → Warning, anything else (INFO / unknown) →
        /// Info. Case-insensitive; tolerates surrounding whitespace.
        /// </summary>
        public static CanonicalSeverity FromLegacySeverity(string? severity)
        {
            var s = severity?.Trim();
            if (string.Equals(s, "CRITICAL", StringComparison.OrdinalIgnoreCase))
                return CanonicalSeverity.Critical;
            if (string.Equals(s, "WARNING", StringComparison.OrdinalIgnoreCase))
                return CanonicalSeverity.Warning;
            return CanonicalSeverity.Info;
        }

        /// <summary>
        /// A synthesized raw-severity stand-in for a legacy row (which has no numeric score),
        /// so the engine + legacy rows sort together within and across bands: Critical → 2.0,
        /// Warning → 1.0, Info → 0.0. Used only for the secondary ordering.
        /// </summary>
        public static double LegacyRawSeverity(CanonicalSeverity band) => band switch
        {
            CanonicalSeverity.Critical => 2.0,
            CanonicalSeverity.Warning => 1.0,
            _ => 0.0
        };

        /// <summary>
        /// Merges the engine + legacy mapped lists into one de-duped, severity-sorted list
        /// (C3). PURE: no DB, no UI, no ordering dependence on the inputs beyond what the
        /// collision rule dictates. The de-dupe key is
        /// (<see cref="NormalizeDatabase"/>(<see cref="RecommendationItem.Database"/>),
        /// <see cref="RecommendationItem.Setting"/>). Rows whose
        /// <see cref="RecommendationItem.Setting"/> is <see cref="RecommendationSetting.None"/>
        /// NEVER de-dupe (they are not per-DB config-setting recs) — every such row passes
        /// through. For a colliding (db, setting) pair the winner is chosen by
        /// <see cref="PreferEngineFor"/>; the loser is dropped. The result is sorted by
        /// canonical severity desc, then raw severity desc, then database, then title — a
        /// total, deterministic order.
        /// </summary>
        public static List<RecommendationItem> Merge(
            IEnumerable<RecommendationItem> engineItems,
            IEnumerable<RecommendationItem> legacyItems)
        {
            // Bucket the dedupe-eligible rows (Setting != None) by (db, setting); collect the
            // pass-through rows (Setting == None) separately so they are never collapsed.
            var byKey = new Dictionary<(string Db, RecommendationSetting Setting), RecommendationItem>();
            var passthrough = new List<RecommendationItem>();

            // Engine rows first, then legacy: the collision resolver is explicit about which
            // source wins per setting, so iteration order does not change the winner — but a
            // stable, source-grouped traversal keeps the logic easy to reason about.
            foreach (var item in Concat(engineItems, legacyItems))
            {
                if (item is null)
                    continue;

                if (item.Setting == RecommendationSetting.None)
                {
                    passthrough.Add(item);
                    continue;
                }

                var key = (NormalizeDatabase(item.Database), item.Setting);
                if (!byKey.TryGetValue(key, out var existing))
                {
                    byKey[key] = item;
                    continue;
                }

                // Same (db, setting) from both stores: keep the winner for this setting.
                byKey[key] = ResolveCollision(existing, item);
            }

            var merged = new List<RecommendationItem>(byKey.Count + passthrough.Count);
            merged.AddRange(byKey.Values);
            merged.AddRange(passthrough);

            merged.Sort(CompareForDisplay);
            return merged;
        }

        /// <summary>
        /// Whether the ENGINE row wins a (db, setting) collision. AutoShrink/AutoClose →
        /// engine wins (it is Apply-capable; the legacy row is advise-only). QueryStore →
        /// legacy wins (the engine has no Query-Store finding/Apply today). PageVerify/Rcsi
        /// are engine-only and cannot collide, but are treated as engine-wins for
        /// completeness. <see cref="RecommendationSetting.None"/> never reaches here.
        /// </summary>
        public static bool PreferEngineFor(RecommendationSetting setting) => setting switch
        {
            RecommendationSetting.QueryStore => false,
            _ => true
        };

        /// <summary>
        /// Picks the surviving row for a (db, setting) collision per
        /// <see cref="PreferEngineFor"/>. When the preferred source is present among the two
        /// it wins; otherwise the other row survives (so a collision between two rows of the
        /// non-preferred source — which should not occur in practice — still yields a row).
        /// </summary>
        private static RecommendationItem ResolveCollision(RecommendationItem a, RecommendationItem b)
        {
            var preferEngine = PreferEngineFor(a.Setting);
            var wantSource = preferEngine ? RecommendationSource.Engine : RecommendationSource.Legacy;

            if (a.Source == wantSource)
                return a;
            if (b.Source == wantSource)
                return b;

            // Neither row is the preferred source — keep the first-seen one deterministically.
            return a;
        }

        /// <summary>
        /// Normalizes a database name for the de-dupe key: trim + lower-invariant. Null/empty
        /// collapse to "" so two server-scoped config rows for the same setting would still
        /// key together (in practice config-setting recs are always per-database).
        /// </summary>
        public static string NormalizeDatabase(string? database) =>
            (database ?? string.Empty).Trim().ToLowerInvariant();

        /// <summary>
        /// Total display order: canonical severity desc, then raw severity desc, then
        /// database (ordinal, case-insensitive), then title — deterministic regardless of
        /// input order.
        /// </summary>
        private static int CompareForDisplay(RecommendationItem x, RecommendationItem y)
        {
            // Higher band first (enum ordinals are Info<Warning<Critical, so descending).
            int bySeverity = ((int)y.CanonicalSeverity).CompareTo((int)x.CanonicalSeverity);
            if (bySeverity != 0)
                return bySeverity;

            int byRaw = y.RawSeverity.CompareTo(x.RawSeverity);
            if (byRaw != 0)
                return byRaw;

            int byDb = string.Compare(
                NormalizeDatabase(x.Database), NormalizeDatabase(y.Database), StringComparison.Ordinal);
            if (byDb != 0)
                return byDb;

            return string.Compare(x.Title, y.Title, StringComparison.Ordinal);
        }

        /// <summary>
        /// Concatenates two possibly-null sequences without allocating an intermediate array,
        /// guarding either being null (the reader always passes non-null lists, but the pure
        /// function tolerates either).
        /// </summary>
        private static IEnumerable<RecommendationItem> Concat(
            IEnumerable<RecommendationItem>? first, IEnumerable<RecommendationItem>? second) =>
            (first ?? Enumerable.Empty<RecommendationItem>())
                .Concat(second ?? Enumerable.Empty<RecommendationItem>());
    }
}
