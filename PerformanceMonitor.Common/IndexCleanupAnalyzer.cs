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
    /// Monitor-side C# reproduction of Erik's <c>sp_IndexCleanup</c> analysis, driven entirely by the
    /// captured Stage-1 <c>index_object_stats</c> snapshot (no live proc, no database). Given the per-index
    /// rows for one or more databases it emits DROP/MERGE/COMPRESS recommendations, the reconstructed T-SQL,
    /// and a reclaimable-space roll-up — the same analysis a live <c>sp_IndexCleanup</c> would, for servers
    /// where the proc is not (or cannot be) installed.
    ///
    /// <para>Faithful to <c>sp_IndexCleanup</c> v2.7 (20260701), cross-checked against its adversarial test
    /// suite. Because Stage 1 captured <c>key_columns</c>/<c>included_columns</c> in the proc's EXACT
    /// delimited <c>QUOTENAME</c> form, the string-comparison dedupe ports directly. <b>Auto-consolidated</b>
    /// (a script is generated): <b>Unused Index</b> (Rule 1, with the &lt;14-day-uptime caveat and
    /// &lt;=7-day auto-dedupe), <b>Exact Duplicate</b> (Rule 2 → DISABLE), <b>Key Duplicate</b> (Rule 5 →
    /// keeper MERGE INCLUDES + loser DISABLE), and <b>Key Subset</b>/<b>Key Superset</b> (Rules 3/4/6 →
    /// subset DISABLE + superset MERGE, with subset-chain flattening and missing-include carry).
    /// <b>Key Subset</b>/<b>Key Superset</b> (Rules 3/4/6 → subset DISABLE + superset MERGE, with subset-chain
    /// flattening and missing-include carry), and <b>Unique Constraint Replacement</b> (Rules 7/7.5/7.5b → a
    /// nonclustered index MAKE UNIQUE + the redundant constraint DROP CONSTRAINT, with the FK-reference
    /// protection, the both-direction key-set-equality guard, and duplicate-constraint collapse; Rule 7.6 then
    /// re-disables any ordinary Key Duplicate of a made-unique index and folds its includes in).
    /// <b>Recognized but deliberately NOT auto-consolidated</b> (surfaced as a review row, matching the
    /// proc's tested behavior — its adversarial tests 9a/10a assert these are left alone): <b>Reverse
    /// Duplicate</b> (same key column set, different order/direction — a different leading column serves
    /// different queries) and <b>Equal Except For Filter</b> (same keys+includes, different filter — the
    /// filters index different rows). Priority scoring, <c>is_eligible_for_dedupe</c>, and the
    /// PK/unique-constraint/FK-reference disable protections match the proc's guards. PAGE-compression
    /// candidacy and the 0.20–0.60 general savings band reproduce the proc's <c>#compression_eligibility</c>
    /// / reporting math.</para>
    ///
    /// <para><b>Deliberately out of scope</b> (see <see cref="IndexCleanupAnalysisResult.Notes"/>): the proc's
    /// <c>Same Keys Different Order</c> (REVIEW) case, largely covered by the Reverse Duplicate review rows and
    /// reproducible from the captured data if later required. Script details that need metadata Stage 1 did not
    /// capture (the trailing <c>ON &lt;filegroup/partition_scheme&gt;</c> placement; sparse/legacy-LOB
    /// compression exclusions) are surfaced, never guessed.</para>
    ///
    /// <para><b>Review-parity (July 2026 sp_IndexCleanup fix waves ae32a4c + 0abb3ef)</b>: this port carries
    /// the proc review's analysis fixes — constraint-backed (PK/UC) indexes are never merge targets
    /// (<see cref="IsSafeMergeTarget"/>, Msg 1907, with a script-generator backstop); filtered and
    /// partitioned indexes are never promoted to MAKE UNIQUE (<see cref="IsPromotableToUnique"/> — the
    /// filtered case opens a uniqueness hole, the partitioned case can't be alignment-verified monitor-side,
    /// Msg 1908); the Rule 7.5 DROP CONSTRAINT / MAKE UNIQUE pairing shares one promotability gate so a
    /// constraint is never dropped pointing at an index the rebuild step refuses; and the
    /// <c>MinReads</c>/<c>MinWrites</c> object gates apply per-term. The phantom-partitioning-key guard
    /// (<c>key_ordinal &gt; 0</c>) lives upstream in <c>IndexObjectStatsCollector</c>. ONE deliberate
    /// non-port: the proc's PER-INDEX usage floors (<c>meets_usage_floor</c>, fix 0abb3ef) — both hosts
    /// hardwire the floors to 0 and no UI exposes them, so that machinery would be unreachable here; port it
    /// when a floor knob ships.</para>
    /// </summary>
    public static class IndexCleanupAnalyzer
    {
        private const string Clustered = "CLUSTERED";
        private const string NonClustered = "NONCLUSTERED";
        private const string CompressionNone = "NONE";

        /// <summary>
        /// Runs the full analysis over the captured per-index rows. Rows are grouped internally by
        /// (database, object) scope, so a single call may span many databases. Never throws on data content;
        /// malformed/heap/columnstore/disabled rows are simply excluded from analysis.
        /// </summary>
        public static IndexCleanupAnalysisResult Analyze(
            IEnumerable<IndexCleanupIndexInput> indexes,
            IndexCleanupOptions? options = null)
        {
            options ??= new IndexCleanupOptions();

            var analyzable = (indexes ?? Array.Empty<IndexCleanupIndexInput>())
                .Where(IsAnalyzable)
                .Select(src => new WorkIndex(src))
                .ToList();

            // Uptime → the unused-index caveat (< 14 days) and sp_IndexCleanup's auto-dedupe (<= 7 days).
            double? uptimeDays = ResolveUptimeDays(analyzable, options);
            bool uptimeWarning = uptimeDays.HasValue && uptimeDays.Value < 14;
            bool dedupeOnly = options.DedupeOnly || (uptimeDays.HasValue && uptimeDays.Value <= 7);

            var recommendations = new List<IndexCleanupRecommendation>();
            var databaseRollups = new List<IndexCleanupRollup>();

            // Rules operate per (database_id, object_id) scope; rollups aggregate per database.
            foreach (var dbGroup in analyzable
                .GroupBy(w => w.Src.DatabaseId)
                .OrderBy(g => g.First().Src.DatabaseName, StringComparer.Ordinal))
            {
                var perDatabaseAnalyzed = new List<WorkIndex>();

                foreach (var scope in dbGroup
                    .GroupBy(w => w.Src.ObjectId))
                {
                    var scopeIndexes = scope.ToList();

                    // Object-level gates (mirror sp_IndexCleanup's #filtered_objects @min_rows/@min_reads/@min_writes).
                    if (!PassesObjectFilters(scopeIndexes, options))
                    {
                        continue;
                    }

                    ApplyRules(scopeIndexes, options, dedupeOnly, uptimeWarning);
                    perDatabaseAnalyzed.AddRange(scopeIndexes);
                }

                // Compression candidacy is a per-index, data-driven eligibility (independent of the dedupe action).
                foreach (var w in perDatabaseAnalyzed)
                {
                    w.CanCompress =
                        options.ServerSupportsCompression
                        && w.Src.IndexId > 0
                        && string.Equals(w.Src.DataCompressionDesc, CompressionNone, StringComparison.OrdinalIgnoreCase)
                        && w.SizeGb >= options.MinSizeGb;
                }

                recommendations.AddRange(BuildRecommendations(perDatabaseAnalyzed, options));
                databaseRollups.Add(BuildRollup(dbGroup.First().Src.DatabaseName, dbGroup.Key, perDatabaseAnalyzed, options));
            }

            var ordered = recommendations
                .OrderBy(r => r.DatabaseName, StringComparer.Ordinal)
                .ThenBy(r => r.SchemaName, StringComparer.Ordinal)
                .ThenBy(r => r.TableName, StringComparer.Ordinal)
                .ThenBy(r => ResultKindSort(r.ResultKind))
                .ThenBy(r => r.IndexName, StringComparer.Ordinal)
                .ToList();

            return new IndexCleanupAnalysisResult
            {
                Recommendations = ordered,
                DatabaseRollups = databaseRollups,
                OverallRollup = BuildOverallRollup(databaseRollups),
                UptimeWarning = uptimeWarning,
                DedupeOnlyApplied = dedupeOnly,
                Notes = BuildNotes(ordered),
            };
        }

        // ────────────────────────────────────────────────────────────────────────────────────────────
        //  Rule application (per object scope)
        // ────────────────────────────────────────────────────────────────────────────────────────────

        private static void ApplyRules(List<WorkIndex> scope, IndexCleanupOptions options, bool dedupeOnly, bool uptimeWarning)
        {
            if (!dedupeOnly)
            {
                ApplyUnused(scope, uptimeWarning);
            }

            // Auto-consolidating rules (consume-and-mark, in sp_IndexCleanup's order). Reverse Duplicate and
            // Equal Except For Filter are intentionally NOT here — the proc recognizes but does not
            // consolidate them; they are detected non-destructively in DetectReviewRelationships.
            ApplyExactDuplicate(scope);
            ApplyKeySubset(scope);
            ResolveSubsetChains(scope);
            ApplyKeySuperset(scope);
            ApplyKeyDuplicate(scope);

            // Rule 7 family: unique-constraint replacement (MAKE UNIQUE / DROP CONSTRAINT). Runs LAST so it
            // sees the final consolidation state (and, exactly like the proc's Rule 7.5, may override an
            // earlier Unused/Key-Duplicate marking on a nonclustered index whose keys EXACTLY match a
            // unique constraint).
            ApplyUniqueConstraintReplacement(scope);
        }

        /// <summary>
        /// Rule 1: an eligible (nonclustered, non-unique, non-PK/UC/FK-reference) index with no reads at all
        /// is unused → DISABLE. Skipped entirely under dedupe-only. The consolidation label carries the
        /// &lt;14-day-uptime caveat when applicable.
        /// </summary>
        private static void ApplyUnused(List<WorkIndex> scope, bool uptimeWarning)
        {
            foreach (var w in scope)
            {
                if (w.Processed || !w.IsEligibleForDedupe)
                {
                    continue;
                }

                if (w.Src.UserSeeks == 0 && w.Src.UserScans == 0 && w.Src.UserLookups == 0
                    && !w.Src.IsPrimaryKey && !w.Src.IsUniqueConstraint && !w.Src.IsUnique
                    && !w.Src.IsForeignKeyReference   // protection (redundant with is_unique, kept per spec item 3)
                    && w.Src.IndexId != 1)
                {
                    w.ConsolidationRule = uptimeWarning
                        ? IndexCleanupRules.UnusedIndexUptimeWarning
                        : IndexCleanupRules.UnusedIndex;
                    w.Action = IndexCleanupAction.Disable;
                    w.Processed = true;
                }
            }
        }

        /// <summary>Rule 2: identical key string (with direction) + identical includes + identical filter.</summary>
        private static void ApplyExactDuplicate(List<WorkIndex> scope)
        {
            foreach (var group in DedupeCandidates(scope)
                .GroupBy(w => (w.KeyColumns, w.IncludedColumns, w.Filter), KeyIncludeFilterComparer.Instance)
                .Where(g => g.Count() > 1))
            {
                LabelDuplicateGroup(group.ToList(), IndexCleanupRules.ExactDuplicate);
            }
        }

        /// <summary>
        /// Rule 3: a narrower non-unique index whose key string is a leading prefix (at a ", " boundary) of a
        /// wider index's key, with a matching filter, is disabled in favor of the closest (shortest-key)
        /// superset. Direction consistency on the shared prefix is enforced by the string prefix itself.
        /// </summary>
        private static void ApplyKeySubset(List<WorkIndex> scope)
        {
            foreach (var narrow in DedupeCandidates(scope)
                .Where(w => !w.Src.IsUnique)   // never disable a unique narrower via supersession
                .OrderBy(w => w.Name, StringComparer.Ordinal))
            {
                if (narrow.Processed)
                {
                    continue;
                }

                var prefix = narrow.KeyColumns + ", ";
                var target = scope
                    .Where(w => !ReferenceEquals(w, narrow)
                        && !w.Processed
                        && w.IsEligibleForDedupe
                        && IsSafeMergeTarget(w)   // never defer to a PK/UC-backed index: its DROP_EXISTING merge rebuild fails (Msg 1907) while this subset's DISABLE succeeds
                        && string.Equals(w.Filter, narrow.Filter, StringComparison.Ordinal)
                        && w.KeyColumns.StartsWith(prefix, StringComparison.Ordinal))
                    .OrderBy(w => w.KeyColumns.Length)          // closest superset = fewest extra key columns
                    .ThenBy(w => w.Name, StringComparer.Ordinal)
                    .FirstOrDefault();

                if (target != null)
                {
                    narrow.ConsolidationRule = IndexCleanupRules.KeySubset;
                    narrow.Action = IndexCleanupAction.Disable;
                    narrow.TargetIndexName = target.Name;
                    narrow.Processed = true;
                    // The superset is labelled in ApplyKeySuperset (it may absorb several subsets).
                }
            }
        }

        /// <summary>Flattens subset chains (A → B → C becomes A → C) so include-merge reaches the final superset.</summary>
        private static void ResolveSubsetChains(List<WorkIndex> scope)
        {
            var byName = scope.Where(w => w.Name.Length > 0)
                .GroupBy(w => w.Name, StringComparer.Ordinal)
                .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);

            bool changed = true;
            while (changed)
            {
                changed = false;
                foreach (var w in scope)
                {
                    if (w.ConsolidationRule == IndexCleanupRules.KeySubset
                        && w.Action == IndexCleanupAction.Disable
                        && w.TargetIndexName != null
                        && byName.TryGetValue(w.TargetIndexName, out var mid)
                        && mid.ConsolidationRule == IndexCleanupRules.KeySubset
                        && mid.Action == IndexCleanupAction.Disable
                        && mid.TargetIndexName != null
                        && !string.Equals(mid.TargetIndexName, w.TargetIndexName, StringComparison.Ordinal))
                    {
                        w.TargetIndexName = mid.TargetIndexName;
                        changed = true;
                    }
                }
            }
        }

        /// <summary>
        /// Rule 4/6: a wider index that one or more subsets defer to becomes a Key Superset — kept but rebuilt
        /// with the subsets' include columns merged in (excluding any already in its key). Carries the
        /// "Supersedes …" list and the newly-merged (missing) include columns.
        /// </summary>
        private static void ApplyKeySuperset(List<WorkIndex> scope)
        {
            foreach (var superset in scope)
            {
                if (superset.Processed)
                {
                    continue;
                }

                var subsets = scope
                    .Where(s => s.ConsolidationRule == IndexCleanupRules.KeySubset
                        && s.Action == IndexCleanupAction.Disable
                        && string.Equals(s.TargetIndexName, superset.Name, StringComparison.Ordinal))
                    .OrderBy(s => s.Name, StringComparer.Ordinal)
                    .ToList();

                if (subsets.Count == 0)
                {
                    continue;
                }

                var keyNames = new HashSet<string>(superset.KeyNames, StringComparer.Ordinal);
                var ownIncludes = new HashSet<string>(superset.IncludeTokens, StringComparer.Ordinal);

                // Merge = superset's own includes + every subset's includes, minus anything already a key column.
                var merged = new List<string>();
                var seen = new HashSet<string>(StringComparer.Ordinal);
                foreach (var token in superset.IncludeTokens.Concat(subsets.SelectMany(s => s.IncludeTokens)))
                {
                    if (keyNames.Contains(token) || !seen.Add(token))
                    {
                        continue;
                    }
                    merged.Add(token);
                }
                merged.Sort(StringComparer.Ordinal); // deterministic, matches the collector's name-ordered includes

                var missing = merged.Where(t => !ownIncludes.Contains(t)).ToList();

                superset.ConsolidationRule = IndexCleanupRules.KeySuperset;
                superset.Action = IndexCleanupAction.MergeIncludes;
                superset.MergedIncludes = merged;
                superset.MissingIncludedColumns = missing.Count > 0 ? string.Join(", ", missing) : null;
                superset.SupersededBy = "Supersedes " + string.Join(", ", subsets.Select(s => s.Name));
                superset.Processed = true;
            }
        }

        /// <summary>
        /// Rule 5: indexes with the same keys (and filter) but different includes. The keeper (unique first,
        /// then priority, then name) absorbs the losers' includes (MERGE INCLUDES); the losers are disabled.
        /// </summary>
        private static void ApplyKeyDuplicate(List<WorkIndex> scope)
        {
            foreach (var group in DedupeCandidates(scope)
                .GroupBy(w => (w.KeyColumns, w.Filter), KeyFilterComparer.Instance)
                .Where(g => g.Count() > 1))
            {
                var members = group.ToList();
                var keeper = PickKeyDuplicateKeeper(members);
                if (keeper == null)
                {
                    continue;
                }

                var mergedIncludes = new List<string>();
                var seen = new HashSet<string>(StringComparer.Ordinal);
                foreach (var token in members.SelectMany(m => m.IncludeTokens))
                {
                    if (seen.Add(token))
                    {
                        mergedIncludes.Add(token);
                    }
                }
                mergedIncludes.Sort(StringComparer.Ordinal);

                var losers = members.Where(m => !ReferenceEquals(m, keeper)).ToList();
                var disabled = new List<WorkIndex>();
                foreach (var loser in losers)
                {
                    loser.ConsolidationRule = IndexCleanupRules.KeyDuplicate;
                    loser.Processed = true;
                    if (loser.Protected)
                    {
                        continue; // never disable a protected index
                    }
                    loser.Action = IndexCleanupAction.Disable;
                    loser.TargetIndexName = keeper.Name;
                    disabled.Add(loser);
                }

                keeper.ConsolidationRule = IndexCleanupRules.KeyDuplicate;
                keeper.Action = IndexCleanupAction.MergeIncludes;
                keeper.MergedIncludes = mergedIncludes;
                keeper.Processed = true;
                if (disabled.Count > 0)
                {
                    keeper.SupersededBy = "Supersedes " + string.Join(", ", disabled.Select(d => d.Name).OrderBy(n => n, StringComparer.Ordinal));
                }
            }
        }

        /// <summary>
        /// Rule 7 family — Unique Constraint Replacement (sp_IndexCleanup Rules 7, 7.5, 7.5b), reproduced
        /// faithfully against the live proc (verified with <c>@debug = 1</c>):
        /// <list type="bullet">
        /// <item><b>Rule 7</b> (set-match, tentative): an eligible nonclustered index whose KEY-COLUMN SET
        /// equals a unique constraint's (both-direction set equality — extra key columns disqualify) is
        /// tentatively marked MAKE UNIQUE (if not already unique) or KEEP (already unique). A unique
        /// constraint self-matches, so a lone/duplicate constraint is marked KEEP here — the precondition for
        /// Rule 7.5b.</item>
        /// <item><b>Rule 7.5 step 1</b>: a unique constraint that has an EXACT-<c>key_columns</c> (order +
        /// direction) match to a TRUE nonclustered index (not another constraint) is marked DISABLE, pointing
        /// at that index — UNLESS the constraint backs an inbound foreign-key reference (dropping it would
        /// break referential integrity).</item>
        /// <item><b>Rule 7.5 step 2</b>: every true nonclustered index with an EXACT-<c>key_columns</c>
        /// matching constraint is (re)marked MAKE UNIQUE. Unconditional on prior state, so it OVERRIDES an
        /// earlier Unused / Key-Duplicate marking on that index (verified against the proc).</item>
        /// <item><b>Rule 7.5 cleanup</b>: any MAKE UNIQUE index with no disabled constraint pointing back at
        /// it (the constraint was FK-protected, or Rule 7 set-matched a reversed-order index with no exact
        /// match) is reverted to no action — matching the proc, this does NOT restore a prior marking.</item>
        /// <item><b>Rule 7.5b</b>: two or more unique constraints with identical key columns + filter and no
        /// nonclustered index to promote (all left KEEP by Rule 7) collapse to the single deterministic winner
        /// (highest priority, then alphabetically-first name); the rest are demoted to DISABLE (DROP
        /// CONSTRAINT). An FK-referenced constraint is never demoted.</item>
        /// </list>
        /// A MAKE UNIQUE index renders as a UNIQUE <c>DROP_EXISTING</c> rebuild (MERGE bucket); a disabled
        /// constraint renders as <c>DROP CONSTRAINT</c> (<see cref="IndexCleanupResultKind.DisableConstraint"/>).
        /// </summary>
        private static void ApplyUniqueConstraintReplacement(List<WorkIndex> scope)
        {
            const string Ucr = IndexCleanupRules.UniqueConstraintReplacement;

            // Rule 7: tentative set-match marking (both-direction key-column SET equality — direction- and
            // order-independent). A unique constraint self-matches (that is how a lone/duplicate constraint
            // gets KEEP here, the Rule 7.5b precondition). Only still-unprocessed eligible rows are touched.
            foreach (var w in scope)
            {
                if (w.Processed || !w.IsEligibleForDedupe || w.Keys.Count == 0)
                {
                    continue;
                }

                bool hasKeySetMatchingConstraint = scope.Any(u =>
                    u.Src.IsUniqueConstraint && u.KeyNameSet.SetEquals(w.KeyNameSet));

                if (hasKeySetMatchingConstraint)
                {
                    // The MAKE UNIQUE arm only fires for a promotable index (not filtered, not
                    // partitioned, not constraint-backed — see IsPromotableToUnique). An already-unique
                    // row still gets its informational KEEP; a non-promotable non-unique row is left
                    // for the other rules rather than mislabelled as a constraint replacement.
                    if (!w.Src.IsUnique && !IsPromotableToUnique(w))
                    {
                        continue;
                    }

                    w.ConsolidationRule = Ucr;
                    w.Action = w.Src.IsUnique ? IndexCleanupAction.Keep : IndexCleanupAction.MakeUnique;
                    w.Processed = true;
                }
            }

            // Rule 7.5 step 1: mark each unique constraint an EXACT-key nonclustered index can replace for
            // DISABLE (→ DROP CONSTRAINT), pointing at that index. Guards: never drop an FK-referenced
            // constraint (silently breaks referential integrity); the replacement must be a TRUE nonclustered
            // index, not another constraint — otherwise two identical constraints would mutually disable and
            // BOTH get dropped (that case is Rule 7.5b's job).
            foreach (var uc in scope)
            {
                if (!uc.Src.IsUniqueConstraint || uc.Src.IsForeignKeyReference)
                {
                    continue;
                }

                var replacement = scope
                    .Where(n => IsPromotableToUnique(n)   // same gate as step 2, so a constraint is never disabled pointing at an index step 2 will refuse to promote (an orphaned DROP CONSTRAINT = uniqueness silently gone)
                        && !string.Equals(n.Name, uc.Name, StringComparison.Ordinal)
                        && string.Equals(n.KeyColumns, uc.KeyColumns, StringComparison.Ordinal))
                    .OrderBy(n => n.Name, StringComparer.Ordinal)   // deterministic pick if several match
                    .FirstOrDefault();

                if (replacement != null)
                {
                    uc.ConsolidationRule = Ucr;
                    uc.Action = IndexCleanupAction.Disable;
                    uc.TargetIndexName = replacement.Name;
                    uc.Processed = true;
                }
            }

            // Rule 7.5 step 2: (re)mark every true nonclustered index with an EXACT-key matching constraint as
            // MAKE UNIQUE. Unconditional on prior state — deliberately overrides an earlier Unused/Key-Duplicate
            // marking (the proc's UPDATE has no such guard; verified live).
            foreach (var nc in scope)
            {
                // IsPromotableToUnique enforces what this step's comment always promised ("every TRUE
                // nonclustered index"): nonclustered, not constraint-backed (a PK-backed or clustered row
                // here would draw a DROP_EXISTING rebuild that fails Msg 1907), not filtered, not
                // partitioned — mirroring sp_IndexCleanup fixes ae32a4c.
                if (!IsPromotableToUnique(nc))
                {
                    continue;
                }

                bool hasExactMatchingConstraint = scope.Any(u =>
                    u.Src.IsUniqueConstraint
                    && string.Equals(u.KeyColumns, nc.KeyColumns, StringComparison.Ordinal));

                if (hasExactMatchingConstraint)
                {
                    // Only the rule/action/target change — the proc's Rule 7.5 step 2 touches nothing else.
                    // Crucially, a Key-Superset index's absorbed includes (MergedIncludes / SupersededBy) must
                    // SURVIVE into the MAKE UNIQUE rebuild (the proc physically rewrote included_columns in
                    // Rule 4/6, and step 2 never clears it); the Rule 7.5 superseded_by pass below appends to
                    // whatever "Supersedes …" text is already there.
                    nc.ConsolidationRule = Ucr;
                    nc.Action = IndexCleanupAction.MakeUnique;
                    nc.TargetIndexName = null;
                    nc.Processed = true;
                }
            }

            // Rule 7.5 cleanup: revert any MAKE UNIQUE index with no disabled constraint pointing back at it
            // (FK-protected constraint, or Rule 7's set-match promoted a reversed-order index with no exact
            // match). Mirrors the proc: reverts to NO action, WITHOUT restoring any prior marking.
            foreach (var nc in scope)
            {
                if (nc.Action != IndexCleanupAction.MakeUnique)
                {
                    continue;
                }

                bool backedByDisabledConstraint = scope.Any(u =>
                    u.Action == IndexCleanupAction.Disable
                    && string.Equals(u.KeyColumns, nc.KeyColumns, StringComparison.Ordinal)
                    && string.Equals(u.TargetIndexName, nc.Name, StringComparison.Ordinal));

                if (!backedByDisabledConstraint)
                {
                    nc.ConsolidationRule = null;
                    nc.Action = IndexCleanupAction.None;
                    nc.TargetIndexName = null;
                    nc.SupersededBy = null;
                    nc.Processed = false;
                }
            }

            // Rule 7.5 superseded_by: annotate each surviving MAKE UNIQUE index with the constraint(s) it replaces.
            foreach (var nc in scope)
            {
                if (nc.Action != IndexCleanupAction.MakeUnique)
                {
                    continue;
                }

                foreach (var uc in scope
                    .Where(u => u.Action == IndexCleanupAction.Disable
                        && string.Equals(u.TargetIndexName, nc.Name, StringComparison.Ordinal))
                    .OrderBy(u => u.Name, StringComparer.Ordinal))
                {
                    nc.SupersededBy = nc.SupersededBy == null
                        ? "Will replace constraint " + uc.Name
                        : nc.SupersededBy + ", will replace constraint " + uc.Name;
                }
            }

            // Rule 7.5b: collapse duplicate unique constraints with no nonclustered index to promote.
            ApplyDuplicateUniqueConstraints(scope);

            // Rule 7.6: re-disable ordinary Key Duplicates of a MAKE UNIQUE winner (an index sharing the
            // winner's exact keys+filter that step 2 briefly promoted then the cleanup reverted, or one no
            // earlier rule had touched) and fold its includes into the winner.
            ApplyKeyDuplicatesOfMakeUnique(scope);
        }

        /// <summary>
        /// Rule 7.6 (sp_IndexCleanup lines ~4143-4272): after a unique constraint is replaced by a MAKE UNIQUE
        /// index, any OTHER nonclustered index with the winner's exact key columns + filter (and no action yet)
        /// is a plain Key Duplicate of it — DISABLE it and fold its includes into the winner. Without this, an
        /// index that Rule 5 had disabled but Rule 7.5 step 2 promoted-then-reverted would be silently left in
        /// place (a redundant index un-disabled). Emits the <b>Key Duplicate</b> rule, not Unique Constraint
        /// Replacement — it is the same consolidation the proc applies. (Rule 7.6's sibling behavior of folding
        /// includes is reproduced; the winner keeps its own + every folded loser's includes.)
        /// </summary>
        private static void ApplyKeyDuplicatesOfMakeUnique(List<WorkIndex> scope)
        {
            foreach (var winner in scope)
            {
                if (winner.Action != IndexCleanupAction.MakeUnique)
                {
                    continue;
                }

                // Exact key columns + filter match (the proc's key_filter_hash), still-unassigned, non-constraint.
                var losers = scope
                    .Where(d => !d.Processed
                        && d.Action == IndexCleanupAction.None
                        && d.ConsolidationRule == null
                        && !d.Src.IsUniqueConstraint
                        && !ReferenceEquals(d, winner)
                        && string.Equals(d.KeyColumns, winner.KeyColumns, StringComparison.Ordinal)
                        && string.Equals(d.Filter, winner.Filter, StringComparison.Ordinal))
                    .OrderBy(d => d.Name, StringComparer.Ordinal)
                    .ToList();

                if (losers.Count == 0)
                {
                    continue;
                }

                foreach (var loser in losers)
                {
                    loser.ConsolidationRule = IndexCleanupRules.KeyDuplicate;
                    loser.Action = IndexCleanupAction.Disable;
                    loser.TargetIndexName = winner.Name;
                    loser.Processed = true;
                }

                // Fold the losers' includes into the winner (dedup; the winner keeps its own/absorbed includes).
                var merged = new List<string>();
                var seen = new HashSet<string>(StringComparer.Ordinal);
                foreach (var token in (winner.MergedIncludes ?? winner.IncludeTokens).Concat(losers.SelectMany(l => l.IncludeTokens)))
                {
                    if (seen.Add(token))
                    {
                        merged.Add(token);
                    }
                }
                merged.Sort(StringComparer.Ordinal);   // deterministic, matches the collector's name-ordered includes
                winner.MergedIncludes = merged;

                var loserNames = string.Join(", ", losers.Select(l => l.Name));
                winner.SupersededBy = winner.SupersededBy == null
                    ? "Supersedes " + loserNames
                    : winner.SupersededBy + ", " + loserNames;
            }
        }

        /// <summary>
        /// Rule 7.5b: two or more unique constraints with identical key columns + filter, all left KEEP by
        /// Rule 7 (no nonclustered index promoted them). Keep the single deterministic winner (highest
        /// priority, then alphabetically-first name); demote every other to DISABLE (→ DROP CONSTRAINT)
        /// pointing at the winner. An FK-referenced constraint is never demoted. Demotions are computed
        /// against the pre-demotion KEEP set (set-based, like the proc's single UPDATE) so the outcome is
        /// order-independent and every loser targets the same survivor.
        /// </summary>
        private static void ApplyDuplicateUniqueConstraints(List<WorkIndex> scope)
        {
            var keepers = scope
                .Where(w => w.Action == IndexCleanupAction.Keep
                    && w.ConsolidationRule == IndexCleanupRules.UniqueConstraintReplacement
                    && w.Src.IsUniqueConstraint)
                .ToList();

            if (keepers.Count < 2)
            {
                return;
            }

            var demotions = new List<(WorkIndex Loser, WorkIndex Keeper)>();
            foreach (var loser in keepers)
            {
                if (loser.Src.IsForeignKeyReference)
                {
                    continue; // never drop an FK-referenced constraint
                }

                var keeper = keepers.FirstOrDefault(k =>
                    !ReferenceEquals(k, loser)
                    && string.Equals(k.KeyColumns, loser.KeyColumns, StringComparison.Ordinal)
                    && string.Equals(k.Filter, loser.Filter, StringComparison.Ordinal)
                    && LosesTo(loser, k)
                    && IsUndisputedWinner(k, keepers));

                if (keeper != null)
                {
                    demotions.Add((loser, keeper));
                }
            }

            foreach (var (loser, keeper) in demotions)
            {
                loser.Action = IndexCleanupAction.Disable;
                loser.TargetIndexName = keeper.Name;
            }
        }

        /// <summary>The keep/drop tiebreak (mirrors Rule 2): lower priority loses; on a tie the alphabetically-later name loses.</summary>
        private static bool LosesTo(WorkIndex loser, WorkIndex keeper) =>
            loser.Priority < keeper.Priority
            || (loser.Priority == keeper.Priority
                && string.Compare(loser.Name, keeper.Name, StringComparison.Ordinal) > 0);

        /// <summary>True when no sibling duplicate constraint beats <paramref name="keeper"/> — makes the winner (and every loser's target) deterministic when 3+ constraints tie.</summary>
        private static bool IsUndisputedWinner(WorkIndex keeper, List<WorkIndex> keepers) =>
            !keepers.Any(other =>
                !ReferenceEquals(other, keeper)
                && string.Equals(other.KeyColumns, keeper.KeyColumns, StringComparison.Ordinal)
                && string.Equals(other.Filter, keeper.Filter, StringComparison.Ordinal)
                && (other.Priority > keeper.Priority
                    || (other.Priority == keeper.Priority
                        && string.Compare(other.Name, keeper.Name, StringComparison.Ordinal) < 0)));

        // ────────────────────────────────────────────────────────────────────────────────────────────
        //  Keeper / grouping helpers
        // ────────────────────────────────────────────────────────────────────────────────────────────

        /// <summary>Indexes eligible to participate in the string-compare dedupe rules (nonclustered, not a unique constraint, still unprocessed).</summary>
        private static IEnumerable<WorkIndex> DedupeCandidates(List<WorkIndex> scope) =>
            scope.Where(w => !w.Processed && w.IsEligibleForDedupe && !w.Src.IsUniqueConstraint);

        /// <summary>
        /// Whether an index may survive a consolidation as the REBUILT side (Rule 5 keeper / Rule 4-6
        /// superset). A constraint-backed index (PK/UC) cannot be rebuilt with CREATE INDEX ...
        /// DROP_EXISTING (Msg 1907), so choosing one pairs a script that fails with loser DISABLEs that
        /// succeed — the losers' covering columns vanish with nothing absorbing them. Mirrors
        /// sp_IndexCleanup fix ae32a4c ("constraints are now excluded as merge targets"). A plain unique
        /// index (no backing constraint) IS a legal target (fix 0abb3ef).
        /// </summary>
        private static bool IsSafeMergeTarget(WorkIndex w) =>
            !w.Src.IsPrimaryKey && !w.Src.IsUniqueConstraint;

        /// <summary>
        /// Whether an index may be promoted to MAKE UNIQUE as a unique-constraint replacement (Rule 7
        /// family). On top of <see cref="IsSafeMergeTarget"/> (the rebuild is a DROP_EXISTING), the
        /// replacement must be a TRUE nonclustered index (both step comments always said so; the checks
        /// now enforce it), must NOT be filtered (a filtered unique index enforces uniqueness only over
        /// rows matching its predicate — replacing a constraint with one opens a uniqueness hole;
        /// sp_IndexCleanup fix ae32a4c), and must NOT be partitioned: a unique index's partitioning column
        /// must be part of its key (Msg 1908), and Stage 1 does not capture the partitioning column, so
        /// the alignment requirement cannot be verified monitor-side — the proc permits the verified
        /// aligned case; we conservatively skip all partitioned candidates.
        /// </summary>
        private static bool IsPromotableToUnique(WorkIndex w) =>
            w.IsEligibleForDedupe
            && IsSafeMergeTarget(w)
            && w.Filter.Length == 0
            && (w.Src.PartitionCount ?? 1) <= 1;

        /// <summary>
        /// Labels a set of equivalent duplicates: the highest-priority (then lowest-name) member is the
        /// keeper (KEEP); the rest are disabled (unless protected, in which case the disable is skipped —
        /// the protection is a hard guarantee). If the natural keeper is unprotected but a protected member
        /// exists, the protected member is promoted to keeper so nothing protected is ever disabled.
        /// </summary>
        private static void LabelDuplicateGroup(List<WorkIndex> members, string rule)
        {
            var keeper = members
                .OrderByDescending(m => m.Protected)   // never disable a protected index → make it the keeper
                .ThenByDescending(m => m.Priority)
                .ThenBy(m => m.Name, StringComparer.Ordinal)
                .First();

            foreach (var m in members)
            {
                m.ConsolidationRule = rule;
                m.Processed = true;

                if (ReferenceEquals(m, keeper))
                {
                    m.Action = IndexCleanupAction.Keep;
                    continue;
                }

                if (m.Protected)
                {
                    // Two protected members in one duplicate set (rare): keep both, disable neither.
                    m.Action = IndexCleanupAction.Keep;
                    continue;
                }

                m.Action = IndexCleanupAction.Disable;
                m.TargetIndexName = keeper.Name;
            }
        }

        /// <summary>Rule 5 keeper: prefer a unique member, then priority, then name.</summary>
        private static WorkIndex? PickKeyDuplicateKeeper(List<WorkIndex> members) =>
            members
                .Where(IsSafeMergeTarget)   // the keeper is REBUILT (MERGE INCLUDES via DROP_EXISTING) — a PK-backed member can't be (Msg 1907); it stays protected among the losers instead (never disabled)
                .OrderByDescending(m => m.Protected)
                .ThenByDescending(m => m.Src.IsUnique)
                .ThenByDescending(m => m.Priority)
                .ThenBy(m => m.Name, StringComparer.Ordinal)
                .FirstOrDefault();

        /// <summary>
        /// Reverse Duplicate: the same key column SET but a different arrangement (reversed/reordered order
        /// or changed sort directions), with the same includes and filter. NOT an exact duplicate (the key
        /// strings differ). A review signal only — never a disable.
        /// </summary>
        private static bool IsReverseDuplicate(WorkIndex a, WorkIndex b) =>
            a.Keys.Count > 0
            && a.Keys.Count == b.Keys.Count
            && !string.Equals(a.KeyColumns, b.KeyColumns, StringComparison.Ordinal)  // not an exact duplicate
            && a.KeyNameSet.SetEquals(b.KeyNameSet)                                  // same column membership
            && string.Equals(a.IncludedColumns, b.IncludedColumns, StringComparison.Ordinal)
            && string.Equals(a.Filter, b.Filter, StringComparison.Ordinal);

        /// <summary>
        /// Equal Except For Filter: byte-identical keys (with direction) and includes, but a different filter
        /// predicate. A review signal only — never a disable (the filters index different rows).
        /// </summary>
        private static bool IsEqualExceptForFilter(WorkIndex a, WorkIndex b) =>
            string.Equals(a.KeyColumns, b.KeyColumns, StringComparison.Ordinal)
            && string.Equals(a.IncludedColumns, b.IncludedColumns, StringComparison.Ordinal)
            && !string.Equals(a.Filter, b.Filter, StringComparison.Ordinal);

        /// <summary>
        /// Non-destructive detection of Reverse Duplicate and Equal Except For Filter relationships among the
        /// indexes a scope KEPT (action None/Keep). Mirrors sp_IndexCleanup's stance (adversarial tests
        /// 9a/10a): recognize the near-duplicate, but do NOT consolidate it — emit a review row so a human can
        /// decide. One row per participating index per relationship type.
        /// </summary>
        private static IEnumerable<IndexCleanupRecommendation> DetectReviewRelationships(List<WorkIndex> scope)
        {
            var survivors = scope
                .Where(w => w.IsEligibleForDedupe
                    && !w.Src.IsUniqueConstraint
                    && (w.Action == IndexCleanupAction.None || w.Action == IndexCleanupAction.Keep))
                .OrderBy(w => w.Name, StringComparer.Ordinal)
                .ToList();

            for (int i = 0; i < survivors.Count; i++)
            {
                var a = survivors[i];
                var reverse = new List<string>();
                var equalExceptFilter = new List<string>();

                foreach (var b in survivors)
                {
                    if (ReferenceEquals(a, b))
                    {
                        continue;
                    }

                    if (IsReverseDuplicate(a, b))
                    {
                        reverse.Add(b.Name);
                    }
                    else if (IsEqualExceptForFilter(a, b))
                    {
                        equalExceptFilter.Add(b.Name);
                    }
                }

                if (reverse.Count > 0)
                {
                    yield return BuildReview(a, IndexCleanupRules.ReverseDuplicate, reverse,
                        "Same key columns in a different order/direction as: " + string.Join(", ", reverse)
                        + ". A different leading column serves different queries — review whether both are needed (not auto-disabled).");
                }

                if (equalExceptFilter.Count > 0)
                {
                    yield return BuildReview(a, IndexCleanupRules.EqualExceptForFilter, equalExceptFilter,
                        "Identical keys and includes but a different filter than: " + string.Join(", ", equalExceptFilter)
                        + ". The filters index different rows — review whether both are needed (not auto-disabled).");
                }
            }
        }

        // ────────────────────────────────────────────────────────────────────────────────────────────
        //  Recommendation + script assembly
        // ────────────────────────────────────────────────────────────────────────────────────────────

        private static IEnumerable<IndexCleanupRecommendation> BuildRecommendations(List<WorkIndex> analyzed, IndexCleanupOptions options)
        {
            foreach (var w in analyzed)
            {
                if (w.Action == IndexCleanupAction.Disable)
                {
                    // A disabled unique constraint (Rule 7.5 / 7.5b) is a DROP CONSTRAINT, not ALTER INDEX DISABLE.
                    yield return w.Src.IsUniqueConstraint ? BuildDisableConstraint(w) : BuildDisable(w);
                }
                else if (w.Action == IndexCleanupAction.MakeUnique)
                {
                    yield return BuildMakeUnique(w, options);
                }
                else if (w.Action == IndexCleanupAction.MergeIncludes)
                {
                    yield return BuildMerge(w, options);
                }
                else if ((w.Action == IndexCleanupAction.None || w.Action == IndexCleanupAction.Keep) && w.CanCompress)
                {
                    // Only untouched / kept indexes get a compression row (matches sp_IndexCleanup).
                    yield return BuildCompress(w, options);
                }
            }

            // Non-destructive Reverse Duplicate / Equal Except For Filter review rows, per object scope. An
            // index may appear here AND in a compression row — the review flag is additive metadata.
            foreach (var scope in analyzed.GroupBy(w => w.Src.ObjectId))
            {
                foreach (var review in DetectReviewRelationships(scope.ToList()))
                {
                    yield return review;
                }
            }
        }

        private static IndexCleanupRecommendation BuildDisable(WorkIndex w)
        {
            string additionalInfo = w.ConsolidationRule switch
            {
                IndexCleanupRules.KeySubset => "This index is superseded by a wider index: " + (w.TargetIndexName ?? "(unknown)"),
                IndexCleanupRules.ExactDuplicate => "This index is an exact duplicate of: " + (w.TargetIndexName ?? "(unknown)"),
                IndexCleanupRules.KeyDuplicate => "This index has the same keys as: " + (w.TargetIndexName ?? "(unknown)"),
                _ when w.ConsolidationRule != null && w.ConsolidationRule.StartsWith(IndexCleanupRules.UnusedIndex, StringComparison.Ordinal) => w.ConsolidationRule,
                _ => "This index is redundant and will be disabled",
            };

            return NewRecommendation(w) with
            {
                Action = IndexCleanupAction.Disable,
                ResultKind = IndexCleanupResultKind.Disable,
                TargetIndexName = w.TargetIndexName,
                ScriptType = "DISABLE SCRIPT",
                Script = DisableScript(w),
                AdditionalInfo = additionalInfo,
            };
        }

        private static IndexCleanupRecommendation BuildMerge(WorkIndex w, IndexCleanupOptions options)
        {
            // Backstop (mirrors sp_IndexCleanup ae32a4c's script-generator guard): a constraint-backed
            // index cannot be rebuilt via DROP_EXISTING (Msg 1907). The rules exclude these as merge
            // targets; if one ever slips through, emit a no-script review row rather than a failing
            // script paired with loser DISABLEs that succeed.
            if (!IsSafeMergeTarget(w))
            {
                return ConstraintBackedRebuildReview(w);
            }

            string additionalInfo = w.ConsolidationRule == IndexCleanupRules.KeySuperset
                ? "This index will absorb includes from the narrower indexes it supersedes"
                : "This index will absorb includes from duplicate indexes";

            var (script, omitsPlacement) = MergeScript(w, options);

            return NewRecommendation(w) with
            {
                Action = IndexCleanupAction.MergeIncludes,
                ResultKind = IndexCleanupResultKind.Merge,
                SupersededBy = w.SupersededBy,
                MissingIncludedColumns = w.MissingIncludedColumns,
                ScriptType = "MERGE SCRIPT",
                Script = script,
                AdditionalInfo = additionalInfo,
                ScriptOmitsPartitionPlacement = omitsPlacement,
            };
        }

        /// <summary>
        /// Rule 7 / 7.5 MAKE UNIQUE: rebuild the nonclustered index as UNIQUE so it can take over a redundant
        /// unique constraint's enforcement. Rendered like a merge (UNIQUE <c>DROP_EXISTING</c> rebuild); the
        /// constraint itself is dropped by a companion <see cref="BuildDisableConstraint"/> row.
        /// </summary>
        private static IndexCleanupRecommendation BuildMakeUnique(WorkIndex w, IndexCleanupOptions options)
        {
            // Same Msg-1907 backstop as BuildMerge — a MAKE UNIQUE render is also a DROP_EXISTING rebuild.
            if (!IsSafeMergeTarget(w))
            {
                return ConstraintBackedRebuildReview(w);
            }

            var (script, omitsPlacement) = MergeScript(w, options, forceUnique: true);

            return NewRecommendation(w) with
            {
                Action = IndexCleanupAction.MakeUnique,
                ResultKind = IndexCleanupResultKind.Merge,
                SupersededBy = w.SupersededBy,
                ScriptType = "MERGE SCRIPT",
                Script = script,
                AdditionalInfo = "This index will replace a unique constraint",
                ScriptOmitsPartitionPlacement = omitsPlacement,
            };
        }

        /// <summary>
        /// Rule 7.5 / 7.5b DROP CONSTRAINT: a redundant unique constraint replaced by a MAKE UNIQUE index, or a
        /// duplicate constraint collapsed into an identical survivor. Emits <c>ALTER TABLE … DROP CONSTRAINT</c>
        /// (sp_IndexCleanup's <c>DISABLE CONSTRAINT SCRIPT</c>) — a constraint cannot be disabled, only dropped.
        /// </summary>
        private static IndexCleanupRecommendation BuildDisableConstraint(WorkIndex w) =>
            NewRecommendation(w) with
            {
                Action = IndexCleanupAction.Disable,
                ResultKind = IndexCleanupResultKind.DisableConstraint,
                TargetIndexName = w.TargetIndexName,
                ScriptType = "DISABLE CONSTRAINT SCRIPT",
                Script = "ALTER TABLE " + FullName(w) + " DROP CONSTRAINT " + QuoteName(w.Name) + ";",
                AdditionalInfo = "This constraint is being replaced by: " + (w.TargetIndexName ?? "(unknown)"),
            };

        private static IndexCleanupRecommendation BuildCompress(WorkIndex w, IndexCleanupOptions options)
        {
            return NewRecommendation(w) with
            {
                Action = IndexCleanupAction.Keep,
                ResultKind = IndexCleanupResultKind.Compress,
                ConsolidationRule = w.ConsolidationRule,
                ScriptType = "COMPRESSION SCRIPT",
                Script = CompressionScript(w, options),
                AdditionalInfo = "Compression type: PAGE (All Partitions)",
            };
        }

        /// <summary>The Msg-1907 backstop row: keep the constraint-backed index, emit no script.</summary>
        private static IndexCleanupRecommendation ConstraintBackedRebuildReview(WorkIndex w) =>
            NewRecommendation(w) with
            {
                Action = IndexCleanupAction.Keep,
                ResultKind = IndexCleanupResultKind.Review,
                ConsolidationRule = w.ConsolidationRule,
                ScriptType = "REVIEW",
                Script = "",   // a DROP_EXISTING rebuild of a constraint-backed index fails (Msg 1907)
                AdditionalInfo = "Constraint-backed index cannot be rebuilt with DROP_EXISTING (Msg 1907) — review manually",
            };

        private static IndexCleanupRecommendation BuildReview(WorkIndex w, string rule, List<string> siblings, string additionalInfo) =>
            NewRecommendation(w) with
            {
                Action = IndexCleanupAction.Keep,
                ResultKind = IndexCleanupResultKind.Review,
                ConsolidationRule = rule,
                TargetIndexName = siblings.Count > 0 ? siblings[0] : null,
                SupersededBy = "Related to " + string.Join(", ", siblings),
                ScriptType = "REVIEW",
                Script = "",   // informational only — no destructive script
                AdditionalInfo = additionalInfo,
            };

        private static IndexCleanupRecommendation NewRecommendation(WorkIndex w) => new()
        {
            DatabaseName = w.Src.DatabaseName,
            SchemaName = w.Src.SchemaName,
            TableName = w.Src.TableName,
            IndexName = w.Name,
            IndexId = w.Src.IndexId,
            ConsolidationRule = w.ConsolidationRule,
            OriginalIndexDefinition = OriginalIndexDefinition(w),
            IndexSizeGb = w.SizeGb,
            IndexRows = w.Src.TotalRows ?? 0,
            IndexReads = w.Reads,
            IndexWrites = w.Src.UserUpdates,
            CanCompress = w.CanCompress,
            IsForeignKey = w.Src.IsForeignKey,
        };

        private static string DisableScript(WorkIndex w) =>
            "ALTER INDEX " + QuoteName(w.Name) + " ON " + FullName(w) + " DISABLE;";

        private static (string Script, bool OmitsPlacement) MergeScript(WorkIndex w, IndexCleanupOptions options, bool forceUnique = false)
        {
            string includes = string.Join(", ", w.MergedIncludes ?? w.IncludeTokens);

            var script = "CREATE " + ((forceUnique || w.Src.IsUnique) ? "UNIQUE " : "") + "INDEX " + QuoteName(w.Name)
                + " ON " + FullName(w) + " (" + w.KeyColumns + ")"
                + (includes.Length > 0 ? " INCLUDE (" + includes + ")" : "")
                + (w.Filter.Length > 0 ? " WHERE " + w.Filter : "")
                + " WITH (DROP_EXISTING = ON, FILLFACTOR = 100, SORT_IN_TEMPDB = ON, ONLINE = "
                + (options.ServerSupportsOnline ? "ON" : "OFF")
                + (w.CanCompress ? ", DATA_COMPRESSION = PAGE" : "")
                + (w.Src.OptimizeForSequentialKey ? ", OPTIMIZE_FOR_SEQUENTIAL_KEY = ON" : "")
                + ");";

            return (script, IsPartitioned(w));
        }

        private static string CompressionScript(WorkIndex w, IndexCleanupOptions options) =>
            "ALTER INDEX " + QuoteName(w.Name) + " ON " + FullName(w)
            + (IsPartitioned(w) ? " REBUILD PARTITION = ALL" : " REBUILD")
            + " WITH (FILLFACTOR = 100, SORT_IN_TEMPDB = ON, ONLINE = "
            + (options.ServerSupportsOnline ? "ON" : "OFF")
            + ", DATA_COMPRESSION = PAGE"
            + (w.Src.OptimizeForSequentialKey ? ", OPTIMIZE_FOR_SEQUENTIAL_KEY = ON" : "")
            + ");";

        /// <summary>
        /// Reconstructs the original CREATE (or ADD CONSTRAINT) from captured metadata — the rollback /
        /// validation reference. Uses the captured delimited key/include strings verbatim. The trailing
        /// <c>ON &lt;filegroup/partition_scheme&gt;</c> is omitted (Stage 1 did not capture it).
        /// </summary>
        private static string OriginalIndexDefinition(WorkIndex w)
        {
            if (w.Src.IsUniqueConstraint)
            {
                return "ALTER TABLE " + FullName(w) + " ADD CONSTRAINT " + QuoteName(w.Name)
                    + " UNIQUE (" + w.KeyColumns + ");";
            }

            string clustering = w.Src.IndexId == 1 ? "CLUSTERED " : "NONCLUSTERED ";
            return "CREATE " + (w.Src.IsUnique ? "UNIQUE " : "") + clustering + "INDEX " + QuoteName(w.Name)
                + " ON " + FullName(w) + " (" + w.KeyColumns + ")"
                + (w.IncludedColumns.Length > 0 ? " INCLUDE (" + w.IncludedColumns + ")" : "")
                + (w.Filter.Length > 0 ? " WHERE " + w.Filter : "")
                + ";";
        }

        private static bool IsPartitioned(WorkIndex w) => (w.Src.PartitionCount ?? 1) > 1;

        private static string FullName(WorkIndex w) =>
            QuoteName(w.Src.DatabaseName) + "." + QuoteName(w.Src.SchemaName) + "." + QuoteName(w.Src.TableName);

        /// <summary>QUOTENAME: wrap in brackets, doubling any embedded closing bracket.</summary>
        private static string QuoteName(string name) =>
            "[" + (name ?? "").Replace("]", "]]", StringComparison.Ordinal) + "]";

        // ────────────────────────────────────────────────────────────────────────────────────────────
        //  Rollups + notes
        // ────────────────────────────────────────────────────────────────────────────────────────────

        private static IndexCleanupRollup BuildRollup(string databaseName, int databaseId, List<WorkIndex> analyzed, IndexCleanupOptions options)
        {
            decimal unusedSize = 0, compMin = 0, compMax = 0;
            int disable = 0, merge = 0, compressable = 0, unused = 0;
            decimal totalSize = 0;

            // Workload-impact aggregates: straight SUMs over EVERY analyzed index (mirrors sp_IndexCleanup's
            // #index_reporting_stats DATABASE roll-up of analyzed_indexes — reads/writes from usage stats,
            // lock/latch from dm_db_index_operational_stats).
            long userSeeks = 0, userScans = 0, userLookups = 0, totalWrites = 0;
            long lockWaitCount = 0, lockWaitInMs = 0, latchWaitCount = 0, latchWaitInMs = 0;

            foreach (var w in analyzed)
            {
                totalSize += w.SizeGb;

                userSeeks += w.Src.UserSeeks;
                userScans += w.Src.UserScans;
                userLookups += w.Src.UserLookups;
                totalWrites += w.Src.UserUpdates;
                lockWaitCount += w.Src.RowLockWaitCount + w.Src.PageLockWaitCount;
                lockWaitInMs += w.Src.RowLockWaitInMs + w.Src.PageLockWaitInMs;
                latchWaitCount += w.Src.PageLatchWaitCount + w.Src.PageIoLatchWaitCount;
                latchWaitInMs += w.Src.PageLatchWaitInMs + w.Src.PageIoLatchWaitInMs;

                if (w.Action == IndexCleanupAction.Disable)
                {
                    disable++;
                    unusedSize += w.SizeGb;
                    if (w.ConsolidationRule != null && w.ConsolidationRule.StartsWith(IndexCleanupRules.UnusedIndex, StringComparison.Ordinal))
                    {
                        unused++;
                    }
                }
                else if (w.Action == IndexCleanupAction.MergeIncludes || w.Action == IndexCleanupAction.MakeUnique)
                {
                    // MAKE UNIQUE is a kept-and-rebuilt index (sp_IndexCleanup's MERGE bucket), not a reclaim.
                    merge++;
                }

                if (w.CanCompress)
                {
                    compressable++;
                    if (w.Action == IndexCleanupAction.None || w.Action == IndexCleanupAction.Keep)
                    {
                        compMin += w.SizeGb * options.CompressionMinSavingsFactor;
                        compMax += w.SizeGb * options.CompressionMaxSavingsFactor;
                    }
                }
            }

            // Table row count = the base row (index_id 0/1) per object, summed across objects.
            long totalRows = analyzed
                .Where(w => w.Src.IndexId <= 1)
                .GroupBy(w => w.Src.ObjectId)
                .Sum(g => g.First().Src.TotalRows ?? 0);

            return new IndexCleanupRollup
            {
                DatabaseName = databaseName,
                DatabaseId = databaseId,
                TablesAnalyzed = analyzed.Select(w => w.Src.ObjectId).Distinct().Count(),
                IndexCount = analyzed.Count,
                TotalSizeGb = totalSize,
                TotalRows = totalRows,
                IndexesToDisable = disable,
                IndexesToMerge = merge,
                CompressableIndexes = compressable,
                UnusedIndexes = unused,
                UnusedSizeGb = unusedSize,
                CompressionMinSavingsGb = compMin,
                CompressionMaxSavingsGb = compMax,
                TotalMinSavingsGb = unusedSize + compMin,
                TotalMaxSavingsGb = unusedSize + compMax,
                UserSeeks = userSeeks,
                UserScans = userScans,
                UserLookups = userLookups,
                TotalWrites = totalWrites,
                LockWaitCount = lockWaitCount,
                LockWaitInMs = lockWaitInMs,
                LatchWaitCount = latchWaitCount,
                LatchWaitInMs = latchWaitInMs,
            };
        }

        private static IndexCleanupRollup BuildOverallRollup(List<IndexCleanupRollup> perDb) => new()
        {
            DatabaseName = null,
            TablesAnalyzed = perDb.Sum(r => r.TablesAnalyzed),
            IndexCount = perDb.Sum(r => r.IndexCount),
            TotalSizeGb = perDb.Sum(r => r.TotalSizeGb),
            TotalRows = perDb.Sum(r => r.TotalRows),
            IndexesToDisable = perDb.Sum(r => r.IndexesToDisable),
            IndexesToMerge = perDb.Sum(r => r.IndexesToMerge),
            CompressableIndexes = perDb.Sum(r => r.CompressableIndexes),
            UnusedIndexes = perDb.Sum(r => r.UnusedIndexes),
            UnusedSizeGb = perDb.Sum(r => r.UnusedSizeGb),
            CompressionMinSavingsGb = perDb.Sum(r => r.CompressionMinSavingsGb),
            CompressionMaxSavingsGb = perDb.Sum(r => r.CompressionMaxSavingsGb),
            TotalMinSavingsGb = perDb.Sum(r => r.TotalMinSavingsGb),
            TotalMaxSavingsGb = perDb.Sum(r => r.TotalMaxSavingsGb),
            // Workload aggregates roll up too (DATABASE = SUM(TABLE), SUMMARY = SUM(DATABASE)); the rollup-row
            // projection renders them 'N/A' for the overall row, matching sp_IndexCleanup's SUMMARY output.
            UserSeeks = perDb.Sum(r => r.UserSeeks),
            UserScans = perDb.Sum(r => r.UserScans),
            UserLookups = perDb.Sum(r => r.UserLookups),
            TotalWrites = perDb.Sum(r => r.TotalWrites),
            LockWaitCount = perDb.Sum(r => r.LockWaitCount),
            LockWaitInMs = perDb.Sum(r => r.LockWaitInMs),
            LatchWaitCount = perDb.Sum(r => r.LatchWaitCount),
            LatchWaitInMs = perDb.Sum(r => r.LatchWaitInMs),
        };

        private static List<string> BuildNotes(List<IndexCleanupRecommendation> recommendations)
        {
            var notes = new List<string>();

            if (recommendations.Any(r => r.ScriptOmitsPartitionPlacement))
            {
                notes.Add("Reconstructed CREATE/MERGE scripts omit the trailing ON <filegroup/partition_scheme> clause: "
                    + "Stage 1 did not capture filegroup/partition placement. For a partitioned index the rebuild would "
                    + "default to the table's scheme mapping; verify placement before running.");
            }

            if (recommendations.Any(r => r.ResultKind == IndexCleanupResultKind.Compress))
            {
                notes.Add("Compression eligibility cannot exclude tables with sparse columns or legacy LOB types "
                    + "(text/ntext/image): Stage 1 did not capture per-column type/sparse metadata, so a small number of "
                    + "compression candidates may be ineligible in practice (sp_IndexCleanup excludes them via a live column scan).");
                notes.Add("Compression savings use sp_IndexCleanup's general 0.20–0.60 × size band, NOT "
                    + "sp_estimate_data_compression_savings (a live-sampling proc, out of scope for data-driven analysis).");
                notes.Add("COMPRESSION REBUILD uses PARTITION = ALL when partition_count > 1 (a proxy; Stage 1 did not "
                    + "capture the partition scheme name).");
            }

            notes.Add("Reverse Duplicate and Equal Except For Filter are recognized and surfaced as REVIEW rows but "
                + "never auto-disabled — matching sp_IndexCleanup's tested behavior (its adversarial tests 9a/10a assert "
                + "reversed-order and filter-differing indexes are left alone; a different leading column or filter serves "
                + "different queries/rows).");

            if (recommendations.Any(r => r.ConsolidationRule == IndexCleanupRules.UniqueConstraintReplacement))
            {
                notes.Add("Unique Constraint Replacement (sp_IndexCleanup Rule 7/7.5/7.5b) reproduces the proc's LITERAL "
                    + "behavior, verified against a live @debug=1 run: a nonclustered index whose key_columns EXACTLY match "
                    + "a unique constraint is made unique (MERGE SCRIPT) and the constraint is dropped (DISABLE CONSTRAINT "
                    + "SCRIPT). This fires even when the index is ALREADY unique — the proc's Rule 7.5 step 2 has no is_unique "
                    + "guard, so it overrides Rule 7's KEEP with MAKE UNIQUE (a redundant-but-harmless UNIQUE rebuild that "
                    + "still drops the constraint). A constraint backing an inbound foreign-key reference is never dropped, "
                    + "and an index whose key columns are a SUPERSET of the constraint's is not promoted.");
            }

            notes.Add("Out of scope of the Unique Constraint Replacement port: sp_IndexCleanup's 'Same Keys Different Order' "
                + "REVIEW case, largely covered by the Reverse Duplicate review rows above and reproducible from the captured "
                + "metadata if later required.");

            return notes;
        }

        // ────────────────────────────────────────────────────────────────────────────────────────────
        //  Input shaping
        // ────────────────────────────────────────────────────────────────────────────────────────────

        /// <summary>Only rowstore CLUSTERED/NONCLUSTERED, enabled, named indexes are analyzed (mirrors <c>i.type IN (1,2) AND i.is_disabled = 0</c>).</summary>
        private static bool IsAnalyzable(IndexCleanupIndexInput src)
        {
            if (src == null || src.IsDisabled || string.IsNullOrEmpty(src.IndexName))
            {
                return false;
            }

            return string.Equals(src.IndexTypeDesc, Clustered, StringComparison.OrdinalIgnoreCase)
                || string.Equals(src.IndexTypeDesc, NonClustered, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>Object-level gates: @min_rows always; @min_reads/@min_writes only when the caller set them (&gt; 0), matching sp_IndexCleanup.</summary>
        private static bool PassesObjectFilters(List<WorkIndex> scopeIndexes, IndexCleanupOptions options)
        {
            long objectRows = scopeIndexes
                .Where(w => w.Src.IndexId <= 1)
                .Select(w => w.Src.TotalRows ?? 0)
                .DefaultIfEmpty(scopeIndexes.Max(w => w.Src.TotalRows ?? 0))
                .First();

            if (objectRows < options.MinRows)
            {
                return false;
            }

            if (options.MinReads > 0 || options.MinWrites > 0)
            {
                long reads = scopeIndexes.Sum(w => w.Reads);
                long writes = scopeIndexes.Sum(w => w.Src.UserUpdates);

                // Each term is gated on ITS OWN threshold being set (sp_IndexCleanup fix ae32a4c):
                // without the per-term gate, a caller who set only MinReads still had `writes >= 0` as
                // the other half of the keep-condition — always true — so the whole filter did nothing.
                // The OR itself is intended: an object counts as used if it clears EITHER set floor.
                bool used =
                    (options.MinReads > 0 && reads >= options.MinReads)
                    || (options.MinWrites > 0 && writes >= options.MinWrites);
                if (!used)
                {
                    return false;
                }
            }

            return true;
        }

        private static double? ResolveUptimeDays(List<WorkIndex> analyzable, IndexCleanupOptions options)
        {
            if (options.ServerUptimeDays.HasValue)
            {
                return options.ServerUptimeDays;
            }

            DateTime? start = analyzable
                .Select(w => w.Src.SqlServerStartTime)
                .Where(t => t.HasValue)
                .DefaultIfEmpty(null)
                .Max();

            if (!start.HasValue)
            {
                return null;
            }

            var reference = options.ReferenceTimeUtc ?? DateTime.UtcNow;
            var days = (reference - start.Value).TotalDays;
            return days < 0 ? null : days;
        }

        private static int ResultKindSort(IndexCleanupResultKind kind) => kind switch
        {
            IndexCleanupResultKind.Merge => 0,               // includes MAKE UNIQUE (proc's MERGE bucket)
            IndexCleanupResultKind.Disable => 1,
            IndexCleanupResultKind.DisableConstraint => 2,   // proc orders CONSTRAINT after DISABLE
            IndexCleanupResultKind.Compress => 3,
            IndexCleanupResultKind.Review => 4,
            _ => 5,
        };

        // ────────────────────────────────────────────────────────────────────────────────────────────
        //  Working state + delimited-string parsing
        // ────────────────────────────────────────────────────────────────────────────────────────────

        /// <summary>Mutable per-index state carried through the sequential rule passes.</summary>
        private sealed class WorkIndex
        {
            public WorkIndex(IndexCleanupIndexInput src)
            {
                Src = src;
                Name = src.IndexName ?? "";
                KeyColumns = src.KeyColumns ?? "";
                IncludedColumns = src.IncludedColumns ?? "";
                Filter = src.FilterDefinition ?? "";
                Keys = ParseKeyTokens(KeyColumns);
                KeyNames = Keys.Select(k => k.Name).ToList();
                KeyNameSet = new HashSet<string>(KeyNames, StringComparer.Ordinal);
                IncludeTokens = SplitDelimited(IncludedColumns);

                // is_eligible_for_dedupe: nonclustered → yes; clustered → no (mirrors sp_IndexCleanup's CASE,
                // which checks type = 2 before the clustered/PK branch).
                IsEligibleForDedupe = string.Equals(src.IndexTypeDesc, NonClustered, StringComparison.OrdinalIgnoreCase);

                Priority = ComputePriority(src);
                SizeGb = (src.ReservedMb ?? 0m) / 1024m;
            }

            public IndexCleanupIndexInput Src { get; }
            public string Name { get; }
            public string KeyColumns { get; }
            public string IncludedColumns { get; }
            public string Filter { get; }
            public List<KeyColumn> Keys { get; }
            public List<string> KeyNames { get; }
            public HashSet<string> KeyNameSet { get; }
            public List<string> IncludeTokens { get; }
            public bool IsEligibleForDedupe { get; }
            public int Priority { get; }
            public decimal SizeGb { get; }

            public long Reads => Src.UserSeeks + Src.UserScans + Src.UserLookups;

            /// <summary>Hard disable protection: PK, unique constraint, or FK-referenced (matches spec item 3).</summary>
            public bool Protected => Src.IsPrimaryKey || Src.IsUniqueConstraint || Src.IsForeignKeyReference;

            // Mutable rule state
            public bool Processed { get; set; }
            public string? ConsolidationRule { get; set; }
            public IndexCleanupAction Action { get; set; } = IndexCleanupAction.None;
            public string? TargetIndexName { get; set; }
            public string? SupersededBy { get; set; }
            public List<string>? MergedIncludes { get; set; }
            public string? MissingIncludedColumns { get; set; }
            public bool CanCompress { get; set; }
        }

        private readonly record struct KeyColumn(string Name, bool Descending);

        /// <summary>sp_IndexCleanup priority: clustered 1000; unique 500 (unique constraint only 50); seeks 200; scans 100; has includes 50.</summary>
        private static int ComputePriority(IndexCleanupIndexInput src)
        {
            int priority = src.IndexId == 1 ? 1000 : 0;

            if (src.IsUnique)
            {
                priority += src.IsUniqueConstraint ? 50 : 500;
            }

            if (src.UserSeeks > 0)
            {
                priority += 200;
            }

            if (src.UserScans > 0)
            {
                priority += 100;
            }

            if (!string.IsNullOrEmpty(src.IncludedColumns))
            {
                priority += 50;
            }

            return priority;
        }

        /// <summary>Parses a delimited key list into (bracketed-name, descending) tokens, respecting QUOTENAME bracketing.</summary>
        private static List<KeyColumn> ParseKeyTokens(string raw)
        {
            var result = new List<KeyColumn>();
            foreach (var token in SplitDelimited(raw))
            {
                if (token.EndsWith(" DESC", StringComparison.Ordinal))
                {
                    result.Add(new KeyColumn(token[..^5].TrimEnd(), true));
                }
                else
                {
                    result.Add(new KeyColumn(token, false));
                }
            }
            return result;
        }

        /// <summary>
        /// Splits a QUOTENAME-delimited list on the ", " separator that sits between tokens, ignoring commas
        /// and spaces inside <c>[ ... ]</c> (where <c>]]</c> is an escaped bracket). Robust to column names
        /// that themselves contain commas, spaces, or brackets.
        /// </summary>
        private static List<string> SplitDelimited(string? raw)
        {
            var tokens = new List<string>();
            if (string.IsNullOrEmpty(raw))
            {
                return tokens;
            }

            int n = raw.Length;
            int start = 0;
            int i = 0;
            bool inBracket = false;

            while (i < n)
            {
                char c = raw[i];
                if (!inBracket)
                {
                    if (c == '[')
                    {
                        inBracket = true;
                        i++;
                    }
                    else if (c == ',' && i + 1 < n && raw[i + 1] == ' ')
                    {
                        AddToken(tokens, raw, start, i);
                        i += 2;
                        start = i;
                    }
                    else
                    {
                        i++;
                    }
                }
                else
                {
                    if (c == ']')
                    {
                        if (i + 1 < n && raw[i + 1] == ']')
                        {
                            i += 2; // escaped ]] inside a name
                        }
                        else
                        {
                            inBracket = false;
                            i++;
                        }
                    }
                    else
                    {
                        i++;
                    }
                }
            }

            AddToken(tokens, raw, start, n);
            return tokens;
        }

        private static void AddToken(List<string> tokens, string raw, int start, int end)
        {
            var token = raw.Substring(start, end - start).Trim();
            if (token.Length > 0)
            {
                tokens.Add(token);
            }
        }

        // Composite grouping-key comparers over the raw delimited strings (byte-for-byte, ordinal).

        private sealed class KeyIncludeFilterComparer : IEqualityComparer<(string Key, string Include, string Filter)>
        {
            public static readonly KeyIncludeFilterComparer Instance = new();
            public bool Equals((string Key, string Include, string Filter) x, (string Key, string Include, string Filter) y) =>
                string.Equals(x.Key, y.Key, StringComparison.Ordinal)
                && string.Equals(x.Include, y.Include, StringComparison.Ordinal)
                && string.Equals(x.Filter, y.Filter, StringComparison.Ordinal);
            public int GetHashCode((string Key, string Include, string Filter) obj) =>
                HashCode.Combine(obj.Key, obj.Include, obj.Filter);
        }

        private sealed class KeyFilterComparer : IEqualityComparer<(string Key, string Filter)>
        {
            public static readonly KeyFilterComparer Instance = new();
            public bool Equals((string Key, string Filter) x, (string Key, string Filter) y) =>
                string.Equals(x.Key, y.Key, StringComparison.Ordinal)
                && string.Equals(x.Filter, y.Filter, StringComparison.Ordinal);
            public int GetHashCode((string Key, string Filter) obj) =>
                HashCode.Combine(obj.Key, obj.Filter);
        }
    }
}
