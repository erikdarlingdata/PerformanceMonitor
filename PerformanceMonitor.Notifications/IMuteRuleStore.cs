/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Persistence boundary for alert mute rules. The shared <c>MuteRuleService</c>
/// keeps the in-memory cache + matching; this store does persistence only.
/// Implemented per-app:
///   Lite      → DuckDB <c>config_mute_rules</c> (async row-level CRUD).
///   Dashboard → <c>alert_mute_rules.json</c> (synchronous whole-file rewrite,
///               wrapped in completed tasks).
/// Insert/Update are kept SEPARATE (not a single Upsert) to preserve Lite's
/// exact SQL — INSERT throws on a duplicate id, UPDATE is a narrow update.
///
/// <para><b>Every member THROWS on a store failure, reads included.</b> An empty result from
/// <see cref="LoadAllAsync"/> means the store holds no rules, and nothing else. A store that swallowed a
/// failed read and returned empty made those two answers one value, and
/// <see cref="MuteRuleService.LoadAsync"/> then replaced a live cache with it — so a store blip un-muted
/// every rule an operator had in force, which is the outcome a permanent rule is chosen to avoid. The
/// service's reload skips its cache assignment when the read throws, so the previously-loaded set stays
/// in force; each caller decides how loudly to report the event.</para>
/// </summary>
public interface IMuteRuleStore
{
    /// <summary>Loads every persisted rule. Lite: SELECT … ORDER BY created_at_utc DESC. Dash: the already-loaded set.
    /// An empty list means the store holds no rules; a failed read throws rather than rendering as one.</summary>
    Task<IReadOnlyList<MuteRule>> LoadAllAsync();

    /// <summary>Persists a new rule. Lite: INSERT one row. Dash: add + rewrite file.</summary>
    Task InsertAsync(MuteRule rule);

    /// <summary>Updates an existing rule's fields. Lite: UPDATE row. Dash: replace + rewrite.</summary>
    Task UpdateAsync(MuteRule rule);

    /// <summary>Toggles a rule's enabled flag. Lite: narrow UPDATE enabled. Dash: mutate + rewrite.</summary>
    Task SetEnabledAsync(string ruleId, bool enabled);

    /// <summary>Deletes a rule by id. Lite: DELETE row. Dash: remove + rewrite.</summary>
    Task DeleteAsync(string ruleId);

    /// <summary>Deletes the given expired rules. Lite: DELETE per id. Dash: removeAll + single rewrite.</summary>
    Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds);
}
