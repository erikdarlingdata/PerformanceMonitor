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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one definition of "an expired mute rule" and of the delete that removes it (#6399). The Viewer's Mute Rules "Purge Expired"
/// button and the Darling MCP <c>purge_expired_mute_rules</c> tool both pick the rules to purge with <see cref="SelectExpired"/> and
/// delete each with <see cref="DeleteByIdSql"/> (which <c>PgMuteRuleStore</c> also runs for a single delete), so the two surfaces
/// cannot disagree on what is expired or how it goes.
/// </summary>
public static class MuteRulePurge
{
    /// <summary>The statement that deletes one mute rule by id ($1).</summary>
    public const string DeleteByIdSql = "DELETE FROM config_mute_rules WHERE id = $1";

    /// <summary>
    /// The rules that are expired at <paramref name="nowUtc"/>: <see cref="MuteRule.IsExpiredAt"/> (an end time at or before now).
    /// A rule with no end time never expires, and a disabled rule that has passed its end still counts, as it does in the Viewer.
    /// </summary>
    public static IReadOnlyList<MuteRule> SelectExpired(IEnumerable<MuteRule> rules, DateTime nowUtc) =>
        rules.Where(r => r is not null && r.IsExpiredAt(nowUtc)).ToList();
}
