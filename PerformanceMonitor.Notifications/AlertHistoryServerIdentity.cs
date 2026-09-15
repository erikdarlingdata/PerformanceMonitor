/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// How a string server key maps onto <c>config_alert_log</c>'s integer <c>server_id</c> column — the write
/// half and the read half in one place, because #3456 was those two halves composing into a defect neither
/// contains alone. Shared by Darling's <c>PgAlertHistoryStore</c> and Lite's
/// <c>DuckDbAlertHistoryStore</c> rather than written out twice, for the reason
/// <see cref="AlertValueParser.ResolveStoredValue"/> is: two stores answering an identity question with
/// separately-maintained parses is the drift risk, and this question is the one #3456 turned on.
///
/// <para><b>The defect this type exists to pin.</b> Most server keys are the deterministic storage-name
/// hash rendered as a string, which parses back to the column's integer losslessly. The self-alert family's
/// are not — <c>cost:&lt;id&gt;:&lt;collector&gt;</c>, the Availability Group composites, the fixed
/// sentinels — and both stores answered that with <c>int.TryParse(...) ? sid : 0</c> on BOTH sides of the
/// table. On the write side that is a tolerable collapse: the row must be recorded somewhere, the column is
/// an integer, and zero is where every non-integer key lands. On the read side it was the bug: the cooldown
/// seed asked "when did THIS key last deliver?" and got back <c>MAX(alert_time)</c> over EVERY collapsed
/// key's rows — the fleet-wide last send of the metric — so one server's delivery throttled every
/// sibling's, first notices included, which is the pre-#1154 defect reintroduced one layer down.</para>
///
/// <para><b>The invariant: the seed never answers one key's question with another key's history.</b>
/// <see cref="SeedScope"/> therefore declines (<c>null</c>) any key whose rows are not distinguishable by
/// <c>server_id</c> alone — the unparseable keys, and the literal id 0, because zero is the bucket the
/// write side collapses every unparseable key INTO and so holds a mixture no per-key question can be
/// answered from. Write-only, never read: even a key's OWN rows are unrecoverable from that bucket, since
/// nothing in the row says which collapsed key wrote it. A declined seed means the cooldown treats the key
/// as a first notice and posts — the alert-history doctrine's direction (see
/// <see cref="RepeatDeliveryBudget"/>'s remarks): the cost of failing this way is a post, the cost of the
/// old way was an unannounced incident, twice measured live (#3456's 09:22:20Z and 10:22:27Z pairs, 3.7 ms
/// apart, the second rendering <c>throttled</c> against the first's just-written row).</para>
///
/// <para><b>What declining costs, stated plainly.</b> The self-alert family loses cross-restart cooldown
/// continuity: with no seed, the in-memory stamp governs within the process lifetime and a restart (or the
/// ~two-window in-memory eviction) reads the next fire as a first notice. That is the pre-seed (#1145)
/// behavior for these keys, and it also keeps #3436's roster from engaging for recurrences slower than the
/// eviction window — a first notice is exempt from folding by #1154's rule. Restoring continuity requires
/// an identity that survives the integer column (persisting the string key, or hashing it into the column
/// on both sides), which is a schema-shaped follow-up, not a parse fix; until it lands,
/// <see cref="StorageId"/> is the one place the write-side collapse lives, so that follow-up changes one
/// line per direction.</para>
/// </summary>
public static class AlertHistoryServerIdentity
{
    /// <summary>
    /// The <c>server_id</c> a history row is WRITTEN with: the key's own integer when it is one, else 0 —
    /// the collapse bucket. Unchanged behavior from the parse both stores carried inline; centralized so
    /// the write half and <see cref="SeedScope"/>'s read half cannot drift apart, and so the rows the
    /// bucket absorbs are demonstrably the same ones the seed refuses to read.
    /// </summary>
    public static int StorageId(string serverId) =>
        int.TryParse(serverId, NumberStyles.Integer, CultureInfo.InvariantCulture, out var sid) ? sid : 0;

    /// <summary>
    /// The <c>server_id</c> a cooldown seed may READ, or <c>null</c> when the question cannot be answered
    /// per-key — an unparseable key, or the literal 0 the write side collapses those keys into. Callers
    /// treat <c>null</c> as "no seed", which reads as a first notice and fails toward posting.
    /// <para>Zero is excluded even for a caller whose key genuinely IS "0": the bucket's rows are
    /// indistinguishable from every collapsed key's, so an answer from it is exactly the cross-key history
    /// the invariant forbids. A genuine id of 0 (one specific FNV output) costs that server its restart
    /// continuity, which is a post; including it would cost the self-alert family correctness, which was
    /// #3456.</para>
    /// </summary>
    public static int? SeedScope(string serverId) =>
        int.TryParse(serverId, NumberStyles.Integer, CultureInfo.InvariantCulture, out var sid) && sid != 0
            ? sid
            : null;
}
