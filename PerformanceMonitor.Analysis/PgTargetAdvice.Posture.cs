/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the durability posture facts (filled by lane 8, #3542 step 8). Durability only: what the setting
/// protects against, what is lost while it is off, and that the choice is the operator's policy. None of the
/// optimisation vocabulary — the five words <c>PgTargetPostureIsolationTests</c> lists — appears anywhere in this
/// file, comments included; that test pins it over the source text (D6). The block is the trade statement and the change mechanism; there
/// is no "consider turning it off" arm to write, and review rejects any PR that adds one.
///
/// <para><b>Value-stated, or nothing.</b> A posture block without its fact cannot say what the setting IS, whether
/// the platform owns it, or how old the snapshot is — and an Aurora-managed <c>fsync</c> rendered from a
/// value-free template would read as an operator's choice. So <see cref="ComposePosture"/> returns
/// <c>null</c> when the fact is absent, which also makes <see cref="PgTargetAdvice.Static"/> <c>null</c> for
/// these keys: the story text is frozen at analysis time from the composed block, and a finding row without
/// frozen text has no honest posture sentence to fall back to.</para>
///
/// <para><b>Change mechanism per setting, from <c>pg_settings.context</c> carried on the fact.</b> <c>fsync</c>
/// and <c>full_page_writes</c> are <c>sighup</c>: <c>ALTER SYSTEM</c> then <c>pg_reload_conf()</c>, no restart.
/// <c>synchronous_commit</c> is <c>user</c>: the server value is a default any session may override, so the
/// block says so and that a reload reaches new sessions only. The fact's <c>requires_restart</c> and
/// <c>pending_restart</c> are read rather than assumed, so a future major that moves a context changes the
/// sentence without a code change.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static partial AdviceBlock? ComposePosture(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (!factsByKey.TryGetValue(key, out var fact))
            return null;

        var settingName = key switch
        {
            PgTargetFactKeys.PostureFsync => "fsync",
            PgTargetFactKeys.PostureFullPageWrites => "full_page_writes",
            PgTargetFactKeys.PostureSynchronousCommit => "synchronous_commit",
            _ => null,
        };
        if (settingName is null)
            return null;

        var snapshot = SnapshotClause(fact);

        if (fact.Metadata.GetValueOrDefault("managed_by_platform") == 1)
        {
            return new AdviceBlock(
                Headline:
                    $"{settingName} is managed by Aurora — reported for posture, not graded",
                Investigation:
                    $"The configuration snapshot{snapshot} reports {settingName} = {(fact.Value == 1 ? "off" : "on")}. On Aurora PostgreSQL the storage layer owns durability for this setting and the parameter group does not expose it, so the value is the platform's, not this server's operator's, and no finding is raised on it.",
                Remediation:
                    "Nothing to change here. Durability on Aurora is a property of the storage service; the one durability setting the operator does own on Aurora is synchronous_commit, which has its own posture fact.");
        }

        if (fact.Value != 1)
        {
            return new AdviceBlock(
                Headline:
                    $"{settingName} is on — the PostgreSQL default",
                Investigation:
                    $"The configuration snapshot{snapshot} reports {settingName} = on, which is the shipped default and the durable setting. This is a statement of posture, not a finding.",
                Remediation:
                    "No action.");
        }

        return key switch
        {
            PgTargetFactKeys.PostureFsync => ComposeFsyncOff(fact, snapshot),
            PgTargetFactKeys.PostureFullPageWrites => ComposeFullPageWritesOff(fact, snapshot),
            PgTargetFactKeys.PostureSynchronousCommit => ComposeSynchronousCommitOff(fact, snapshot),
            _ => null,
        };
    }

    private static AdviceBlock ComposeFsyncOff(Fact fact, string snapshot) => new(
        Headline:
            "fsync is off — an operating-system crash or power loss can leave this cluster unrecoverable",
        Investigation:
            $"The configuration snapshot{snapshot} reports fsync = off; PostgreSQL's default is on. With fsync off the server does not wait for its writes to reach durable storage before it reports a commit or completes a checkpoint, so after an operating-system crash or a power loss the data files and the WAL can disagree in ways crash recovery cannot repair, and the failure is discovered when the cluster will not start or when a page reads back wrong. Nothing about the workload changes this statement: fsync is a durability policy, and this finding does not weigh it against anything the server is doing.",
        Remediation:
            $"This is the operator's policy to own, and the durable choice is the default. If this cluster holds data that cannot be rebuilt from somewhere else, set it back: ALTER SYSTEM SET fsync = on; then SELECT pg_reload_conf(); — {ChangeMechanism(fact)}. If it is off deliberately (a disposable load target, a replica that will be rebuilt from its primary), record that decision where the next operator will find it, because the setting leaves no trace until the failure.");

    private static AdviceBlock ComposeFullPageWritesOff(Fact fact, string snapshot) => new(
        Headline:
            "full_page_writes is off — a page torn by a crash cannot be repaired from WAL, and the damage surfaces later as corruption",
        Investigation:
            $"The configuration snapshot{snapshot} reports full_page_writes = off; PostgreSQL's default is on. The server writes 8 kB pages that the operating system and the storage may commit in smaller pieces; with full_page_writes on, the first change to a page after each checkpoint writes the whole page into WAL so recovery can put the page back whole. With it off, a crash in the middle of a page write leaves a half-old, half-new page that recovery then replays changes onto, and the corruption is found on a later read, not at restart. The setting is safe only where the storage itself guarantees an 8 kB write is atomic — a property of the file system or array that the operator has to verify; this finding cannot see it and does not assume it.",
        Remediation:
            $"This is the operator's policy to own, and the durable choice is the default. Unless the storage's atomic-write guarantee has been verified and written down, set it back: ALTER SYSTEM SET full_page_writes = on; then SELECT pg_reload_conf(); — {ChangeMechanism(fact)}. If it stays off on the strength of that guarantee, record the guarantee and the storage it rests on beside the setting, so a storage migration re-opens the question.");

    private static AdviceBlock ComposeSynchronousCommitOff(Fact fact, string snapshot) => new(
        Headline:
            "synchronous_commit is off — a crash discards the most recent commits the server already acknowledged, up to about three wal_writer_delay cycles",
        Investigation:
            $"The configuration snapshot{snapshot} reports synchronous_commit = off; PostgreSQL's default is on. The server acknowledges a commit before that commit's WAL has reached disk, and the WAL writer flushes on its own cycle, so a crash between the acknowledgement and the flush discards transactions the client was told had succeeded — a window of at most three times wal_writer_delay, 600 ms at the shipped 200 ms. The database stays consistent: this is a bounded loss of the newest commits, not corruption, which is why it is an advisory posture and not a critical one. {SessionScopeSentence(fact)}",
        Remediation:
            $"Decide this as policy, not as tuning — it is the operator's call, and the durable choice is the default. If every acknowledged commit on this server must survive a crash, set the server default back: ALTER SYSTEM SET synchronous_commit = on; then SELECT pg_reload_conf(); — {ChangeMechanism(fact)}. If a bounded loss of the newest commits is acceptable for this server's data, leave it and write the decision down. A middle path is to keep the server default on and make the durability choice per transaction (SET LOCAL synchronous_commit = off) only for work whose loss has been accepted, so the exception is visible in the code that takes it.");

    /// <summary>The snapshot's age against the window end, from the fact — the collector stamps it off
    /// <c>TimeRangeEnd</c>, never the wall clock.</summary>
    private static string SnapshotClause(Fact fact)
    {
        if (!fact.Metadata.TryGetValue("snapshot_age_minutes", out var ageMinutes))
            return string.Empty;
        if (ageMinutes < 1)
            return " taken at the end of the analysis window";
        if (ageMinutes < 120)
            return $" taken {ageMinutes.ToString("0", CultureInfo.InvariantCulture)} minutes before the end of the analysis window";
        return $" taken {(ageMinutes / 60).ToString("0.0", CultureInfo.InvariantCulture)} hours before the end of the analysis window";
    }

    /// <summary>Reload versus restart, read from the fact's <c>requires_restart</c> / <c>pending_restart</c>
    /// (the collector's reading of <c>pg_settings.context</c>) rather than hard-coded.</summary>
    private static string ChangeMechanism(Fact fact)
    {
        var requiresRestart = fact.Metadata.GetValueOrDefault("requires_restart") == 1;
        var pendingRestart = fact.Metadata.GetValueOrDefault("pending_restart") == 1;
        var sessionSettable = fact.Metadata.GetValueOrDefault("session_settable") == 1;

        var mechanism = requiresRestart
            ? "this setting takes effect only after a server restart (pg_settings.context = postmaster)"
            : sessionSettable
                ? "a reload, not a restart; new sessions pick the default up, and a session that has SET its own value keeps it until it reconnects or resets"
                : "a reload, not a restart (pg_settings.context = sighup)";

        return pendingRestart
            ? mechanism + ". The snapshot already shows pending_restart for this setting: the file and the running server disagree, so check which value is actually in force before changing anything"
            : mechanism;
    }

    private static string SessionScopeSentence(Fact fact) =>
        fact.Metadata.GetValueOrDefault("session_settable") == 1
            ? "The setting is per-session (pg_settings.context = user): the server value is a default that any session may override in either direction, and this fact reads the server default."
            : "This fact reads the server-wide value.";
}
