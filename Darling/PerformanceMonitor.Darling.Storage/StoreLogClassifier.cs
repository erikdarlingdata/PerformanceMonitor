/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Turns a slab of the managed store's OWN server log into a per-class census (#3021).
///
/// <para><b>Why a census and not rows.</b> A day of a production store's log holds ~1,100
/// <c>canceling statement due to user request</c> lines — client <c>CommandTimeout</c> cancels rendered
/// server-side, which is what the service's own hourly sweep documents itself producing (see
/// <see cref="StoreSelfMetrics.SweepTimeoutSeconds"/>: Npgsql cancels, the server logs the cancel, the
/// client sees a torn stream). Those 1,100 lines are the NORMAL consequence of having timeouts at all, so a
/// surface that lists them trains its reader to ignore it, which is the cry-wolf failure #1852's
/// <c>target_has_user_databases</c> exists to prevent. What a reader needs from that class is not the lines,
/// it is the RATE and whether the rate moved. So the high-volume classes are counted and named, and only
/// the classes whose individual instances are worth reading keep their text.</para>
///
/// <para><b>Nothing is dropped, and that is the whole safety property.</b> Every entry lands in exactly one
/// class, and the two DEFAULT classes are chosen so a shape nobody anticipated stays visible: an entry at
/// WARNING or above that matches no rule becomes <see cref="UnclassifiedClass"/> and KEEPS its text. The
/// rule table's job is therefore to shrink the noise, never to find the signal — a rule this file forgot
/// costs one row under <c>unclassified</c>, never a missed condition. That inverts #3014's defect
/// (a benign-noise exclusion that skipped a whole file, so a real shape could vanish into it) rather than
/// re-implementing it: an exclusion here moves an entry into a named, COUNTED bucket, and a bucket with a
/// count in it is not an exclusion a reader can fail to notice.</para>
///
/// <para><b>The exclusions read a CONSTRUCT, not a phrase (#3014).</b> A rule matches only when the entry's
/// SEVERITY is one the rule names and the rule's text sits at a position the rule names. The two
/// EXCLUDING rules — the ones whose entries are counted without text — are additionally restricted to
/// <see cref="MatchKind.StartsWith"/>, because an over-broad exclusion fails toward a MISSED shape, which is
/// the half of a guard whose mistakes are silent (the <c>NonStoreTeardownTokens</c> reasoning, #3036).
/// <see cref="MatchKind.Contains"/> is legal only on a RETAINED rule, where the worst a loose match can do
/// is file a real entry under a friendlier heading while still storing its text.
/// <see cref="EveryExcludingRuleIsAnchoredAndSeverityScoped"/> is not a comment — it is
/// <c>StoreLogClassifierTests</c>' pin.</para>
///
/// <para><b>It does not read the log's prefix or its clock.</b> The anchor is PostgreSQL's own severity
/// field — <c>elog.c</c> writes <c>"%s:  "</c> (severity, colon, TWO spaces) immediately after whatever
/// <c>log_line_prefix</c> rendered — so the classifier is indifferent to the prefix, including the
/// <c>%Q</c> form that puts a query id against the severity with no separator at all
/// (<c>[1549] 322048460535975151ERROR:  ...</c>), which is the shape that cost #3030 six days of silent
/// zero capture. And it reads NO timestamp: PostgreSQL renders <c>%m</c> in <c>log_timezone</c>, which
/// <see cref="DarlingManagedPostgres"/> deliberately does not set (its v9 block pins the session
/// <c>timezone</c> only, asserted by <c>DarlingManagedPostgresTests</c>), so the store's own log stamps are
/// in the HOST's zone. A census binned on the sweep's own UTC capture instant needs none of them; the raw
/// line is kept verbatim for the retained classes so a person still reads the server's own stamp, and
/// nothing here interprets it. That is the same trade <c>PgDeadlockLogParser</c> makes the other way and for
/// the same reason — it needs the instant, so it must refuse a zone it cannot verify; this needs no instant,
/// so it refuses nothing.</para>
/// </summary>
public static class StoreLogClassifier
{
    /// <summary>Where a rule's text has to sit for the rule to match.</summary>
    public enum MatchKind
    {
        /// <summary>The severity alone decides; <see cref="Rule.Text"/> is empty.</summary>
        SeverityOnly,

        /// <summary>The message BEGINS with the rule's text. The only kind an excluding rule may use.</summary>
        StartsWith,

        /// <summary>The message contains the rule's text anywhere. Retained rules only.</summary>
        Contains,
    }

    /// <summary>
    /// One classification rule. <paramref name="Severities"/> is exact, case-sensitive membership against
    /// what <c>error_severity()</c> writes; <paramref name="Retained"/> decides whether the entry's message
    /// and raw text are stored or only counted.
    /// </summary>
    /// <param name="EventClass">The stored class name — a consumer API, so add a class rather than
    /// redefining one.</param>
    /// <param name="Severities">The severities this rule may match, exactly. Never empty.</param>
    /// <param name="Match">Where <paramref name="Text"/> must sit.</param>
    /// <param name="Text">The message text the rule looks for; empty for
    /// <see cref="MatchKind.SeverityOnly"/>.</param>
    /// <param name="Retained">True to keep the message and the raw entry; false to count only.</param>
    /// <param name="Why">Why this class exists, in one line. Carried on the rule rather than left in a
    /// comment so the surface can say it and the reader never has to open this file.</param>
    public readonly record struct Rule(
        string EventClass,
        string[] Severities,
        MatchKind Match,
        string Text,
        bool Retained,
        string Why);

    /// <summary>The residue class: WARNING or above, matching no rule. RETAINED, which is what makes the
    /// rule table below an aid to reading rather than a filter.</summary>
    public const string UnclassifiedClass = "unclassified";

    /// <summary>Everything below WARNING that matches no rule — checkpoints, connection authorisations,
    /// continuous-aggregate refresh chatter. Counted, never retained.</summary>
    public const string RoutineClass = "routine";

    /// <summary>
    /// How many DISTINCT retained messages one class may keep per capture before the rest are folded into
    /// the class's count and the drop is reported. A bound is necessary: an operator or an agent running
    /// ad-hoc SQL against the store produces one distinct <c>ERROR</c> per typo, and the production store's
    /// log demonstrably records exactly that. Reported as <c>groups_dropped</c> rather than applied
    /// silently — a display or storage budget must not evict state without saying so.
    /// </summary>
    public const int MaxRetainedGroupsPerClass = 20;

    /// <summary>Cap on a stored message. Long enough for any message PostgreSQL writes.</summary>
    public const int MaxMessageLength = 2000;

    /// <summary>Cap on a stored raw entry, which carries the DETAIL/HINT/STATEMENT body with it.</summary>
    public const int MaxSampleLength = 8000;

    /// <summary>What <c>error_severity()</c> can write at the head of a NEW entry.</summary>
    public static readonly string[] PrimarySeverities =
        ["DEBUG", "LOG", "INFO", "NOTICE", "WARNING", "ERROR", "FATAL", "PANIC"];

    /// <summary>The fields PostgreSQL emits as their own prefixed lines BELOW an entry. Each belongs to the
    /// entry above it, so a line whose first field token is one of these never starts a new entry — which is
    /// also what stops a <c>STATEMENT:</c> line echoing SQL that contains the text <c>ERROR:  </c> from being
    /// read as an error of its own.</summary>
    public static readonly string[] ContinuationFields =
        ["DETAIL", "HINT", "QUERY", "CONTEXT", "STATEMENT", "LOCATION"];

    private static readonly string[] Error = ["ERROR"];
    private static readonly string[] Fatal = ["FATAL"];
    private static readonly string[] Log = ["LOG"];
    private static readonly string[] Panic = ["PANIC"];

    /// <summary>
    /// The rule table, evaluated IN ORDER; the first match wins.
    ///
    /// <para>The ordering has one deliberate consequence worth naming. The severity-only <c>panic</c> rule
    /// sits BELOW <c>data_integrity</c> and <c>crash_recovery</c>, so <c>PANIC:  could not write to file</c>
    /// is filed as a disk problem rather than as a bare panic — the more useful of the two labels, and both
    /// are retained either way, so the ordering decides only the heading. The two EXCLUDING rules sit above
    /// it too, and cannot reach a PANIC at all: each names ERROR, FATAL or LOG explicitly, which is the
    /// severity scoping <see cref="EveryExcludingRuleIsAnchoredAndSeverityScoped"/> pins.</para>
    /// </summary>
    public static readonly Rule[] Rules =
    [
        /* Retained — a store complaining about its own storage is the reason to read this at all. The list
           is deliberately NOT exhaustive and does not need to be: an unrecognised corruption message is
           still retained under `unclassified`, so these entries buy a better heading, not visibility. Every
           one is Contains because PostgreSQL puts the relation, block or file first in several of them.

           Every rule's text in this table is a string PostgreSQL or TimescaleDB itself authors. None is an
           OS strerror reached through %m ("No space left on device", and whatever Windows says instead),
           because that text is platform- AND locale-dependent: a rule written against one host's wording is
           a rule that silently does not fire on the store this actually runs on. The disk-full case is
           covered by the PostgreSQL-authored message that wraps it, `could not extend file`, and by the
           retained residue for anything else. */
        new("data_integrity", ["WARNING", "ERROR", "FATAL", "PANIC"], MatchKind.Contains, "page verification failed", true,
            "the store's own pages did not verify - a checksum or corruption complaint about its data"),
        new("data_integrity", ["WARNING", "ERROR", "FATAL", "PANIC"], MatchKind.Contains, "invalid page", true,
            "the store's own pages did not verify - a checksum or corruption complaint about its data"),
        new("data_integrity", ["WARNING", "ERROR", "FATAL", "PANIC"], MatchKind.Contains, "chunk number", true,
            "a TOAST chunk the store expected is missing or unexpected - large-value corruption"),
        new("data_integrity", ["WARNING", "ERROR", "FATAL", "PANIC"], MatchKind.Contains, "could not read block", true,
            "the store could not read its own data - a disk or file problem"),
        new("data_integrity", ["WARNING", "ERROR", "FATAL", "PANIC"], MatchKind.Contains, "could not write block", true,
            "the store could not write its own data - a disk or file problem"),
        new("data_integrity", ["WARNING", "ERROR", "FATAL", "PANIC"], MatchKind.Contains, "could not fsync", true,
            "the store could not flush its own writes - a disk problem, and the one PostgreSQL panics on"),
        new("data_integrity", ["WARNING", "ERROR", "FATAL", "PANIC"], MatchKind.Contains, "could not extend file", true,
            "the store could not grow a file - almost always the volume is full"),

        /* Retained - the store restarting uncleanly is the largest single fact this source can report, and
           nothing else in the product notices it: the service reconnects, collection resumes, and the only
           record that the store lost its memory state is these lines. */
        new("crash_recovery", Log, MatchKind.StartsWith, "database system was not properly shut down", true,
            "the store came up from an unclean shutdown and ran recovery"),
        new("crash_recovery", Log, MatchKind.StartsWith, "database system was interrupted", true,
            "the store came up from an unclean shutdown and ran recovery"),
        new("crash_recovery", Log, MatchKind.StartsWith, "all server processes terminated", true,
            "a backend crash took the whole store down and it reinitialised"),
        new("crash_recovery", Log, MatchKind.StartsWith, "terminating any other active server processes", true,
            "a backend crash took the whole store down and it reinitialised"),
        new("crash_recovery", Log, MatchKind.StartsWith, "server process (PID", true,
            "a backend died on a signal or exception rather than exiting"),
        new("crash_recovery", Fatal, MatchKind.StartsWith, "the database system is in recovery mode", true,
            "the service reached the store while it was still recovering"),
        /* Sits HERE and not in admin_termination, which is the reason that rule names the whole
           administrative phrase rather than the word `terminating`: this message is a crash, its wording
           begins identically, and a floor rule anchored on the shorter prefix would have counted the
           clearest crash signal the store can emit as expected job churn. */
        new("crash_recovery", Fatal, MatchKind.StartsWith, "terminating connection because of crash of another server process", true,
            "a backend crash took every other connection down with it"),

        /* Retained, and Contains because TimescaleDB embeds it: `failed to launch job 42 "Compression
           Policy [42]": out of background workers`. This is the silent compression-stopper - the store's
           archival tier simply stops advancing, and the only place it is ever said out loud is here. */
        new("worker_slots_exhausted", ["WARNING", "ERROR", "LOG", "FATAL"], MatchKind.Contains, "out of background worker", true,
            "a background job could not start because the store had no worker slot left"),

        /* Retained - the store deadlocking against ITSELF, which is a different fact from any monitored
           target's deadlock and is stored nowhere else. The DETAIL body rides along in the raw entry. */
        new("deadlock", Error, MatchKind.StartsWith, "deadlock detected", true,
            "two of the store's own sessions deadlocked against each other"),

        /* Retained - the convoy signature. 71 in the production day that sized this, clustered inside the
           refresh windows rather than spread across them, which is exactly the shape a count-only class
           would render invisible. Fixed message text, so retaining it costs one row per capture. */
        new("client_connection_lost", Fatal, MatchKind.StartsWith, "connection to client lost", true,
            "a backend found the service gone mid-statement - the store's half of a torn connection"),
        new("client_connection_lost", Log, MatchKind.StartsWith, "could not receive data from client", true,
            "a backend found the service gone mid-statement - the store's half of a torn connection"),
        new("client_connection_lost", Log, MatchKind.StartsWith, "unexpected EOF on client connection", true,
            "a backend found the service gone mid-statement - the store's half of a torn connection"),

        /* Retained despite being a cancel, because it is RARE (a handful a day) and because the entry's
           STATEMENT continuation names the query that ran out of budget - which is the actionable half and
           is thrown away by a count. Distinct from the user-request cancel below: this one means the STORE
           enforced its own limit, not that the client gave up. */
        new("statement_timeout", Error, MatchKind.StartsWith, "canceling statement due to statement timeout", true,
            "a statement hit the store's own statement_timeout"),
        new("lock_timeout", Error, MatchKind.StartsWith, "canceling statement due to lock timeout", true,
            "a statement hit the store's own lock_timeout waiting for a lock"),

        /* EXCLUDING - the ~1,100/day floor. StartsWith and ERROR-scoped, so it cannot reach a FATAL or a
           PANIC and cannot match a message that merely mentions a cancel. */
        new("user_request_cancel", Error, MatchKind.StartsWith, "canceling statement due to user request", false,
            "the store's rendering of a client-side CommandTimeout cancel - expected, and the ordinary floor"),

        /* EXCLUDING - the ~190/day floor from job churn and service restarts. Three separate texts rather
           than one loose `terminating` prefix, because `terminating connection because of crash of another
           server process` is NOT administrative and must stay in crash_recovery's reach, not here. */
        new("admin_termination", Fatal, MatchKind.StartsWith, "terminating background worker", false,
            "a background worker was told to stop - job churn and service restarts, expected"),
        new("admin_termination", Fatal, MatchKind.StartsWith, "terminating connection due to administrator command", false,
            "a connection was told to stop - job churn and service restarts, expected"),
        new("admin_termination", Fatal, MatchKind.StartsWith, "terminating autovacuum process due to administrator command", false,
            "an autovacuum worker was told to stop - job churn and service restarts, expected"),

        /* EXCLUDING - the Windows fork-emulation artifact, ~170-290/day, auto-retried by PostgreSQL itself
           and documented at length behind the 1 GB shared_buffers cap (#1559, pgsql-bugs BUG #14050 /
           #18954). NAMED and counted rather than deleted, so "we are ignoring this" is a number on the
           surface instead of a decision buried in a filter. LOG-scoped and StartsWith, so an ERROR or PANIC
           that mentions shared memory - the case where it is no longer benign - is not swallowed by it. */
        new("shared_memory_reservation_retry", Log, MatchKind.StartsWith, "could not reserve shared memory region", false,
            "the Windows fork-emulation ASLR retry - PostgreSQL retries it itself, and it is benign"),

        /* Retained, severity alone. Anything that panicked and did not name a storage problem above. */
        new("panic", Panic, MatchKind.SeverityOnly, "", true,
            "the store panicked and restarted every backend"),
    ];

    /// <summary>One classified entry, before grouping.</summary>
    /// <param name="EventClass">The matched class.</param>
    /// <param name="Severity">What <c>error_severity()</c> wrote.</param>
    /// <param name="Message">The message, prefix and severity field stripped, capped.</param>
    /// <param name="RawText">The entry's own lines verbatim, continuations included, capped. The parsed
    /// fields are an interpretation; this is the evidence, and a shape no rule recognises is still readable
    /// by a person.</param>
    /// <param name="Retained">Whether the matched rule keeps text.</param>
    public readonly record struct Entry(
        string EventClass,
        string Severity,
        string Message,
        string RawText,
        bool Retained);

    /// <summary>One stored row: a class, a severity, how many of them, and — for a retained class — the
    /// message and one verbatim entry.</summary>
    public readonly record struct Group(
        string EventClass,
        string Severity,
        int Occurrences,
        string? MessageText,
        string? SampleLine);

    /// <summary>
    /// What one read of the log produced. <paramref name="GroupsDropped"/> is the honest half: distinct
    /// retained messages past <see cref="MaxRetainedGroupsPerClass"/> are folded into their class's count
    /// and reported here rather than vanishing.
    /// </summary>
    public readonly record struct Census(
        IReadOnlyList<Group> Groups,
        int LinesRead,
        int EntriesRead,
        int ContinuationLines,
        int GroupsDropped);

    /// <summary>
    /// The invariant behind every excluding rule: it names its severities explicitly and anchors its text at
    /// the START of the message. Exported so the pin asserts the shipped table rather than a copy of it — an
    /// excluding rule added with <see cref="MatchKind.Contains"/> is how a real shape becomes invisible, and
    /// that is the failure this returns false for.
    /// </summary>
    public static bool EveryExcludingRuleIsAnchoredAndSeverityScoped()
    {
        foreach (var rule in Rules)
        {
            if (rule.Retained)
            {
                continue;
            }

            if (rule.Match != MatchKind.StartsWith || rule.Severities.Length == 0 || rule.Text.Length == 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The second invariant, and the one that is ORDER-INDEPENDENT: no excluding rule's text may be a prefix
    /// of a retained rule's text at a severity they share.
    ///
    /// <para>Why this is needed on top of the anchoring check. <c>admin_termination</c> excludes
    /// <c>terminating connection due to administrator command</c> and <c>crash_recovery</c> retains
    /// <c>terminating connection because of crash of another server process</c> — two FATAL messages whose
    /// wording begins identically, one expected job churn and the other the clearest crash signal the store
    /// can emit. Shortening the excluding rule to <c>terminating connection</c> is still anchored, still
    /// severity-scoped, and would still classify the crash correctly TODAY only because the retained rule
    /// happens to sit above it in the table. Order is not a property anyone reading a rule can see, so a
    /// safety argument that rests on it is a safety argument that breaks the next time the table is
    /// re-sorted. This returns the offending pair instead.</para>
    ///
    /// <para>Stated limit: it compares STARTS-WITH texts. A retained <see cref="MatchKind.Contains"/> rule's
    /// population is not decidable from the two strings alone, so those are not checked here — the reason
    /// <c>Contains</c> is permitted only on RETAINED rules in the first place is that its mistakes add
    /// visibility rather than remove it.</para>
    /// </summary>
    public static IReadOnlyList<string> ExcludingRulesThatCouldShadowARetainedRule()
    {
        var offenders = new List<string>();

        foreach (var excluding in Rules)
        {
            if (excluding.Retained || excluding.Match != MatchKind.StartsWith)
            {
                continue;
            }

            foreach (var retained in Rules)
            {
                if (!retained.Retained || retained.Match != MatchKind.StartsWith)
                {
                    continue;
                }

                if (!retained.Text.StartsWith(excluding.Text, StringComparison.Ordinal))
                {
                    continue;
                }

                foreach (var severity in excluding.Severities)
                {
                    if (Array.IndexOf(retained.Severities, severity) >= 0)
                    {
                        offenders.Add($"{excluding.EventClass} ('{excluding.Text}') can shadow "
                            + $"{retained.EventClass} ('{retained.Text}') at {severity}");
                        break;
                    }
                }
            }
        }

        return offenders;
    }

    /// <summary>
    /// Splits a slab of log text into entries, classifies each, and groups them for storage.
    ///
    /// <para>Every line in <paramref name="slab"/> is assumed COMPLETE — <see cref="StoreLogSlab"/> is what
    /// guarantees that, by never advancing a read marker past the slab's last newline, so a subsequent read
    /// always begins at a line start and the issue's "treat the first partial line as noise" case is
    /// unreachable by construction rather than by convention. A trailing line with no newline is still
    /// classified, because a fixture legitimately ends that way and dropping it would make the pin's own
    /// last case silently untested.</para>
    /// </summary>
    public static Census Classify(string? slab)
    {
        var entries = new List<Entry>();
        var lines = 0;
        var continuations = 0;

        if (!string.IsNullOrEmpty(slab))
        {
            /* Split on '\n' and strip a trailing '\r' rather than splitting on Environment.NewLine or on
               "\r\n": the store runs on Windows and PostgreSQL's logging collector opens the file in the
               platform's default translation mode, so the same server writes '\n' on one host and '\r\n' on
               another, and a slab can hold both across a rotation. Splitting on the one byte both forms end
               with is the only form that reads either. */
            var start = 0;
            string? currentSeverity = null;
            string? currentMessage = null;
            var currentRaw = new StringBuilder();

            void Flush()
            {
                if (currentSeverity is null || currentMessage is null)
                {
                    return;
                }

                var (eventClass, retained) = Match(currentSeverity, currentMessage);
                entries.Add(new Entry(
                    EventClass: eventClass,
                    Severity: currentSeverity,
                    Message: Cap(currentMessage, MaxMessageLength),
                    RawText: Cap(currentRaw.ToString(), MaxSampleLength),
                    Retained: retained));

                currentSeverity = null;
                currentMessage = null;
                currentRaw.Clear();
            }

            while (start <= slab.Length)
            {
                var newline = slab.IndexOf('\n', start);
                var end = newline < 0 ? slab.Length : newline;
                var line = slab.AsSpan(start, end - start);
                if (line.Length > 0 && line[^1] == '\r')
                {
                    line = line[..^1];
                }

                start = newline < 0 ? slab.Length + 1 : newline + 1;

                if (line.Length == 0)
                {
                    continue;
                }

                lines++;
                var field = FindField(line);

                if (field.Kind == FieldKind.Primary)
                {
                    Flush();
                    currentSeverity = field.Name;
                    currentMessage = line[field.MessageStart..].ToString();
                    currentRaw.Append(line);
                }
                else if (currentSeverity is not null)
                {
                    /* A continuation field, or a bare line: PostgreSQL renders an embedded newline in a
                       message as newline-plus-tab, so a multi-line statement arrives as tab-indented lines
                       with no field of their own. Either way it belongs to the entry above it. */
                    continuations++;
                    currentRaw.Append('\n').Append(line);
                }
                else
                {
                    /* A continuation with no entry above it in THIS slab: the entry it belongs to was
                       classified by the previous read. Counted as a continuation so lines_read and
                       entries_read still reconcile, and otherwise ignored - re-opening it as an entry of its
                       own would invent a severity the server never wrote. */
                    continuations++;
                }
            }

            Flush();
        }

        return new Census(
            Groups: GroupEntries(entries, out var dropped),
            LinesRead: lines,
            EntriesRead: entries.Count,
            ContinuationLines: continuations,
            GroupsDropped: dropped);
    }

    /// <summary>
    /// Groups classified entries into stored rows: a retained class groups by its exact message text (so
    /// 1,100 byte-identical cancels would be one row if they were retained, and a novel message is
    /// immediately its own), and an excluded class groups by class and severity alone with no text at all.
    /// </summary>
    private static List<Group> GroupEntries(List<Entry> entries, out int dropped)
    {
        var counts = new Dictionary<(string Class, string Severity, string? Message), (int Count, string? Sample)>();
        var perClass = new Dictionary<string, int>(StringComparer.Ordinal);
        var order = new List<(string Class, string Severity, string? Message)>();

        /* Which over-budget MESSAGES have already been folded. Needed because a folded message's own key is
           never inserted into `counts` — only the untexted fold row is — so every repeat of it re-enters the
           budget branch below. Counting those repeats would report an operator's ONE retried typo as five
           hundred distinct messages folded, which is the opposite of what this figure is documented to mean
           in three places (this struct, CaptureSummary.MessagesFolded, and the sentence get_store_log
           renders) and would read to a human as five hundred separate problems. */
        var folded = new HashSet<(string Class, string Severity, string? Message)>();
        dropped = 0;

        foreach (var entry in entries)
        {
            var message = entry.Retained ? entry.Message : null;
            var key = (entry.EventClass, entry.Severity, message);

            if (counts.TryGetValue(key, out var existing))
            {
                counts[key] = (existing.Count + 1, existing.Sample);
                continue;
            }

            if (entry.Retained)
            {
                perClass.TryGetValue(entry.EventClass, out var distinct);
                if (distinct >= MaxRetainedGroupsPerClass)
                {
                    /* Past the budget: fold into the class's untexted row so the OCCURRENCE is never lost,
                       and report the fold ONCE PER DISTINCT MESSAGE, not once per occurrence of it. */
                    if (folded.Add(key))
                    {
                        dropped++;
                    }

                    var foldKey = (entry.EventClass, entry.Severity, (string?)null);
                    if (counts.TryGetValue(foldKey, out var fold))
                    {
                        counts[foldKey] = (fold.Count + 1, fold.Sample);
                    }
                    else
                    {
                        counts[foldKey] = (1, null);
                        order.Add(foldKey);
                    }

                    continue;
                }

                perClass[entry.EventClass] = distinct + 1;
            }

            counts[key] = (1, entry.Retained ? entry.RawText : null);
            order.Add(key);
        }

        var groups = new List<Group>(order.Count);
        foreach (var key in order)
        {
            var (count, sample) = counts[key];
            groups.Add(new Group(key.Class, key.Severity, count, key.Message, sample));
        }

        return groups;
    }

    /// <summary>The first rule whose severity and text both match, else the two defaults.</summary>
    private static (string EventClass, bool Retained) Match(string severity, string message)
    {
        foreach (var rule in Rules)
        {
            if (Array.IndexOf(rule.Severities, severity) < 0)
            {
                continue;
            }

            var matched = rule.Match switch
            {
                MatchKind.SeverityOnly => true,
                MatchKind.StartsWith => message.StartsWith(rule.Text, StringComparison.Ordinal),
                MatchKind.Contains => message.Contains(rule.Text, StringComparison.Ordinal),
                _ => false,
            };

            if (matched)
            {
                return (rule.EventClass, rule.Retained);
            }
        }

        /* The default that makes the table above an aid rather than a filter. WARNING and above keeps its
           text; below it is chatter. */
        return IsAtLeastWarning(severity)
            ? (UnclassifiedClass, true)
            : (RoutineClass, false);
    }

    /// <summary>Whether a severity is one a reader wants to see an unrecognised message from.</summary>
    public static bool IsAtLeastWarning(string? severity) =>
        severity is "WARNING" or "ERROR" or "FATAL" or "PANIC";

    /// <summary>
    /// Every class this classifier can emit, in the order it decides them, with the two defaults last.
    ///
    /// <para>DERIVED from <see cref="Rules"/> rather than written out a second time. The read surface names
    /// classes and says why each exists, and a hand-kept second list is how that surface comes to describe a
    /// class set the classifier no longer has.</para>
    /// </summary>
    public static IReadOnlyList<string> ClassNames { get; } = BuildClassNames();

    private static string[] BuildClassNames()
    {
        var names = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var rule in Rules)
        {
            if (seen.Add(rule.EventClass))
            {
                names.Add(rule.EventClass);
            }
        }

        names.Add(UnclassifiedClass);
        names.Add(RoutineClass);
        return [.. names];
    }

    /// <summary>
    /// Whether a class keeps its message and one verbatim entry, or is counted only. The two defaults are
    /// answered here too: <see cref="UnclassifiedClass"/> is retained (that is what makes the rule table an
    /// aid rather than a filter) and <see cref="RoutineClass"/> is not.
    /// </summary>
    public static bool IsRetainedClass(string? eventClass)
    {
        if (string.Equals(eventClass, UnclassifiedClass, StringComparison.Ordinal))
        {
            return true;
        }

        if (string.Equals(eventClass, RoutineClass, StringComparison.Ordinal))
        {
            return false;
        }

        foreach (var rule in Rules)
        {
            if (string.Equals(rule.EventClass, eventClass, StringComparison.Ordinal))
            {
                return rule.Retained;
            }
        }

        return false;
    }

    /// <summary>Why a class exists, in one line — the rule's own <see cref="Rule.Why"/>, so the surface and
    /// the classifier cannot disagree about what a class means.</summary>
    public static string WhyFor(string? eventClass)
    {
        if (string.Equals(eventClass, UnclassifiedClass, StringComparison.Ordinal))
        {
            return "a WARNING or worse that no rule recognises - kept with its text on purpose, so a shape "
                + "nobody anticipated is visible rather than filtered";
        }

        if (string.Equals(eventClass, RoutineClass, StringComparison.Ordinal))
        {
            return "everything below WARNING that no rule names - checkpoints, connection authorisations, "
                + "continuous-aggregate refresh chatter";
        }

        foreach (var rule in Rules)
        {
            if (string.Equals(rule.EventClass, eventClass, StringComparison.Ordinal))
            {
                return rule.Why;
            }
        }

        return "a class this build's classifier does not name - the store was written by a newer build";
    }

    private enum FieldKind
    {
        None,
        Primary,
        Continuation,
    }

    private readonly record struct Field(FieldKind Kind, string Name, int MessageStart);

    /// <summary>
    /// The FIRST severity-or-continuation field in the line, and where its message starts.
    ///
    /// <para>First, not any: a <c>STATEMENT:  </c> line echoing SQL that itself contains the characters
    /// <c>ERROR:  </c> would otherwise be read as an error of its own. Because both field sets are searched
    /// together and the earliest hit wins, the <c>STATEMENT</c> token is found first and the line is
    /// correctly a continuation.</para>
    ///
    /// <para>The separator is exactly <c>":  "</c> — colon and TWO spaces, which is what
    /// <c>elog.c</c>'s <c>"%s:  "</c> writes and is what makes this a construct rather than a phrase. A
    /// prefix rendering <c>%u@%d</c> for a role literally named <c>ERROR</c> puts <c>ERROR@</c> in the line
    /// and cannot match.</para>
    /// </summary>
    private static Field FindField(ReadOnlySpan<char> line)
    {
        var best = new Field(FieldKind.None, string.Empty, 0);
        var bestAt = int.MaxValue;

        foreach (var name in PrimarySeverities)
        {
            var at = IndexOfField(line, name);
            if (at >= 0 && at < bestAt)
            {
                bestAt = at;
                best = new Field(FieldKind.Primary, name, at + name.Length + 3);
            }
        }

        foreach (var name in ContinuationFields)
        {
            var at = IndexOfField(line, name);
            if (at >= 0 && at < bestAt)
            {
                bestAt = at;
                best = new Field(FieldKind.Continuation, name, at + name.Length + 3);
            }
        }

        return best;
    }

    private static int IndexOfField(ReadOnlySpan<char> line, string name)
    {
        var from = 0;
        while (from <= line.Length - name.Length)
        {
            var at = line[from..].IndexOf(name, StringComparison.Ordinal);
            if (at < 0)
            {
                return -1;
            }

            at += from;
            var after = at + name.Length;
            if (line[after..].StartsWith(":  ", StringComparison.Ordinal)
                && !ContinuesAnUpperCaseToken(line, at))
            {
                return at;
            }

            from = at + 1;
        }

        return -1;
    }

    /// <summary>
    /// Whether the field name found at <paramref name="at"/> is really the tail of a longer ALL-CAPS token —
    /// <c>PG_CATALOG:  </c> ends in <c>LOG:  </c>, and the earliest-match rule would otherwise read that as a
    /// <c>LOG</c> entry whose message begins mid-line.
    ///
    /// <para>Reachable because the scan searches the WHOLE line, prefix included, which is exactly what makes
    /// the classifier indifferent to <c>log_line_prefix</c>: an all-caps token ending in a field name and
    /// followed by colon-two-spaces can be rendered by <c>%a</c> (application_name is arbitrary client text)
    /// or <c>%u</c> / <c>%d</c> ahead of the real severity. The cost is not a missed line, it is a
    /// MANUFACTURED one: the real entry's severity is replaced by the spurious token's and its message
    /// starts mid-line, so an ERROR the reader needed becomes a <c>routine</c> row.</para>
    ///
    /// <para><b>Upper case and underscore ONLY, which is the whole design of this check.</b> A digit before
    /// the name must stay legal: <c>%Q</c> writes the query id with no separator, so the real field genuinely
    /// arrives as <c>…322048460535975151ERROR:  </c> — refusing a digit there would reintroduce #3030's
    /// defect from the other direction, which is the failure worth designing against. Lower case stays legal
    /// too, so a prefix ending in <c>%c</c>'s lower-case hex session id keeps working. Every field name is
    /// entirely upper-case ASCII, so a token that a field name can hide inside is one written in the
    /// characters all-caps identifiers use, and those are the only two this refuses.</para>
    ///
    /// <para>The stated limit: a prefix that concatenates an upper-case-ending field directly onto the
    /// severity with no separator at all (an application_name of <c>MYAPP</c> under
    /// <c>log_line_prefix = '%a'</c>) has its real field refused here. No measured prefix does that — every
    /// one puts a space, a colon or a digit there — and a missed line under a prefix nobody writes is a
    /// better trade than a manufactured line under one somebody can.</para>
    /// </summary>
    private static bool ContinuesAnUpperCaseToken(ReadOnlySpan<char> line, int at)
    {
        if (at == 0)
        {
            return false;
        }

        var before = line[at - 1];
        return before == '_' || (before >= 'A' && before <= 'Z');
    }

    private static string Cap(string value, int max) =>
        value.Length <= max ? value : value[..max];
}
