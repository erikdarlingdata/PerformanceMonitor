/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One deadlock event as the alert engine consumes it (Phase-5 slice B) — the superset of fields
/// the shared <see cref="AlertContextBuilders.BuildDeadlockContext"/> builder, the
/// <c>DeadlockIncidentGrouper</c>/<c>DeadlockObjectExtractor</c> projections, and the
/// excluded-database check (<see cref="AlertContextBuilders.IsDeadlockExcluded"/>) actually touch.
/// Lite's grid row (<c>DeadlockRow</c>) derives from this so its store reads flow into the shared
/// builders without any mapping copy; the Darling adapter materializes this type directly.
/// </summary>
public class DeadlockAlertRow
{
    public string VictimProcessId { get; set; } = "";
    public string VictimSqlText { get; set; } = "";
    public string DeadlockGraphXml { get; set; } = "";
    public bool HasDeadlockXml => !string.IsNullOrEmpty(DeadlockGraphXml);

    /// <summary>
    /// Parses the deadlock graph XML and returns a summary of all processes involved
    /// ("SPID 55 (victim) vs SPID 60"). Computed on access, exactly like the pre-extraction
    /// Lite property — see <see cref="DeadlockGraphSummary.Summarize"/>.
    /// </summary>
    public string ProcessSummary => DeadlockGraphSummary.Summarize(DeadlockGraphXml, VictimProcessId);

    /// <summary>
    /// One labelled fact per party to the deadlock — session, isolation level, requested lock mode and
    /// statement — see <see cref="DeadlockGraphSummary.PartyFacts"/>. Computed on access, like
    /// <see cref="ProcessSummary"/>; empty when the graph is absent or does not parse.
    /// </summary>
    public IReadOnlyList<(string Label, string Value)> PartyFacts =>
        DeadlockGraphSummary.PartyFacts(DeadlockGraphXml, VictimProcessId);
}

/// <summary>
/// Everything the alert body says about a deadlock's processes: the one-line roster the "Processes"
/// field carries, and the per-process facts that name what each party was running and under which
/// isolation level.
///
/// <para><b>One walk.</b> Both come off
/// <see cref="DeadlockGraphProcessParser.TryParseGraph{T}"/>, the same graph walk behind both apps'
/// deadlock grids. A deadlock is a conflict between at least two statements and the survivor is
/// frequently the one to change, so the body has to reach fields — <c>isolationlevel</c>,
/// <c>lockMode</c>, the execution stack's <c>procname</c>, <c>inputbuf</c> — that a roster of session
/// ids does not carry. Reading them here through a second parse of the same XML is how the two
/// readings of one graph drift apart; the roster is a projection of the shared walk's output for
/// exactly that reason, not a walk of its own.</para>
///
/// <para><b>What the roster still renders, unchanged.</b> <see cref="Summarize"/> is a consumer
/// contract — it is the alert body's "Processes" field AND the <c>process_summary</c> field of both
/// apps' deadlock MCP tools — so its output is what it always was, including the two cases where the
/// shared walk's own defaults would have read as values: a process with no parseable <c>spid</c>
/// renders <c>SPID ?</c> rather than the walk's numeric default, and the victim marker is placed by
/// matching the STORED victim process id rather than by the walk's victim-list membership, so a
/// graph without a victim-list cannot acquire a marker the stored column does not support.</para>
/// </summary>
public static class DeadlockGraphSummary
{
    /// <summary>
    /// How many distinct parties the per-process facts render. A deadlock is usually two-party; a
    /// parallel one carries a <c>&lt;process&gt;</c> per TASK and can run to double figures, which is why
    /// the budget is a stated cap with a visible remainder rather than whichever parties happen to come
    /// first. The complete graph is attached to the same alert (#3330), so the cap costs a reader the
    /// convenience of the remainder and never the evidence.
    /// <para>Bounded at a letter per party by <see cref="PartyLabel"/>, which is what keeps the labels
    /// inside the grammar the web alert-history parser accepts.</para>
    /// </summary>
    public const int MaxParties = 4;

    /// <summary>
    /// The per-party statement budget, below the 300 of "Victim SQL" because up to
    /// <see cref="MaxParties"/> of these render on one card: 4 x (200 + ~60 of spid/isolation/lock) is
    /// about 1 KB added to a body, and one party's whole value stays well inside the 2 000 characters a
    /// single Slack field entry carries.
    /// </summary>
    public const int StatementMaxLength = 200;

    /// <summary>The fact naming how many parties the <see cref="MaxParties"/> cap left out.</summary>
    public const string OmittedFactName = "Processes Omitted";

    /// <summary>
    /// The fact name for the nth party: "Process A", "Process B", … in the graph's own process order.
    ///
    /// <para><b>Lettered rather than numbered, because a consumer reads these back.</b> The Darling web
    /// alert-history detail cell parses the flattened body with a label pattern of letters and spaces
    /// only, so <c>Process 1:</c> is not a label to it — the line folds into the PRECEDING field's value
    /// and the parties arrive as a run-on appended to Victim SQL. Letters are also the less ambiguous
    /// rendering beside a value that opens with a session number.</para>
    ///
    /// <para>The labels must stay distinct per item as well as parseable: the PagerDuty channel keys its
    /// <c>custom_details</c> by heading plus label through an indexer, so two parties sharing a label
    /// would silently overwrite each other there. One letter per party, capped by
    /// <see cref="MaxParties"/>.</para>
    /// </summary>
    public static string PartyLabel(int index) => "Process " + (char)('A' + index);

    /// <summary>
    /// What separates the segments inside one party's value.
    ///
    /// <para>Not a pipe: the generic-webhook channel flattens the whole body by joining every field with
    /// <c>" | "</c>, so a pipe inside a value is indistinguishable from a field boundary there. That
    /// collision already exists for any query text containing one; putting a pipe in EVERY party fact
    /// would make it certain rather than content-dependent. A comma matches what the body already uses to
    /// join the Database and Involved Objects lists.</para>
    /// </summary>
    private const string SegmentSeparator = ", ";

    /// <summary>
    /// Parses the deadlock graph XML and returns a summary of all processes involved
    /// ("SPID 55 (victim) vs SPID 60"). Returns "" for empty or unparseable XML (the builder omits the
    /// field then).
    /// </summary>
    public static string Summarize(string? deadlockGraphXml, string? victimProcessId)
    {
        if (!TryParties(deadlockGraphXml, out var processes))
        {
            return "";
        }

        var summaries = new List<string>(processes.Count);
        foreach (var proc in processes)
        {
            summaries.Add($"SPID {SpidLabel(proc)}{(IsVictim(proc, victimProcessId) ? " (victim)" : "")}");
        }

        return string.Join(" vs ", summaries);
    }

    /// <summary>
    /// One labelled fact per party — <see cref="PartyLabel"/> in the graph's own process order, so a
    /// reader can line them up against <see cref="Summarize"/>'s roster — each naming that party's
    /// session, isolation level, requested lock mode and statement. Empty for empty or unparseable XML.
    ///
    /// <para><b>Why the statement can come from either of two places.</b> A party that entered through an
    /// RPC has no batch text for SQL Server to write, so its <c>inputbuf</c> holds
    /// <c>Proc [Database Id = N Object Id = M]</c> while its execution stack names the procedure; a party
    /// running an ad-hoc batch has the batch in <c>inputbuf</c> and an execution stack reading
    /// <c>adhoc</c>/<c>unknown</c>. The stack's procedure name therefore wins when the walk resolved one,
    /// and <c>inputbuf</c> is the fallback — which is #3307's argument (the field an on-call engineer
    /// reads first must name code, not an identifier) reaching the parties that argument was not applied
    /// to. The rendered value says which source it took, because "a procedure name" and "the batch that
    /// was submitted" answer different questions and a reader should not have to guess which one they
    /// have.</para>
    ///
    /// <para><b>Identical parties collapse.</b> A party to a deadlock is a SESSION; a parallel deadlock
    /// carries one <c>&lt;process&gt;</c> per task, all sharing that session's id, isolation level, lock
    /// mode and statement. Those render as one fact with a task count, because a party restated six times
    /// spends the whole <see cref="MaxParties"/> budget on itself and pushes the genuinely different party
    /// into the remainder. The victim marker is the one thing a group inherits from ANY member rather than
    /// matching on: the victim-list names a task, and the party that task belongs to is the party that
    /// lost. Every other rendered fact is part of the identity, so the collapse cannot hide a difference a
    /// reader could otherwise have seen — and the roster still lists every process individually.</para>
    ///
    /// <para><b>No timestamps.</b> A graph carries per-process transaction and batch instants on the
    /// MONITORED server's clock, and #3421 requires every instant in an alert body to declare its frame
    /// through <see cref="PerformanceMonitor.Notifications.AlertTimestamp"/>, which needs that server's
    /// collected UTC offset. This builder is not given one. None of the instants is surfaced, so none is
    /// rendered in an undeclared frame.</para>
    ///
    /// <para><b>No client identity.</b> <c>clientapp</c>, <c>hostname</c> and <c>loginname</c> are on
    /// every process and are left off: they name who ran the statement, not why the two statements
    /// conflict, which is what this fact exists to carry.</para>
    /// </summary>
    public static List<(string Label, string Value)> PartyFacts(
        string? deadlockGraphXml, string? victimProcessId)
    {
        var facts = new List<(string Label, string Value)>();
        if (!TryParties(deadlockGraphXml, out var processes))
        {
            return facts;
        }

        var order = new List<string>();
        var first = new Dictionary<string, DeadlockProcessInfo>(StringComparer.Ordinal);
        var tasks = new Dictionary<string, int>(StringComparer.Ordinal);
        var victims = new HashSet<string>(StringComparer.Ordinal);

        foreach (var proc in processes)
        {
            var key = PartyKey(proc);
            if (!tasks.TryGetValue(key, out var seen))
            {
                order.Add(key);
                first[key] = proc;
                seen = 0;
            }
            tasks[key] = seen + 1;
            if (IsVictim(proc, victimProcessId))
            {
                victims.Add(key);
            }
        }

        var shown = order.Count < MaxParties ? order.Count : MaxParties;
        for (var n = 0; n < shown; n++)
        {
            var key = order[n];
            facts.Add((PartyLabel(n), PartyValue(first[key], victims.Contains(key), tasks[key])));
        }

        if (order.Count > shown)
        {
            facts.Add((OmittedFactName, $"{order.Count - shown} (see the attached deadlock graph)"));
        }

        return facts;
    }

    /* The shared walk over one graph, with the row fields this project has no use for left null: the
       deadlock time and victim plan belong to the grids' per-process rows, and the victim SQL only feeds
       the fallback row TryParseGraph does not produce. */
    private static bool TryParties(string? deadlockGraphXml, out List<DeadlockProcessInfo> processes) =>
        DeadlockGraphProcessParser.TryParseGraph(
            new DeadlockGraphProcessParser.DeadlockGraphInput(deadlockGraphXml, null, null, null),
            out processes);

    /* The STORED victim process id decides the marker, not the walk's victim-list membership, so the
       roster renders exactly what it always did. Empty is never a match: a graph whose process carries no
       id attribute would otherwise be marked the victim by a row whose victim id column is blank. */
    private static bool IsVictim(DeadlockProcessInfo proc, string? victimProcessId) =>
        !string.IsNullOrEmpty(victimProcessId) &&
        string.Equals(proc.ProcessId, victimProcessId, StringComparison.OrdinalIgnoreCase);

    /* The walk parses spid into an int and leaves 0 when the attribute is absent or not a number. 0 is
       not a session id, so it renders as the same "?" the roster has always shown for that case. */
    private static string SpidLabel(DeadlockProcessInfo proc) =>
        proc.Spid > 0 ? proc.Spid.ToString(System.Globalization.CultureInfo.InvariantCulture) : "?";

    /* Identity for the task collapse: every rendered fact except the victim marker, which the group takes
       from any member. Two processes differing in any of these stay separate parties, so the collapse can
       never hide a difference a reader can see. */
    private static string PartyKey(DeadlockProcessInfo proc) =>
        string.Join("\u001f", SpidLabel(proc), proc.IsolationLevel, proc.LockMode, Statement(proc));

    private static string PartyValue(DeadlockProcessInfo proc, bool isVictim, int tasks)
    {
        var value = new System.Text.StringBuilder();
        value.Append("SPID ").Append(SpidLabel(proc));
        if (isVictim)
        {
            value.Append(" (victim)");
        }

        /* Ahead of the statement, like every other fact, so the statement's truncation is the only thing
           truncation can reach. */
        if (tasks > 1)
        {
            value.Append(" [").Append(tasks.ToString(System.Globalization.CultureInfo.InvariantCulture))
                 .Append(" tasks]");
        }

        if (!string.IsNullOrWhiteSpace(proc.IsolationLevel))
        {
            value.Append(SegmentSeparator).Append("isolation: ").Append(proc.IsolationLevel.Trim());
        }

        if (!string.IsNullOrWhiteSpace(proc.LockMode))
        {
            value.Append(SegmentSeparator).Append("lock: ").Append(proc.LockMode.Trim());
        }

        var statement = Statement(proc);
        if (statement.Length > 0)
        {
            /* Last, so the truncation can only ever cut the statement and never a fact behind it.
               TruncateText also collapses newlines to spaces, which is what keeps an inputbuf carrying a
               multi-statement batch from emitting body lines that the mute pre-fill's label matcher would
               read as fields of their own. */
            value.Append(SegmentSeparator).Append(HasProcName(proc) ? "proc: " : "sql: ")
                 .Append(AlertContextBuilders.TruncateText(statement, StatementMaxLength));
        }

        return value.ToString();
    }

    private static bool HasProcName(DeadlockProcessInfo proc) => !string.IsNullOrWhiteSpace(proc.ProcName);

    private static string Statement(DeadlockProcessInfo proc) =>
        HasProcName(proc) ? proc.ProcName.Trim() : proc.SqlText.Trim();
}
