/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3208: a stored column holding the monitored server's LOCAL wall clock, handed to a consumer that
/// assumes naive UTC. Four guards already exist and none of them reaches this boundary — the gap is a
/// diagonal, not a hole in any one of them:
///
/// <list type="bullet">
/// <item><c>CreationTimeClockFrameDisciplineTests</c> has the right discriminator shape and is rooted only
/// at the two Analysis trees, so neither <c>Service/Mcp/</c> nor either viewer is in its globs.</item>
/// <item><c>StoreSqlClockDisciplineTests</c> has the right roots — <c>Mcp/</c> included — but hunts a naive
/// column compared against a BARE CLOCK, and disclaims projections outright at its own <c>:42-46</c>:
/// "a bare clock in a SET, a VALUES row or a projection is a different defect shape … deliberately out of
/// scope". Every site inventoried below is a projection.</item>
/// <item><c>CollectorTimestampFrameTests</c> has the right per-column reasoning but reads collector source
/// only. It says what a frame IS; it never asks who consumes it.</item>
/// <item><c>ServerLocalReadFrameDisciplineTests</c> (#3202) is at the right boundary and is scoped to
/// <c>default_trace_events</c> by design.</item>
/// </list>
///
/// <para><b>This reports; it does not repair.</b> #3206 owns the MCP payload defects and #3207 the desktop
/// render ones. <see cref="Inventory"/> carries every offending site instead, labelled, in the shape
/// <c>DocCommentHygieneTests</c> uses for its unresolved cref targets: pinned at SET EQUALITY, which is a
/// ratchet in BOTH directions. A new offender fails, and so does a FIXED one whose row is still here —
/// that second direction is the only thing keeping an inventory from becoming a permanent excuse list.</para>
///
/// <para><b>The frame is never keyed on the column name.</b> Two sibling guards say why:
/// <c>StoreSqlClockDisciplineTests.AmbiguousFrameColumns</c> names three columns that are two frames
/// across several tables each, and <c>CollectorTimestampFrameTests</c> records that a store-wide "all
/// naive timestamps are UTC" rule forbids the CPU collector's intentional local clock. So classification
/// here is scoped per (TABLE, COLUMN), and whether a NAME may stand in for its column at a consumer site
/// is itself DERIVED:
/// <see cref="FrameAmbiguousColumnNames_AreDerived_AndAgreeWithTheSiblingGuardsRegister"/> computes the
/// names whose tables disagree and checks the answer against that hand-written register. Two instruments,
/// independently arrived at, and drift in either one fails.</para>
///
/// <para><b>Unreadable provenance fails; it does not pass as UTC.</b> #3202's words, kept: "a frame nobody
/// can read is the state the de-skew defects shipped in." A column whose T-SQL provenance cannot be read is
/// an UNKNOWN, and every unknown must be answered by <see cref="NonTsqlProvenance"/> — a per-column
/// declaration carrying the C#-side evidence, pinned at set equality against the classifier's own
/// unreadable set and counted, so the hole cannot quietly grow.</para>
/// </summary>
public sealed class ConsumedTimestampFrameDisciplineTests
{
    /// <summary>Which clock a stored value is in. Naive UTC is the store's default and the frame every
    /// consumer below assumes unless it says otherwise; ServerLocal is the monitored server's wall clock.</summary>
    private enum ClockFrame
    {
        Utc,
        ServerLocal,
    }

    /* ═══════════════════════ 1. the population, derived from the catalog ═══════════════════════ */

    /* Floors, so a broken walk cannot report a clean bill of health. Measured on dev at cb208f6a4:
       66 Timestamp columns over 69 catalog definitions, 49 of them on SqlServer definitions and 17 on
       PostgreSql ones. Pinned exactly rather than as a floor because the whole point is a closed census —
       a floor would let a column vanish. */
    private const int TimestampColumnCount = 66;
    private const int SqlServerTimestampColumnCount = 49;
    private const int PostgresTimestampColumnCount = 17;

    private static IReadOnlyList<(string Table, string Column, string Collector, CollectorTargetEngine Engine)>
        TimestampColumns() =>
        CollectorCatalog.All
            .SelectMany(c => c.PayloadColumns
                .Where(col => col.Type == CollectorColumnType.Timestamp)
                .Select(col => (Table: c.TargetTable, Column: col.Name, Collector: c.GetType().Name, Engine: c.TargetEngine)))
            .OrderBy(r => r.Table, StringComparer.Ordinal)
            .ThenBy(r => r.Column, StringComparer.Ordinal)
            .ToArray();

    /// <summary>
    /// The census is the catalog's own <see cref="CollectorColumnType.Timestamp"/> columns, and the engine
    /// split is <see cref="ICollectorSchemaInfo.TargetEngine"/> rather than a name prefix or a hand list.
    ///
    /// <para>The T-SQL classifier below reaches the SqlServer arm ONLY. The 17 PostgreSQL columns are a
    /// different provenance mechanism, not a shortfall of this one: PostgreSQL has no
    /// <c>column = expression</c> alias form at all (there it is a boolean comparison), so those collectors
    /// write <c>expression AS column</c>, and their timestamps arrive as <c>timestamptz</c> normalised in
    /// four different spellings — <c>AT TIME ZONE 'UTC'</c> inline, the same inside an interpolated SQL
    /// fragment, a bare <c>min()</c> handled in the reader, and two computed entirely in C#
    /// (<c>pg_cpu_utilization.sample_time</c>, <c>pg_deadlocks.occurred_at</c>). Counting them here is what
    /// makes the T-SQL arm's reach a measured fact instead of an implied one.</para>
    /// </summary>
    [Fact]
    public void TimestampColumnPopulation_IsDerivedFromTheCatalog_AndSplitByTargetEngine()
    {
        var columns = TimestampColumns();

        Assert.Equal(TimestampColumnCount, columns.Count);
        Assert.Equal(
            SqlServerTimestampColumnCount,
            columns.Count(c => c.Engine == CollectorTargetEngine.SqlServer));
        Assert.Equal(
            PostgresTimestampColumnCount,
            columns.Count(c => c.Engine == CollectorTargetEngine.PostgreSql));

        /* Every column resolves to a collector SOURCE FILE, or the classifier below is judging nothing.
           Named separately from the count because a catalog entry whose file was renamed would otherwise
           silently drop out of the population it is supposed to be classified in. */
        foreach (var (table, column, collector, _) in columns)
        {
            Assert.True(
                File.Exists(CollectorSourcePath(collector)),
                $"{table}.{column}: no source file for {collector} — the provenance classifier cannot see it");
        }
    }

    /* ═══════════════════════ 2. the T-SQL provenance classifier ═══════════════════════ */

    /// <summary>
    /// Clock and decode markers read straight out of an assignment's own text. This codebase writes
    /// <c>alias = expression</c>, so the right-hand side IS the provenance (#3202's technique).
    /// </summary>
    private static readonly (string Marker, ClockFrame Frame, string Why)[] ExpressionMarkers =
    [
        ("@timestamp", ClockFrame.Utc,
            "the Extended Events event attribute, which the engine stamps in UTC"),
        ("AT TIME ZONE 'UTC'", ClockFrame.Utc,
            "an explicit conversion into UTC in the collector's own SQL"),
        ("SYSUTCDATETIME", ClockFrame.Utc, "a UTC clock function"),
        ("GETUTCDATE", ClockFrame.Utc, "a UTC clock function"),
        ("SYSDATETIME", ClockFrame.ServerLocal, "a LOCAL clock function"),
        ("GETDATE", ClockFrame.ServerLocal, "a LOCAL clock function"),
        ("msdb.dbo.agent_datetime", ClockFrame.ServerLocal,
            "Agent's own date/time decode, over columns Agent writes in the server's local clock"),
    ];

    /// <summary>
    /// Source relations, scoped PER COLUMN of that relation. A relation is not a frame: the same
    /// <c>sys.dm_os_sys_info</c> supplies both the server-local <c>sqlserver_start_time</c> and the
    /// frame-free <c>ms_ticks</c> counter, and a relation-keyed rule reads the ring-buffer arithmetic in
    /// <c>MemoryPressureEventsCollector</c> as local when the collector deliberately bases it on
    /// <c>SYSUTCDATETIME()</c> — the exact regression <c>CollectorTimestampFrameTests</c> exists to catch.
    ///
    /// <para>A relation/column pair absent from this list yields NO evidence, so a column that has no other
    /// marker is UNREADABLE and must be declared in <see cref="NonTsqlProvenance"/>. Adding a pair is a
    /// deliberate edit that has to carry its documented frame; that is why this fails toward "unknown"
    /// instead of toward "UTC".</para>
    /// </summary>
    private static readonly (string Relation, string[] Columns, ClockFrame Frame, string Why)[] RelationMarkers =
    [
        ("sys.fn_trace_gettable", ["StartTime", "EndTime"], ClockFrame.ServerLocal,
            "the .trc files store the server's local time; #3198/#3202"),
        ("sys.dm_hadr_database_replica_states",
            ["last_commit_time", "last_hardened_time", "last_received_time", "last_redone_time"],
            ClockFrame.ServerLocal, "an AG DMV shipped verbatim"),
        ("sys.dm_db_index_usage_stats",
            ["last_user_seek", "last_user_scan", "last_user_lookup", "last_user_update"],
            ClockFrame.ServerLocal, "a DMV shipped verbatim"),
        ("sys.dm_os_sys_info", ["sqlserver_start_time"], ClockFrame.ServerLocal, "a DMV shipped verbatim"),
        ("sys.dm_exec_query_stats", ["creation_time", "last_execution_time"], ClockFrame.ServerLocal,
            "QueryStatsCollector states it outright: \"creation_time is in the monitored server's local time "
            + "while collection times are UTC\"; #2991/#2992"),
        ("sys.dm_exec_procedure_stats", ["cached_time", "last_execution_time"], ClockFrame.ServerLocal,
            "a dm_exec_* stats DMV shipped verbatim"),
        ("sys.dm_exec_trigger_stats", ["cached_time", "last_execution_time"], ClockFrame.ServerLocal,
            "a dm_exec_* stats DMV shipped verbatim"),
        ("sys.dm_exec_function_stats", ["cached_time", "last_execution_time"], ClockFrame.ServerLocal,
            "a dm_exec_* stats DMV shipped verbatim"),
        ("sys.dm_tran_active_transactions", ["transaction_begin_time"], ClockFrame.ServerLocal,
            "a dm_tran_* DMV shipped verbatim"),
        ("sys.dm_tran_persistent_version_store_stats",
            ["aborted_version_cleaner_start_time", "aborted_version_cleaner_end_time",
             "offrow_version_cleaner_start_time", "offrow_version_cleaner_end_time"],
            ClockFrame.ServerLocal, "a dm_tran_* DMV shipped verbatim"),
        ("sys.dm_db_tuning_recommendations",
            ["valid_since", "last_refresh", "execute_action_initiated_time", "execute_action_start_time",
             "revert_action_initiated_time", "revert_action_start_time"],
            ClockFrame.ServerLocal, "an automatic-tuning DMV shipped verbatim"),
        ("msdb.dbo.sysjobactivity", ["start_execution_date"], ClockFrame.ServerLocal,
            "Agent writes msdb in the server's local clock; RunningJobsCollector's own next line "
            + "DATEDIFFs it against GETDATE()"),
        ("msdb.dbo.sysjobhistory", ["run_date", "run_time"], ClockFrame.ServerLocal,
            "Agent's local-clock HHMMSS-integer encoding; the collector's fallback bound is GETDATE()"),
        ("msdb.dbo.sysjobschedules", ["next_run_date", "next_run_time"], ClockFrame.ServerLocal,
            "Agent's local-clock next-run encoding"),
    ];

    /// <summary>
    /// Columns whose frame the T-SQL arm CANNOT read, each answered by the evidence that does decide it.
    /// Pinned at set equality against the classifier's own unreadable set below, so a column cannot be
    /// declared here without the classifier agreeing it needs to be, and cannot become unreadable without
    /// an entry appearing. <see cref="NonTsqlProvenanceCount"/> pins the size, so the hole cannot grow
    /// quietly.
    ///
    /// <para>#3208 puts this hole at seven columns. It is NINE: the two <c>query_store_stats</c> execution
    /// times are unreadable from T-SQL too, because Query Store returns <c>datetimeoffset</c> and the frame
    /// of the STORED value is decided by the reader's <c>.UtcDateTime</c> — which is the very fact #3207's
    /// sites 3-5 turn on.</para>
    /// </summary>
    private static readonly (string Table, string Column, ClockFrame Frame, string EvidenceFile, string Evidence, string Why)[]
        NonTsqlProvenance =
    [
        ("blocked_process_reports", "blocked_last_tran_started", ClockFrame.ServerLocal,
            "PerformanceMonitor.Collectors/BlockedProcessReportCollector.cs", "lasttranstarted",
            "parsed in C# from the blocked-process-report XML, whose attributes carry the server's local clock"),
        ("blocked_process_reports", "blocking_last_tran_started", ClockFrame.ServerLocal,
            "PerformanceMonitor.Collectors/BlockedProcessReportCollector.cs", "lasttranstarted",
            "parsed in C# from the blocked-process-report XML, whose attributes carry the server's local clock"),
        ("blocked_process_reports", "blocked_last_batch_started", ClockFrame.ServerLocal,
            "PerformanceMonitor.Collectors/BlockedProcessReportCollector.cs", "lastbatchstarted",
            "parsed in C# from the blocked-process-report XML, whose attributes carry the server's local clock"),
        ("blocked_process_reports", "blocking_last_batch_started", ClockFrame.ServerLocal,
            "PerformanceMonitor.Collectors/BlockedProcessReportCollector.cs", "lastbatchstarted",
            "parsed in C# from the blocked-process-report XML, whose attributes carry the server's local clock"),
        ("blocked_process_reports", "blocked_last_batch_completed", ClockFrame.ServerLocal,
            "PerformanceMonitor.Collectors/BlockedProcessReportCollector.cs", "lastbatchcompleted",
            "parsed in C# from the blocked-process-report XML, whose attributes carry the server's local clock"),
        ("blocked_process_reports", "blocking_last_batch_completed", ClockFrame.ServerLocal,
            "PerformanceMonitor.Collectors/BlockedProcessReportCollector.cs", "lastbatchcompleted",
            "parsed in C# from the blocked-process-report XML, whose attributes carry the server's local clock"),
        ("dmv_blocking_snapshots", "event_time", ClockFrame.Utc,
            "PerformanceMonitor.Collectors/DmvBlockingSnapshotCollector.cs", "context.CollectionTime",
            "stamped by the host from the collection cycle's own clock, which is naive UTC — so on a "
            + "DMV-fallback row this UTC column sits beside two server-local *_last_tran_started ones"),
        ("query_store_stats", "first_execution_time", ClockFrame.Utc,
            "PerformanceMonitor.Collectors/QueryStoreCollector.cs", ").UtcDateTime",
            "Query Store returns datetimeoffset; the reader normalises through DateTimeOffset.UtcDateTime "
            + "before storing, so the T-SQL says nothing about the stored frame"),
        ("query_store_stats", "last_execution_time", ClockFrame.Utc,
            "PerformanceMonitor.Collectors/QueryStoreCollector.cs", ").UtcDateTime",
            "Query Store returns datetimeoffset; the reader normalises through DateTimeOffset.UtcDateTime "
            + "before storing, so the T-SQL says nothing about the stored frame"),
    ];

    private const int NonTsqlProvenanceCount = 9;
    private const int TsqlServerLocalCount = 34;
    private const int TsqlUtcCount = 6;

    /// <summary>The assignment's balanced right-hand side. Balanced rather than to-end-of-line because four
    /// of the columns are assigned a multi-line <c>DATEADD</c> or a correlated subquery, and a line-scoped
    /// read of those sees the function name and none of its clock base.</summary>
    private static IReadOnlyList<string> AssignmentsOf(string sql, string name, bool variable)
    {
        var pattern = variable
            ? @"(?<![\w.])" + Regex.Escape("@" + name) + @"(?:\s+[A-Za-z_]\w*(?:\s*\([^)]*\))?)?\s*=\s*"
            : @"(?<![\w.@])" + Regex.Escape(name) + @"\s*=\s*";

        return Regex.Matches(sql, pattern)
            .Select(m => BalancedFrom(sql, m.Index + m.Length))
            .ToArray();
    }

    private static string BalancedFrom(string sql, int start)
    {
        var depth = 0;

        for (var i = start; i < sql.Length; i++)
        {
            var c = sql[i];

            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                if (depth == 0)
                {
                    return sql[start..i].Trim();
                }

                depth--;
            }
            else if (depth == 0 && (c == ',' || c == ';'))
            {
                return sql[start..i].Trim();
            }
        }

        return sql[start..].Trim();
    }

    /// <summary>
    /// Every <c>AS &lt;alias&gt;</c> in the collector's SQL, mapped to the relation text it names. Two
    /// forms, because both carry provenance: a plain relation or table-valued function after
    /// <c>FROM</c>/<c>JOIN</c>/<c>APPLY</c>, and a DERIVED TABLE whose closing paren the <c>AS</c> follows —
    /// there the whole parenthesised body plus the function name in front of it is the relation, which is
    /// how <c>DmvBlockingSnapshotCollector</c>'s two <c>tat_b</c>/<c>tat_k</c> wraps and
    /// <c>DefaultTraceEventsCollector</c>'s <c>fn_trace_gettable</c> apply both resolve.
    /// </summary>
    private static Dictionary<string, HashSet<string>> AliasMap(string sql)
    {
        var map = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (var alias in Regex.Matches(sql, @"\bAS\s+(\w+)\b").Cast<Match>())
        {
            var head = sql[..alias.Index];
            var end = head.Length - 1;

            while (end >= 0 && char.IsWhiteSpace(head[end]))
            {
                end--;
            }

            string? relation = null;

            if (end >= 0 && head[end] == ')')
            {
                var depth = 0;

                for (var i = end; i >= 0; i--)
                {
                    if (head[i] == ')')
                    {
                        depth++;
                    }
                    else if (head[i] == '(')
                    {
                        depth--;

                        if (depth == 0)
                        {
                            /* Carry the callee name in front of the paren: it is the whole provenance of a
                               table-valued function like sys.fn_trace_gettable(...). */
                            var before = i;

                            while (before > 0 && char.IsWhiteSpace(head[before - 1]))
                            {
                                before--;
                            }

                            var name = Regex.Match(head[..before], @"[\w.\[\]]+$");
                            relation = head[(name.Success ? name.Index : i)..(end + 1)];
                            break;
                        }
                    }
                }
            }

            if (relation is null)
            {
                var keywords = Regex.Matches(head, @"\b(?:FROM|JOIN|APPLY)\b", RegexOptions.IgnoreCase);

                if (keywords.Count == 0)
                {
                    continue;
                }

                var last = keywords[^1];
                var span = head[(last.Index + last.Length)..];

                /* An AS that closes nothing but sits past an unbalanced ')' belongs to a construct this
                   walk did not parse; contribute no relation rather than a wrong one. */
                if (span.Count(c => c == ')') > span.Count(c => c == '('))
                {
                    continue;
                }

                relation = span;
            }

            relation = relation.Trim();

            if (relation.Length == 0)
            {
                continue;
            }

            if (!map.TryGetValue(alias.Groups[1].Value, out var set))
            {
                map[alias.Groups[1].Value] = set = new HashSet<string>(StringComparer.Ordinal);
            }

            set.Add(relation);
        }

        return map;
    }

    private static HashSet<ClockFrame> FramesInText(string text)
    {
        var found = new HashSet<ClockFrame>();

        foreach (var (marker, frame, _) in ExpressionMarkers)
        {
            if (text.Contains(marker, StringComparison.OrdinalIgnoreCase))
            {
                found.Add(frame);
            }
        }

        return found;
    }

    private static HashSet<ClockFrame> FramesInRelation(string relation, string column)
    {
        var found = new HashSet<ClockFrame>();

        foreach (var (name, columns, frame, _) in RelationMarkers)
        {
            if (relation.Contains(name, StringComparison.OrdinalIgnoreCase)
                && columns.Contains(column, StringComparer.Ordinal))
            {
                found.Add(frame);
            }
        }

        return found;
    }

    /// <summary>
    /// The frames one assignment's right-hand side yields: its own clock markers, plus a ONE-HOP resolution
    /// of every <c>alias.column</c> reference to its relation and of every <c>@variable</c> to its own
    /// assignments. A term that resolves to nothing contributes nothing — <c>t.timestamp</c> in the
    /// ring-buffer arithmetic is a tick COUNT, not a clock — so the verdict rests on the markers actually
    /// found, and a column with none of them is unreadable.
    /// </summary>
    private static HashSet<ClockFrame> FramesOfAssignment(string rhs, Dictionary<string, HashSet<string>> aliases, string sql, int depth = 0)
    {
        var found = FramesInText(rhs);

        foreach (var reference in Regex.Matches(rhs, @"(?<![\w.@])(\w+)\.(\w+)").Cast<Match>())
        {
            var alias = reference.Groups[1].Value;

            if (string.Equals(alias, "sys", StringComparison.OrdinalIgnoreCase)
                || string.Equals(alias, "msdb", StringComparison.OrdinalIgnoreCase)
                || string.Equals(alias, "dbo", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (aliases.TryGetValue(alias, out var relations))
            {
                foreach (var relation in relations)
                {
                    found.UnionWith(FramesInRelation(relation, reference.Groups[2].Value));
                }
            }
        }

        if (depth < 2)
        {
            foreach (var variable in Regex.Matches(rhs, @"@\w+").Cast<Match>().Select(m => m.Value).Distinct(StringComparer.OrdinalIgnoreCase))
            {
                if (string.Equals(variable, "@timestamp", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                foreach (var assignment in AssignmentsOf(sql, variable[1..], variable: true))
                {
                    if (string.Equals(assignment.Trim().TrimEnd(','), "NULL", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    found.UnionWith(FramesOfAssignment(assignment, aliases, sql, depth + 1));
                }
            }
        }

        return found;
    }

    /// <summary>The classifier's verdict for one (table, column): a single frame, or null when the T-SQL
    /// yields no marker at all or yields two that disagree. Null is an UNKNOWN and never a UTC.</summary>
    private static ClockFrame? ClassifyFromTsql(string collector, string column)
    {
        var sql = WithoutComments(File.ReadAllText(CollectorSourcePath(collector)));
        var aliases = AliasMap(sql);
        var frames = new HashSet<ClockFrame>();

        foreach (var assignment in AssignmentsOf(sql, column, variable: false))
        {
            frames.UnionWith(FramesOfAssignment(assignment, aliases, sql));
        }

        return frames.Count == 1 ? frames.Single() : null;
    }

    /// <summary>The whole SqlServer arm's frame register: the T-SQL verdict where there is one, the declared
    /// evidence where there is not.</summary>
    private static Dictionary<(string Table, string Column), ClockFrame> FrameRegister()
    {
        var register = new Dictionary<(string, string), ClockFrame>();

        foreach (var (table, column, collector, engine) in TimestampColumns())
        {
            if (engine != CollectorTargetEngine.SqlServer)
            {
                continue;
            }

            var frame = ClassifyFromTsql(collector, column)
                ?? NonTsqlProvenance
                    .Where(d => d.Table == table && d.Column == column)
                    .Select(d => (ClockFrame?)d.Frame)
                    .FirstOrDefault();

            if (frame is { } resolved)
            {
                register[(table, column)] = resolved;
            }
        }

        return register;
    }

    /// <summary>
    /// Every SqlServer timestamp column's frame comes from its own collector's query text, and a column
    /// whose provenance cannot be read there is answered by a declaration carrying the evidence that does
    /// decide it — never by a default. The unreadable set and the declaration set are pinned to EACH OTHER
    /// at set equality, so neither can drift alone.
    /// </summary>
    [Fact]
    public void EveryTimestampColumnsFrame_IsReadFromItsOwnCollectorsQueryText_OrDeclaredWithEvidence()
    {
        var unreadable = new List<string>();
        var serverLocal = 0;
        var utc = 0;

        foreach (var (table, column, collector, engine) in TimestampColumns())
        {
            if (engine != CollectorTargetEngine.SqlServer)
            {
                continue;
            }

            switch (ClassifyFromTsql(collector, column))
            {
                case ClockFrame.ServerLocal:
                    serverLocal++;
                    break;
                case ClockFrame.Utc:
                    utc++;
                    break;
                default:
                    unreadable.Add(table + "." + column);
                    break;
            }
        }

        Assert.Equal(TsqlServerLocalCount, serverLocal);
        Assert.Equal(TsqlUtcCount, utc);

        Assert.Equal(
            NonTsqlProvenance.Select(d => d.Table + "." + d.Column).OrderBy(x => x, StringComparer.Ordinal).ToArray(),
            unreadable.OrderBy(x => x, StringComparer.Ordinal).ToArray());

        Assert.Equal(NonTsqlProvenanceCount, NonTsqlProvenance.Length);

        /* And the register is CLOSED over the SqlServer arm: no column falls out of both arms. That is the
           property "must fail when provenance cannot be read" reduces to, stated as a count. */
        Assert.Equal(SqlServerTimestampColumnCount, FrameRegister().Count);
    }

    /// <summary>
    /// Each declaration's evidence is present in the file it names. Without this the declared arm is a bare
    /// assertion of a frame — the shape #3202 refused: "a frame nobody can read is the state the de-skew
    /// defects shipped in."
    /// </summary>
    [Fact]
    public void EveryNonTsqlProvenanceDeclaration_CarriesItsEvidenceInTheFileItNames()
    {
        foreach (var (table, column, _, evidenceFile, evidence, why) in NonTsqlProvenance)
        {
            var path = RepoPath(evidenceFile);

            Assert.True(File.Exists(path), $"{table}.{column}: {evidenceFile} is gone — update this deliberately");
            Assert.Contains(evidence, File.ReadAllText(path), StringComparison.Ordinal);
            Assert.False(string.IsNullOrWhiteSpace(why), $"{table}.{column}: a declaration with no reasoning");
        }
    }

    /* ═══════════════════════ 3. whether a NAME may stand in for its column ═══════════════════════ */

    /// <summary>
    /// A column NAME may be judged at a consumer site only when every table declaring it agrees on the
    /// frame. That set is DERIVED from the register above, and checked against
    /// <c>StoreSqlClockDisciplineTests.AmbiguousFrameColumns</c> — a list hand-written from per-table
    /// evidence, by a different guard, for a different hazard. Two instruments that arrived at the same
    /// three names independently; drift in either one fails here.
    ///
    /// <para>The register's literal declaration is asserted present FIRST. Without that anchor a rename in
    /// the sibling guard would leave this comparing an empty parse against an empty expectation, which is
    /// the shape of a check that passes by measuring nothing.</para>
    /// </summary>
    [Fact]
    public void FrameAmbiguousColumnNames_AreDerived_AndAgreeWithTheSiblingGuardsRegister()
    {
        var derived = FrameAmbiguousNames();

        Assert.Equal(new[] { "event_time", "last_execution_time", "sample_time" }, derived.OrderBy(x => x, StringComparer.Ordinal).ToArray());

        var sibling = File.ReadAllText(RepoPath("Darling/Darling.Tests/StoreSqlClockDisciplineTests.cs"));

        Assert.Contains("AmbiguousFrameColumns = [", sibling, StringComparison.Ordinal);

        var declaration = Regex.Match(sibling, @"AmbiguousFrameColumns\s*=\s*\[(?<items>[^\]]*)\]");

        Assert.True(declaration.Success, "StoreSqlClockDisciplineTests.AmbiguousFrameColumns no longer parses");

        var registered = Regex.Matches(declaration.Groups["items"].Value, @"""([^""]+)""")
            .Select(m => m.Groups[1].Value)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(3, registered.Length);
        Assert.Equal(registered, derived.OrderBy(x => x, StringComparer.Ordinal).ToArray());
    }

    private static HashSet<string> FrameAmbiguousNames()
    {
        var byName = new Dictionary<string, HashSet<ClockFrame>>(StringComparer.Ordinal);

        foreach (var ((_, column), frame) in FrameRegister())
        {
            if (!byName.TryGetValue(column, out var frames))
            {
                byName[column] = frames = new HashSet<ClockFrame>();
            }

            frames.Add(frame);
        }

        return byName.Where(kv => kv.Value.Count > 1).Select(kv => kv.Key).ToHashSet(StringComparer.Ordinal);
    }

    private static Dictionary<string, HashSet<string>> TablesByColumn()
    {
        var byName = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (var ((table, column), _) in FrameRegister())
        {
            if (!byName.TryGetValue(column, out var tables))
            {
                byName[column] = tables = new HashSet<string>(StringComparer.Ordinal);
            }

            tables.Add(table);
        }

        return byName;
    }

    /* ═══════════════════════ 4. the consumers ═══════════════════════ */

    /// <summary>A payload field stamped by <c>ToString("o")</c>. On a <c>DateTimeKind.Unspecified</c> value
    /// that renders NO offset suffix, so a server-local and a naive-UTC field serialise identically and the
    /// payload carries no in-band signal a caller could key on.</summary>
    private static Regex McpPayloadEmission(string field) =>
        new(@"(?<![\w.])" + Regex.Escape(field) + @"\s*=[^;,\r\n]*?ToString\(""o""\)");

    /// <summary>The two WPF renderers that ADD the collected offset — i.e. that take naive UTC — and the
    /// one that renders raw, i.e. that takes the server's own clock. <c>FormatServerTime</c> is Lite's
    /// <c>ForDisplay</c>, not its <c>FormatServerClock</c>: it adds the offset and names its parameter
    /// <c>utcTime</c>. Lite has no raw renderer at all, which is why a comment in the Darling viewer
    /// claiming parity with it is wrong in the direction that hid this.</summary>
    private static readonly (string Renderer, ClockFrame Expects)[] Renderers =
    [
        ("ForDisplay", ClockFrame.Utc),
        ("FormatServerTime", ClockFrame.Utc),
        ("FormatServerClock", ClockFrame.ServerLocal),
    ];

    private enum SiteLabel
    {
        /// <summary>A server-local column stamped into an MCP payload with no marking and no conversion,
        /// beside naive-UTC fields in the same object. #3206.</summary>
        McpPayloadUnmarked,

        /// <summary>A column rendered through the WPF renderer for the OTHER frame. #3207.</summary>
        DesktopRenderFrameMismatch,

        /// <summary>A server-local column whose READ converts it before the payload sees it, so the site is
        /// correct. Carried rather than filtered out, with the conversion's own text as evidence, because a
        /// silent exclusion is indistinguishable from a scan that stopped matching.</summary>
        DeSkewedAtRead,
    }

    /// <summary>
    /// Every consumer site the scans below find, labelled, pinned at SET EQUALITY with a per-file site
    /// COUNT. A bare total would let a Darling site vanish and a Lite one appear and still add up — the
    /// one-sided-port regression #2992 found nothing guarding against, and the shape three of the entries
    /// below are: Darling is correct exactly where Lite is wrong.
    ///
    /// <para><b>Nothing here is repaired by this change.</b> The MCP rows are #3206's work and the render
    /// rows are #3207's. Set equality is the ratchet in both directions: adding an offender fails, and so
    /// does fixing one without deleting its row.</para>
    ///
    /// <para><b>Tables</b> is the census table(s) the site's column can come from, after resolution —
    /// joined with <c>+</c> when several agree on the frame. It is part of the KEY, not a note: without it
    /// a fix at one <c>last_execution_time</c> site in <c>ViewerHistoryRows.cs</c> and a regression at
    /// another in the same file would cancel in a bare count.</para>
    /// </summary>
    private static readonly (SiteLabel Label, string File, string Column, string Tables, int Sites, string Why)[] Inventory =
    [
        /* ── MCP payloads: a server-local value stamped straight into the object (#3206) ── */
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs",
            "blocked_last_tran_started", "blocked_process_reports+dmv_blocking_snapshots", 1,
            "get_blocking; the same payload's event_time is UTC on both arms"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs",
            "blocking_last_tran_started", "blocked_process_reports+dmv_blocking_snapshots", 1,
            "get_blocking; on a DMV-fallback row the two frames are mixed within one row"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs",
            "blocked_last_batch_started", "blocked_process_reports", 1, "get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs",
            "blocking_last_batch_started", "blocked_process_reports", 1, "get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs",
            "blocked_last_batch_completed", "blocked_process_reports", 1, "get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs",
            "blocking_last_batch_completed", "blocked_process_reports", 1, "get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpJobTools.cs",
            "start_time", "running_jobs", 1,
            "get_running_jobs; collection_time in the same object is UTC, so a job started seconds ago "
            + "reads as a four-hour runner — a long-running-job alert's exact signature"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectStatsTools.cs",
            "last_user_access", "index_object_stats", 1,
            "get_index_usage; the payload field is a GREATEST() alias over the four last_user_* columns, and "
            + "this payload carries no UTC field at all, so nothing in it contradicts the wrong reading"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCorrectionTools.cs",
            "valid_since", "plan_correction", 1, "get_plan_corrections; as_of and collection_time are UTC"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCorrectionTools.cs",
            "last_refresh", "plan_correction", 1, "get_plan_corrections"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCorrectionTools.cs",
            "execute_action_initiated_time", "plan_correction", 1, "get_plan_corrections"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCorrectionTools.cs",
            "revert_action_initiated_time", "plan_correction", 1, "get_plan_corrections"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPvsTools.cs",
            "aborted_version_cleaner_start_time", "pvs_stats", 1, "get_pvs_stats; as_of in the same object is UTC"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPvsTools.cs",
            "aborted_version_cleaner_end_time", "pvs_stats", 1, "get_pvs_stats"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPvsTools.cs",
            "offrow_version_cleaner_start_time", "pvs_stats", 1,
            "get_pvs_stats; the tool's own description says a start without an end means mid-run, so the "
            + "phantom staleness lands on the question it exists to answer"),
        (SiteLabel.McpPayloadUnmarked, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPvsTools.cs",
            "offrow_version_cleaner_end_time", "pvs_stats", 1, "get_pvs_stats"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpBlockingTools.cs",
            "blocked_last_tran_started", "blocked_process_reports+dmv_blocking_snapshots", 1, "Lite get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpBlockingTools.cs",
            "blocking_last_tran_started", "blocked_process_reports+dmv_blocking_snapshots", 1, "Lite get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpBlockingTools.cs",
            "blocked_last_batch_started", "blocked_process_reports", 1, "Lite get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpBlockingTools.cs",
            "blocking_last_batch_started", "blocked_process_reports", 1, "Lite get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpBlockingTools.cs",
            "blocked_last_batch_completed", "blocked_process_reports", 1, "Lite get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpBlockingTools.cs",
            "blocking_last_batch_completed", "blocked_process_reports", 1, "Lite get_blocking"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpJobTools.cs", "start_time", "running_jobs", 1,
            "Lite get_running_jobs"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpObjectStatsTools.cs", "last_user_access", "index_object_stats", 1,
            "Lite get_index_usage"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpPlanCacheSchedulerTools.cs", "oldest_plan_create_time",
            "plan_cache_stats", 1,
            "Lite get_plan_cache_bloat, and the only offending field with NO Darling counterpart: Darling's "
            + "plan-cache tool does not emit it, so this one is Lite-side only"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpPlanCorrectionTools.cs", "valid_since", "plan_correction", 1,
            "Lite get_plan_corrections"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpPlanCorrectionTools.cs", "last_refresh", "plan_correction", 1,
            "Lite get_plan_corrections"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpPlanCorrectionTools.cs", "execute_action_initiated_time",
            "plan_correction", 1, "Lite get_plan_corrections"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpPlanCorrectionTools.cs", "revert_action_initiated_time",
            "plan_correction", 1, "Lite get_plan_corrections"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpPvsTools.cs", "aborted_version_cleaner_start_time", "pvs_stats", 1,
            "Lite get_pvs_stats"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpPvsTools.cs", "aborted_version_cleaner_end_time", "pvs_stats", 1,
            "Lite get_pvs_stats"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpPvsTools.cs", "offrow_version_cleaner_start_time", "pvs_stats", 1,
            "Lite get_pvs_stats"),
        (SiteLabel.McpPayloadUnmarked, "Lite/Mcp/McpPvsTools.cs", "offrow_version_cleaner_end_time", "pvs_stats", 1,
            "Lite get_pvs_stats"),

        /* ── MCP payloads that are CORRECT because the read converts first (#3202, #1262) ── */
        (SiteLabel.DeSkewedAtRead, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDefaultTraceTools.cs",
            "event_time", "default_trace_events", 1,
            "#3202: the read projects event_time_utc and the payload stamps THAT, so the field name is the "
            + "column's while the value is not"),
        (SiteLabel.DeSkewedAtRead, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs",
            "sample_time", "cpu_utilization_stats", 1,
            "#1262: get_cpu_utilization de-skews sample_time in SQL by the per-batch quantised offset, then "
            + "buckets the de-skewed value, so the emitted expression is the bucket key"),

        /* ── desktop renders: the column's frame against the renderer's (#3207) ── */
        (SiteLabel.DesktopRenderFrameMismatch,
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.QueryStore.cs",
            "first_execution_time", "query_store_stats", 1,
            "naive UTC rendered RAW, so four hours late; Lite gets this one right"),
        (SiteLabel.DesktopRenderFrameMismatch,
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.QueryStore.cs",
            "last_execution_time", "query_store_stats", 1, "naive UTC rendered RAW, so four hours late"),
        (SiteLabel.DesktopRenderFrameMismatch,
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.QueryStoreRegressions.cs",
            "last_execution_time", "query_store_stats", 1,
            "naive UTC rendered RAW; the doc at :29 states the wrong frame outright and appeals to the "
            + "sibling Query Store tab, which is how one wrong site became three"),
        (SiteLabel.DesktopRenderFrameMismatch,
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.RunningJobs.cs",
            "start_time", "running_jobs", 1,
            "server-local through ForDisplay, so four hours EARLY, beside a CollectionTimeLocal that is right"),
        (SiteLabel.DesktopRenderFrameMismatch,
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerHistoryRows.cs",
            "first_execution_time", "query_store_stats", 1,
            "the file header lumps Query Store in with the DMVs; true of query_stats/procedure_stats in the "
            + "same file, false of query_store_stats"),
        (SiteLabel.DesktopRenderFrameMismatch,
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerHistoryRows.cs",
            "last_execution_time", "query_store_stats", 1,
            "same file, same header; the query_stats and procedure_stats rows beside it are correct, which "
            + "is why the Tables column is part of this key"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.Blocking.cs",
            "blocked_last_tran_started", "blocked_process_reports+dmv_blocking_snapshots", 1,
            "server-local through FormatServerTime, which adds the offset again; EventTimeLocal in the same "
            + "class is correct"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.Blocking.cs",
            "blocked_last_batch_started", "blocked_process_reports", 1, "server-local through FormatServerTime"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.Blocking.cs",
            "blocked_last_batch_completed", "blocked_process_reports", 1, "server-local through FormatServerTime"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.Blocking.cs",
            "tran_start_time", "query_snapshots", 1,
            "server-local through FormatServerTime; Darling renders the same column through FormatServerClock "
            + "and its comment claims the two agree"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.PlanCorrection.cs",
            "valid_since", "plan_correction", 1,
            "server-local through FormatServerTime; not in #3207's list, found by deriving the census"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.PlanCorrection.cs",
            "last_refresh", "plan_correction", 1, "server-local through FormatServerTime; not in #3207's list"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.PlanCorrection.cs",
            "execute_action_initiated_time", "plan_correction", 1,
            "server-local through FormatServerTime; not in #3207's list"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.PlanCorrection.cs",
            "revert_action_initiated_time", "plan_correction", 1,
            "server-local through FormatServerTime; not in #3207's list"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.QueryStats.cs",
            "creation_time", "query_stats", 2,
            "server-local through FormatServerTime, in two row types; the Lite read de-skews only the WINDOW "
            + "BOUND ($2 + $5 * INTERVAL '1' MINUTE) and returns the raw local value, which is #3198's "
            + "half-fix shape. Darling renders the same column through FormatServerClock"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.QueryStats.cs",
            "cached_time", "procedure_stats", 2, "server-local through FormatServerTime; not in #3207's list"),
        (SiteLabel.DesktopRenderFrameMismatch, "Lite/Services/LocalDataService.QueryStats.cs",
            "last_execution_time", "procedure_stats+query_stats", 4,
            "server-local through FormatServerTime in four row types; both tables agree on the frame, so the "
            + "ambiguous NAME resolves here even though it does not in general"),
    ];

    private const int McpPayloadUnmarkedSites = 33;
    private const int DesktopRenderMismatchSites = 22;
    private const int DeSkewedAtReadSites = 2;

    /* ═══════════════════════ 5. resolving which table a site's column came from ═══════════════════════ */

    /// <summary>A table or view named by a <c>FROM</c>/<c>JOIN</c> in a SQL literal, with Lite's
    /// parquet-union <c>v_</c> prefix and Darling's <c>collect.</c> schema normalised away — the same
    /// column, reached through the reading shape each SKU uses.</summary>
    private static readonly Regex SqlRelation =
        new(@"\b(?:FROM|JOIN)\s+(?:collect\.)?(?:v_)?([a-z][a-z0-9_]*)", RegexOptions.IgnoreCase);

    /// <summary>
    /// Row-type files carry no SQL, so the table comes from the enclosing type instead. Three entries, each
    /// asserted present in the file it names by
    /// <see cref="RowTypeTables_AreDeclaredAgainstTypesThatExist"/>. This exists because
    /// <c>ViewerHistoryRows.cs</c> holds all three history row types side by side and the frame differs
    /// between them — the header comment that lumps them together is what #3207 identifies as the cause.
    /// </summary>
    private static readonly (string RowType, string Table)[] RowTypeTables =
    [
        ("ViewerQueryStatsHistoryRow", "query_stats"),
        ("ViewerProcedureStatsHistoryRow", "procedure_stats"),
        ("ViewerQueryStoreHistoryRow", "query_store_stats"),
    ];

    [Fact]
    public void RowTypeTables_AreDeclaredAgainstTypesThatExist()
    {
        var rows = File.ReadAllText(RepoPath("Darling/PerformanceMonitor.Darling.Viewer/ViewerHistoryRows.cs"));
        var tables = TimestampColumns().Select(c => c.Table).ToHashSet(StringComparer.Ordinal);

        foreach (var (rowType, table) in RowTypeTables)
        {
            Assert.Contains("class " + rowType, rows, StringComparison.Ordinal);
            Assert.Contains(table, tables);
        }
    }

    /// <summary>
    /// The census tables a site's column can have come from, joined with <c>+</c>, or null when the site
    /// cannot be resolved to a single frame and this guard therefore declines to judge it.
    ///
    /// <para>An unambiguous NAME needs no resolution — every table declaring it agrees. An ambiguous one is
    /// narrowed by the tables the site's own file reads, plus those read by any same-directory
    /// <c>*Reader</c> type the file names (the Darling MCP tools hold no SQL; their readers do), plus the
    /// enclosing row type for files that hold neither.</para>
    /// </summary>
    private static string? ResolveTables(string path, string text, int position, string column)
    {
        var candidates = TablesByColumn().TryGetValue(column, out var all)
            ? all
            : new HashSet<string>(StringComparer.Ordinal);

        if (candidates.Count == 0)
        {
            return null;
        }

        if (!FrameAmbiguousNames().Contains(column))
        {
            return Join(candidates);
        }

        var rowType = EnclosingType(text, position);
        var declared = RowTypeTables.FirstOrDefault(r => r.RowType == rowType);

        if (declared.RowType is not null && candidates.Contains(declared.Table))
        {
            return declared.Table;
        }

        var reachable = ReadableRelations(path, text);
        var narrowed = candidates.Where(reachable.Contains).ToHashSet(StringComparer.Ordinal);

        if (narrowed.Count == 0)
        {
            return null;
        }

        var register = FrameRegister();
        var frames = narrowed.Select(t => register[(t, column)]).Distinct().ToArray();

        return frames.Length == 1 ? Join(narrowed) : null;
    }

    private static string Join(IEnumerable<string> tables) =>
        string.Join("+", tables.OrderBy(t => t, StringComparer.Ordinal));

    private static HashSet<string> ReadableRelations(string path, string text)
    {
        var relations = SqlRelation.Matches(text).Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        var directory = Path.GetDirectoryName(path)!;

        foreach (var reader in Regex.Matches(text, @"\b([A-Z]\w*Reader)\b").Cast<Match>()
                     .Select(m => m.Groups[1].Value)
                     .Distinct(StringComparer.Ordinal))
        {
            var sibling = Path.Combine(directory, reader + ".cs");

            if (File.Exists(sibling))
            {
                relations.UnionWith(SqlRelation.Matches(File.ReadAllText(sibling)).Select(m => m.Groups[1].Value));
            }
        }

        return relations;
    }

    private static string? EnclosingType(string text, int position)
    {
        string? enclosing = null;

        foreach (var declaration in Regex.Matches(text, @"\bclass\s+(\w+)").Cast<Match>())
        {
            if (declaration.Index >= position)
            {
                break;
            }

            enclosing = declaration.Groups[1].Value;
        }

        return enclosing;
    }

    private static string Pascal(string column) =>
        string.Concat(column.Split('_').Select(part => part.Length == 0
            ? part
            : char.ToUpperInvariant(part[0]) + part[1..]));

    /* ═══════════════════════ 6. the two scans ═══════════════════════ */

    /* Corpus floors, so a broken glob fails instead of reporting a clean bill of health. Measured on dev
       at cb208f6a4: 130 MCP source files across both SKUs, 131 ToString("o") sites over 27 files in
       Darling's Mcp/ alone, and 459 production .cs files across the Darling viewer and Lite. */
    private const int MinimumMcpFiles = 100;
    private const int MinimumMcpEmissionSites = 110;
    private const int MinimumRenderFiles = 380;

    /// <summary>
    /// A payload field named after a store column whose frame is ServerLocal, stamped by
    /// <c>ToString("o")</c>. Derived end to end: the field name comes from the catalog, the frame from the
    /// collector's own SQL, the table from the reader the tool names.
    ///
    /// <para>A field can also be a projection ALIAS over census columns rather than a column name —
    /// <c>get_index_usage</c>'s <c>last_user_access</c> is a <c>GREATEST()</c> over four of them — so those
    /// aliases are derived too and pinned by
    /// <see cref="RenamedProjections_ThatInheritAServerLocalFrame_ArePinned"/>. Without that hop the one
    /// offending payload with NO UTC field beside it is the one this scan would miss.</para>
    /// </summary>
    [Fact]
    public void EveryMcpPayloadSiteOfAServerLocalColumn_IsInTheInventory()
    {
        var files = McpSourceFiles().ToArray();
        var emissions = 0;
        var darlingEmissionFiles = new HashSet<string>(StringComparer.Ordinal);

        foreach (var path in files.Where(p => Relative(p).StartsWith("Darling/", StringComparison.Ordinal)))
        {
            var count = Regex.Matches(File.ReadAllText(path), @"ToString\(""o""\)").Count;
            emissions += count;

            if (count > 0)
            {
                darlingEmissionFiles.Add(Relative(path));
            }
        }

        Assert.True(files.Length >= MinimumMcpFiles, $"only {files.Length} MCP files scanned — check the globs");
        Assert.True(emissions >= MinimumMcpEmissionSites, $"only {emissions} ToString(\"o\") sites in Darling's Mcp/");
        Assert.True(darlingEmissionFiles.Count >= 25, $"only {darlingEmissionFiles.Count} Darling Mcp/ files emit one");

        var register = FrameRegister();
        var found = new Dictionary<(string File, string Column, string Tables), int>();
        var declined = new Dictionary<(string File, string Column), int>();
        var fields = ServerLocalEmittableFields();

        foreach (var path in files)
        {
            var text = File.ReadAllText(path);

            foreach (var (field, aliasTable) in fields)
            {
                foreach (var site in McpPayloadEmission(field).Matches(text).Cast<Match>())
                {
                    var tables = aliasTable ?? ResolveTables(path, text, site.Index, field);

                    if (tables is null)
                    {
                        Bump(declined, (Relative(path), field));
                        continue;
                    }

                    /* A ServerLocal column NAME can also belong to a UTC table — event_time is five
                       tables and two frames — so the frame is re-read from the RESOLVED table and never
                       inherited from the field's presence in the ServerLocal list. */
                    if (aliasTable is null && register[(tables.Split('+')[0], field)] != ClockFrame.ServerLocal)
                    {
                        continue;
                    }

                    Bump(found, (Relative(path), field, tables));
                }
            }
        }

        AssertMatchesInventory(found, [SiteLabel.McpPayloadUnmarked, SiteLabel.DeSkewedAtRead]);

        Assert.Equal(
            DeclinedAmbiguousMcpSites.OrderBy(d => d.File + "|" + d.Column, StringComparer.Ordinal)
                .Select(d => $"{d.File}|{d.Column}|{d.Sites}").ToArray(),
            declined.OrderBy(d => d.Key.File + "|" + d.Key.Column, StringComparer.Ordinal)
                .Select(d => $"{d.Key.File}|{d.Key.Column}|{d.Value}").ToArray());
    }

    /// <summary>
    /// Sites this guard DECLINES: a frame-ambiguous field name whose table cannot be narrowed to one frame
    /// from the file's own reach. Pinned at set equality so the decline cannot grow quietly, which is the
    /// only thing that makes "cannot key on the column name" a cost rather than an excuse.
    ///
    /// <para>All nineteen are Query Store, system_health, long-query or PostgreSQL reads, and #3206's own
    /// sweep checked each of them rather than assuming: <c>query_store_stats.last_execution_time</c> is
    /// genuinely naive UTC, Lite's Default Trace read already de-skews (#2967), and
    /// <c>memory_pressure_events.sample_time</c> is UTC per #2932. None is an offender — but this guard is
    /// not what establishes that, and it does not pretend to.</para>
    /// </summary>
    private static readonly (string File, string Column, int Sites)[] DeclinedAmbiguousMcpSites =
    [
        ("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs", "last_execution_time", 1),
        ("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgCpuUtilizationTools.cs", "sample_time", 1),
        ("Lite/Mcp/McpBlockingTools.cs", "event_time", 2),
        ("Lite/Mcp/McpCpuTools.cs", "sample_time", 1),
        ("Lite/Mcp/McpDefaultTraceTools.cs", "event_time", 1),
        ("Lite/Mcp/McpHealthParserTools.cs", "event_time", 9),
        ("Lite/Mcp/McpLongQueryTools.cs", "event_time", 1),
        ("Lite/Mcp/McpMemoryTools.cs", "sample_time", 1),
        ("Lite/Mcp/McpQueryTools.cs", "last_execution_time", 2),
    ];

    /// <summary>Payload field names that carry a ServerLocal frame: the ServerLocal census columns, plus
    /// projection aliases derived from the MCP SQL that inherit one.</summary>
    private static IReadOnlyList<(string Field, string? AliasTable)> ServerLocalEmittableFields()
    {
        var register = FrameRegister();
        var fields = register
            .Where(kv => kv.Value == ClockFrame.ServerLocal)
            .Select(kv => (Field: kv.Key.Column, AliasTable: (string?)null))
            .Distinct()
            .ToList();

        fields.AddRange(RenamedServerLocalProjections().Select(r => (r.Alias, (string?)r.Table)));

        return fields;
    }

    /// <summary>
    /// A projection alias in MCP SQL whose expression names only ServerLocal census columns: the alias
    /// inherits the frame, and the payload field is the ALIAS, so a scan keyed on column names alone cannot
    /// see it.
    /// </summary>
    private static IReadOnlyList<(string Alias, string Table)> RenamedServerLocalProjections()
    {
        var register = FrameRegister();
        var byColumn = TablesByColumn();
        var found = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var path in McpSourceFiles())
        {
            foreach (var projection in Regex.Matches(File.ReadAllText(path), @"([^\r\n]*?)\s+AS\s+([a-z][a-z0-9_]*)\s*,",
                         RegexOptions.IgnoreCase).Cast<Match>())
            {
                var alias = projection.Groups[2].Value;

                if (byColumn.ContainsKey(alias))
                {
                    continue;
                }

                var sourced = byColumn.Keys
                    .Where(c => Regex.IsMatch(projection.Groups[1].Value, @"(?<![\w.])" + Regex.Escape(c) + @"(?![\w])"))
                    .ToArray();

                if (sourced.Length == 0
                    || sourced.SelectMany(c => byColumn[c].Select(t => register[(t, c)])).Distinct().Count() != 1
                    || register[(byColumn[sourced[0]].First(), sourced[0])] != ClockFrame.ServerLocal)
                {
                    continue;
                }

                found[alias] = Join(byColumn[sourced[0]]);
            }
        }

        return found.Select(kv => (kv.Key, kv.Value)).OrderBy(r => r.Key, StringComparer.Ordinal).ToArray();
    }

    [Fact]
    public void RenamedProjections_ThatInheritAServerLocalFrame_ArePinned()
    {
        Assert.Equal(
            new[] { ("last_user_access", "index_object_stats") },
            RenamedServerLocalProjections().ToArray());
    }

    /// <summary>
    /// Every desktop render site whose column's frame disagrees with its renderer's. Both directions are
    /// hazards and both are inventoried: a server-local value through <c>ForDisplay</c>/<c>FormatServerTime</c>
    /// subtracts the offset a second time and displays four hours EARLY on the fleet's measured -240, while a
    /// naive-UTC value through <c>FormatServerClock</c> displays four hours LATE.
    /// </summary>
    [Fact]
    public void EveryDesktopRenderSiteWhoseFrameDisagreesWithItsRenderer_IsInTheInventory()
    {
        var files = RenderSourceFiles().ToArray();

        Assert.True(files.Length >= MinimumRenderFiles, $"only {files.Length} viewer/Lite files scanned — check the globs");

        var register = FrameRegister();
        var found = new Dictionary<(string File, string Column, string Tables), int>();
        var judged = 0;
        var declined = new List<string>();

        foreach (var path in files)
        {
            var text = File.ReadAllText(path);

            foreach (var column in register.Keys.Select(k => k.Column).Distinct(StringComparer.Ordinal))
            {
                foreach (var (renderer, expects) in Renderers)
                {
                    var pattern = @"\b" + renderer + @"\s*\(\s*" + Regex.Escape(Pascal(column)) + @"\b";

                    foreach (var site in Regex.Matches(text, pattern).Cast<Match>())
                    {
                        var tables = ResolveTables(path, text, site.Index, column);

                        if (tables is null)
                        {
                            declined.Add($"{Relative(path)}|{column}|{renderer}");
                            continue;
                        }

                        judged++;

                        if (register[(tables.Split('+')[0], column)] != expects)
                        {
                            Bump(found, (Relative(path), column, tables));
                        }
                    }
                }
            }
        }

        /* A floor on the sites actually JUDGED, not only on the files opened: a matcher that stopped
           recognising the renderers would otherwise report an empty offender set over 459 files. */
        Assert.True(judged >= 30, $"only {judged} render sites were judged — check the renderer patterns");
        Assert.Empty(declined);

        AssertMatchesInventory(found, [SiteLabel.DesktopRenderFrameMismatch]);
    }

    /// <summary>Each <see cref="SiteLabel.DeSkewedAtRead"/> entry's conversion is present in the reader it
    /// depends on. Without this the label is a way to delete a site from the census by asserting it is
    /// fine.</summary>
    [Fact]
    public void EveryDeSkewedAtReadSite_CarriesItsConversionInTheReaderItDependsOn()
    {
        var evidence = new (string Column, string ReaderFile, string Conversion)[]
        {
            ("event_time", "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingDefaultTraceReader.cs",
                "dte.event_time - make_interval(mins => svr.offset_minutes) AS event_time_utc"),
            ("sample_time", "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingDataReader.cs",
                "MAX(sample_time) OVER (PARTITION BY server_id, collection_time) - collection_time"),
        };

        Assert.Equal(
            Inventory.Count(i => i.Label == SiteLabel.DeSkewedAtRead),
            evidence.Length);

        foreach (var (column, readerFile, conversion) in evidence)
        {
            Assert.Contains(column, Inventory.Where(i => i.Label == SiteLabel.DeSkewedAtRead).Select(i => i.Column));
            Assert.Contains(conversion, File.ReadAllText(RepoPath(readerFile)), StringComparison.Ordinal);
        }
    }

    [Fact]
    public void InventorySiteCounts_MatchTheirLabels()
    {
        Assert.Equal(McpPayloadUnmarkedSites, Inventory.Where(i => i.Label == SiteLabel.McpPayloadUnmarked).Sum(i => i.Sites));
        Assert.Equal(DesktopRenderMismatchSites, Inventory.Where(i => i.Label == SiteLabel.DesktopRenderFrameMismatch).Sum(i => i.Sites));
        Assert.Equal(DeSkewedAtReadSites, Inventory.Where(i => i.Label == SiteLabel.DeSkewedAtRead).Sum(i => i.Sites));

        foreach (var (label, file, column, tables, sites, why) in Inventory)
        {
            Assert.True(File.Exists(RepoPath(file)), $"{label} {file}: gone — update this guard deliberately");
            Assert.True(sites > 0, $"{file}.{column}: a zero-site inventory row asserts nothing");
            Assert.False(string.IsNullOrWhiteSpace(why), $"{file}.{column}: an inventory row with no reasoning");
            Assert.False(string.IsNullOrWhiteSpace(tables), $"{file}.{column}: an inventory row with no table");
        }
    }

    /* ═══════════════════════ 7. the discriminators, both directions ═══════════════════════ */

    /// <summary>
    /// Every matcher this guard rests on, pinned against the shapes that shipped in the defects AND against
    /// the benign forms it must not drag in. A census scan whose matcher has quietly stopped matching
    /// reports a clean bill of health over hundreds of files, which is strictly worse than no scan — and
    /// this shape has more ways to assert nothing than any other, so each one is checked in both
    /// directions rather than trusted because the suite is green.
    /// </summary>
    [Fact]
    public void TheDiscriminators_FlagTheShippedShapes_AndPassTheBenignOnes()
    {
        /* The MCP emission: the real forms, and the forms that must not count. */
        Assert.Matches(McpPayloadEmission("start_time"), "                start_time = r.StartTime.ToString(\"o\"),");
        Assert.Matches(
            McpPayloadEmission("aborted_version_cleaner_start_time"),
            "                aborted_version_cleaner_start_time = r.AbortedCleanerStartTime?.ToString(\"o\"),");
        Assert.Matches(
            McpPayloadEmission("last_user_access"),
            "                last_user_access = r.LastUserAccess?.ToString(\"o\")");
        /* A same-named field on a DIFFERENT line must not be reached across the newline: the emission and
           its field have to be one statement, or every payload object would match every field name in it. */
        Assert.DoesNotMatch(McpPayloadEmission("start_time"), "start_time = r.StartTime,\r\n  other = x.ToString(\"o\"),");
        Assert.DoesNotMatch(McpPayloadEmission("start_time"), "                start_time = r.StartTime,");
        Assert.DoesNotMatch(McpPayloadEmission("event_time"), "    /// windowed on event_time, which is UTC");
        /* Substring safety in both directions: the field is a whole token. */
        Assert.DoesNotMatch(McpPayloadEmission("event_time"), "last_event_time = r.X.ToString(\"o\"),");
        Assert.DoesNotMatch(McpPayloadEmission("start_time"), "r.start_time.ToString(\"o\")");

        /* SqlRelation: the reading forms both SKUs use, and the mentions that are not reads. */
        Assert.Equal("query_store_stats", SqlRelation.Match("        FROM query_store_stats AS qss").Groups[1].Value);
        Assert.Equal("blocked_process_reports", SqlRelation.Match("FROM v_blocked_process_reports").Groups[1].Value);
        Assert.Equal("default_trace_events", SqlRelation.Match("FROM collect.default_trace_events AS f").Groups[1].Value);
        Assert.Equal("query_snapshots", SqlRelation.Match("LEFT JOIN query_snapshots AS qs").Groups[1].Value);
        Assert.DoesNotMatch(SqlRelation, "CREATE TABLE IF NOT EXISTS running_jobs (");

        /* AssignmentsOf: the alias-on-left form, the DECLARE-with-initialiser and SELECT-assignment forms of
           a variable, and the projections it must not mistake for an assignment. Balanced, so the
           multi-line DATEADD keeps its clock base. */
        Assert.Equal("ft.StartTime", Assert.Single(AssignmentsOf("    event_time = ft.StartTime,", "event_time", variable: false)));
        Assert.Empty(AssignmentsOf("    x.event_time,", "event_time", variable: false));
        Assert.Empty(AssignmentsOf("    @sqlserver_start_time = osi.sqlserver_start_time", "sqlserver_start_time", variable: false));
        Assert.Equal(
            "SYSUTCDATETIME()",
            Assert.Single(AssignmentsOf("    @now datetime2(7) = SYSUTCDATETIME();", "now", variable: true)));
        /* The SELECT-assignment form runs to the statement terminator, not the line end, which is what
           carries ServerPropertiesCollector's FROM into the span the relation markers are read from. */
        Assert.Equal(
            "osi.sqlserver_start_time\r\nFROM sys.dm_os_sys_info AS osi",
            Assert.Single(AssignmentsOf(
                "        @sqlserver_start_time = osi.sqlserver_start_time\r\nFROM sys.dm_os_sys_info AS osi;",
                "sqlserver_start_time",
                variable: true)));
        Assert.Contains(
            "SYSDATETIME()",
            Assert.Single(AssignmentsOf(
                "    sample_time = DATEADD(\r\n        MILLISECOND, -(x % 1000),\r\n        DATEADD(SECOND, -(y / 1000), SYSDATETIME())),\r\n    next = 1,",
                "sample_time",
                variable: false)));

        /* AliasMap: the plain relation, the table-valued function whose AS follows its argument list, and
           the derived table whose body carries the provenance. All three ship in the collectors, and the
           middle one is default_trace_events' whole frame. */
        var plain = AliasMap("FROM sys.dm_exec_query_stats AS qs");
        Assert.Contains("sys.dm_exec_query_stats", Assert.Single(plain["qs"]));

        var applied = AliasMap("CROSS APPLY sys.fn_trace_gettable\r\n(\r\n    @path,\r\n    DEFAULT\r\n) AS ft");
        Assert.Contains("sys.fn_trace_gettable", Assert.Single(applied["ft"]));

        var derived = AliasMap("JOIN\r\n(\r\n    SELECT tat.transaction_begin_time\r\n    FROM sys.dm_tran_active_transactions AS tat\r\n) AS tat_b");
        Assert.Contains("sys.dm_tran_active_transactions", Assert.Single(derived["tat_b"]));

        /* FramesInRelation is scoped per (relation, column) and NOT by column name: the same DMV supplies a
           server-local clock and a frame-free tick counter, and a relation-keyed rule reads the ring-buffer
           arithmetic as local when the collector deliberately bases it on SYSUTCDATETIME(). */
        Assert.Equal([ClockFrame.ServerLocal], FramesInRelation("sys.dm_os_sys_info", "sqlserver_start_time"));
        Assert.Empty(FramesInRelation("sys.dm_os_sys_info", "ms_ticks"));
        Assert.Empty(FramesInRelation("sys.dm_db_resource_stats", "end_time"));
        Assert.Empty(FramesInRelation("sys.query_store_runtime_stats", "last_execution_time"));

        /* FramesInText: the clock markers, and the fact that a UTC one is not a local one. */
        Assert.Equal([ClockFrame.Utc], FramesInText("evt.value('(@timestamp)[1]', 'datetime2')"));
        Assert.Equal([ClockFrame.Utc], FramesInText("CONVERT(datetime2, qsrsi.start_time AT TIME ZONE 'UTC')"));
        Assert.Equal([ClockFrame.ServerLocal], FramesInText("DATEDIFF(SECOND, ja.start_execution_date, GETDATE())"));
        Assert.Empty(FramesInText("qs.creation_time"));

        /* Pascal: the column-to-property transform the render scan keys on. */
        Assert.Equal("FirstExecutionTime", Pascal("first_execution_time"));
        Assert.Equal("BlockedLastTranStarted", Pascal("blocked_last_tran_started"));

        /* And the two renderer families are distinguished, not merged: three names, two expectations. */
        Assert.Equal(3, Renderers.Length);
        Assert.Equal(2, Renderers.Select(r => r.Expects).Distinct().Count());
        Assert.Equal(ClockFrame.ServerLocal, Renderers.Single(r => r.Renderer == "FormatServerClock").Expects);
        Assert.Equal(ClockFrame.Utc, Renderers.Single(r => r.Renderer == "FormatServerTime").Expects);
    }

    /// <summary>
    /// The marker vocabulary is not allowed to collapse: every entry has to be reachable, or a mis-typed
    /// relation name would sit in the list forever contributing nothing while the column it was added for
    /// silently became unreadable — and unreadable columns are answered by declaration, so the collapse
    /// would surface as a growing declared arm rather than as a failure here.
    /// </summary>
    [Fact]
    public void EveryProvenanceMarker_IsReachedByAtLeastOneColumnInTheCensus()
    {
        var used = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (_, column, collector, engine) in TimestampColumns())
        {
            if (engine != CollectorTargetEngine.SqlServer)
            {
                continue;
            }

            var sql = WithoutComments(File.ReadAllText(CollectorSourcePath(collector)));
            var aliases = AliasMap(sql);

            foreach (var assignment in AssignmentsOf(sql, column, variable: false))
            {
                foreach (var (marker, _, _) in ExpressionMarkers)
                {
                    if (assignment.Contains(marker, StringComparison.OrdinalIgnoreCase))
                    {
                        used.Add(marker);
                    }
                }

                foreach (var reference in Regex.Matches(assignment, @"(?<![\w.@])(\w+)\.(\w+)").Cast<Match>())
                {
                    if (!aliases.TryGetValue(reference.Groups[1].Value, out var relations))
                    {
                        continue;
                    }

                    foreach (var (name, columns, _, _) in RelationMarkers)
                    {
                        if (columns.Contains(reference.Groups[2].Value, StringComparer.Ordinal)
                            && relations.Any(r => r.Contains(name, StringComparison.OrdinalIgnoreCase)))
                        {
                            used.Add(name);
                        }
                    }
                }
            }
        }

        /* SYSUTCDATETIME and GETDATE arrive through a variable or a sibling assignment rather than the
           column's own right-hand side; naming them keeps this from demanding an unreachable directness. */
        var reachedIndirectly = new[] { "SYSUTCDATETIME", "GETDATE", "GETUTCDATE" };

        foreach (var (marker, _, why) in ExpressionMarkers)
        {
            Assert.True(
                used.Contains(marker) || reachedIndirectly.Contains(marker, StringComparer.Ordinal),
                $"expression marker '{marker}' ({why}) is reached by no census column — it is either mis-typed "
                + "or the column it was added for has gone unreadable");
        }

        foreach (var (name, _, _, why) in RelationMarkers)
        {
            Assert.True(used.Contains(name), $"relation marker '{name}' ({why}) is reached by no census column");
        }
    }

    /* ═══════════════════════ plumbing ═══════════════════════ */

    private static void Bump<TKey>(Dictionary<TKey, int> counts, TKey key)
        where TKey : notnull =>
        counts[key] = counts.TryGetValue(key, out var existing) ? existing + 1 : 1;

    private static void AssertMatchesInventory(
        Dictionary<(string File, string Column, string Tables), int> found,
        SiteLabel[] labels)
    {
        static string Render(string file, string column, string tables, int sites) =>
            string.Create(CultureInfo.InvariantCulture, $"{file} | {column} | {tables} | {sites}");

        var expected = Inventory
            .Where(i => labels.Contains(i.Label))
            .Select(i => Render(i.File, i.Column, i.Tables, i.Sites))
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToArray();

        var actual = found
            .Select(f => Render(f.Key.File, f.Key.Column, f.Key.Tables, f.Value))
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(actual);
        Assert.Equal(expected, actual);
    }

    private static IEnumerable<string> McpSourceFiles() =>
        ProductionSourceFiles("Darling/PerformanceMonitor.Darling.Service/Mcp", "Lite/Mcp");

    private static IEnumerable<string> RenderSourceFiles() =>
        ProductionSourceFiles("Darling/PerformanceMonitor.Darling.Viewer", "Lite");

    private static IEnumerable<string> ProductionSourceFiles(params string[] roots)
    {
        foreach (var root in roots)
        {
            foreach (var path in Directory.EnumerateFiles(RepoPath(root), "*.cs", SearchOption.AllDirectories))
            {
                if (path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).Any(segment =>
                        string.Equals(segment, "bin", StringComparison.Ordinal)
                        || string.Equals(segment, "obj", StringComparison.Ordinal)
                        || string.Equals(segment, "Darling.Tests", StringComparison.Ordinal)
                        || string.Equals(segment, "Lite.Tests", StringComparison.Ordinal)))
                {
                    continue;
                }

                yield return path;
            }
        }
    }

    /// <summary>Comment spans out, string literals kept — the SQL under test IS a verbatim literal, and
    /// this codebase's collectors carry their clock reasoning in comments that name the very functions
    /// matched here. Same inversion <c>CollectorTimestampFrameTests.QueryTextOf</c> makes.</summary>
    private static string WithoutComments(string text)
    {
        var withoutBlocks = Regex.Replace(text, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        var withoutSlashes = Regex.Replace(withoutBlocks, @"//[^\r\n]*", " ");

        return Regex.Replace(withoutSlashes, @"--[^\r\n]*", " ");
    }

    private static string CollectorSourcePath(string collector) =>
        RepoPath("PerformanceMonitor.Collectors/" + collector + ".cs");

    private static string Relative(string absolute) =>
        Path.GetRelativePath(RepoPath("."), absolute).Replace(Path.DirectorySeparatorChar, '/');

    private static string RepoPath(string relative, [CallerFilePath] string thisFile = "")
    {
        /* This file lives at <repo>/Darling/Darling.Tests/, so the repo root is two levels up. */
        var repoRoot = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));

        return Path.GetFullPath(Path.Combine(repoRoot, relative.Replace('/', Path.DirectorySeparatorChar)));
    }
}
