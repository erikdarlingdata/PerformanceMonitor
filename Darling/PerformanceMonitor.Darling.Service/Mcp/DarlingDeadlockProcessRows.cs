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
using PerformanceMonitor.Common;
using static PerformanceMonitor.Common.DeadlockGraphProcessParser;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The per-process rows of one deadlock graph for <c>get_deadlock_detail</c>: the same rows the desktop
/// viewer's Deadlocks grid shows, produced by the same shared walk (<see cref="DeadlockGraphProcessParser"/>)
/// so the browser never parses the XML. A value the graph did not carry is left off the row rather than
/// sent as an empty string.
/// </summary>
public static class DarlingDeadlockProcessRows
{
    /// <summary>Most process rows sent for one deadlock; the rest are counted in <c>processes_truncated</c>.
    /// A real graph has two to a handful of parties, and a parallel deadlock can list dozens.</summary>
    public const int MaxProcessesPerDeadlock = 12;

    /// <summary>Statement text is previewed to this length per process when the caller asked for the whole graph
    /// (<c>full_graph</c> or a <c>dedup_key</c>); the statement is also in the graph XML.</summary>
    public const int StatementPreviewLength = 500;

    /// <summary>The default call's statement preview, shorter so a page of deadlocks fits the response budget.</summary>
    public const int DefaultStatementPreviewLength = 100;

    /// <summary>Process rows across one default page; later deadlocks get what is left, and the rest are counted
    /// in each deadlock's <c>processes_truncated</c>. A whole-graph call is exempt.</summary>
    public const int DefaultPageRowBudget = 24;

    /// <summary>The parsed rows for one graph, or an empty list when the graph does not parse (the shared walk's
    /// victim-only fallback row carries a fabricated session id, so it is not used here).</summary>
    public static List<DeadlockProcessInfo> Parse(string? graphXml, DateTime? deadlockTime)
        => TryParseGraph<DeadlockProcessInfo>(new DeadlockGraphInput(graphXml, deadlockTime, null, null), out var rows)
            ? rows
            : new List<DeadlockProcessInfo>();

    /// <summary>One row as a name-to-value map with the absent values left out.</summary>
    public static Dictionary<string, object> ToRow(DeadlockProcessInfo p, int statementLength = StatementPreviewLength)
    {
        var row = new Dictionary<string, object>
        {
            ["victim"] = p.IsVictim,
            ["spid"] = p.Spid,
            ["wait_time_ms"] = p.WaitTime,
            ["log_used"] = p.LogUsed,
            ["transaction_count"] = p.TransactionCount,
            ["priority"] = p.Priority,
        };
        Put(row, "process_id", p.ProcessId);
        Put(row, "deadlock_type", p.DeadlockType);
        Put(row, "database_name", p.DatabaseName);
        Put(row, "object_names", p.ObjectNames);
        Put(row, "proc_name", p.ProcName);
        Put(row, "lock_mode", p.LockMode);
        Put(row, "owner_mode", p.OwnerMode);
        Put(row, "waiter_mode", p.WaiterMode);
        Put(row, "wait_resource", p.WaitResource);
        Put(row, "isolation_level", p.IsolationLevel);
        Put(row, "transaction_name", p.TransactionName);
        Put(row, "login_name", p.LoginName);
        Put(row, "host_name", p.HostName);
        Put(row, "client_app", p.ClientApp);
        Put(row, "status", p.Status);
        Put(row, "sql_text", McpHelpers.TruncateStatement(p.SqlText, statementLength));
        return row;
    }

    /// <summary>The rows for one graph, capped, plus how many were left off.</summary>
    public static (List<Dictionary<string, object>> Rows, int Truncated) Build(
        string? graphXml, DateTime? deadlockTime, int maxRows = MaxProcessesPerDeadlock, int statementLength = StatementPreviewLength)
    {
        var all = Parse(graphXml, deadlockTime);
        var rows = all.Take(Math.Min(maxRows, MaxProcessesPerDeadlock)).Select(p => ToRow(p, statementLength)).ToList();
        return (rows, all.Count - rows.Count);
    }

    private static void Put(Dictionary<string, object> row, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value)) row[key] = value;
    }
}
