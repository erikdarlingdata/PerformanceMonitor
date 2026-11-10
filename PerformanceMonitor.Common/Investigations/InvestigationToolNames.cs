/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// #8461: the investigation tools by kind, one list for both apps. A shared tool name is gated by the same rule in Lite and in
/// Darling, so each app's MCP policy builds its gated set from <see cref="StateChanging"/> and treats <see cref="Reads"/> as ordinary
/// reads, and a tool added to one app has to be placed here before the other app's classification test passes.
/// <para>
/// The state-changing tools (a claim, a report, a message to the request's thread) write investigation state and nothing else. Both
/// apps serve them only with their investigations setting on, the configuration writes off and a usable token, because one token cannot
/// tell an agent from a person, so an agent that holds it must not also hold the configuration writes. The reads answer in every
/// configuration, like every other read, at the served data-detail level: a client gets an empty list rather than an unknown tool when
/// there is nothing to show. Darling alone has the evidence read, the wait tool and the thread message; a name an app has no tool for
/// is a no-op in its gate.
/// </para>
/// </summary>
public static class InvestigationToolNames
{
    /// <summary>The tools that change investigation state, served only under the investigations setting.</summary>
    public static readonly IReadOnlySet<string> StateChanging = new HashSet<string>(StringComparer.Ordinal)
    {
        "claim_investigation_request",
        "submit_investigation_report",
        "post_investigation_message",
    };

    /// <summary>The investigation reads, served in every configuration.</summary>
    public static readonly IReadOnlySet<string> Reads = new HashSet<string>(StringComparer.Ordinal)
    {
        "list_investigation_requests",
        "get_investigation_reports",
        "get_investigation_evidence",
        "wait_for_investigation_requests",
    };

    public static bool IsStateChanging(string toolName) => StateChanging.Contains(toolName);

    public static bool IsRead(string toolName) => Reads.Contains(toolName);

    /// <summary>True for a name on either list: an investigation tool whose kind has been decided.</summary>
    public static bool IsPlaced(string toolName) => IsStateChanging(toolName) || IsRead(toolName);
}
