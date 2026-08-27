/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Which seat this viewer is connected with, as one status-bar field (#2479, items 3 and 4).
///
/// <para><b>Why this exists.</b> #2400 asked why "Current Active Queries" refuses on a read-write-looking
/// connection, and #2403 answered it by rewriting the individual refusals. That improved every refusal and
/// left the question intact: you still discover your seat one surface at a time, at the moment a write is
/// denied, which is the worst moment to learn it. The viewer has known the answer since V8 — one
/// <c>has_table_privilege</c> probe at connect — and never showed it anywhere you could look on purpose.</para>
///
/// <para><b>Why it matters more in UAT than it did before.</b> The two defaults are deliberately opposite:
/// a local seat takes <c>postgres.connectAs</c>, which defaults to <c>admin</c> (read-write), while a seat
/// reached over the LAN takes <c>postgres.network.role</c>, which defaults to <c>viewer</c> (read-only). A
/// UAT tester connecting from their own desktop is the second case by construction, so the read-only seat
/// is the EXPECTED default rather than a misconfiguration — and nothing in the product said so.</para>
///
/// <para><b>Unreachable is its own state, not a read-only verdict.</b> <c>DetectReadOnlyAsync</c> fails safe
/// to read-only for an unexpected RESPONSE, but raises <see cref="ViewerStoreUnreachableException"/> for a
/// failure to REACH the store — a distinction #2117 built deliberately and this field must not undo. A
/// status line claiming "read-only" when the service is simply down would send an operator to fix a role
/// they do not need to touch. So the state machine carries four values and the two failures never share
/// one.</para>
/// </summary>
internal enum ViewerSeatState
{
    /// <summary>Not probed yet. The field's initial value, and where a schema-skew refusal leaves it —
    /// the store answered, but the seat question was never asked, so nothing is claimed about it.</summary>
    Unknown,

    /// <summary>The store could not be reached, so there is no seat to report. Never collapsed into
    /// <see cref="ReadOnly"/>: see the type remarks.</summary>
    Unreachable,

    /// <summary>The probe said this connection cannot write the operator-config tables — either the
    /// restricted role, or a response that could not be read as a yes (the V8 fail-safe).</summary>
    ReadOnly,

    /// <summary>The probe said this connection can write the operator-config tables.</summary>
    ReadWrite,
}

/// <summary>The status-bar text and tooltip for a <see cref="ViewerSeatState"/>. Pure, so the wording is
/// pinned by tests rather than by reading a XAML file.</summary>
internal static class ViewerSeatIndicator
{
    /// <summary>
    /// The status-bar field. Prefixed "Seat:" to match the other four fields ("Collectors:", "Database:",
    /// "Collection:", "Servers:"), which is what makes it findable without a legend.
    /// </summary>
    public static string Text(ViewerSeatState state) => state switch
    {
        ViewerSeatState.ReadOnly => "Seat: read-only",
        ViewerSeatState.ReadWrite => "Seat: read-write",
        ViewerSeatState.Unreachable => "Seat: not connected",
        _ => "Seat: --",
    };

    /// <summary>
    /// The tooltip, which is where the ANSWER lives rather than in a doc.
    ///
    /// <para><paramref name="storeIsOnThisMachine"/> orders the two defaults by which one is likelier to
    /// have decided this seat; it never claims which one DID. A non-loopback store host does not prove a
    /// remote seat — a bring-your-own store on another host reached from the service host reads
    /// identically — so the wording says the role arrived in the connection string and then names both
    /// defaults, which is true either way.</para>
    /// </summary>
    public static string ToolTip(ViewerSeatState state, bool storeIsOnThisMachine) => state switch
    {
        ViewerSeatState.ReadOnly => ReadOnlyToolTip(storeIsOnThisMachine),
        ViewerSeatState.ReadWrite => ReadWriteToolTip,
        ViewerSeatState.Unreachable => UnreachableToolTip,
        _ => UnknownToolTip,
    };

    internal const string LocalDefaultSentence =
        "A seat on the service host connects as postgres.connectAs, which defaults to \"admin\" — read-write.";

    internal const string NetworkDefaultSentence =
        "A seat reached over the LAN connects as postgres.network.role, which defaults to \"viewer\" — read-only.";

    /* Said in full on the read-only tooltip because it is the actual #2400 question. The refusals say it
       per-dialog already; a reader who has not tripped one yet has no way to know a "write" here is a row
       in the MONITORING store rather than something aimed at their SQL Server. */
    internal const string NothingIsWrittenSentence =
        "Nothing is ever written to a monitored SQL Server. The actions a read-only seat cannot take "
        + "(Refresh on Current Active Queries, Get Actual Plan, Purge Now, Generate now, dismissing an "
        + "alert, editing mute rules, adding a server) work by enqueueing a request for the service in the "
        + "MONITORING store, and that row is the write a read-only seat cannot make.";

    private static string ReadOnlyToolTip(bool storeIsOnThisMachine)
    {
        /* Order by likelihood, claim neither. The likelier default goes first so the sentence that
           explains THIS seat is the one the reader hits first. */
        var defaults = storeIsOnThisMachine
            ? LocalDefaultSentence + " " + NetworkDefaultSentence
            : NetworkDefaultSentence + " " + LocalDefaultSentence;

        var where = storeIsOnThisMachine
            ? "The store is on this machine."
            : "The store is not on this machine, so the role came from the connection string the service handed out.";

        return "This viewer is connected with a read-only role, so write affordances are hidden or disabled."
            + Environment.NewLine + Environment.NewLine
            + where + " " + defaults
            + " The two defaults are deliberately opposite — the local seat is the operator's, the remote "
            + "seat is a laptop — so a remote seat being read-only is the expected default, not a fault."
            + Environment.NewLine + Environment.NewLine
            + NothingIsWrittenSentence
            + Environment.NewLine + Environment.NewLine
            + "To change it: set postgres.network.role to \"admin\" on the SERVICE host and restart the "
            + "service (then re-run --export-viewer-config) for a LAN seat, or set postgres.connectAs to "
            + "\"admin\" and restart the viewer for a seat on the service host.";
    }

    /* static readonly rather than const so the paragraph breaks come from Environment.NewLine, exactly as
       the read-only tooltip's do. A const cannot call it, and two tooltips breaking lines two different
       ways is the kind of drift nobody notices until a test pins one of them. */
    internal static readonly string ReadWriteToolTip =
        "This viewer is connected with a read-write role, so it can enqueue requests for the service "
        + "(Refresh on Current Active Queries, Get Actual Plan, Purge Now, adding a server, editing mute "
        + "rules) by writing to the MONITORING store."
        + Environment.NewLine + Environment.NewLine
        + "Nothing is ever written to a monitored SQL Server. The service reads those under the same "
        + "least-privilege login the collectors use."
        + Environment.NewLine + Environment.NewLine
        + LocalDefaultSentence + " " + NetworkDefaultSentence;

    /* Deliberately says nothing about the seat. The probe never ran, so there is no verdict to report,
       and the full-window message already carries the diagnosis and the connection details. */
    internal const string UnreachableToolTip =
        "The Darling store could not be reached, so this viewer has not probed which seat it has. This is "
        + "NOT a read-only seat — it is no connection at all. See the message on the main panel for the "
        + "store address it tried and why it failed.";

    internal const string UnknownToolTip =
        "This viewer has not probed which seat it has yet.";
}
