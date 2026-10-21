/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// The single source of truth for how a SQL Server error number is judged.
///
/// Three questions are asked about errors across the products, and they must never disagree:
///
///   "Is this temporary — should we retry?"              <see cref="IsTransient"/>
///   "Does this login lack rights to master?"            <see cref="IsMasterAccessDenied"/>
///   "Should database-scoped collection go single-DB?"   <see cref="ShouldFallBackToSingleDatabase"/>
///
/// The last two are deliberately NOT the same question, which is the #1631 lesson: an error can make
/// the single-database fallback the right move without saying anything about the login's rights.
///
/// The retryable set MUST be disjoint from both. Issue #1506 is what happens when it isn't: error 40613
/// ("database not currently available, please retry later") sat on the retryable list AND on the
/// give-up list at the same time, so the same error meant both "this will pass" and "this is
/// forever" depending on which code path saw it first.
///
/// These lists lived separately in Lite and in Darling, which is how the bug was ported from one
/// to the other. They live here now so that cannot happen again, and so the disjointness invariant
/// can be asserted from both test suites. Plain integers, no SqlClient dependency, so every project
/// can reference this regardless of how it talks to SQL Server.
/// </summary>
public static class SqlErrorClassification
{
    /// <summary>
    /// Errors that are temporary by nature: the operation may well succeed if tried again.
    /// </summary>
    public static readonly IReadOnlySet<int> TransientErrorNumbers = new HashSet<int>
    {
        -2,     // Timeout
        -1,     // General network error
        2,      // Timeout (alternative)
        53,     // Named pipe / network not found
        233,    // Connection closed
        10053,  // Transport-level error (connection was forcibly closed)
        10054,  // Connection reset by peer
        10060,  // Connection timed out
        10061,  // Connection refused
        40143,  // Azure SQL transient
        40197,  // Azure SQL transient - service encountered an error
        40501,  // Azure SQL - service is currently busy
        40613,  // Azure SQL - database not currently available
        49918,  // Azure SQL - not enough resources to process request
        49919,  // Azure SQL - cannot process create/update request
        49920   // Azure SQL - cannot process request (too many operations)
    };

    /// <summary>
    /// Errors meaning a login genuinely cannot read <c>master</c>, so database-scoped collectors on
    /// Azure SQL DB must fall back to the connection's own catalog. This is the #857 case: a contained
    /// user that exists only inside a user database really does get these when it opens master.
    ///
    /// Every entry must describe the login's RIGHTS, never the server's REACHABILITY. Two reachability
    /// errors used to be here and caused #1506:
    ///
    ///   40613 — "not currently available, please retry later". Transient on its face, and listed as
    ///           retryable above — the contradiction this class exists to prevent.
    ///   40615 — "client with IP address ... is not allowed to access the server". A firewall rejection
    ///           at the logical server, which says nothing about this login's rights.
    ///
    /// Reported by a user whose public IP rotated daily: each rotation rejected the connection, the
    /// rejection was misread as "this login may not read master", and the verdict was cached — so
    /// collection stayed broken until the app was restarted.
    ///
    /// 40615 nonetheless SHOULD still trigger the single-database fallback — that is a separate
    /// question, answered by <see cref="SingleDatabaseFallbackErrorNumbers"/>. Removing it from the
    /// fallback path entirely (rather than only from this rights list) is what regressed #1631.
    ///
    /// 4060 and 18456 stay, because a contained user really does get them on master. Callers must
    /// still treat the verdict as revocable — both can also be thrown transiently.
    /// </summary>
    public static readonly IReadOnlySet<int> MasterAccessDeniedErrorNumbers = new HashSet<int>
    {
        229,   // Permission denied on object
        230,   // Permission denied on column
        916,   // Server principal cannot access the database under the current security context
        4060,  // Cannot open database requested by the login
        18456  // Login failed for user
    };

    /// <summary>
    /// Errors after which database-scoped collection on Azure SQL DB should stop trying to enumerate
    /// databases from <c>master</c> and fall back to the connection's own catalog.
    ///
    /// This is a superset of <see cref="MasterAccessDeniedErrorNumbers"/> — every rights denial implies
    /// the fallback — plus the one REACHABILITY error where the fallback genuinely works:
    ///
    ///   40615 — "client with IP address ... is not allowed to access the server". Azure SQL DB has two
    ///           independent IP firewall layers, and the DATABASE-level rules are evaluated FIRST: a
    ///           client whose IP matches a database-level rule is granted a connection to that database
    ///           even with no server-level rule permitting it, while <c>master</c> (where server-level
    ///           rules live) still requires a server-level rule. So "blocked at the server, allowed at
    ///           the database" is a real, fully supported configuration — Microsoft in fact recommends
    ///           database-level rules "whenever possible" — and it is exactly what #857 shipped for.
    ///
    /// #1506 removed 40615 from the rights list for good reason, but ALSO dropped it from the fallback
    /// path on the mistaken premise that "the same rule blocks that connection too". It does not, and
    /// #1631 was the result: every database-scoped collector failed outright on a server whose user
    /// database was perfectly reachable.
    ///
    /// This is safe to act on because the resulting verdict is a revocable throttle, never a latch: it
    /// expires (15 minutes), is cleared when a server returns from an outage, and is skipped entirely
    /// when there is no target database to fall back to. A client that is merely firewalled out at the
    /// server with no database-level rule therefore just fails its fallback attempt with the same 40615,
    /// logs an ordinary collector error, and recovers on the next successful connect — the #1506
    /// scenario, without the permanent wedge.
    ///
    /// 40613 stays out: it is transient, and retrying is the correct response.
    /// </summary>
    public static readonly IReadOnlySet<int> SingleDatabaseFallbackErrorNumbers = new HashSet<int>(
        MasterAccessDeniedErrorNumbers)
    {
        40615  // Azure SQL - client IP not allowed at the logical server (database-level rule may still allow)
    };

    /// <summary>
    /// True when the error is temporary and the operation is worth retrying.
    /// </summary>
    public static bool IsTransient(int errorNumber) => TransientErrorNumbers.Contains(errorNumber);

    /// <summary>
    /// True when the error means this login cannot read <c>master</c>. Never true for an error that
    /// merely means the server was unreachable — see the remarks on
    /// <see cref="MasterAccessDeniedErrorNumbers"/>.
    /// </summary>
    public static bool IsMasterAccessDenied(int errorNumber) => MasterAccessDeniedErrorNumbers.Contains(errorNumber);

    /// <summary>
    /// True when database-scoped collection should fall back to the connection's own catalog instead of
    /// enumerating from <c>master</c>. Broader than <see cref="IsMasterAccessDenied"/> on purpose — see
    /// the remarks on <see cref="SingleDatabaseFallbackErrorNumbers"/>. This is the question the Azure
    /// database-enumeration catch sites in both SKUs actually need answered.
    /// </summary>
    public static bool ShouldFallBackToSingleDatabase(int errorNumber) =>
        SingleDatabaseFallbackErrorNumbers.Contains(errorNumber);

    /// <summary>
    /// True when the error means the connection to a database could not be made or kept, as opposed to a query that
    /// ran and failed (#6312): a firewall rejection (40615), a database that is not available (40613, a paused
    /// serverless database), a login or database the connection cannot open (18456, 4060), a refused or dropped
    /// network connection and a timeout. Built from the two sets above, so it cannot disagree with them about what
    /// "cannot reach" means.
    ///
    /// <para>A per-database fan-out uses it to pick which failure to report when several databases failed: the one
    /// that says the server could not be reached outranks one that says a database has no XE session, because the
    /// session is missing from a database that was never opened as readily as from one that was.</para>
    /// </summary>
    public static bool IsConnectionFailure(int errorNumber) =>
        TransientErrorNumbers.Contains(errorNumber) || SingleDatabaseFallbackErrorNumbers.Contains(errorNumber);
}
