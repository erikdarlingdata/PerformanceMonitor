/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3410, Darling half of the third state: a target whose <c>logging_collector</c> is off records as
/// <c>PERMISSIONS</c> with the exception's own reason — not <c>ERROR</c>, and not a successful empty read —
/// and the deadlock read quotes that reason back instead of shrugging.
///
/// <para>The refusal itself — the gate in both queries, the marker row, the exception both
/// <c>ReadAsync</c>s throw, and the message's contents — is pinned in Lite.Tests
/// (<c>PgLoggingCollectorGateTests</c>) against the shared definitions both SKUs compile. What is only
/// pinnable here is the CLASSIFICATION and the READ WIRING, which are try/catch glue around a live
/// collection sweep and a <c>??</c> chain in an MCP tool — the same shape and the same reason as
/// <see cref="PgDeadlockLogTimezoneTests"/>: source-level, because there is no way to reach either without
/// a monitored PostgreSQL target.</para>
///
/// <para>The status is the load-bearing part, twice over. <c>ERROR</c> would put a deliberate logging
/// destination inside every error count that feeds collector health, the daily band and the
/// collection-failure self-alerts, forever; <c>PERMISSIONS</c> is the store's non-fatal degradation bucket,
/// which those counts exclude, and it is also the status <c>CollectorRuntimePrecondition</c>'s arm reads —
/// so the same row that keeps the failure counts honest is what lets <c>get_pg_deadlocks</c> answer
/// not-collected WITH THE REASON, the shape the store's own log read (<c>get_store_log</c>) established for
/// an empty log directory.</para>
/// </summary>
public sealed class PgLoggingCollectorOffTests
{
    [Fact]
    public void Worker_Records_A_LoggingCollectorOff_As_Permissions_Not_Error()
    {
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        var armIndex = source.IndexOf("catch (PgLoggingCollectorOffException ex)", System.StringComparison.Ordinal);
        Assert.True(armIndex >= 0, "The #3410 classification arm is gone — a logging_collector = off target would fall to the general handler and record ERROR every cycle.");

        /* Bounded to the arm's own body. An unscoped Contains would pass on any of the other PERMISSIONS
           arms in this file and prove nothing about this one. */
        var body = source[armIndex..];
        var close = body.IndexOf("\n            return 0;", System.StringComparison.Ordinal);
        body = close > 0 ? body[..close] : body;

        Assert.Contains("\"PERMISSIONS\"", body, System.StringComparison.Ordinal);
        Assert.DoesNotContain("\"ERROR\"", body, System.StringComparison.Ordinal);

        /* The message is the exception's own, which is where logging_collector and the restart are named.
           Re-authoring it in the arm would be a second copy of prose whose whole value is naming one
           setting correctly. */
        Assert.Contains("ex.Message", body, System.StringComparison.Ordinal);
    }

    /// <summary>
    /// The deadlock read asks the precondition question, and asks it in the only defensible order:
    /// capability first (a permanent engine gap outranks a fixable setting, #2511's lesson), then the
    /// collector's own recorded outcome, then the read's own miss. <c>get_pg_plans</c> already asks
    /// #2546's question for <c>pg_plan_capture</c>; the deadlock read reads the same file the same way and
    /// had never asked, so a denied grant, a missing file or a switched-off logging collector all fell through
    /// to a message enumerating everything that COULD be wrong instead of quoting what the last run said
    /// WAS wrong. Anchored on code, never on the sentence, for the reason
    /// <c>RuntimePreconditionMissTests</c>' own wiring pin gives: prose about a call is not the call.
    /// </summary>
    [Fact]
    public void GetPgDeadlocks_AsksThePreconditionQuestion_AfterCapability_BeforeTheEmptyMiss()
    {
        /* LF-normalised, because two of the anchors below span a line break and this checkout is CRLF —
           the anchor that embeds the wrong newline matches NOTHING and reads as clean, which is RepoFile's
           own warning about multi-line pins. */
        var source = ReadRepoFileLf(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgDeadlockTools.cs"));

        var capability = source.IndexOf("DarlingEngineCapability.NotCollectedStatusAsync", System.StringComparison.Ordinal);
        var precondition = source.IndexOf("DarlingRuntimePrecondition.StatusAsync", System.StringComparison.Ordinal);
        var miss = source.IndexOf("McpHelpers.Status(\n                        \"no_deadlocks\"", System.StringComparison.Ordinal);

        Assert.True(capability >= 0, "get_pg_deadlocks no longer asks the capability question at all");
        Assert.True(precondition >= 0, "get_pg_deadlocks no longer asks the precondition question, so a recorded log-read denial, missing file or switched-off logging collector falls through to the generic empty miss");
        Assert.True(miss >= 0, "get_pg_deadlocks no longer has its own empty miss — the last resort is gone");

        Assert.True(capability < precondition, "capability must be asked before the precondition: permanent outranks fixable (#2511)");
        Assert.True(precondition < miss, "the empty miss must remain the LAST resort");

        /* And the question is asked about the collector that serves this read — a retyped name would make
           the branch dead against a collection_log that has no rows under it. */
        Assert.Contains(
            "DarlingRuntimePrecondition.StatusAsync(\n                        postgres, resolved.ServerId, resolved.ServerName, \"pg_deadlocks\")",
            source,
            System.StringComparison.Ordinal);
    }
}
