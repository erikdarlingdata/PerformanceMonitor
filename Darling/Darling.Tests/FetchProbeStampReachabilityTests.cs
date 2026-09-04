/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para>Pins that the fetch PROBE phase reports its elapsed time even when the probe throws (#2816).</para>
///
/// <para><b>Why this is an IL test and not an arithmetic one.</b> <c>StatementSplitTimingTests</c> already
/// asserts that probe + target + write + other equals the parent, and it passed throughout the defect —
/// because the arithmetic was never wrong. The residual did exactly its job. The bug was that
/// <c>FetchAndStorePlansAsync</c> stamped <c>PerItemPlanProbeMs</c> on a plain assignment at the END of the
/// probe phase, so an <c>await</c> that threw jumped over it, the phase reported <c>probe:0ms</c>, and the
/// residual dutifully absorbed the entire cost into <c>other:</c> — which is documented as the method's own
/// bookkeeping, "the cost would be in our own code, not in either database". For a store round trip that
/// timed out that is precisely inverted, and the split is being used to decide where to optimise.</para>
///
/// <para>Measured on use1 on 2026-09-03: 311 split rows, every one summing exactly to its parent, and a
/// single failed probe contributing 43,053ms of the fleet's 44,319ms total <c>other:</c> — 97% of the whole
/// residual budget for the day was a store probe misfiled as our own bookkeeping.</para>
///
/// <para>The target and write phases already stamped from <c>finally</c> blocks for exactly this reason;
/// the probe was the one phase that did not. So the invariant worth holding is not a value but a
/// REACHABILITY property: the probe setter must be invoked from inside an exception-handling region, on
/// both fetch paths. A value assertion cannot express that, which is why the existing suite could not
/// see it.</para>
/// </summary>
public sealed class FetchProbeStampReachabilityTests
{
    /* The two probe stamps, one per fetch path. Named rather than enumerated from the type so that deleting
       a stamp fails the test loudly instead of shrinking the set it checks. */
    private static readonly string[] ProbeSetters =
    [
        "set_PerItemPlanProbeMs",
        "set_PerItemTextProbeMs",
    ];

    [Fact]
    public void ProbeStamp_IsReachableFromAnExceptionHandler_SoAThrowingProbeStillReportsItsTime()
    {
        var counts = ScanServiceAssembly();

        foreach (var setter in ProbeSetters)
        {
            var (total, inHandler) = counts[setter];

            /* Guards the scanner itself: if the assembly were unreadable, or the setter renamed, every
               count would be zero and the handler assertion below would fail for a reason that has nothing
               to do with the defect. A witness that can fail silently in the reassuring direction is worse
               than no witness. */
            Assert.True(
                total > 0,
                $"{setter} was not called anywhere in the service assembly — the scanner resolved nothing, " +
                "so this test cannot say anything about handler reachability.");

            Assert.True(
                inHandler > 0,
                $"{setter} is never invoked from inside an exception handler ({total} call site(s), all on " +
                "success paths). A probe that throws will report probe:0ms and its cost will be silently " +
                "reattributed to the other: residual, which is documented as time spent in neither database.");
        }
    }

    /// <summary>
    /// For each probe setter, how many times it is called in the built service assembly and how many of
    /// those calls sit inside an exception-handler region. The walk itself lives in
    /// <see cref="IlCallSiteScanner"/> — this pin carried its own copy until #2898, and that copy advanced its
    /// cursor four bytes past a match, which can step over a genuine call instruction's own token. A false
    /// negative is the dangerous direction here: it would read a probe that stopped stamping from its handler
    /// as still stamping.
    /// </summary>
    private static Dictionary<string, (int Total, int InHandler)> ScanServiceAssembly()
    {
        var assemblyPath = typeof(PerformanceMonitor.Darling.Service.DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        return IlCallSiteScanner.CountCalls(assemblyPath, ProbeSetters);
    }
}
