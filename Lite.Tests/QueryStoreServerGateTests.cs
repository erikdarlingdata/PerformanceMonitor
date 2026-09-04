/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the #2165 gate: the per-server mutual exclusion that stops the tick's Query Store collection and the
/// #2058 backfill slice from running heavy QS text extraction against ONE server at the same time.
///
/// <para><b>The field evidence.</b> On a 4-core multi-tenant box mid-consolidation, a 64 MB backfill slice for a
/// freshly restored database overlapped the tick's collection of a sibling database — a 12:50:58 ship against a
/// 12:51:09 tick completion — so roughly 128 MB of text extraction was in flight on the box least able to afford
/// it. The two loops collide precisely when a server is already drowning, because a big catalog arriving is what
/// triggers both.</para>
///
/// <para>The two properties worth pinning are that it EXCLUDES, and that it never WAITS. The second is as
/// important as the first: these are shared fleet loops, so a blocking acquire would let one server's slice
/// stall collection for every other server, which is the #2148 wedge arriving through a lock instead of a
/// hang.</para>
/// </summary>
public sealed class QueryStoreServerGateTests
{
    /// <summary>THE POINT: a second acquirer is refused while the first holds the gate.</summary>
    [Fact]
    public void ASecondAcquirerIsRefusedWhileTheFirstHoldsIt()
    {
        var gate = new QueryStoreServerGate();

        using var first = gate.TryAcquire();

        Assert.NotNull(first);
        Assert.Null(gate.TryAcquire());
        Assert.True(gate.IsHeld);
    }

    /// <summary>
    /// Releasing hands the gate to the next caller. The loops are long-lived, so a gate that could only be taken
    /// once would permanently stop one server's Query Store collection — and it would look like "that server has
    /// no Query Store data" rather than like a bug.
    /// </summary>
    [Fact]
    public void ReleasingLetsTheOtherLoopIn()
    {
        var gate = new QueryStoreServerGate();

        var first = gate.TryAcquire();
        Assert.NotNull(first);
        first!.Dispose();

        Assert.False(gate.IsHeld);
        using var second = gate.TryAcquire();
        Assert.NotNull(second);
    }

    /// <summary>
    /// It never blocks. Asserted as elapsed time against a HELD gate, because the failure this guards is a
    /// blocking acquire silently replacing the try-acquire — which would still pass an exclusion-only test while
    /// reintroducing the fleet stall.
    /// </summary>
    [Fact]
    public void ARefusedAcquireReturnsImmediatelyRatherThanWaiting()
    {
        var gate = new QueryStoreServerGate();
        using var held = gate.TryAcquire();

        var started = Environment.TickCount64;
        for (var i = 0; i < 1_000; i++)
        {
            Assert.Null(gate.TryAcquire());
        }

        Assert.True(Environment.TickCount64 - started < 1_000,
            "a thousand refused acquires must not block — a blocking acquire here stalls the whole sweep");
    }

    /// <summary>
    /// Double-dispose must not release a gate a DIFFERENT loop has since taken. Without idempotence the sequence
    /// "tick disposes twice, backfill acquires in between" leaves both running against one server — the exact
    /// condition the gate exists to prevent, reached through a stray extra Dispose rather than through missing
    /// exclusion.
    /// </summary>
    [Fact]
    public void DisposingALeaseTwiceCannotReleaseSomebodyElsesHold()
    {
        var gate = new QueryStoreServerGate();

        var tick = gate.TryAcquire();
        Assert.NotNull(tick);
        tick!.Dispose();

        var backfill = gate.TryAcquire();
        Assert.NotNull(backfill);

        tick.Dispose();                      /* the stray second dispose */

        Assert.True(gate.IsHeld, "the backfill's hold must survive the tick's double-dispose");
        Assert.Null(gate.TryAcquire());
        backfill!.Dispose();
    }

    /// <summary>
    /// Under real contention exactly ONE holder exists at a time. The count of concurrent holders is what the
    /// field bug was about — two heavy extractions at once — so it is asserted directly rather than inferred.
    /// </summary>
    [Fact]
    public async Task UnderContentionOnlyOneHolderEverExistsAtOnce()
    {
        var gate = new QueryStoreServerGate();
        var concurrent = 0;
        var maxObserved = 0;
        var acquisitions = 0;

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < 2_000; i++)
            {
                using var lease = gate.TryAcquire();
                if (lease is null)
                {
                    continue;
                }

                Interlocked.Increment(ref acquisitions);
                var now = Interlocked.Increment(ref concurrent);
                var seen = Volatile.Read(ref maxObserved);
                if (now > seen)
                {
                    Interlocked.Exchange(ref maxObserved, now);
                }

                Interlocked.Decrement(ref concurrent);
            }
        })).ToArray());

        Assert.Equal(1, maxObserved);
        Assert.True(acquisitions > 0, "the test must have actually acquired the gate");
        Assert.False(gate.IsHeld, "every lease was disposed, so the gate must be free");
    }

    /// <summary>
    /// <c>NotGated</c> is a distinct, non-null, safely re-disposable sentinel. It is what lets a call site decide
    /// "am I gated?" and "did I get the gate?" in one expression while keeping <c>null</c> meaning only
    /// "skip" — conflating the two would silently skip collectors nobody meant to gate.
    /// </summary>
    [Fact]
    public void NotGatedIsANonNullNoOpSentinel()
    {
        Assert.NotNull(QueryStoreServerGate.NotGated);

        QueryStoreServerGate.NotGated.Dispose();
        QueryStoreServerGate.NotGated.Dispose();

        /* Still usable afterwards: it is a shared singleton every non-gated collector run disposes. */
        Assert.NotNull(QueryStoreServerGate.NotGated);
        Assert.Same(QueryStoreServerGate.NotGated, QueryStoreServerGate.NotGated);
    }

    /// <summary>
    /// Gates are PER SERVER: one server's collection must never gate another's. The registries are keyed
    /// dictionaries for this reason, and a single shared gate would serialize Query Store collection across the
    /// entire fleet — a fleet-wide throughput regression dressed as a fix.
    /// </summary>
    [Fact]
    public void GatesAreIndependentPerServer()
    {
        var gates = new ConcurrentDictionary<string, QueryStoreServerGate>(StringComparer.Ordinal);

        using var serverA = gates.GetOrAdd("server-a", static _ => new QueryStoreServerGate()).TryAcquire();
        using var serverB = gates.GetOrAdd("server-b", static _ => new QueryStoreServerGate()).TryAcquire();

        Assert.NotNull(serverA);
        Assert.NotNull(serverB);

        /* And the same key resolves the SAME gate — which is what makes the tick and the backfill exclude each
           other rather than each holding a private one. */
        Assert.Null(gates.GetOrAdd("server-a", static _ => new QueryStoreServerGate()).TryAcquire());
    }

    /* ──────────────── the WIRING, pinned at the source ────────────────

       Behavioral coverage cannot reach these: reproducing the overlap needs two live loops against one real
       monitored server with a big Query Store catalog. A correct gate that one of the two loops does not take is
       exactly the bug still present, and it builds and passes every other test. So both apps' call sites are
       asserted textually, per the ThemeCompletenessTests idiom. */

    private static string ReadRepoFile(string relativePath, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var parts = relativePath.Split('/');
        while (dir is not null && !File.Exists(Path.Combine(new[] { dir }.Concat(parts).ToArray())))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()));
    }

    /// <summary>
    /// LITE: both loops resolve the gate from the SAME dictionary, and the backfill takes it OUTSIDE its
    /// AbandonableStep.
    ///
    /// <para>The "same dictionary" half is the one that would silently fail: two loops each holding a private
    /// registry compile, pass the gate's own unit tests, and exclude nothing. The "outside the step" half matters
    /// because an abandoned-but-wedged slice must keep the gate closed — the statement is still running on the
    /// server, so the tick must keep yielding to it. Taking it outside is necessary but was not sufficient: the
    /// LEASE has to outlive the abandonment too, which is what
    /// <see cref="TheBackfillHandsItsLeaseToTheStepRatherThanScopingIt"/> pins.</para>
    /// </summary>
    [Fact]
    public void Lite_BothLoopsShareOneGateRegistry()
    {
        var tick = ReadRepoFile("Lite/Services/RemoteCollectorService.QueryStore.cs");
        var backfill = ReadRepoFile("Lite/Services/RemoteCollectorService.QueryStoreBackfill.cs");

        Assert.Contains("_queryStoreGates", tick, StringComparison.Ordinal);
        Assert.Contains("_queryStoreGates", backfill, StringComparison.Ordinal);
        Assert.Contains(".TryAcquire()", tick, StringComparison.Ordinal);
        Assert.Contains(".TryAcquire()", backfill, StringComparison.Ordinal);

        /* Declared exactly once, so the two partials cannot drift onto separate registries. */
        Assert.Equal(1, CountOccurrences(tick + backfill, "ConcurrentDictionary<string, QueryStoreServerGate>"));

        /* The gate is taken before the step is even constructed. */
        var gateIndex = backfill.IndexOf(".TryAcquire()", StringComparison.Ordinal);
        var stepIndex = backfill.IndexOf("_backfillSliceSteps.GetOrAdd", StringComparison.Ordinal);
        Assert.True(gateIndex > 0 && gateIndex < stepIndex,
            "the backfill must take the gate OUTSIDE the AbandonableStep, so a wedged slice keeps it closed");
    }

    /// <summary>
    /// DARLING: the same two properties, plus that the tick gates on the collector's OWN declared name rather
    /// than a string literal — renaming the collector must not silently unhook the gate.
    /// </summary>
    [Fact]
    public void Darling_BothLoopsShareOneGateRegistry()
    {
        var worker = ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        Assert.Equal(1, CountOccurrences(worker, "ConcurrentDictionary<int, QueryStoreServerGate>"));
        Assert.Equal(2, CountOccurrences(worker, "_queryStoreGates.GetOrAdd"));
        Assert.Contains("QueryStoreCollector.Instance.Name", worker, StringComparison.Ordinal);
        Assert.Contains("QueryStoreServerGate.NotGated", worker, StringComparison.Ordinal);

        var gateIndex = worker.IndexOf("_queryStoreGates.GetOrAdd(runtime.ServerId", StringComparison.Ordinal);
        var stepIndex = worker.IndexOf("_backfillSliceSteps.GetOrAdd", StringComparison.Ordinal);
        Assert.True(gateIndex > 0 && stepIndex > 0);
    }


    /* ──────────── the LEASE'S LIFETIME, which "outside the step" did not buy ────────────

       Found while working #2874 group D. Both SKUs took the gate outside the AbandonableStep, as the
       tests above pin — and then scoped the lease with `using var` inside the foreach BODY, so it was
       disposed at the end of the ITERATION. Including the iteration that ended Abandoned, which is the
       one outcome where the slice really is still running against the monitored server: #2148's
       in-flight guard went on quarantining that server's backfill while this gate stood wide open, so
       the tick was free to start its own heavy extraction beside the wedged slice. The #2148 half held;
       the #2165 half lapsed exactly where it was needed.

       The lease is handed to the step now (holdUntilStepEnds), which releases it when its own guard
       clears. Both halves of that are load-bearing and both are asserted: it must still be closed after
       an abandonment, and it must OPEN when the wedge finally dies — a gate that never reopened would
       stop one server's Query Store collection for the life of the process, which is a worse failure
       than the overlap. */

    /// <summary>
    /// The end-to-end property, driven through the REAL gate and the REAL step in the shape both SKUs'
    /// loop bodies use: after an abandonment the tick still cannot collect for that server, and the gate
    /// opens on its own the moment the wedged slice truly ends.
    /// </summary>
    [Fact]
    public async Task AnAbandonedSliceKeepsTheServersGateClosedUntilItTrulyEnds()
    {
        var gates = new ConcurrentDictionary<string, QueryStoreServerGate>(StringComparer.Ordinal);
        var steps = new ConcurrentDictionary<string, AbandonableStep>(StringComparer.Ordinal);
        var wedge = new TaskCompletionSource();

        /* ── one iteration of the backfill loop body ── */
        var gate = gates.GetOrAdd("alpha", static _ => new QueryStoreServerGate()).TryAcquire();
        Assert.NotNull(gate);

        var step = steps.GetOrAdd("alpha", static _ => new AbandonableStep());
        var result = await step.RunAsync(
            () => wedge.Task, TimeSpan.FromMilliseconds(200), holdUntilStepEnds: gate);
        /* ── the iteration ends here: whatever the old `using var` covered is out of scope now ── */

        Assert.Equal(AbandonableStepOutcome.Abandoned, result.Outcome);

        Assert.True(gates["alpha"].IsHeld,
            "an abandoned slice is still running on the server, so its gate must still be closed");
        Assert.Null(gates.GetOrAdd("alpha", static _ => new QueryStoreServerGate()).TryAcquire());
        Assert.True(steps["alpha"].IsInFlight, "the #2148 quarantine and the #2165 gate move together");

        /* Not a leak in the other direction: the wedge dying opens both. */
        wedge.SetResult();
        for (var i = 0; i < 200 && gates["alpha"].IsHeld; i++)
        {
            await Task.Delay(10);
        }

        Assert.False(gates["alpha"].IsHeld, "the gate must reopen when the wedged slice truly ends");
        Assert.False(steps["alpha"].IsInFlight);
        using var tick = gates.GetOrAdd("alpha", static _ => new QueryStoreServerGate()).TryAcquire();
        Assert.NotNull(tick);
    }

    /// <summary>
    /// Neither SKU may scope the backfill's lease to its own loop iteration.
    ///
    /// <para>A COUNT cannot catch this. Moving a <c>using</c> — into the loop body, out of it, around the
    /// call — leaves every occurrence count in the file invariant, which is exactly how the defect
    /// survived the wiring pins above. So the assertion is on the SPAN between the acquire and the
    /// handoff: the only region where a lease-scoping <c>using</c> could sit and still cover the slice.
    /// The span is derived from offsets, so a relocation moves it.</para>
    /// </summary>
    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs")]
    [InlineData("Lite/Services/RemoteCollectorService.QueryStoreBackfill.cs")]
    public void TheBackfillHandsItsLeaseToTheStepRatherThanScopingIt(string relativePath)
    {
        var source = ReadRepoFile(relativePath);

        Assert.Equal(1, CountOccurrences(source, AcquireAnchor));
        Assert.Equal(1, CountOccurrences(source, HandoffAnchor));
        Assert.Equal(1, CountOccurrences(source, StepCallAnchor));

        Assert.Null(FindLeaseScopedToTheSlice(source));
    }

    /// <summary>
    /// The positive control for the test above, through the IDENTICAL scanner. A negative assertion that
    /// can only ever pass is not an assertion: this feeds the scanner the pre-fix shape — the real text
    /// from before the change — and requires it to name the offending offset.
    /// </summary>
    [Fact]
    public void TheLeaseScopeScannerCatchesThePreFixShape()
    {
        var preFix = string.Join("\n",
            "                var gate = _queryStoreGates.GetOrAdd(runtime.ServerId, static _ => new QueryStoreServerGate()).TryAcquire();",
            "                if (gate is null)",
            "                {",
            "                    continue;",
            "                }",
            "",
            "                using var backfillGate = gate;",
            "",
            "                var step = _backfillSliceSteps.GetOrAdd(runtime.ServerId, static _ => new AbandonableStep());",
            "                var result = await step.RunAsync(",
            "                    () => backfill.RunServerSliceAsync(runtime, stoppingToken),",
            "                    BackfillSliceDeadline,",
            "                    holdUntilStepEnds: gate,",
            "                    cancellationToken: stoppingToken);");

        var offender = FindLeaseScopedToTheSlice(preFix);

        Assert.NotNull(offender);
        Assert.Contains("using var backfillGate", offender!, StringComparison.Ordinal);
        /* The offset is reported, and it is the one the `using` actually sits at — the property a count
           of occurrences cannot express. */
        Assert.Contains(
            preFix.IndexOf("using var backfillGate", StringComparison.Ordinal).ToString(CultureInfo.InvariantCulture),
            offender!,
            StringComparison.Ordinal);

        /* Positive control for the statement-boundary arm, through the same scanner: a handoff that has
           drifted out of the call's argument list must be named, not passed over. Asserted because a
           "must not contain" check is the kind that can only ever pass if nothing feeds it. */
        var driftedOut = string.Join("\n",
            "                var gate = _queryStoreGates.GetOrAdd(runtime.ServerId, static _ => new QueryStoreServerGate()).TryAcquire();",
            "                var result = await step.RunAsync(() => backfill.RunServerSliceAsync(runtime, stoppingToken), BackfillSliceDeadline);",
            "                Hand(holdUntilStepEnds: gate);");

        var drifted = FindLeaseScopedToTheSlice(driftedOut);
        Assert.NotNull(drifted);
        Assert.Contains("statement boundary", drifted!, StringComparison.Ordinal);
        Assert.Contains(
            driftedOut.IndexOf(';', driftedOut.IndexOf(StepCallAnchor, StringComparison.Ordinal))
                      .ToString(CultureInfo.InvariantCulture),
            drifted!,
            StringComparison.Ordinal);

        /* And the scanner reports a missing anchor rather than silently passing — the other way a source
           pin rots into a no-op. Each of the three anchors, so none of them can go missing quietly. */
        Assert.Contains("anchor", FindLeaseScopedToTheSlice("nothing relevant here")!, StringComparison.Ordinal);
        Assert.Contains(HandoffAnchor, FindLeaseScopedToTheSlice(AcquireAnchor)!, StringComparison.Ordinal);
        Assert.Contains(StepCallAnchor, FindLeaseScopedToTheSlice(AcquireAnchor + " " + HandoffAnchor)!, StringComparison.Ordinal);
    }

    private const string AcquireAnchor = "var gate = _queryStoreGates";
    private const string HandoffAnchor = "holdUntilStepEnds: gate";
    private const string StepCallAnchor = "step.RunAsync(";

    /* `using var x = ...` and `using (...)`, the only two forms that could scope the lease. Matched as
       code rather than as the bare word, so prose in the surrounding comments — which now discusses the
       `using` this pin forbids — cannot trip it. */
    private static readonly Regex LeaseScope = new(@"using\s+var|using\s*\(", RegexOptions.Compiled);

    /// <summary>
    /// Returns a description of a <c>using</c> scope covering the backfill slice, or <c>null</c> when the
    /// lease is handed off instead. Never returns null for a source it could not actually check.
    /// </summary>
    private static string? FindLeaseScopedToTheSlice(string source)
    {
        var acquire = source.IndexOf(AcquireAnchor, StringComparison.Ordinal);
        if (acquire < 0)
        {
            return $"the acquire anchor '{AcquireAnchor}' is gone — this pin cannot see the call site";
        }

        var handoff = source.IndexOf(HandoffAnchor, StringComparison.Ordinal);
        if (handoff < 0)
        {
            return $"the handoff anchor '{HandoffAnchor}' is gone — the lease is no longer given to the step";
        }

        if (handoff <= acquire)
        {
            return "the handoff must follow the acquire";
        }

        var call = source.IndexOf(StepCallAnchor, StringComparison.Ordinal);
        if (call < 0)
        {
            return $"the step-call anchor '{StepCallAnchor}' is gone — this pin cannot see the call site";
        }

        /* The handoff must be an ARGUMENT of the step call, not a statement after it. A statement
           boundary between the two means the lease is being disposed (or re-taken) by the caller again,
           which is the defect wearing different syntax. */
        if (call > handoff)
        {
            return $"the handoff at offset {handoff} precedes the step call at offset {call}";
        }

        var betweenCallAndHandoff = source[call..handoff];
        if (betweenCallAndHandoff.Contains(';', StringComparison.Ordinal))
        {
            return $"a statement boundary at offset {call + betweenCallAndHandoff.IndexOf(';', StringComparison.Ordinal)} "
                   + "separates the step call from the handoff — the lease is not an argument of the call";
        }

        var match = LeaseScope.Match(source[acquire..handoff]);
        if (!match.Success)
        {
            return null;
        }

        /* The whole offending LINE, so the failure message names the declaration rather than just the
           keyword that matched — and the offset, which is the part a count of occurrences cannot say. */
        var at = acquire + match.Index;
        var lineStart = source.LastIndexOf('\n', at) + 1;
        var lineEnd = source.IndexOf('\n', at);
        var line = (lineEnd < 0 ? source[lineStart..] : source[lineStart..lineEnd]).Trim();

        return $"'{line}' scopes the lease at offset {at} — an abandoned slice would release the gate " +
               "while it is still running on the server (#2165)";
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
