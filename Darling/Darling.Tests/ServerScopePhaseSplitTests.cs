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
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para>Pins the SERVER-SCOPED phase split (#2851) — the <c>open: + drain: + other:</c> breakdown for
/// collectors that read a whole server in one query rather than enumerating databases.</para>
///
/// <para><b>Why it exists.</b> The breakdown was emitted only behind <c>PerItemOpenMs &gt; 0</c>, which the
/// per-database path sets, so the largest collector on the fleet could not be attributed at all.
/// procedure_stats runs 4,900ms p50 on use1 while the same shipped query, run from the same box against the
/// same target, takes 247ms — an 18.8x gap measured with the box at 4% CPU. Same shape as the store probe's
/// ~40x (36ms in a harness, ~1,451ms in production), and that one was only tractable because #2811/#2816 had
/// split its phases.</para>
///
/// <para><b>Two pins, because one cannot see what the other misses.</b> The arithmetic pin below holds the
/// residual honest. The IL pin holds the STAMPS reachable — and #2816 is the reason both are needed: the
/// probe's arithmetic pin passed throughout that defect, because the arithmetic was never wrong. A plain
/// assignment after a throwing <c>await</c> left the phase at zero and the residual silently absorbed the
/// cost, which for a timed-out store round trip is precisely inverted.</para>
/// </summary>
public sealed class ServerScopePhaseSplitTests
{
    /* The two server-scoped stamps. Named rather than enumerated off the type so that DELETING a stamp fails
       loudly instead of quietly shrinking the set this test checks — the failure mode where a guard keeps
       passing while covering less. */
    private static readonly string[] PhaseSetters =
    [
        "set_ServerScopeOpenMs",
        "set_ServerScopeDrainMs",
    ];

    /* A setter assigned WITHOUT an exception handler, in the same assembly, resolved through the same
       metadata tables and the same IL walk. Two jobs: if the scanner stops resolving tokens its Total goes to
       zero and says so, and if handler-detection degenerates to "true for everything" its InHandler stops
       being zero. A control that cannot fail in the same way as the thing it guards proves nothing — the
       first control tried here (set_PerItemTextBudgetExceeded) resolved to zero call sites because it is
       assigned in the Collectors assembly, not this one, and would have failed for the wrong reason.
       set_Watermark is plain data assignment with six call sites, none of them in a handler, so it is
       unlikely to migrate into one and quietly stop discriminating. */
    private const string ControlSetter = "set_Watermark";

    [Fact]
    public void TheSplitSumsToItsParent_SoTheResidualIsAResidualAndNotASlushFund()
    {
        /* Ordinary run: open and drain are measured, other absorbs query building, command construction,
           the optional probe-failure rowset and the supplemental query. */
        var result = new CollectorRunResult(
            Rows: 150,
            SqlMs: 4644,
            StorageMs: 184,
            ServerPhasesMeasured: true,
            ServerOpenMs: 3900,
            ServerDrainMs: 700,
            ServerWatermarkMs: 12);

        Assert.Equal(44, result.ServerOtherMs);
        Assert.Equal(
            result.SqlMs,
            result.ServerOpenMs + result.ServerDrainMs + result.ServerOtherMs);
    }

    [Fact]
    public void TheResidualClampsAtZero_SoStopwatchSkewNeverPrintsNegative()
    {
        /* open + drain measured on separate stopwatches can exceed the parent by a millisecond or two. That
           must surface as zero, not as a negative term that makes the whole line look broken. */
        var skewed = new CollectorRunResult(
            Rows: 1,
            SqlMs: 100,
            StorageMs: 0,
            ServerPhasesMeasured: true,
            ServerOpenMs: 60,
            ServerDrainMs: 45);

        Assert.Equal(0, skewed.ServerOtherMs);
    }

    [Fact]
    public void AnUnmeasuredRunReportsNotMeasured_RatherThanAZeroThatLooksLikeAnInstantOpen()
    {
        /* The enumerated and Azure branches leave the flag false. The log site gates on the FLAG, so their
           zeros never print as a split — which is the distinction `PerItemOpenMs > 0` cannot make, because
           it reads a genuinely instant open and a path that measures nothing as the same thing. */
        var enumerated = new CollectorRunResult(Rows: 3956, SqlMs: 316065, StorageMs: 41);

        Assert.False(enumerated.ServerPhasesMeasured);
        Assert.Equal(0, enumerated.ServerOpenMs);
        Assert.Equal(0, enumerated.ServerDrainMs);

        /* And a measured run whose open really was instant still reports measured, with a zero that means
           what it says. */
        var instant = new CollectorRunResult(
            Rows: 0, SqlMs: 0, StorageMs: 0, ServerPhasesMeasured: true);

        Assert.True(instant.ServerPhasesMeasured);
        Assert.Equal(0, instant.ServerOtherMs);
    }

    [Fact]
    public void TheWatermarkIsCarriedButExcludedFromTheSum_BecauseItIsNotInsideSqlOnThisPath()
    {
        /* The server-scoped watermark read runs BEFORE the sql: stopwatch starts, so it is not part of
           SqlMs. #2851's own text assumes it is ("sql_duration_ms is wm: + open: + drain:"), and folding it
           into the decomposition would have printed a permanent wm:0ms — teaching every future reader that a
           store read #2796 clocked at 50s cold is free. It is carried and reported, outside the sum. */
        var result = new CollectorRunResult(
            Rows: 10,
            SqlMs: 1000,
            StorageMs: 5,
            ServerPhasesMeasured: true,
            ServerOpenMs: 600,
            ServerDrainMs: 300,
            ServerWatermarkMs: 5000);

        Assert.Equal(5000, result.ServerWatermarkMs);
        Assert.Equal(100, result.ServerOtherMs);
        Assert.Equal(
            result.SqlMs,
            result.ServerOpenMs + result.ServerDrainMs + result.ServerOtherMs);
    }

    [Fact]
    public void PhaseStamps_AreReachableFromExceptionHandlers_SoAThrowingPhaseStillReportsItsTime()
    {
        var counts = ScanServiceAssembly();

        var (controlTotal, controlInHandler) = counts[ControlSetter];
        Assert.True(
            controlTotal > 0,
            $"The control setter {ControlSetter} resolved to zero call sites — the scanner read nothing, so " +
            "the assertions below would pass or fail for reasons unrelated to the defect.");
        Assert.Equal(0, controlInHandler);

        foreach (var setter in PhaseSetters)
        {
            var (total, inHandler) = counts[setter];

            Assert.True(
                total > 0,
                $"{setter} was not called anywhere in the service assembly — either the stamp was removed or " +
                "the scanner resolved nothing. Either way this test can say nothing about reachability.");

            Assert.True(
                inHandler > 0,
                $"{setter} is never invoked from inside an exception handler ({total} call site(s), all on " +
                "success paths). A phase that throws — a command timeout on the open, a budget expiry mid-drain — " +
                "will report 0ms and its cost lands in the other: residual, which is documented as time spent " +
                "in neither the target nor the store. That is the #2816 defect, on a new path.");
        }
    }

    /// <summary>
    /// Walks every method body in the built service assembly and, for each call to one of the tracked
    /// setters, records whether the call offset falls inside an exception-handler region. Reads IL rather
    /// than source text or strings: a UTF-16-from-offset-0 scan of this same assembly recently reported a
    /// shipped change as absent on both the box and the artifact, because it found only strings at even byte
    /// offsets and its positive controls did not share that failure mode.
    /// </summary>
    private static Dictionary<string, (int Total, int InHandler)> ScanServiceAssembly()
    {
        var assemblyPath = typeof(DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        var tracked = PhaseSetters.Append(ControlSetter).ToArray();
        var results = tracked.ToDictionary(name => name, _ => (Total: 0, InHandler: 0));

        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);
        var metadata = peReader.GetMetadataReader();

        /* A setter resolves through a MemberReference or a MethodDefinition depending on whether the caller
           sits in this assembly or another, and async state machines move the call around. Collect both so
           the scan does not depend on where the compiler happened to put it. */
        var tokenToName = new Dictionary<int, string>();

        foreach (var handle in metadata.MemberReferences)
        {
            var name = metadata.GetString(metadata.GetMemberReference(handle).Name);
            if (results.ContainsKey(name))
            {
                tokenToName[MetadataTokens.GetToken(handle)] = name;
            }
        }

        foreach (var handle in metadata.MethodDefinitions)
        {
            var name = metadata.GetString(metadata.GetMethodDefinition(handle).Name);
            if (results.ContainsKey(name))
            {
                tokenToName[MetadataTokens.GetToken(handle)] = name;
            }
        }

        foreach (var handle in metadata.MethodDefinitions)
        {
            var method = metadata.GetMethodDefinition(handle);
            if (method.RelativeVirtualAddress == 0)
            {
                continue;
            }

            var body = peReader.GetMethodBody(method.RelativeVirtualAddress);
            var il = body.GetILBytes();
            if (il is null)
            {
                continue;
            }

            var regions = body.ExceptionRegions;

            for (var i = 0; i + 4 < il.Length; i++)
            {
                /* call (0x28) and callvirt (0x6F) are the only two forms a property setter is emitted as,
                   each followed by a 4-byte metadata token. */
                if (il[i] != 0x28 && il[i] != 0x6F)
                {
                    continue;
                }

                var token = BitConverter.ToInt32(il, i + 1);
                if (!tokenToName.TryGetValue(token, out var setterName))
                {
                    continue;
                }

                var current = results[setterName];
                var inHandler = current.InHandler;

                foreach (var region in regions)
                {
                    if (i >= region.HandlerOffset && i < region.HandlerOffset + region.HandlerLength)
                    {
                        inHandler++;
                        break;
                    }
                }

                results[setterName] = (current.Total + 1, inHandler);
                i += 4;
            }
        }

        return results;
    }
}
