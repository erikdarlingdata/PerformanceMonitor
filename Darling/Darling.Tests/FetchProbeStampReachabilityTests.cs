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
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;
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
    /// Walks every method body in the built service assembly and, for each call to one of the probe
    /// setters, records whether the call offset falls inside an exception-handler region. Reads IL rather
    /// than strings deliberately: a UTF-16-from-offset-0 scan of this same assembly recently reported a
    /// shipped change as absent on both the box and the artifact, because it only found strings at even
    /// byte offsets and its positive controls did not share that failure mode.
    /// </summary>
    private static Dictionary<string, (int Total, int InHandler)> ScanServiceAssembly()
    {
        var assemblyPath = typeof(PerformanceMonitor.Darling.Service.DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        var results = ProbeSetters.ToDictionary(name => name, _ => (Total: 0, InHandler: 0));

        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);
        var metadata = peReader.GetMetadataReader();

        /* A setter is reachable through a MemberReference or a MethodDefinition depending on whether the
           caller is in this assembly or another; collect both so the scan does not depend on where the
           compiler happened to put the state machine. */
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
