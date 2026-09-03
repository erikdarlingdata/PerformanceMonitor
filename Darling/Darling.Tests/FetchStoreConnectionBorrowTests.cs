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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para>Pins that the Query Store plan and text fetches BORROW the caller's store connection rather than
/// acquiring their own (#2819).</para>
///
/// <para><b>Why this is a reachability test and not a timing one.</b> #2811 folded the store connection open
/// into the <c>probe:</c> phase and declined to split them, on the reasoning that doing so "would name a
/// connection pool rather than a cost". Measurement said it was exactly a connection pool: the probe SQL runs
/// in 0.4ms with nothing stale and 37ms with 40 stale ids, while the phase measured 673-6,663ms — and a cycle
/// with zero ids, which issues no SQL whatsoever, still cost 673ms. That floor is acquisition against a
/// <c>MaxPoolSize</c> of 24 sized for a "4-wide sweep" that is now 114 (server, database) pairs, each
/// formerly opening two connections of its own on top of the one the collector body already held.</para>
///
/// <para>A duration assertion cannot hold this. The number it would check is the very thing that varies with
/// pool contention, so it would be flaky when honest and vacuous when loose. The invariant that actually
/// matters is structural: <b>these two methods must not call <c>OpenConnectionAsync</c> at all.</b> That is an
/// IL reachability property, and it is what this test asserts.</para>
///
/// <para>It also guards the amplifier the acquisition created. The old private connection was scoped
/// <c>await using</c> across the whole method, so a pool slot was held for the duration of the SQL Server
/// target fetch — measured at 104,799ms on one database, one slot of twenty-four doing nothing store-side
/// for a hundred seconds. Borrowing removes the extra slot entirely, so no hold can span anything.</para>
///
/// <para>Reads IL rather than source text or strings deliberately: a UTF-16-from-offset-0 scan of this same
/// assembly recently reported a shipped change as absent on both the box and the artifact, because it found
/// strings only at even byte offsets and its positive controls did not share that failure mode.</para>
/// </summary>
public sealed class FetchStoreConnectionBorrowTests
{
    /* The two fetch paths, named rather than enumerated so that renaming one fails loudly instead of
       quietly shrinking the set under test. Matched as a prefix because the compiler appends its own
       state-machine ordinal (<FetchAndStorePlansAsync>d__NN). */
    private static readonly string[] FetchStateMachines =
    [
        "<FetchAndStorePlansAsync>",
        "<FetchAndStoreQueryTextAsync>",
    ];

    /* The call that must NOT appear. */
    private const string Acquire = "OpenConnectionAsync";

    /* Controls that MUST appear, and the reason they are these two specifically: both live in the Storage
       assembly, both are reached through exactly the same MemberReference machinery as the acquisition, and
       both exist unchanged on either side of this fix. So a scanner that has stopped resolving tokens fails
       the controls too, and cannot report the acquisition as absent for the wrong reason. A control that
       survives the failure mode it is meant to catch is decoration (#2816). */
    private static readonly string[] Controls =
    [
        "TouchAndProbePlansAsync",
        "TouchAndProbeTextsAsync",
    ];

    [Fact]
    public void FetchPaths_BorrowTheCallersStoreConnection_RatherThanAcquiringTheirOwn()
    {
        var scan = ScanFetchStateMachines();

        /* Scanner guard first: if the state machines were not found at all, every count is zero and the
           acquisition assertion below would pass for a reason that has nothing to do with the fix. */
        foreach (var machine in FetchStateMachines)
        {
            Assert.True(
                scan.ContainsKey(machine),
                $"No async state machine matching '{machine}' was found in the service assembly. The scanner " +
                "resolved nothing, so this test cannot say anything about connection acquisition.");
        }

        /* Positive controls, checked BEFORE the real assertion for the same reason. */
        foreach (var machine in FetchStateMachines)
        {
            var controlHits = Controls.Sum(c => scan[machine].GetValueOrDefault(c));
            Assert.True(
                controlHits > 0,
                $"{machine} contains no call to any of [{string.Join(", ", Controls)}]. Token resolution is " +
                "broken, so a zero count for " + Acquire + " would be meaningless.");
        }

        /* The property. */
        foreach (var machine in FetchStateMachines)
        {
            var acquisitions = scan[machine].GetValueOrDefault(Acquire);
            Assert.True(
                acquisitions == 0,
                $"{machine} calls {Acquire} {acquisitions} time(s). This fetch must borrow the store " +
                "connection the collector body already holds, not open its own: each acquisition costs a " +
                "measured 673-893ms floor against a pool of 24, at ~228 acquisitions per cycle, and a " +
                "privately-scoped connection is held across the SQL Server target fetch (104,799ms on one " +
                "measured database) doing nothing store-side.");
        }
    }

    /// <summary>
    /// Returns, per fetch state machine, a count of calls to each name of interest. Walks the compiler
    /// -generated state machine types rather than the source methods, because that is where an async
    /// method's body actually lives after compilation.
    /// </summary>
    private static Dictionary<string, Dictionary<string, int>> ScanFetchStateMachines()
    {
        var assemblyPath = typeof(PerformanceMonitor.Darling.Service.DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        var wanted = new HashSet<string>(Controls, StringComparer.Ordinal) { Acquire };
        var results = new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal);

        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);
        var metadata = peReader.GetMetadataReader();

        /* A callee is reachable through a MemberReference (defined in another assembly, which both the
           acquisition and the controls are) or a MethodDefinition (defined here). Collect both so the scan
           does not depend on where the callee happens to live. */
        var tokenToName = new Dictionary<int, string>();

        foreach (var handle in metadata.MemberReferences)
        {
            var name = metadata.GetString(metadata.GetMemberReference(handle).Name);
            if (wanted.Contains(name))
            {
                tokenToName[MetadataTokens.GetToken(handle)] = name;
            }
        }

        foreach (var handle in metadata.MethodDefinitions)
        {
            var name = metadata.GetString(metadata.GetMethodDefinition(handle).Name);
            if (wanted.Contains(name))
            {
                tokenToName[MetadataTokens.GetToken(handle)] = name;
            }
        }

        foreach (var typeHandle in metadata.TypeDefinitions)
        {
            var type = metadata.GetTypeDefinition(typeHandle);
            var typeName = metadata.GetString(type.Name);

            var machine = FetchStateMachines.FirstOrDefault(
                m => typeName.StartsWith(m, StringComparison.Ordinal));
            if (machine is null)
            {
                continue;
            }

            if (!results.TryGetValue(machine, out var counts))
            {
                counts = wanted.ToDictionary(n => n, _ => 0, StringComparer.Ordinal);
                results[machine] = counts;
            }

            foreach (var methodHandle in type.GetMethods())
            {
                var method = metadata.GetMethodDefinition(methodHandle);
                if (method.RelativeVirtualAddress == 0)
                {
                    continue;
                }

                var il = peReader.GetMethodBody(method.RelativeVirtualAddress).GetILBytes();
                if (il is null)
                {
                    continue;
                }

                for (var i = 0; i + 4 < il.Length; i++)
                {
                    /* call (0x28) and callvirt (0x6F), each followed by a 4-byte metadata token. */
                    if (il[i] != 0x28 && il[i] != 0x6F)
                    {
                        continue;
                    }

                    var token = BitConverter.ToInt32(il, i + 1);
                    if (tokenToName.TryGetValue(token, out var name))
                    {
                        counts[name]++;
                    }

                    i += 4;
                }
            }
        }

        return results;
    }
}
