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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para>Pins that each Query Store fetch records the probe's INPUT size (#2823) — the number of references
/// handed to the touch/probe round trip — separately from the ids it later attempted against the target.</para>
///
/// <para><b>Why this needs a pin rather than a comment.</b> The split line has always logged
/// <c>PerItemPlanIdsAttempted</c>, which is incremented only inside the target chunk loop, so it counts the
/// ids that came back MISSING. But <c>probe:</c> scales with what went IN. Measured on production, the probe
/// costs ~0.61ms per reference, dead linear: 78 references took 49ms, 272 took 166ms, 847 took 517ms. A
/// database that probes 847 references and owes nothing logs <c>0 ids</c> beside half a second of real store
/// work.</para>
///
/// <para>Dividing one by the other produced a phantom finding twice. #2819 read a 673ms cost on
/// <c>ids=0</c> passes as pure <c>OpenConnectionAsync</c> and derived a "~140x gap" between production and a
/// benchmark. #2822 was then expected to drive that floor to zero; it did not, and briefly read as a failed
/// fix. It was not — the floor fell 544ms to 397ms, and the 147ms delta matches this box's measured 139ms
/// connect cost almost exactly. What remained was never acquisition: it was the probe round trip over
/// hundreds of references that the log never showed.</para>
///
/// <para>So the invariant is structural, not numeric: <b>both fetches must stamp the probe input size where
/// the probe input is known.</b> A duration assertion would pin the very number that varies with workload.
/// This reads IL for the same reason #2822 does — a string scan of this assembly once reported a shipped
/// change as absent on both the box and the artifact, and its positive controls did not share that failure
/// mode.</para>
/// </summary>
public sealed class ProbeDenominatorTests
{
    /* Prefix-matched: the compiler appends its own ordinal (<FetchAndStorePlansAsync>d__NN). Named rather
       than enumerated so renaming a fetch fails loudly instead of quietly shrinking the set under test. */
    private static readonly string[] FetchStateMachines =
    [
        "<FetchAndStorePlansAsync>",
        "<FetchAndStoreQueryTextAsync>",
    ];

    /* The setters that MUST appear — one per fetch, keyed by the state machine that has to call it. Pairing
       them this way is the point: a refactor that stamped the plan size in both paths, or collapsed the two
       fields into one, would leave the text side's denominator wrong again. */
    private static readonly Dictionary<string, string> RequiredSetter = new(StringComparer.Ordinal)
    {
        ["<FetchAndStorePlansAsync>"] = "set_PerItemPlanProbeIds",
        ["<FetchAndStoreQueryTextAsync>"] = "set_PerItemTextProbeIds",
    };

    /* Controls that MUST appear. Both are the probe calls themselves: they live in another assembly, resolve
       through exactly the same MemberReference machinery as the setters, and exist unchanged either side of
       this change. A scanner that has stopped resolving tokens fails these too, so it cannot report a
       missing setter for the wrong reason. A control that survives the failure mode it guards is decoration
       (#2816). */
    private static readonly string[] Controls =
    [
        "TouchAndProbePlansAsync",
        "TouchAndProbeTextsAsync",
    ];

    [Fact]
    public void EachFetch_StampsItsOwnProbeInputSize_SoProbeCostHasAnHonestDenominator()
    {
        var scan = ScanFetchStateMachines();

        /* Scanner guard first: if the state machines were not found, every count is zero and the assertions
           below would pass or fail for reasons unrelated to the code under test. */
        foreach (var machine in FetchStateMachines)
        {
            Assert.True(
                scan.ContainsKey(machine),
                $"No async state machine matching '{machine}' was found in the service assembly. The scanner " +
                "resolved nothing, so this test cannot say anything about the probe denominator.");
        }

        /* Positive controls, checked BEFORE the real assertion, same discipline as #2822. */
        foreach (var machine in FetchStateMachines)
        {
            var controlHits = Controls.Sum(c => scan[machine].GetValueOrDefault(c));
            Assert.True(
                controlHits > 0,
                $"{machine} contains no call to any of [{string.Join(", ", Controls)}]. Token resolution is " +
                "broken, so a zero count for the probe-size setter would be meaningless.");
        }

        foreach (var machine in FetchStateMachines)
        {
            var setter = RequiredSetter[machine];
            Assert.True(
                scan[machine].GetValueOrDefault(setter) > 0,
                $"{machine} never calls {setter}. The split line would then report the probe's cost beside " +
                "the count of ids it attempted against the TARGET, which is a different quantity — the " +
                "exact conflation that produced a phantom 140x gap in #2819 and made #2822 read as a " +
                "failed fix (#2823).");
        }
    }

    [Fact]
    public void ProbeInputSize_IsSeparateFromAttemptedIds_AndBothDefaultToZero()
    {
        /* Distinct storage, not an alias: a pass can probe hundreds of references and attempt zero ids, and
           the log has to be able to say so. Defaulting to zero matters for the same reason the rest of the
           split does — a host that does not measure this must read as "not measured", never as "free". */
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = new DateTime(2026, 9, 3, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
        };

        Assert.Equal(0, context.PerItemPlanProbeIds);
        Assert.Equal(0, context.PerItemTextProbeIds);

        context.PerItemPlanProbeIds = 847;
        context.PerItemTextProbeIds = 300;

        Assert.Equal(0, context.PerItemPlanIdsAttempted);
        Assert.Equal(0, context.PerItemTextIdsAttempted);
        Assert.Equal(847, context.PerItemPlanProbeIds);
        Assert.Equal(300, context.PerItemTextProbeIds);
    }

    /// <summary>
    /// Scans the compiler-generated state machine types for the two fetches, counting calls by callee name.
    /// Same shape as <see cref="FetchStoreConnectionBorrowTests"/>: an async method's body lives in its
    /// state machine after compilation, not in the source method.
    /// </summary>
    private static Dictionary<string, Dictionary<string, int>> ScanFetchStateMachines()
    {
        var assemblyPath = typeof(DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        var wanted = new HashSet<string>(Controls, StringComparer.Ordinal);
        foreach (var setter in RequiredSetter.Values)
        {
            wanted.Add(setter);
        }

        var results = new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal);

        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);
        var metadata = peReader.GetMetadataReader();

        /* A callee is reachable through a MemberReference (defined in another assembly — both the setters
           and the controls are) or a MethodDefinition. Collect both so the scan does not depend on where
           the callee happens to live. */
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

                /* Every offset is tested and nothing is skipped, matching #2822's scanner. Here the
                   assertion is "greater than zero" rather than "zero", so over-counting is the risk rather
                   than under-counting — but the controls are counted identically and a spurious match on a
                   four-byte operand cannot conjure the specific setter token this test names. Advancing
                   past a match would risk stepping over a genuine call, which is the worse direction for
                   the paired must-appear assertion. */
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
                }
            }
        }

        return results;
    }
}
