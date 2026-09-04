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
/// <para>Tests for <see cref="IlCallSiteScanner"/> itself (#2898) — the instrument four reachability pins
/// now depend on. A scanner that reports fewer call sites than exist makes every one of those pins pass
/// vacuously, so the scanner needs its own witnesses, and they have to fail for the reasons that actually
/// broke the idiom rather than for a reason nobody would hit.</para>
///
/// <para><b>Two unrelated defects, so two shapes of proof.</b> The MethodSpec gap is reproducible against
/// the shipped assembly and is pinned that way below. The <c>i += 4</c> arithmetic is not: measured across
/// the whole service assembly, the skipping form loses none of the tracked stamps for ANY choice of tracked
/// name, because no byte that merely looks like a call happens to carry a live metadata token within four
/// bytes of a real one. That is a property of one build's token values, not of the scan — the same
/// measurement finds 415 offsets that look like a call and are not, and 587 real call sites the arithmetic
/// can never visit. So the arithmetic is pinned on a hand-built body where the coincidence is arranged
/// rather than hoped for, which is also the only form that stays red on a later build.</para>
/// </summary>
public sealed class IlCallSiteScannerTests
{
    /// <summary>
    /// A generic method on the collector runner — the same class the phase-stamp pins scan. Calls to it are
    /// emitted against a MethodSpec token, so it is invisible to a MemberRef+MethodDef-only map, which is
    /// exactly the trap this scanner exists to have closed.
    /// </summary>
    private const string GenericCallee = "WriteBatchAsync";

    /* A four-byte metadata token whose LAST byte is 0x28, chosen so that the same four bytes can appear both
       as a genuine call's operand and, shifted by four, as the bytes a stray 0x28 would read as its token.
       Little-endian on disk: 01 02 03 28. */
    private const int ContrivedToken = 0x28030201;

    /// <summary>
    /// <para>The reason this scanner decodes instructions instead of testing every byte offset. On this
    /// hand-built body a <c>ldc.i4</c> operand contains a byte that looks like <c>call</c>, and the four
    /// bytes after it happen to equal the tracked token — so the skipping form matches at that byte, jumps
    /// four bytes past it, and lands beyond the genuine <c>call</c> that follows. It reports one call site
    /// and the one it reports is not real.</para>
    ///
    /// <para>Both discarded forms are exercised here rather than described, because a comment saying the
    /// skip is unsound cannot fail. If someone reintroduces <c>i += 4</c> into
    /// <see cref="IlCallSiteScanner"/>, this is the test that goes red.</para>
    /// </summary>
    [Fact]
    public void TheSkippingByteScanLosesARealCall_WhichIsWhyTheScannerDecodesInstructions()
    {
        /* ldc.i4 0x03020128  |  call 0x28030201  |  ret
           offset 0 .. 4         offset 5 .. 9       offset 10

           il[1] is the phantom: a 0x28 sitting inside the ldc.i4 operand. Reading il[2..5] as a token gives
           0x28030201 — the tracked token — because il[5] is the genuine call's own opcode byte. */
        byte[] il =
        [
            0x20, 0x28, 0x01, 0x02, 0x03,   // ldc.i4 <operand containing a call-shaped byte at offset 1>
            0x28, 0x01, 0x02, 0x03, 0x28,   // call 0x28030201  <- the genuine call site, at offset 5
            0x2A,                            // ret
        ];

        const int PhantomOffset = 1;
        const int GenuineOffset = 5;

        /* What the scanner does now: one call site, at the real instruction boundary. */
        var decoded = IlCallSiteScanner.DecodeCallSites(il, nameof(TheSkippingByteScanLosesARealCall_WhichIsWhyTheScannerDecodesInstructions));

        Assert.Equal(1, decoded.Count);
        Assert.Equal(GenuineOffset, decoded[0].Offset);
        Assert.Equal(ContrivedToken, decoded[0].Token);

        /* The form two of the four pins shipped: it loses the genuine call entirely and substitutes a
           phantom, so a "must be called from a handler" assertion would be answering about a byte inside an
           unrelated operand. A false negative, and that is the dangerous direction for these pins. */
        var skipping = LegacyByteScan(il, ContrivedToken, advancePastMatch: true);

        Assert.DoesNotContain(GenuineOffset, skipping);
        Assert.Contains(PhantomOffset, skipping);

        /* The form the other two shipped, and #2890's sibling pin: a superset. It finds the genuine call, so
           it is safe for a must-appear assertion, but it also reports the phantom — which is why it cannot
           be used for a must-NOT-appear one, and why decoding is better than either. */
        var everyOffset = LegacyByteScan(il, ContrivedToken, advancePastMatch: false);

        Assert.Contains(GenuineOffset, everyOffset);
        Assert.Contains(PhantomOffset, everyOffset);
        Assert.True(
            everyOffset.Count > decoded.Count,
            "The every-offset form is supposed to over-report here. If it stopped doing so this body no " +
            "longer arranges the coincidence it was built to arrange, and the assertions above prove nothing.");
    }

    /// <summary>
    /// <para>A two-byte opcode carrying its own operand, decoded ahead of a genuine call. This pins the one
    /// piece of the decoder that looks redundant and is not: <c>OpCodes</c> contains a ONE-byte entry at
    /// <c>0xFE</c> (<c>Prefix1</c>, <c>FlowControl.Meta</c>, <c>InlineNone</c>) representing the second-page
    /// prefix rather than an instruction, so the scanner must test that byte before consulting the table.</para>
    ///
    /// <para>Collapse it into a single lookup and <c>ldftn</c> below reads as a zero-operand <c>Prefix1</c>;
    /// the decode then resumes inside <c>ldftn</c>'s operand, reads the <c>0x20</c> there as a <c>ldc.i4</c>
    /// whose own four bytes swallow the real <c>call</c>, and the call site disappears. That is the same
    /// class of miss as the <c>i += 4</c> skip, arrived at from the other direction.</para>
    /// </summary>
    [Fact]
    public void ATwoByteOpcodeIsDecodedAsTwoBytes_NotAsTheOneByteInlineNonePrefixEntry()
    {
        /* ldftn <4-byte operand>  |  call 0x28030201  |  ret
           offset 0 .. 5              offset 6 .. 10      offset 11

           The operand's first byte is 0x20 (ldc.i4) on purpose: if the prefix is mis-decoded as one byte, the
           decode lands there and ldc.i4's four operand bytes consume the genuine call's opcode at offset 6. */
        byte[] il =
        [
            0xFE, 0x06,                      // ldftn (two-byte opcode, InlineMethod)
            0x20, 0x00, 0x00, 0x00,          // its operand — first byte doubles as a ldc.i4 opcode
            0x28, 0x01, 0x02, 0x03, 0x28,    // call 0x28030201  <- the genuine call site, at offset 6
            0x2A,                             // ret
        ];

        var decoded = IlCallSiteScanner.DecodeCallSites(il, nameof(ATwoByteOpcodeIsDecodedAsTwoBytes_NotAsTheOneByteInlineNonePrefixEntry));

        /* ldftn is InlineMethod but is neither call nor callvirt, so it is correctly not a call site. */
        Assert.Equal(1, decoded.Count);
        Assert.Equal(6, decoded[0].Offset);
        Assert.Equal(ContrivedToken, decoded[0].Token);
    }

    /// <summary>
    /// A body whose last operand runs off the end must throw rather than return the call sites it managed to
    /// read. A partial answer from this scanner is indistinguishable from a complete one at the call site, and
    /// every pin that depends on it asserts on a count.
    /// </summary>
    [Fact]
    public void ATruncatedBodyThrows_RatherThanReturningTheCallSitesItManagedToRead()
    {
        /* call <token>, then a ldc.i4 with only two of its four operand bytes present. */
        byte[] truncated =
        [
            0x28, 0x01, 0x02, 0x03, 0x28,   // call 0x28030201
            0x20, 0x00, 0x00,                // ldc.i4 — operand cut short
        ];

        var ex = Assert.Throws<InvalidOperationException>(
            () => IlCallSiteScanner.DecodeCallSites(truncated, "cut-short-body"));

        /* Asserted on the diagnostic's own content, not on the body name this test passed in: the point is
           that it names WHERE the decode ran out, so a real failure is actionable from the message alone. */
        Assert.Contains("cut-short-body", ex.Message, StringComparison.Ordinal);
        Assert.Contains("IL offset 5", ex.Message, StringComparison.Ordinal);
        Assert.Contains("only 8 bytes", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The decoder against the shipped artifact rather than a hand-built body: every method body in the
    /// service assembly must decode to exactly its own length with no unknown opcode. A mis-stepped decode
    /// walks off an instruction boundary and then reports garbage offsets, and there is no way to notice
    /// that from a call-site count — but it cannot land exactly on the end of thousands of bodies by luck.
    /// </summary>
    [Fact]
    public void EveryMethodBodyInTheServiceAssembly_DecodesToExactlyItsOwnLength()
    {
        Assert.True(
            IlCallSiteScanner.KnownOpCodeCount > 200,
            $"The operand table holds only {IlCallSiteScanner.KnownOpCodeCount} opcodes. It is built by " +
            "reflecting over System.Reflection.Emit.OpCodes' fields, so a near-empty table means that " +
            "reflection found nothing and every body below would 'decode' by throwing.");

        var assemblyPath = typeof(DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);
        var metadata = peReader.GetMetadataReader();

        var bodies = 0;
        var callSites = 0;
        var failures = new List<string>();

        foreach (var handle in metadata.MethodDefinitions)
        {
            var method = metadata.GetMethodDefinition(handle);
            if (method.RelativeVirtualAddress == 0)
            {
                continue;
            }

            var il = peReader.GetMethodBody(method.RelativeVirtualAddress).GetILBytes();
            if (il is null)
            {
                continue;
            }

            var name = metadata.GetString(method.Name);
            bodies++;

            try
            {
                callSites += IlCallSiteScanner.DecodeCallSites(il, name).Count;
            }
            catch (InvalidOperationException ex)
            {
                if (failures.Count < 10)
                {
                    failures.Add(ex.Message);
                }
            }
        }

        Assert.True(
            failures.Count == 0,
            $"{failures.Count} method body/bodies did not decode cleanly:{Environment.NewLine}" +
            string.Join(Environment.NewLine, failures));

        /* Floors rather than exact counts: the assembly grows every release, and a pin on the exact number
           would be a chore that teaches nothing. These are low enough to survive ordinary growth and high
           enough that a scanner reading nothing — the failure mode that makes every dependent pin vacuous —
           cannot satisfy them. Measured at the time of writing: 13,007 bodies, 70,775 call sites. */
        Assert.True(bodies > 5_000, $"Only {bodies} method bodies were read; the scan found almost nothing.");
        Assert.True(callSites > 20_000, $"Only {callSites} call sites were decoded across {bodies} bodies.");
    }

    /// <summary>
    /// <para>MethodSpec resolution, pinned against the real assembly. <see cref="GenericCallee"/> is generic,
    /// so every call to it is emitted against a MethodSpec token; the MemberRef+MethodDef map that the four
    /// original scanners built cannot see a single one.</para>
    ///
    /// <para>Asserted as a comparison rather than as a count, so it states the thing that matters: dropping
    /// the MethodSpec loop takes a genuinely-called method to zero call sites. Both halves are computed here,
    /// which is what makes this the red-first witness for that half of #2898 — remove the loop from
    /// <see cref="IlCallSiteScanner"/> and the two numbers converge and this fails.</para>
    /// </summary>
    [Fact]
    public void AGenericCalleeIsInvisibleWithoutMethodSpecResolution_WhichIsTheTrapForAnyoneExtendingAPin()
    {
        var assemblyPath = typeof(DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        var withSpec = IlCallSiteScanner.CountCalls(assemblyPath, [GenericCallee])[GenericCallee];

        Assert.True(
            withSpec.Total > 0,
            $"{GenericCallee} resolved to zero call sites even WITH MethodSpec resolution. Either it was " +
            "renamed or made non-generic — in which case this pin needs a different generic callee, and " +
            "should not simply be deleted, because the trap it documents has not gone anywhere.");

        var withoutSpec = CountIgnoringMethodSpec(assemblyPath, GenericCallee);

        Assert.Equal(0, withoutSpec);
        Assert.True(
            withSpec.Total > withoutSpec,
            $"{GenericCallee} has {withSpec.Total} call site(s) with MethodSpec resolution and " +
            $"{withoutSpec} without. Equal counts mean the MethodSpec loop is no longer doing anything, so " +
            "a generic stamp added to any of the reachability pins would read as never called.");
    }

    /// <summary>
    /// The dev-era byte scan, kept here as the thing being ruled out rather than as a comment claiming it is
    /// wrong. Returns the offsets it would treat as call sites for a single tracked token.
    /// </summary>
    private static List<int> LegacyByteScan(byte[] il, int trackedToken, bool advancePastMatch)
    {
        var hits = new List<int>();

        for (var i = 0; i + 4 < il.Length; i++)
        {
            if (il[i] != 0x28 && il[i] != 0x6F)
            {
                continue;
            }

            if (BitConverter.ToInt32(il, i + 1) != trackedToken)
            {
                continue;
            }

            hits.Add(i);

            if (advancePastMatch)
            {
                i += 4;
            }
        }

        return hits;
    }

    /// <summary>
    /// Counts call sites for one name using the MemberRef+MethodDef map only — the map the four original
    /// scanners built. Exists so the MethodSpec assertion above can compare against it instead of asserting
    /// a bare number that would pass whether or not the loop was there.
    /// </summary>
    private static int CountIgnoringMethodSpec(string assemblyPath, string calleeName)
    {
        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);
        var metadata = peReader.GetMetadataReader();

        var tokens = new HashSet<int>();

        foreach (var handle in metadata.MemberReferences)
        {
            if (metadata.GetString(metadata.GetMemberReference(handle).Name) == calleeName)
            {
                tokens.Add(MetadataTokens.GetToken(handle));
            }
        }

        foreach (var handle in metadata.MethodDefinitions)
        {
            if (metadata.GetString(metadata.GetMethodDefinition(handle).Name) == calleeName)
            {
                tokens.Add(MetadataTokens.GetToken(handle));
            }
        }

        var count = 0;

        foreach (var handle in metadata.MethodDefinitions)
        {
            var method = metadata.GetMethodDefinition(handle);
            if (method.RelativeVirtualAddress == 0)
            {
                continue;
            }

            var il = peReader.GetMethodBody(method.RelativeVirtualAddress).GetILBytes();
            if (il is null)
            {
                continue;
            }

            foreach (var call in IlCallSiteScanner.DecodeCallSites(il, metadata.GetString(method.Name)))
            {
                if (tokens.Contains(call.Token))
                {
                    count++;
                }
            }
        }

        return count;
    }
}
