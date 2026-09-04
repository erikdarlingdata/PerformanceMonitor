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
using System.Reflection.Emit;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;

namespace Darling.Tests;

/// <summary>
/// <para>Finds call sites in a built assembly's IL by NAME of the callee, reporting for each one whether it
/// sits inside an exception-handler region. Five reachability pins were each carrying their own copy of this
/// walk (#2898); this is the one implementation they now share.</para>
///
/// <para><b>Why these pins read IL at all.</b> Reachability from a handler is not a value, so no arithmetic
/// assertion can express it — #2816's defect passed every arithmetic test in the suite because the arithmetic
/// was never wrong. Nor can it be read from source text: a UTF-16-from-offset-0 string scan of this same
/// assembly once reported a shipped change as absent on both the box and the artifact, because it found only
/// strings at even byte offsets and its positive controls did not share that failure mode.</para>
///
/// <para><b>Why this decodes instructions instead of scanning for opcode-shaped bytes.</b> The five original
/// copies tested every byte offset for <c>0x28</c>/<c>0x6F</c> and read the next four as a metadata token.
/// That is not a decoder: a byte inside some other instruction's operand can look like a call opcode. Two of
/// the five then advanced the cursor by four on a match, which can step over a genuine call instruction's own
/// token — a false NEGATIVE, and that is the dangerous direction for a "must be reachable" pin, because a
/// stamp that stopped being called reads as still called. Measured on the service assembly at the time this
/// was written: 13,007 method bodies, 70,775 real call sites, 415 byte offsets that look like a call and are
/// not, and 587 real call sites the skipping arithmetic can never visit. That none of the 587 happened to be
/// one of the tracked stamps was a property of that build's token VALUES, not of the scan.</para>
///
/// <para><b>The operand table is derived, not transcribed.</b> Instruction lengths come from
/// <see cref="OpCodes"/>' own fields via reflection, so the table is the runtime's rather than a hand-copied
/// one that can disagree with it in a corner. An opcode the table does not know throws instead of guessing a
/// length, because a mis-stepped decode silently reports the wrong offsets.</para>
///
/// <para><b>MethodSpec is the third token form, and the trap for anyone extending a pin.</b> A call to a
/// GENERIC method is emitted against a <c>MethodSpec</c> token (table <c>0x2B</c>) that points at the
/// underlying <c>MethodDef</c>/<c>MemberRef</c>, so collecting only those two tables reports a generic callee
/// as never called. It cost the #2890 pin its first red — <c>ServerWatermarkIsDiscarded</c> is generic in
/// <c>TRow</c> and was called on every cycle. On the service assembly 4,356 real call sites carry a
/// MethodSpec token and 51 distinct callee names are reachable ONLY that way. Most are BCL generics (LINQ,
/// <c>CollectionsMarshal.SetCount</c>, <c>DbDataReader.GetFieldValue</c>), but three of them are ours:
/// <c>WithGeminiCompatibleTools</c> (50 call sites), <c>CollectAsync</c> (7) and <c>WriteBatchAsync</c> (4) —
/// the last defined in <c>DarlingCollectorRunner</c>, the same class the phase-stamp pins scan. Resolved back
/// to the underlying member so a caller still asks by plain name.</para>
/// </summary>
internal static class IlCallSiteScanner
{
    /// <summary>
    /// One resolved call site: who contains it, what it calls, and whether it can run while unwinding.
    /// <para><c>MethodToken</c> is the containing body's MethodDefinition token, and is the only safe way to
    /// group call sites BY BODY: two overloads share a declaring type and a name, so grouping on those would
    /// merge them and report a call in one overload as co-occurring with a call in another.</para>
    /// </summary>
    internal readonly record struct TrackedCall(
        string DeclaringType,
        string MethodName,
        int MethodToken,
        string CalleeName,
        int Offset,
        bool InExceptionHandler);

    /// <summary>One decoded call instruction, before any name resolution.</summary>
    internal readonly record struct CallInstruction(int Offset, int Token);

    private const byte Call = 0x28;
    private const byte Callvirt = 0x6F;

    /* 0xFE introduces the second opcode page, so a leading 0xFE always means a two-byte opcode. OpCode.Value
       for those is the packed 0xFExx as a SIGNED short (Ceq is -511, not 65025), which is what the key
       computation below reproduces; single-byte values stay 0..255, so the two ranges cannot collide.

       The table does contain a one-byte entry at 0xFE — OpCodes.Prefix1, FlowControl.Meta, InlineNone — which
       represents the prefix rather than an instruction. Testing this byte BEFORE consulting the table is what
       shadows it. Collapsing that into a single lookup would read every two-byte opcode as a zero-operand
       Prefix1 and mis-step the rest of the body, which is the failure this class exists to prevent. */
    private const byte TwoBytePrefix = 0xFE;

    private static readonly Dictionary<short, OperandType> OperandTypes = BuildOperandTable();

    private static Dictionary<short, OperandType> BuildOperandTable()
    {
        var table = new Dictionary<short, OperandType>();

        foreach (var field in typeof(OpCodes).GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            if (field.GetValue(null) is OpCode opCode)
            {
                table[opCode.Value] = opCode.OperandType;
            }
        }

        return table;
    }

    /// <summary>How many entries the derived table holds, so a test can assert it was actually populated.</summary>
    internal static int KnownOpCodeCount => OperandTypes.Count;

    /// <summary>
    /// Decodes one method body and returns every <c>call</c>/<c>callvirt</c> instruction in it, at real
    /// instruction boundaries. Throws on an opcode the derived table does not know, or on a body that does
    /// not decode to exactly its own length — both mean the offsets this returns cannot be trusted, and a
    /// scanner that reports untrustworthy offsets quietly is worse than one that stops.
    /// </summary>
    internal static List<CallInstruction> DecodeCallSites(byte[] il, string bodyName = "<body>")
    {
        ArgumentNullException.ThrowIfNull(il);

        var calls = new List<CallInstruction>();
        var offset = 0;

        while (offset < il.Length)
        {
            var instructionStart = offset;

            short value;
            if (il[offset] == TwoBytePrefix)
            {
                if (offset + 1 >= il.Length)
                {
                    throw new InvalidOperationException(
                        $"{bodyName}: truncated two-byte opcode at IL offset {instructionStart}.");
                }

                value = unchecked((short)((il[offset] << 8) | il[offset + 1]));
                offset += 2;
            }
            else
            {
                value = il[offset];
                offset += 1;
            }

            if (!OperandTypes.TryGetValue(value, out var operandType))
            {
                throw new InvalidOperationException(
                    $"{bodyName}: unknown opcode 0x{value:X4} at IL offset {instructionStart}. The operand " +
                    "table is derived from System.Reflection.Emit.OpCodes, so this is an opcode the running " +
                    "runtime does not define — the decode cannot continue without guessing a length.");
            }

            var operandLength = OperandLength(operandType, il, offset, bodyName, instructionStart);

            if (operandType == OperandType.InlineMethod && (value == Call || value == Callvirt))
            {
                calls.Add(new CallInstruction(instructionStart, BitConverter.ToInt32(il, offset)));
            }

            offset += operandLength;
        }

        if (offset != il.Length)
        {
            throw new InvalidOperationException(
                $"{bodyName}: decode ran to IL offset {offset} in a {il.Length}-byte body. The last " +
                "instruction's operand length is wrong, so every offset reported here is suspect.");
        }

        return calls;
    }

    private static int OperandLength(OperandType type, byte[] il, int operandStart, string bodyName, int instructionStart)
    {
        var length = type switch
        {
            OperandType.InlineNone => 0,
            OperandType.ShortInlineBrTarget or OperandType.ShortInlineI or OperandType.ShortInlineVar => 1,
            OperandType.InlineVar => 2,
            OperandType.InlineBrTarget or OperandType.InlineField or OperandType.InlineI
                or OperandType.InlineMethod or OperandType.InlineSig or OperandType.InlineString
                or OperandType.InlineTok or OperandType.InlineType or OperandType.ShortInlineR => 4,
            OperandType.InlineI8 or OperandType.InlineR => 8,

            /* switch carries a uint32 case count followed by that many int32 targets, so its length is the
               only one that depends on the operand's own contents. Getting this wrong mis-steps the rest of
               the body, which is exactly the class of error the whole decode exists to avoid. */
            OperandType.InlineSwitch => SwitchLength(il, operandStart, bodyName, instructionStart),

            _ => throw new InvalidOperationException(
                $"{bodyName}: unhandled operand type {type} at IL offset {instructionStart}."),
        };

        if (operandStart + length > il.Length)
        {
            throw new InvalidOperationException(
                $"{bodyName}: operand for the instruction at IL offset {instructionStart} needs {length} " +
                $"byte(s) at {operandStart} but the body is only {il.Length} bytes.");
        }

        return length;
    }

    private static int SwitchLength(byte[] il, int operandStart, string bodyName, int instructionStart)
    {
        if (operandStart + 4 > il.Length)
        {
            throw new InvalidOperationException(
                $"{bodyName}: truncated switch case count at IL offset {instructionStart}.");
        }

        var cases = BitConverter.ToUInt32(il, operandStart);

        /* A corrupt or mis-stepped read here would multiply out to something absurd; fail with the number
           rather than overflowing into a negative length and walking backwards. */
        if (cases > (uint)(il.Length / 4))
        {
            throw new InvalidOperationException(
                $"{bodyName}: switch at IL offset {instructionStart} claims {cases} cases in a " +
                $"{il.Length}-byte body — the decode is mis-stepped.");
        }

        return 4 + (4 * (int)cases);
    }

    /// <summary>
    /// Finds every call to one of <paramref name="trackedNames"/> in the assembly at
    /// <paramref name="assemblyPath"/>. <paramref name="declaringTypeFilter"/>, when supplied, restricts the
    /// walk to types it accepts — the state-machine pins want one collector's generated type, not the whole
    /// assembly.
    /// </summary>
    internal static List<TrackedCall> FindCalls(
        string assemblyPath,
        IEnumerable<string> trackedNames,
        Func<string, bool>? declaringTypeFilter = null)
    {
        ArgumentNullException.ThrowIfNull(assemblyPath);
        ArgumentNullException.ThrowIfNull(trackedNames);

        var tracked = new HashSet<string>(trackedNames, StringComparer.Ordinal);
        var found = new List<TrackedCall>();

        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);
        var metadata = peReader.GetMetadataReader();

        var tokenToName = BuildTokenMap(metadata, tracked);

        foreach (var typeHandle in metadata.TypeDefinitions)
        {
            var type = metadata.GetTypeDefinition(typeHandle);
            var typeName = metadata.GetString(type.Name);

            if (declaringTypeFilter is not null && !declaringTypeFilter(typeName))
            {
                continue;
            }

            foreach (var methodHandle in type.GetMethods())
            {
                var method = metadata.GetMethodDefinition(methodHandle);
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

                var methodName = metadata.GetString(method.Name);
                var regions = body.ExceptionRegions;

                foreach (var call in DecodeCallSites(il, $"{typeName}.{methodName}"))
                {
                    if (!tokenToName.TryGetValue(call.Token, out var calleeName))
                    {
                        continue;
                    }

                    found.Add(new TrackedCall(
                        typeName,
                        methodName,
                        MetadataTokens.GetToken(methodHandle),
                        calleeName,
                        call.Offset,
                        InAnyHandler(regions, call.Offset)));
                }
            }
        }

        return found;
    }

    /// <summary>
    /// Aggregates <see cref="FindCalls"/> into the shape the reachability pins assert on: every tracked name
    /// present as a key, so a name with no call sites reads as an explicit zero rather than a missing entry.
    /// </summary>
    internal static Dictionary<string, (int Total, int InHandler)> CountCalls(
        string assemblyPath,
        IEnumerable<string> trackedNames,
        Func<string, bool>? declaringTypeFilter = null)
    {
        var tracked = trackedNames as IReadOnlyCollection<string> ?? trackedNames.ToArray();
        var counts = tracked.Distinct(StringComparer.Ordinal)
            .ToDictionary(name => name, _ => (Total: 0, InHandler: 0), StringComparer.Ordinal);

        foreach (var call in FindCalls(assemblyPath, tracked, declaringTypeFilter))
        {
            var current = counts[call.CalleeName];
            counts[call.CalleeName] =
                (current.Total + 1, current.InHandler + (call.InExceptionHandler ? 1 : 0));
        }

        return counts;
    }

    /// <summary>
    /// <para>Counts tracked calls per compiler-generated state machine, keyed by the name PREFIX the caller
    /// supplies. An async method's body lives in its generated <c>MoveNext</c> rather than in the source
    /// method, and the compiler appends its own ordinal
    /// (<c>&lt;FetchAndStorePlansAsync&gt;d__NN</c>), so callers name the stable prefix.</para>
    ///
    /// <para>A prefix that matches no type gets NO key, deliberately: the pins that use this check
    /// <c>ContainsKey</c> first, because a machine that was renamed would otherwise report zero calls to
    /// everything and satisfy a must-not-appear assertion for the wrong reason.</para>
    /// </summary>
    internal static Dictionary<string, Dictionary<string, int>> CountCallsByStateMachine(
        string assemblyPath,
        IEnumerable<string> stateMachinePrefixes,
        IEnumerable<string> trackedNames)
    {
        ArgumentNullException.ThrowIfNull(stateMachinePrefixes);
        ArgumentNullException.ThrowIfNull(trackedNames);

        var prefixes = stateMachinePrefixes.ToArray();
        var tracked = trackedNames.Distinct(StringComparer.Ordinal).ToArray();
        var results = new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal);

        string? MatchPrefix(string typeName) =>
            prefixes.FirstOrDefault(p => typeName.StartsWith(p, StringComparison.Ordinal));

        /* Two passes over the same filter so a matched machine gets its zero-filled row even when it calls
           nothing tracked — which is precisely the case a must-not-appear assertion is asserting. */
        foreach (var call in FindCalls(assemblyPath, tracked, typeName => MatchPrefix(typeName) is not null))
        {
            var machine = MatchPrefix(call.DeclaringType)!;
            var counts = Row(machine);
            counts[call.CalleeName]++;
        }

        foreach (var typeName in DeclaringTypeNames(assemblyPath))
        {
            if (MatchPrefix(typeName) is { } machine)
            {
                Row(machine);
            }
        }

        return results;

        Dictionary<string, int> Row(string machine)
        {
            if (!results.TryGetValue(machine, out var counts))
            {
                counts = tracked.ToDictionary(n => n, _ => 0, StringComparer.Ordinal);
                results[machine] = counts;
            }

            return counts;
        }
    }

    private static IEnumerable<string> DeclaringTypeNames(string assemblyPath)
    {
        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);
        var metadata = peReader.GetMetadataReader();

        foreach (var handle in metadata.TypeDefinitions)
        {
            yield return metadata.GetString(metadata.GetTypeDefinition(handle).Name);
        }
    }

    private static bool InAnyHandler(
        System.Collections.Immutable.ImmutableArray<ExceptionRegion> regions,
        int offset)
    {
        foreach (var region in regions)
        {
            if (offset >= region.HandlerOffset && offset < region.HandlerOffset + region.HandlerLength)
            {
                return true;
            }
        }

        return false;
    }

    private static Dictionary<int, string> BuildTokenMap(MetadataReader metadata, HashSet<string> tracked)
    {
        var tokenToName = new Dictionary<int, string>();

        /* A callee resolves through a MemberReference when it is defined in another assembly and through a
           MethodDefinition when it is defined in this one. Both, so the scan does not depend on which side
           of an assembly boundary the member happens to sit. */
        foreach (var handle in metadata.MemberReferences)
        {
            var name = metadata.GetString(metadata.GetMemberReference(handle).Name);
            if (tracked.Contains(name))
            {
                tokenToName[MetadataTokens.GetToken(handle)] = name;
            }
        }

        foreach (var handle in metadata.MethodDefinitions)
        {
            var name = metadata.GetString(metadata.GetMethodDefinition(handle).Name);
            if (tracked.Contains(name))
            {
                tokenToName[MetadataTokens.GetToken(handle)] = name;
            }
        }

        /* And the third form. Must run after the two above, because it resolves THROUGH them. */
        var methodSpecCount = metadata.GetTableRowCount(TableIndex.MethodSpec);
        for (var row = 1; row <= methodSpecCount; row++)
        {
            var specHandle = MetadataTokens.MethodSpecificationHandle(row);
            var target = metadata.GetMethodSpecification(specHandle).Method;

            var name = target.Kind switch
            {
                HandleKind.MethodDefinition =>
                    metadata.GetString(metadata.GetMethodDefinition((MethodDefinitionHandle)target).Name),
                HandleKind.MemberReference =>
                    metadata.GetString(metadata.GetMemberReference((MemberReferenceHandle)target).Name),
                _ => null,
            };

            if (name is not null && tracked.Contains(name))
            {
                tokenToName[MetadataTokens.GetToken(specHandle)] = name;
            }
        }

        return tokenToName;
    }
}
