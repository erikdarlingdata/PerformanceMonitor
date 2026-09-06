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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1922: <c>CREATE EXTENSION IF NOT EXISTS timescaledb</c> TERMINATES THE BACKEND when the library is on
/// disk but absent from <c>shared_preload_libraries</c> — it is not an ordinary ERROR.
/// <c>TimescaleSupport.TryEnableAsync</c> turns that into <c>false</c> like any other failure, so its
/// contract reads as "carry on in plain-PostgreSQL mode" while the connection it was handed is dead. Anything
/// that keeps using that connection then fails with <c>InvalidOperationException: Connection is not open</c>
/// from a frame several calls downstream, naming the real cause nowhere: the #1794 masking signature, in
/// setup rather than teardown.
///
/// <para>Both invariants below are source-parsed, and that is the honest way to hold them. Reproducing the
/// crash needs a PostgreSQL deliberately misconfigured in one specific way, which no CI rig is and none
/// should be made to be; what actually regresses is a call site being written the unsafe way, and that is
/// textual.</para>
/// </summary>
public sealed class TimescaleEnableConnectionSafetyTests
{
    /// <summary>
    /// Every test-side call goes through <see cref="LiveTimescaleProbe"/> (which risks only its own
    /// connection) or is asserted true on the spot.
    ///
    /// <para>The <c>Assert.True</c> sites are deliberately still allowed rather than swept: on a dead
    /// connection they fail immediately with their own message ("the rig is expected to have TimescaleDB"),
    /// which is a true and useful diagnosis rather than a masked one. What is banned is the shape that
    /// masks — taking the result and carrying on with the same connection.</para>
    /// </summary>
    [Fact]
    public void NoLiveTest_KeepsUsingAConnectionItHandedToTryEnableAsync()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not locate Darling/Darling.Tests from the test output directory.");

        var offenders = new List<string>();

        foreach (var file in Directory.EnumerateFiles(directory!, "*.cs", SearchOption.AllDirectories))
        {
            var name = Path.GetFileName(file);

            /* The probe is where the guarded call is SUPPOSED to live, so its own call is not an offender.
               This file used to be skipped beside it because it names the method throughout in prose and in
               regex literals; the walk below removes both, so the exemption stopped earning its place and an
               unsafe call written HERE is now caught like any other. */
            if (name is nameof(LiveTimescaleProbe) + ".cs")
            {
                continue;
            }

            /* Comments and literal text are blanked before anything is matched, because prose about the call
               is not a call. A line-prefix filter cannot deliver that: a block comment's continuation lines
               in this codebase carry no asterisk, and the worked example in this very file's
               `IsCheckedOnTheSpot` note is one — an indented `await TimescaleSupport.TryEnableAsync(...)`
               inside a block comment, which a prefix filter reads as an unchecked call. Newlines survive the
               walk, so the reported line is still the file's own. */
            var text = CSharpSourceWalker.StripCommentsAndStrings(
                File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal));
            var lines = text.Split('\n');

            /* Whitespace-tolerant, and matched against the whole FILE rather than line by line: a call split
               as `TimescaleSupport\n    .TryEnableAsync(...)` is the same call, and a guard that a line break
               can slip past is not a guard. Same reason the window below is flattened before it is read. */
            foreach (Match call in Regex.Matches(text, @"TimescaleSupport\s*\.\s*TryEnableAsync\s*\("))
            {
                var lineIndex = text.Take(call.Index).Count(c => c == '\n');

                if (!IsCheckedOnTheSpot(lines, lineIndex))
                {
                    offenders.Add($"{name}:{lineIndex + 1}");
                }
            }
        }

        Assert.True(offenders.Count == 0,
            "these call TimescaleSupport.TryEnableAsync on a connection they keep using, so an unpreloaded " +
            "TimescaleDB masks the real cause behind \"Connection is not open\" (#1922). Route them through " +
            "LiveTimescaleProbe.TryEnableAsync, or check the result before anything else touches the " +
            "connection — either inline in Assert.True(await ...), or captured and checked in the very NEXT " +
            $"statement with Assert.True(x) / Assert.SkipWhen(!x): {string.Join(", ", offenders)}");
    }

    /// <summary>
    /// The product half of the same invariant, and the reason #1922 was fixed test-side ONLY.
    ///
    /// <para><c>DarlingWorker</c> is safe by construction: it opens a DEDICATED connection for the TimescaleDB
    /// block and gates every subsequent call on the returned flag, so a <c>false</c> return means nothing
    /// touches that connection again before it is disposed. That is a property of how the block happens to be
    /// written, not something the compiler holds — move one call out from under the flag and the service
    /// acquires exactly the masking the tests just shed. This pins it so "the product is safe" stays a fact
    /// rather than a thing that was true once.</para>
    /// </summary>
    [Fact]
    public void DarlingWorker_TouchesTheTimescaleConnectionOnlyBehindTheAvailabilityFlag()
    {
        var worker = File.ReadAllText(FindRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs")));

        /* The block: from the dedicated connection to the catch that degrades to plain-PostgreSQL mode. */
        var open = worker.IndexOf("await using var timescaleConnection", StringComparison.Ordinal);
        Assert.True(open >= 0, "DarlingWorker no longer opens a dedicated connection for the TimescaleDB block.");

        var close = worker.IndexOf("catch (Exception ex) when (ex is not OperationCanceledException)", open, StringComparison.Ordinal);
        Assert.True(close > open, "could not find the end of DarlingWorker's TimescaleDB block.");

        var block = worker[open..close];

        var gate = block.IndexOf("if (_timescaleAvailable)", StringComparison.Ordinal);
        Assert.True(gate >= 0, "DarlingWorker's TimescaleDB block no longer gates on _timescaleAvailable.");

        /* Every TimescaleSupport call in the block except the enable itself must sit after the gate. */
        var ungated = Regex.Matches(block, @"TimescaleSupport\.(\w+)")
            .Where(m => m.Groups[1].Value != "TryEnableAsync" && m.Index < gate)
            .Select(m => m.Groups[1].Value)
            .ToList();

        Assert.True(ungated.Count == 0,
            "these run on the TimescaleDB connection before _timescaleAvailable has been checked, so a " +
            "CREATE EXTENSION that killed the backend would surface as \"Connection is not open\" instead of " +
            $"the real cause (#1922): {string.Join(", ", ungated)}");

        /* And the connection stays dedicated — a shared one would carry the damage out of the block whatever
           the gate does. */
        Assert.Contains("await using var timescaleConnection = await postgres.OpenConnectionAsync", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether the call on <paramref name="index"/> has its result checked before the connection could be
    /// used again — either asserted inline, or captured and asserted within the next few lines.
    ///
    /// <para>Both shapes are accepted deliberately. An inline-only rule would flag this, which is safe:</para>
    /// <code>
    /// var enabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
    /// Assert.True(enabled, "the rig is expected to have TimescaleDB");
    /// </code>
    /// <para>and a guard that fires on a safe shape is worse than no guard — the next person routes around it
    /// rather than reading it. The capture form is tied to its OWN variable, and the check must be the very
    /// NEXT statement: "checked somewhere nearby" is not the property that matters, since anything between
    /// the capture and the check would itself be running on a possibly-dead connection.</para>
    ///
    /// <para><paramref name="lines"/> is <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output,
    /// which is what makes the statement split below sound: a semicolon inside a comment between the call
    /// and its assertion would otherwise end the statement early and report a checked site as an
    /// offender.</para>
    /// </summary>
    private static bool IsCheckedOnTheSpot(string[] lines, int index)
    {
        /* The statement plus the next few, flattened to one string. Flattening is what lets a call or an
           assertion wrapped across lines read the same as the one-line form — the blind spot a purely
           line-by-line scan leaves. The window starts at the call's line, so an assertion ABOVE it is not
           mistaken for a check of it. */
        var window = string.Join(' ', lines.Skip(index).Take(4));

        if (Regex.IsMatch(window, @"Assert\s*\.\s*True\s*\(\s*await\s+TimescaleSupport\s*\.\s*TryEnableAsync"))
        {
            return true;
        }

        /* Split into STATEMENTS rather than scanning the window as one blob, because "the result is checked
           somewhere nearby" is not the property that matters. This would otherwise pass:

               var enabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
               await SomethingElse(connection, ct);   // runs on a possibly-dead connection, unchecked
               Assert.True(enabled, "...");

           The check has to come BEFORE anything else touches the connection, so the assertion must be the
           very next statement — which is the discipline every call site already follows. */
        var statements = window.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        for (var i = 0; i < statements.Length - 1; i++)
        {
            var captured = Regex.Match(statements[i], @"var\s+(\w+)\s*=\s*await\s+TimescaleSupport\s*\.\s*TryEnableAsync\s*\(");
            if (!captured.Success)
            {
                continue;
            }

            /* Tied to the CAPTURED variable, so an unrelated assertion cannot launder an unchecked call.
               Assert.True or Assert.SkipWhen: both stop before the connection is used again, which is the
               property, and a rig legitimately without TimescaleDB should skip rather than fail. */
            var variable = Regex.Escape(captured.Groups[1].Value);
            return Regex.IsMatch(statements[i + 1], $@"Assert\s*\.\s*(True\s*\(\s*{variable}\b|SkipWhen\s*\(\s*!\s*{variable}\b)");
        }

        return false;
    }

    private static string? FindTestProjectDirectory()
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, "Darling", "Darling.Tests");
            if (Directory.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }

        return null;
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }

        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
