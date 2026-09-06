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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The seam between the managed store's <c>postgresql.conf</c> and the store-log parser's severity
/// vocabulary (#3053).
///
/// <para><b>What was wrong.</b> initdb takes <c>lc_messages</c> from the host OS, and PostgreSQL translates
/// the SEVERITY LABEL along with the message body — a German host writes <c>FEHLER:</c> where an English one
/// writes <c>ERROR:</c>. <see cref="StoreLogClassifier"/> anchors on that field, and its residue class is
/// gated on <see cref="StoreLogClassifier.IsAtLeastWarning"/>: a token the gate does not recognise cannot
/// reach <c>unclassified</c>-retained and falls to the <c>routine</c> arm, counted with its text dropped. So
/// under a localised label the classifier's load-bearing property — a rule the table forgot costs a heading,
/// never a row — inverted and cost the row.</para>
///
/// <para><b>Why the pins are shaped this way.</b> A test that merely asserted the parser knows four English
/// tokens would pass on a German store, and a test that merely asserted the conf carries a line would pass
/// with the parser rewritten underneath it. These assert the RELATION: every token the parser accepts must be
/// one <c>error_severity()</c> can write under the locale the managed conf actually pins. Both sides are
/// derived — the accepted set by walking <see cref="StoreLogClassifier"/>'s own source, the pinned locale by
/// scanning every conf block the product appends, the guaranteed vocabulary by reading
/// <see cref="StoreLogClassifier.PrimarySeverities"/> — so neither side can change without this being
/// consulted, which a hand-written list of either could not manage.</para>
///
/// <para><b>Scope, stated rather than implied.</b> These are SOURCE and PURE-FUNCTION pins. No cluster is
/// started, so what is verified is that the product writes the pin and that the parser's vocabulary is
/// inside what the pin guarantees — not that a live server honoured it. And the pin reaches the MANAGED
/// store only: a bring-your-own store, and every monitored PostgreSQL target, keeps whatever
/// <c>lc_messages</c> its owner gave it.</para>
/// </summary>
public class StoreLogSeverityLocaleTests
{
    /// <summary>
    /// The locales under which PostgreSQL emits its message catalogue UNTRANSLATED, so
    /// <c>error_severity()</c> writes the English token names <see cref="StoreLogClassifier"/> matches.
    /// <c>C</c> and its <c>POSIX</c> alias are the only two that are guaranteed present with no locale
    /// installed on the host, which is why the conf pins one of them rather than <c>en_US.UTF-8</c>.
    /// </summary>
    private static readonly string[] UntranslatedLocales = ["C", "POSIX"];

    /// <summary>
    /// THE pin. Every severity token <see cref="StoreLogClassifier.IsAtLeastWarning"/> accepts has to be one
    /// <c>error_severity()</c> can write under the locale the managed <c>postgresql.conf</c> pins — which
    /// ties the parser's English assumption to the configuration that makes it true. Deleting the
    /// <c>lc_messages</c> pin from the conf turns this red instead of silently breaking the parser on a
    /// non-English host.
    ///
    /// <para>Three independent failures, so a regression says which half moved: no pin in the conf, a pin to
    /// a locale that translates, or a token the guaranteed vocabulary does not contain.</para>
    /// </summary>
    [Fact]
    public void EverySeverityTheParserAcceptsIsOneTheManagedConfGuarantees()
    {
        /* Side one: what the conf actually pins, read out of the blocks the product appends. */
        var found = PinnedMessageLocale(ManagedConfBlocks());

        Assert.True(
            found is not null,
            "No managed postgresql.conf block pins lc_messages. Without it initdb takes the locale from the host OS and "
            + "PostgreSQL translates the severity label, so StoreLogClassifier.IsAtLeastWarning stops recognising the "
            + "token that decides whether an entry keeps its text.");

        var pinned = found!;

        Assert.True(
            UntranslatedLocales.Contains(pinned, StringComparer.Ordinal),
            $"The managed conf pins lc_messages = '{pinned}', which is not a locale that leaves PostgreSQL's message "
            + $"catalogue untranslated ({string.Join(" or ", UntranslatedLocales)}). Under a translated catalogue "
            + "error_severity() writes localised labels and the severity tokens below are unreachable.");

        /* Side two: what the parser accepts, derived from its own source rather than restated here. */
        var accepted = AcceptedSeverities();
        var guaranteed = StoreLogClassifier.PrimarySeverities;

        var unguaranteed = accepted.Where(s => !guaranteed.Contains(s, StringComparer.Ordinal))
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            unguaranteed.Length == 0,
            $"IsAtLeastWarning accepts {string.Join(", ", unguaranteed.Select(s => $"'{s}'"))}, which error_severity() "
            + $"cannot write under lc_messages = '{pinned}'. StoreLogClassifier.PrimarySeverities is what that locale "
            + "guarantees; a token outside it can never match, so the class it was meant to reach is dead code.");
    }

    /// <summary>
    /// The parse behind the pin above, checked against the compiled method so a broken anchor fails loudly
    /// instead of vacuously. Over-reading shows up as a parsed token the method rejects; under-reading shows
    /// up as a guaranteed token the method accepts but the parse never saw. Without this a regex that stopped
    /// matching would make the pin pass on an empty set — a pass for the wrong reason, which is worth less
    /// than a failure.
    /// </summary>
    [Fact]
    public void TheAcceptedSeverityParseAgreesWithTheCompiledGate()
    {
        var accepted = AcceptedSeverities();

        Assert.NotEmpty(accepted);

        foreach (var severity in accepted)
        {
            Assert.True(
                StoreLogClassifier.IsAtLeastWarning(severity),
                $"The source walk read '{severity}' out of IsAtLeastWarning's expression, but the compiled method "
                + "rejects it — the parse is reading something other than the gate's severity literals.");
        }

        foreach (var severity in StoreLogClassifier.PrimarySeverities)
        {
            if (accepted.Contains(severity, StringComparer.Ordinal))
            {
                continue;
            }

            Assert.False(
                StoreLogClassifier.IsAtLeastWarning(severity),
                $"The compiled method accepts '{severity}' but the source walk did not find it — the parse is missing "
                + "literals, so the pin above is checking an incomplete set.");
        }
    }

    /// <summary>
    /// The v10 block itself: the marker, the pin, and nothing else. It rides after the v8 hardware check, whose
    /// staleness test keys on the LAST fingerprint line in the text read before any of these appends, so a
    /// fingerprint or a sizing line here would be both invisible to that check and able to override it.
    /// </summary>
    [Fact]
    public void MessageLocaleConfAppend_PinsV10Marker_AndCarriesNothingButTheLocale()
    {
        var block = DarlingManagedPostgres.BuildMessageLocaleConfAppend();

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV10, block, StringComparison.Ordinal);
        Assert.Contains("lc_messages = 'C'", block, StringComparison.Ordinal);

        Assert.DoesNotContain(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, block, StringComparison.Ordinal);
        Assert.DoesNotContain("shared_buffers", block, StringComparison.Ordinal);
        Assert.DoesNotContain("maintenance_work_mem", block, StringComparison.Ordinal);
        Assert.DoesNotContain("max_worker_processes", block, StringComparison.Ordinal);

        /* lc_messages ALONE. lc_monetary/lc_numeric/lc_time govern how the server renders values the product
           reads as typed parameters rather than as text, so pinning them would be a behaviour change with no
           defect behind it. */
        Assert.DoesNotContain("lc_monetary", block, StringComparison.Ordinal);
        Assert.DoesNotContain("lc_numeric", block, StringComparison.Ordinal);
        Assert.DoesNotContain("lc_time", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// A conf block nobody appends is inert, and the failure is silent — the builder compiles, its own pin
    /// passes, and no store ever gains the setting. So for EVERY <c>ConfMarkerV*</c> the class declares,
    /// <c>EnsureConfAppended</c> has to test that marker. Derived from the declared constants rather than
    /// listed, so the next versioned block is covered the moment its marker exists.
    /// </summary>
    [Fact]
    public void EveryDeclaredConfMarkerIsCheckedByEnsureConfAppended()
    {
        var markers = typeof(DarlingManagedPostgres)
            .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.StartsWith("ConfMarker", StringComparison.Ordinal))
            .Select(f => f.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(markers);

        var body = EnsureConfAppendedBody();

        foreach (var marker in markers)
        {
            Assert.Contains(marker, body, StringComparison.Ordinal);
        }
    }

    // ── Deriving each side ────────────────────────────────────────────────────────────────

    /// <summary>
    /// The severity literals in <see cref="StoreLogClassifier.IsAtLeastWarning"/>'s expression, read from the
    /// file. The gate is a pattern (<c>severity is "WARNING" or ...</c>), which reflection cannot see into,
    /// so the source is the only place the accepted set exists as data. Walked with
    /// <see cref="CSharpSourceWalker"/> so the anchor cannot land in a comment and a comment inside the span
    /// cannot contribute a token.
    /// </summary>
    private static string[] AcceptedSeverities()
    {
        var source = File.ReadAllText(ClassifierSourcePath());
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        const string Anchor = "bool IsAtLeastWarning(";
        var declaration = code.IndexOf(Anchor, StringComparison.Ordinal);

        Assert.True(
            declaration >= 0,
            $"'{Anchor}' was not found in the code of {ClassifierSourcePath()} — the gate was renamed or reshaped and "
            + "this walk is no longer reading it.");

        Assert.True(
            code.IndexOf(Anchor, declaration + Anchor.Length, StringComparison.Ordinal) < 0,
            $"'{Anchor}' appears more than once, so this walk would silently read only the first. Point it at the "
            + "declaration that decides retention.");

        var end = code.IndexOf(';', declaration);

        Assert.True(end > declaration, "IsAtLeastWarning's expression has no terminating ';' in the code stream.");

        return CSharpSourceWalker.StringLiteralBodies(source)
            .Where(l => l.Start > declaration && l.Start < end)
            .Select(l => l.Text)
            .ToArray();
    }

    /// <summary>
    /// Every conf block the product appends, concatenated — discovered by reflection over the
    /// <c>Build*ConfAppend</c> factories rather than listed, so a locale pinned from a future block is still
    /// found and a block that stops existing is not silently skipped. The arguments are placeholders: these
    /// factories are pure, and nothing read out of the text here depends on a size or a port.
    /// </summary>
    private static string ManagedConfBlocks()
    {
        var factories = typeof(DarlingManagedPostgres)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
            .Where(m => m.Name.StartsWith("Build", StringComparison.Ordinal)
                        && m.Name.EndsWith("ConfAppend", StringComparison.Ordinal)
                        && m.ReturnType == typeof(string))
            .OrderBy(m => m.Name, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(factories);

        var blocks = new List<string>(factories.Length);

        foreach (var factory in factories)
        {
            var arguments = factory.GetParameters().Select(p => Placeholder(factory, p)).ToArray();
            blocks.Add((string)factory.Invoke(null, arguments)!);
        }

        return string.Concat(blocks);
    }

    /// <summary>A stand-in value for a conf factory's parameter, or a failure naming the signature this scan
    /// cannot fill — never a skip, which would drop the block from the scan without saying so.</summary>
    private static object Placeholder(MethodInfo factory, ParameterInfo parameter)
    {
        if (parameter.ParameterType == typeof(int))
        {
            return 32;
        }

        if (parameter.ParameterType == typeof(long))
        {
            return 16L * 1024 * 1024 * 1024;
        }

        Assert.Fail(
            $"{factory.Name} takes a {parameter.ParameterType.Name} parameter '{parameter.Name}' this scan cannot "
            + "supply, so its block would go unscanned. Extend the placeholders rather than excluding the block.");

        return null!;
    }

    /// <summary>The value of the LAST <c>lc_messages</c> assignment in <paramref name="conf"/>, or null when
    /// there is none. Last, not first: postgresql.conf takes the last occurrence of a setting, which is the
    /// whole mechanism the versioned blocks override each other through.</summary>
    private static string? PinnedMessageLocale(string conf)
    {
        var matches = Regex.Matches(
            conf,
            @"^[ \t]*lc_messages[ \t]*=[ \t]*'(?<locale>[^']*)'",
            RegexOptions.Multiline | RegexOptions.CultureInvariant);

        return matches.Count == 0 ? null : matches[^1].Groups["locale"].Value;
    }

    /// <summary>The brace-balanced body of <c>EnsureConfAppended</c>, comments and literals blanked so a
    /// marker named only in a comment cannot satisfy the wiring pin.</summary>
    private static string EnsureConfAppendedBody()
    {
        var source = File.ReadAllText(ManagedPostgresSourcePath());
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        const string Anchor = "void EnsureConfAppended(";
        var declaration = code.IndexOf(Anchor, StringComparison.Ordinal);

        Assert.True(
            declaration >= 0,
            $"'{Anchor}' was not found in the code of {ManagedPostgresSourcePath()} — the append path was renamed and "
            + "this pin is no longer reading it.");

        var open = code.IndexOf('{', declaration);

        Assert.True(open > declaration, "EnsureConfAppended has no body in the code stream.");

        return CSharpSourceWalker.BraceBalanced(code, open);
    }

    private static string ClassifierSourcePath() =>
        Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Storage", "StoreLogClassifier.cs");

    private static string ManagedPostgresSourcePath() =>
        Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedPostgres.cs");

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);

        return dir!;
    }
}
