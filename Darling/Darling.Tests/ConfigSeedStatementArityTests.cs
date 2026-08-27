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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The config-store seed statements agree with themselves: as many values as columns, and every
/// parameter the command supplies is actually referenced by the SQL.
///
/// <para>Written after a live first-run failed with <c>42601: INSERT has more target columns than
/// expressions</c>. <c>config_service</c> named sixteen columns and supplied fifteen values, because
/// <c>$12</c> was bound on the command and never written into the VALUES list. Every subsequent value
/// shifted one column left, so <c>'seed'</c> landed on <c>updated_at</c> and <c>updated_by</c> had
/// nothing at all.</para>
///
/// <para><b>What that cost, which is why this is worth a pin.</b> The seed is how a FRESH store learns
/// the file's settings. It failed, the service logged it and carried on with the file config — and then
/// the control plane, which is authoritative after first contact, answered from an unseeded row where
/// every toggle is false. So <c>web.enabled</c> and <c>mcp.enabled</c> were true in darling.json, false
/// in the store, the store won, and neither endpoint ever listened. On a container deployment that is
/// the entire first-run experience, and nothing in the suite noticed because no test drives the seed
/// against an empty store.</para>
///
/// <para>Checked by parsing the shipped source rather than by executing it: the seed methods are private
/// and reaching them needs a live store, which is exactly why this went unpinned. Arity is a property of
/// the text, so the text is what gets read.</para>
/// </summary>
public sealed class ConfigSeedStatementArityTests
{
    private static readonly Regex Insert = new(
        @"INSERT\s+INTO\s+(?<table>[a-z_.]+)\s*\((?<cols>[^)]*)\)\s*VALUES\s*\((?<vals>[^)]*)\)",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

    [Fact]
    public void EverySeedInsert_SuppliesExactlyAsManyValuesAsColumns()
    {
        var source = File.ReadAllText(ProviderPath());
        var problems = new List<string>();
        var seen = 0;

        foreach (Match m in Insert.Matches(source))
        {
            var table = m.Groups["table"].Value;
            var cols = Split(m.Groups["cols"].Value);
            var vals = Split(m.Groups["vals"].Value);
            seen++;

            if (cols.Length != vals.Length)
            {
                problems.Add(
                    $"{table}: {cols.Length} columns but {vals.Length} values" +
                    $" — first unmatched column is '{cols.ElementAtOrDefault(Math.Min(vals.Length, cols.Length - 1))}'");
            }
        }

        /*
            A regex that stops matching passes for free, which is the failure this whole file is about.
            The floor is DERIVED from the file rather than a literal: `seen >= 4` was true of today's four
            statements, and would have gone on being true the day a fifth was added and one stopped
            parsing. Counting the INSERTs independently of the pattern that parses them means the two
            have to agree, so a statement the pattern cannot read is a failure rather than an absence.

            The specific way it could stop reading one: the column and value lists are captured with
            [^)]*, so a future seed carrying a function call — COALESCE($1, 0) — closes the capture early
            or fails the match outright. Loud is fine; silent is not, and this is what makes it loud.
        */
        var declared = Regex.Matches(source, @"INSERT\s+INTO\s+[a-z_.]+", RegexOptions.IgnoreCase).Count;
        Assert.True(
            seen == declared,
            $"parsed {seen} seed INSERTs but the file declares {declared} — the pattern cannot read one of "
          + "them (a parenthesis inside a column or value list will do it), so its arity is unchecked");

        Assert.True(problems.Count == 0,
            "a seed INSERT disagrees with itself, which fails at RUN time as 42601 and leaves a fresh "
          + "store unseeded — after which the control plane answers from defaults and the file's "
          + "settings are silently overridden:" + Environment.NewLine + string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void EverySeedInsert_ReferencesEveryParameterItBinds()
    {
        var source = File.ReadAllText(ProviderPath());
        var problems = new List<string>();

        foreach (Match m in Insert.Matches(source))
        {
            var table = m.Groups["table"].Value;
            var used = Regex.Matches(m.Groups["vals"].Value, @"\$(\d+)")
                .Select(x => int.Parse(x.Groups[1].Value))
                .ToHashSet();

            if (used.Count == 0)
            {
                continue;
            }

            /*
                Positional binding means the parameters are $1..$max with no gaps. A hole is not a
                cosmetic gap: it means a value the caller computed is silently dropped, and every
                parameter after the hole lands on the wrong column.
            */
            var missing = Enumerable.Range(1, used.Max()).Where(n => !used.Contains(n)).ToArray();
            if (missing.Length > 0)
            {
                problems.Add($"{table}: binds up to ${used.Max()} but never references " +
                    string.Join(", ", missing.Select(n => "$" + n)));
            }
        }

        Assert.True(problems.Count == 0,
            "a seed INSERT skips a positional parameter, so a supplied value is dropped and the ones "
          + "after it shift onto the wrong columns:" + Environment.NewLine + string.Join(Environment.NewLine, problems));
    }

    private static string[] Split(string list) =>
        list.Split(',')
            .Select(x => x.Trim())
            .Where(x => x.Length > 0)
            .ToArray();

    private static string ProviderPath() =>
        Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
