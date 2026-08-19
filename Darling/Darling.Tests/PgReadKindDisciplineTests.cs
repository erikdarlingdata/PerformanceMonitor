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
/// The naive-UTC bind discipline for the PostgreSQL read family (#2213 round 2). The store's timestamp
/// columns are <c>timestamp without time zone</c>; a <c>Kind=Utc</c> DateTime makes Npgsql infer
/// timestamptz, and PostgreSQL resolves the mixed comparison by converting the NAIVE side at the store
/// session's TimeZone — which initdb takes from the host OS. East of UTC every fresh row falls outside the
/// freshness window and the three Tier 0 outage predictors silently never fire; west of UTC the window
/// stretches and alerts grade stale data. No error is raised anywhere, and every UTC-hosted test store
/// hides it, which is why this is a SOURCE pin rather than a live assertion: the live store that would
/// catch it is exactly the one nobody runs.
///
/// <para>The established adapter documents the same convention (DarlingAlertReadAdapter's NaiveUtcNow and
/// DarlingDataReader's AddTimestamp); this pin holds the NEW family to it. It scans for the hazard —
/// binding a window parameter or a raw <c>DateTime.UtcNow</c> without stripping Kind — rather than for the
/// idiom, so a refactor that binds safely through a different helper still passes.</para>
/// </summary>
public sealed class PgReadKindDisciplineTests
{
    [Fact]
    public void NoPgReaderBindsAKindUtcTimestamp()
    {
        var offenders = new List<string>();

        foreach (var path in PgReadFamilyFiles())
        {
            var source = File.ReadAllText(path);
            var name = Path.GetFileName(path);

            /* Bare window-parameter binds: any *Utc-named identifier bound raw. The callers hand these
               down from DateTime.UtcNow, so an unwrapped bind ships Kind=Utc — and the name-suffix match
               survives a rename to fooUtc where a literal startUtc/endUtc list would not. */
            foreach (Match match in Regex.Matches(
                source, @"AddWithValue\(\s*\w*[Uu]tc\s*\)"))
            {
                offenders.Add(name + ": " + match.Value);
            }

            /* Direct now-arithmetic binds: AddWithValue(DateTime.UtcNow ...) — the alert adapter's
               original form. */
            foreach (Match match in Regex.Matches(
                source, @"AddWithValue\(\s*DateTime\.UtcNow"))
            {
                offenders.Add(name + ": " + match.Value);
            }
        }

        Assert.True(
            offenders.Count == 0,
            "PostgreSQL read binds a Kind=Utc timestamp against the store's naive columns: "
            + string.Join(", ", offenders)
            + ". Wrap the value in DateTime.SpecifyKind(..., DateTimeKind.Unspecified) at the bind (or a "
            + "NaiveUtcNow helper) — Kind=Utc infers timestamptz, the session zone shifts the window, and "
            + "east of UTC the Tier 0 alerts silently never fire.");
    }

    [Fact]
    public void TheAdapterHelper_ActuallyStripsKind()
    {
        /* The three adapter binds route through NaiveUtcNow(), so the scan above cannot see them revert
           if the HELPER quietly becomes DateTime.UtcNow again. Pin its body: the SpecifyKind call is the
           whole point of the function. */
        var adapter = PgReadFamilyFiles().Last();
        var source = File.ReadAllText(adapter);
        var body = Regex.Match(
            source,
            @"private static DateTime NaiveUtcNow\(\)\s*=>\s*(?<body>[^;]+);",
            RegexOptions.Singleline);

        Assert.True(body.Success, "DarlingPostgresAlertReadAdapter must carry the NaiveUtcNow helper.");
        Assert.Contains("SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified)", body.Groups["body"].Value, StringComparison.Ordinal);
    }

    private static IEnumerable<string> PgReadFamilyFiles([CallerFilePath] string thisFile = "")
    {
        var testsDir = Path.GetDirectoryName(thisFile)!;
        var service = Path.GetFullPath(Path.Combine(testsDir, "..", "PerformanceMonitor.Darling.Service"));

        foreach (var reader in Directory.EnumerateFiles(Path.Combine(service, "Mcp"), "DarlingPg*Reader.cs"))
        {
            yield return reader;
        }

        /* The Tools siblings create the DateTime.UtcNow values the readers bind — a Tools file that
           starts binding directly is the same hazard one hop up. */
        foreach (var tools in Directory.EnumerateFiles(Path.Combine(service, "Mcp"), "DarlingMcpPg*Tools.cs"))
        {
            yield return tools;
        }

        yield return Path.Combine(service, "DarlingPostgresAlertReadAdapter.cs");
    }
}
