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
using System.Text;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The contract behind <see cref="ICollectorSchemaInfo.RequiredPgExtensions"/> (#3187):
/// <c>Darling/README.md</c>'s permissions paragraph is pinned to that declaration by
/// <see cref="ReadmeDerivedCountPinTests"/>, so a declaration nobody makes is a paragraph nothing holds.
///
/// <para><b>What this file adds that the README pin cannot.</b> The README pin fires when the declaration
/// and the paragraph disagree. It says nothing when a NEW collector reads an extension and simply never
/// declares it — both sides stay consistent and both stay wrong, which is exactly how the paragraph came
/// to name four of six. So the declaration is checked against the collectors' own query TEXT: a
/// PostgreSQL collector whose SQL touches an object an extension owns has to declare that extension.</para>
///
/// <para><b>The enumeration here is of PostgreSQL, not of this product.</b>
/// <see cref="ExtensionObjects"/> maps an object name to the extension that ships it —
/// <c>pgstatindex</c> comes from <c>pgstattuple</c>, <c>pg_wait_sampling_profile</c> from
/// <c>pg_wait_sampling</c>. That map goes stale only when PostgreSQL's extensions change, not when this
/// repo adds a collector, which is the drift that actually happened.</para>
/// </summary>
public sealed class PgExtensionDependencyContractTests
{
    /// <summary>
    /// Object names an extension owns, and which extension owns each. A collector's query touching one of
    /// these cannot run on a server without that extension.
    ///
    /// <para><c>pg_wait_sampling</c> and <c>auto_explain</c> appear only through their FUNCTIONS, never as
    /// bare names: <c>pg_wait_sampling</c> is also a GUC prefix and a collector name, and
    /// <c>auto_explain</c> owns no SQL object at all — it is a hook module read through its GUCs, which is
    /// why nothing declares it and <c>pg_plan_capture_readiness</c> can report on it without needing
    /// it.</para>
    /// </summary>
    private static readonly (string Token, string Extension)[] ExtensionObjects =
    {
        ("pg_stat_statements", "pg_stat_statements"),
        ("pg_stat_kcache", "pg_stat_kcache"),
        ("pg_qualstats", "pg_qualstats"),
        ("pg_buffercache", "pg_buffercache"),
        ("pgstattuple", "pgstattuple"),
        ("pgstatindex", "pgstattuple"),
        ("pg_wait_sampling_profile", "pg_wait_sampling"),
        ("pg_wait_sampling_history", "pg_wait_sampling"),
        ("hypopg", "hypopg"),
    };

    /// <summary>
    /// The collectors that name an extension in their query text in order to REPORT on it rather than to
    /// read it, each with why. These are the collectors whose whole job is answering "is this installed",
    /// so a rule that made them declare what they mention would make them undeployable on the servers they
    /// exist to describe.
    /// </summary>
    private static readonly Dictionary<string, string> ReportsRatherThanReads = new(StringComparer.Ordinal)
    {
        ["pg_extension_availability"] =
            "carries the monitoring-relevant roster as a VALUES list so that ABSENCE is reportable, and "
            + "reads pg_available_extensions and pg_extension, which are core catalogs",
        ["pg_table_bloat_stats"] =
            "records a pgstattuple_available flag read from pg_extension so a consumer knows whether an "
            + "exact measurement was possible; its own bloat figure is statistics-based and needs nothing "
            + "installed",
        ["pg_plan_capture_readiness"] =
            "reads GUCs and pg_available_extensions to report whether plan capture could work, and names "
            + "pg_stat_statements only inside the operator advice it emits",
    };

    [Fact]
    public void EveryDeclaration_IsWellFormedAndDeclaredOnlyByPostgresCollectors()
    {
        var declaring = CollectorCatalog.All.Where(c => c.RequiredPgExtensions.Count > 0).ToList();

        /* Non-vacuity first. Every assertion below is over `declaring`, so an empty list would pass all of
           them while proving that the declaration surface exists and nothing uses it. */
        Assert.NotEmpty(declaring);

        foreach (var definition in declaring)
        {
            Assert.True(definition.TargetEngine == CollectorTargetEngine.PostgreSql,
                $"{definition.Name} declares a PostgreSQL extension dependency but its TargetEngine is "
                + $"{definition.TargetEngine}. RequiredPgExtensions lives on the engine-neutral schema "
                + "surface so the catalog can enumerate it without the row type, which means nothing in the "
                + "type system stops a T-SQL definition from filling it in.");

            var names = definition.RequiredPgExtensions.Select(e => e.ExtensionName).ToList();

            Assert.Equal(names.Count, names.Distinct(StringComparer.Ordinal).Count());

            foreach (var extension in definition.RequiredPgExtensions)
            {
                Assert.False(string.IsNullOrWhiteSpace(extension.ExtensionName));

                /* Spelled exactly as CREATE EXTENSION and shared_preload_libraries spell it: both are
                   case-sensitive in the places this name gets compared, and the README pin matches it as a
                   literal. */
                Assert.Equal(
                    extension.ExtensionName,
                    extension.ExtensionName.ToLowerInvariant(),
                    StringComparer.Ordinal);

                Assert.True(
                    extension.InstallKind is PgExtensionInstallKind.CreateExtension
                        or PgExtensionInstallKind.SharedPreloadLibraries,
                    $"{definition.Name} declares {extension.ExtensionName} with an install kind this "
                    + "contract does not know how to describe to an operator.");
            }
        }
    }

    /// <summary>
    /// The guard that makes "a new collector with an extension dependency declares it" true rather than
    /// hoped for. Reads each PostgreSQL definition's own query text and requires every extension-owned
    /// object it touches to be declared.
    /// </summary>
    [Fact]
    public void EveryPostgresCollectorTouchingAnExtensionObject_DeclaresThatExtension()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null,
            "repo root not found -- this pin reads the collector sources and cannot run without them");

        var postgres = CollectorCatalog.All
            .Where(c => c.TargetEngine == CollectorTargetEngine.PostgreSql)
            .ToList();

        Assert.NotEmpty(postgres);

        var found = new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);

        foreach (var definition in postgres)
        {
            var path = Path.Combine(
                root!, "PerformanceMonitor.Collectors", definition.GetType().Name + ".cs");

            /* Derived from the catalog rather than from a glob, so a definition whose file moved fails here
               instead of dropping out of the scan silently -- a scan that quietly covers fewer files each
               time is the failure mode this whole family of source pins exists to avoid. */
            Assert.True(File.Exists(path),
                $"{definition.Name}: expected its definition at {path} and it is not there, so this scan "
                + "would skip it and report no dependency for a collector it never read.");

            var sql = string.Join("\n", VerbatimLiterals(File.ReadAllText(path)));

            var touched = new SortedSet<string>(
                ExtensionObjects
                    .Where(o => sql.Contains(o.Token, StringComparison.OrdinalIgnoreCase))
                    .Select(o => o.Extension),
                StringComparer.Ordinal);

            if (touched.Count > 0)
            {
                found[definition.Name] = touched;
            }

            if (ReportsRatherThanReads.ContainsKey(definition.Name))
            {
                continue;
            }

            var declared = definition.RequiredPgExtensions
                .Select(e => e.ExtensionName)
                .ToHashSet(StringComparer.Ordinal);

            var undeclared = touched.Except(declared, StringComparer.Ordinal).ToArray();

            Assert.True(undeclared.Length == 0,
                $"{definition.Name}'s query text touches {string.Join(", ", undeclared)} without declaring "
                + "it in RequiredPgExtensions. Declare it, with the install kind, so Darling/README.md's "
                + "permissions paragraph is forced to name it -- an undeclared dependency degrades to a "
                + "non-fatal PERMISSIONS skip that stores nothing, so nothing else will tell anyone. If the "
                + "collector REPORTS on the extension rather than reading it, add it to "
                + "ReportsRatherThanReads with the reason.");
        }

        /* The scan proves it can see the thing it is checking. A token map that stopped matching -- a
           renamed object, a query moved out of a verbatim literal into a raw string -- would otherwise
           report a clean tree by finding nothing anywhere. */
        Assert.True(found.Count >= ReportsRatherThanReads.Count + 1,
            $"the scan found extension objects in only {found.Count} collector source(s), which is fewer "
            + "than the collectors already known to name one. The extraction has stopped working; fix it "
            + "rather than lowering this floor.");

        foreach (var declaring in postgres.Where(c => c.RequiredPgExtensions.Count > 0))
        {
            Assert.True(found.ContainsKey(declaring.Name),
                $"{declaring.Name} declares a dependency, but the scan found no extension object in its "
                + "query text. Either the declaration is wrong or the extraction cannot see that query, and "
                + "in the second case this pin is not checking that collector at all.");
        }

        foreach (var excluded in ReportsRatherThanReads.Keys)
        {
            Assert.True(postgres.Any(c => string.Equals(c.Name, excluded, StringComparison.Ordinal)),
                $"ReportsRatherThanReads excuses '{excluded}', which is not a PostgreSQL collector in the "
                + "catalog. A stale exclusion silently stops checking whatever later takes that name.");
        }
    }

    /// <summary>
    /// The verification rig has to be able to exercise every declared dependency, or the collector that
    /// needs it can only be tested against a server where it fails.
    ///
    /// <para><c>shared_preload_libraries</c> is the half that cannot be fixed after the fact: it is set on
    /// the container command line because it is list-valued and <c>ALTER SYSTEM</c> stops the server from
    /// starting, so a module missing from that line is a module no run of the rig can load.</para>
    /// </summary>
    [Fact]
    public void TheVerificationRig_LoadsAndCreatesEveryDeclaredDependency()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null,
            "repo root not found -- this pin reads the verification rig and cannot run without it");

        var rig = Path.Combine(root!, "tools", "pg-verification-rig");
        var compose = File.ReadAllText(Path.Combine(rig, "docker-compose.yml"));
        var seed = File.ReadAllText(Path.Combine(rig, "seed.sql"));

        var preloadLine = compose.Split('\n')
            .Select(l => l.Trim())
            .SingleOrDefault(l => l.StartsWith("- shared_preload_libraries=", StringComparison.Ordinal));

        Assert.True(preloadLine is not null,
            "the rig's docker-compose.yml no longer carries a single '- shared_preload_libraries=' "
            + "command argument. It has to be a command-line argument rather than ALTER SYSTEM, so find "
            + "where it moved rather than deleting this pin.");

        var declarations = CollectorCatalog.All.SelectMany(c => c.RequiredPgExtensions).ToList();
        Assert.NotEmpty(declarations);

        foreach (var extension in declarations
            .Where(e => e.InstallKind == PgExtensionInstallKind.SharedPreloadLibraries))
        {
            Assert.Contains(extension.ExtensionName, preloadLine!, StringComparison.Ordinal);
        }

        foreach (var extension in declarations.Select(e => e.ExtensionName).Distinct(StringComparer.Ordinal))
        {
            Assert.Contains(
                $"CREATE EXTENSION IF NOT EXISTS {extension};", seed, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The contents of every <c>@"..."</c> verbatim string literal, which is where these definitions keep
    /// their query text. Reading literals rather than the whole file is the point: every one of these
    /// collectors discusses extensions at length in its doc comments, including the ones that discuss an
    /// extension precisely to explain why they do NOT use it.
    /// </summary>
    private static IEnumerable<string> VerbatimLiterals(string source)
    {
        var index = 0;

        while (true)
        {
            var start = source.IndexOf("@\"", index, StringComparison.Ordinal);
            if (start < 0)
            {
                yield break;
            }

            var cursor = start + 2;
            var buffer = new StringBuilder();

            while (cursor < source.Length)
            {
                if (source[cursor] == '"')
                {
                    if (cursor + 1 < source.Length && source[cursor + 1] == '"')
                    {
                        buffer.Append('"');
                        cursor += 2;
                        continue;
                    }

                    break;
                }

                buffer.Append(source[cursor]);
                cursor++;
            }

            yield return buffer.ToString();
            index = cursor + 1;
        }
    }

    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
