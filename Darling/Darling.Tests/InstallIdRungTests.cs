/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling rung that holds the install id (#4961): one table, one row, made by the service at start and
/// read by everything else. The id is the eight characters that tell this install's Extended Events sessions from
/// another install's on a server both monitor, so the row carries the id and the binding that says which store it
/// was made for (the cluster's <c>system_identifier</c> and the store database's OID). The rung is DDL only: the
/// service inserts the row, so a store that has never started this build holds an empty table and the reads that
/// need an id say so.
///
/// <para>Every fact finds the rung by NAME, so a renumber (another rung landing first) is one edit to the
/// registration, the <c>StorageVersion</c> constant and the viewer's <c>return</c>.</para>
/// </summary>
public sealed class InstallIdRungTests
{
    /// <summary>The rung's registered name - how every test here (and the live reset helper) finds it.</summary>
    public const string RungName = "install-id";

    /// <summary>This rung's sentinel ordinal in the probe; the ordinal is a fact of the probe's shape, not of the
    /// rung's number, so a renumber leaves it alone.</summary>
    private const int ProbeOrdinal = 133;

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    /// <summary>The rung's version, read off the migration registered under <see cref="RungName"/>.</summary>
    public static int RungVersion => Rung.Version;

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        /* No longer the top rung: V159 (the install id's table OID) landed above it. */
        Assert.True(Rung.Version < StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
    }

    /// <summary>
    /// ONE table in the config schema, shaped like its neighbours: a single row pinned to <c>id = 1</c>, the id
    /// under a CHECK for exactly eight lowercase hex digits, and the two binding columns: the database's OID NOT NULL
    /// (every login can read it) and the cluster's identifier nullable (a managed or hardened server may refuse a login
    /// the call that reads it, and the id is then bound to the database alone). No row is inserted here (the service
    /// makes it) and nothing else is touched.
    /// </summary>
    [Fact]
    public void TheRungCreatesOneSingleRowTable_WithTheIdFormatCheckAndTheBinding_AndNothingElse()
    {
        var sql = Rung.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);

        Assert.Contains("CREATE TABLE IF NOT EXISTS config.config_install_id", body, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(body, "CREATE TABLE"));
        Assert.Contains("id smallint NOT NULL PRIMARY KEY DEFAULT 1 CHECK (id = 1)", body, StringComparison.Ordinal);
        Assert.Contains("install_id text NOT NULL", body, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT ck_config_install_id_format CHECK (install_id ~ '^[0-9a-f]{8}$')", body, StringComparison.Ordinal);
        Assert.Contains("system_identifier bigint,", body, StringComparison.Ordinal);
        Assert.DoesNotContain("system_identifier bigint NOT NULL", body, StringComparison.Ordinal);
        Assert.Contains("database_oid bigint NOT NULL", body, StringComparison.Ordinal);
        Assert.Contains("created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')", body, StringComparison.Ordinal);

        foreach (var absent in new[] { "INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE INDEX", "TRIGGER", "VIEW" })
        {
            Assert.DoesNotContain(absent, body, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung and the map has an arm for it: a missing arm maps a store that
    /// reached this rung one rung short, permanently. It is no longer the top rung, so the arm sits below the newer
    /// rung's and above the previous one's.
    /// </summary>
    [Fact]
    public void TheProbeCarriesTheTable_AndMapsAStoreThroughThisRungToIt()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = "FROM information_schema.tables WHERE table_schema = 'config' AND table_name = 'config_install_id'";
        Assert.Contains(arm, probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1, "a newer rung's sentinel follows this one");
        Assert.Equal("hasInstallId", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(RungVersion - 1, (int)method.Invoke(null, behind)!);

        var nextArm = viewer.IndexOf("if (hasInstallIdTableOid)", StringComparison.Ordinal);
        var thisArm = viewer.IndexOf("if (hasInstallId)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasMuteRuleServerId)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no sentinel arm for this rung - a store at this rung would map one rung short");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(nextArm >= 0 && nextArm < thisArm, "the newer rung's arm sits above this one's");
        Assert.True(thisArm < previousArm, "this rung's arm sits below the newer rung's and above the previous one's");
        Assert.Contains("return " + RungVersion + ";", viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table is named in the probe line and nowhere in the arm's prose - the coverage ratchet
           strips information_schema lines but cannot strip a comment. The comment block sits ABOVE the `if`. */
        var armProseStart = viewer.LastIndexOf("/* V", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        var prose = viewer[armProseStart..thisArm];
        foreach (var name in new[] { "config_install_id", "install_id", "system_identifier" })
        {
            Assert.DoesNotContain(name, prose, StringComparison.Ordinal);
        }
    }
}
