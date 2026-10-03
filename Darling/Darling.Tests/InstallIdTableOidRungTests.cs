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
/// Pins the Darling rung that adds the install id table's own OID (<c>config.config_install_id.table_oid</c>, nullable
/// bigint) as a third binding for the id (#4961). A managed major upgrade keeps the database's OID and the table's OID
/// and makes a new cluster id, so the id now follows the two OIDs. The rung is one catalog-only ALTER: a row made before
/// it has no table OID, and the service fills it in on its first start after the rung.
///
/// <para>Every fact finds the rung by NAME, so a renumber is one edit to the registration, the constant and the
/// viewer's <c>return</c>.</para>
/// </summary>
public sealed class InstallIdTableOidRungTests
{
    /// <summary>The rung's registered name - how every test here (and the live reset helpers) finds it.</summary>
    public const string RungName = "install-id-table-oid";

    /// <summary>This rung's sentinel ordinal in the probe; the ordinal is a fact of the probe's shape, not of the
    /// rung's number, so a renumber leaves it alone.</summary>
    private const int ProbeOrdinal = 134;

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    /// <summary>The rung's version, read off the migration registered under <see cref="RungName"/>.</summary>
    public static int RungVersion => Rung.Version;

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        /* No longer the top rung: V160 (the collector run time) landed above it. */
        Assert.True(Rung.Version < StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
    }

    [Fact]
    public void TheRungAddsOneNullableColumn_IdempotentlyAndSchemaQualified_AndNothingElse()
    {
        var sql = Rung.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);

        Assert.Contains("ALTER TABLE config.config_install_id ADD COLUMN IF NOT EXISTS table_oid bigint;", body, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(body, "ALTER TABLE"));
        foreach (var absent in new[] { "NOT NULL", "DEFAULT", "INSERT ", "UPDATE ", "DELETE ", "DROP ", "CREATE ", "TRIGGER" })
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
    public void TheProbeCarriesTheColumn_AndMapsAStoreThroughThisRungToIt()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = "table_schema = 'config' AND table_name = 'config_install_id' AND column_name = 'table_oid'";
        Assert.Contains(arm, probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1, "a newer rung's sentinel follows this one");
        Assert.Equal("hasInstallIdTableOid", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(RungVersion - 1, (int)method.Invoke(null, behind)!);

        var nextArm = viewer.IndexOf("if (hasCollectorRunAt)", StringComparison.Ordinal);
        var thisArm = viewer.IndexOf("if (hasInstallIdTableOid)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasInstallId)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no sentinel arm for this rung - a store at this rung would map one rung short");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(nextArm >= 0 && nextArm < thisArm, "the newer rung's arm sits above this one's");
        Assert.True(thisArm < previousArm, "this rung's arm sits below the newer rung's and above the previous one's");
        Assert.Contains("return " + RungVersion + ";", viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the column is named in the probe line and nowhere in the arm's prose - the coverage ratchet
           strips information_schema lines but cannot strip a comment. The comment block sits ABOVE the `if`. */
        var armProseStart = viewer.LastIndexOf("/* V", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        var prose = viewer[armProseStart..thisArm];
        foreach (var name in new[] { "config_install_id", "table_oid" })
        {
            Assert.DoesNotContain(name, prose, StringComparison.Ordinal);
        }
    }
}
