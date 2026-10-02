/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// After #4336's Step A, a migrated store's <c>shared_preload_libraries</c> line lives in
/// <c>darling-managed.conf</c>, reached from <c>postgresql.conf</c> through the
/// <c>include 'darling-managed.conf'</c> line — not in <c>postgresql.conf</c> itself, where three upgrade-path
/// tests used to look for it. This is their shared, file-level check: exactly one effective
/// <c>shared_preload_libraries</c> assignment across the two files, carried by the managed file, reached through
/// the include, and merged to keep both <c>timescaledb</c> and <c>pg_stat_statements</c> (the v13 block always
/// merges the two in, so both are expected on every managed store this test suite builds).
/// </summary>
internal static class ManagedPreloadAssert
{
    /// <summary>
    /// Reads <paramref name="dataDirectory"/>'s <c>postgresql.conf</c> and <c>darling-managed.conf</c>, and
    /// asserts the "no managed key duplicated" rule: exactly one effective (non-comment) assignment of
    /// <c>shared_preload_libraries</c> across both files, that one assignment living in the managed file, the
    /// operator file carrying the include line that reaches it, and its value holding both libraries the v13
    /// merge always keeps.
    /// </summary>
    public static async Task FileLevel_HasOneManagedPreloadLine_WithBothLibraries(
        string dataDirectory, CancellationToken cancellationToken)
    {
        var postgresqlConfPath = Path.Combine(dataDirectory, "postgresql.conf");
        var managedConfPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);

        var postgresqlConf = await File.ReadAllTextAsync(postgresqlConfPath, cancellationToken);
        var managedConf = File.Exists(managedConfPath)
            ? await File.ReadAllTextAsync(managedConfPath, cancellationToken)
            : string.Empty;

        Assert.True(ManagedConfFile.HasManagedInclude(postgresqlConf),
            $"expected {postgresqlConfPath} to carry the darling-managed.conf include after the #4336 migration.");

        var inPostgresqlConf = EffectiveValues(postgresqlConf);
        var inManagedConf = EffectiveValues(managedConf);

        /* (a) exactly one effective assignment across both files — the "no managed key duplicated" rule #4
           already checked for postgresql.conf alone; here it spans both files, since Step A's whole job is to
           move the line, not duplicate it. */
        Assert.Equal(1, inPostgresqlConf.Count + inManagedConf.Count);

        /* (b) that one assignment lives in the managed file, reached from postgresql.conf through the include
           just asserted above, not restated in the operator file itself. */
        Assert.True(inManagedConf.Count == 1 && inPostgresqlConf.Count == 0,
            $"expected the sole shared_preload_libraries assignment to live in {managedConfPath}, reached " +
            $"through postgresql.conf's include; found {inPostgresqlConf.Count} in postgresql.conf and " +
            $"{inManagedConf.Count} in {ManagedConfFile.FileName}.");

        var value = inManagedConf[0];
        Assert.Contains(DarlingManagedPostgres.TimescaleLibrary, value, StringComparison.Ordinal);
        Assert.Contains(DarlingManagedPostgres.StatementStatisticsLibrary, value, StringComparison.Ordinal);
    }

    /// <summary>
    /// The live counterpart of <see cref="FileLevel_HasOneManagedPreloadLine_WithBothLibraries"/>: reads the
    /// EFFECTIVE value PostgreSQL itself reports for <c>shared_preload_libraries</c>
    /// (<c>pg_settings.setting</c> plus <c>sourcefile</c>) on a <c>Pooling=false</c> connection, and asserts it
    /// names both libraries and traces back to <c>darling-managed.conf</c> — the field canary a store's own
    /// server proves, rather than a file this test parsed by hand.
    /// </summary>
    public static async Task Live_ReportsBothLibraries_SourcedFromManagedConf(
        string connectionString, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(
            new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            "SELECT setting, sourcefile FROM pg_settings WHERE name = 'shared_preload_libraries'", connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        Assert.True(await reader.ReadAsync(cancellationToken), "pg_settings has no shared_preload_libraries row");

        var setting = reader.GetString(0);
        var sourcefile = reader.IsDBNull(1) ? null : reader.GetString(1);

        Assert.Contains(DarlingManagedPostgres.TimescaleLibrary, setting, StringComparison.Ordinal);
        Assert.Contains(DarlingManagedPostgres.StatementStatisticsLibrary, setting, StringComparison.Ordinal);
        Assert.True(
            sourcefile is not null
                && sourcefile.EndsWith(ManagedConfFile.FileName, StringComparison.OrdinalIgnoreCase),
            $"expected pg_settings.sourcefile for shared_preload_libraries to end with {ManagedConfFile.FileName}, but it was '{sourcefile}'.");
    }

    /// <summary>Every effective (non-comment) <c>shared_preload_libraries</c> assignment in
    /// <paramref name="confText"/>'s own text, in order (no include following — each file this test reads is
    /// checked on its own, so the two calls' counts together are the cross-file count).</summary>
    private static System.Collections.Generic.List<string> EffectiveValues(string confText)
    {
        var values = new System.Collections.Generic.List<string>();
        foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(confText))
        {
            if (name.Equals("shared_preload_libraries", StringComparison.OrdinalIgnoreCase))
            {
                values.Add(value);
            }
        }

        return values;
    }
}
