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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3953 B4: three sites each carry their own compile-time minimum schema version for
/// <c>collect.query_store_interval_wide</c> -- <see cref="DarlingDataReader"/>'s MCP top read (its own private
/// constant, by design: see that constant's doc for why it compares the compiled
/// <see cref="StorageVersion.SchemaVersion"/> instead of probing the store), and the viewer's grid and slicer
/// reads (one shared constant, since both live in <see cref="ViewerDataService"/> and gate on the identical
/// table). None of the three is derived from <see cref="PgMigrations"/> at runtime, so nothing stops a future
/// renumber of the migration that creates the table from drifting out of step with one, some, or all of them --
/// silently: a stale, too-low constant does not fail loudly, it just lets a read attempt the table one version
/// too early (or, after a renumber that raises the version, one too late, permanently skipping the table). This
/// finds that migration by what it creates, not by today's version number, and asserts every constant still
/// equals it.
/// </summary>
public sealed class QueryStoreIntervalWideMinSchemaVersionPinTests
{
    [Fact]
    public void EveryMinSchemaVersionConstant_EqualsTheMigrationThatCreatesTheWideTable()
    {
        var migration = PgMigrations.Scripts.Single(m =>
            Regex.IsMatch(m.Sql, @"CREATE TABLE IF NOT EXISTS collect\.query_store_interval_wide\s"));

        var mcpConstant = (int)typeof(DarlingDataReader)
            .GetField("QueryStoreTopTableMinSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetRawConstantValue()!;
        Assert.Equal(migration.Version, mcpConstant);

        var viewerConstant = (int)typeof(ViewerDataService)
            .GetField("QueryStoreIntervalWideMinSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetRawConstantValue()!;
        Assert.Equal(migration.Version, viewerConstant);
    }
}
