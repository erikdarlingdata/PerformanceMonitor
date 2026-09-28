/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4659: the Query Store backfill writes backdated collections, which do not move the newest collection the
/// forced-plan failure read is keyed on. These pins hold the two source facts that make the saved answer exact:
/// the backfill reports each batch it writes, and the worker hands it the alert adapter's invalidation.
/// </summary>
public sealed class ForcePlanFailuresInvalidationPinsTests
{
    private static string Read(string file, string thisFile) =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Service", file));

    [Fact]
    public void TheBackfill_ReportsABatchOnlyAfterItWroteRows()
    {
        var text = Read("QueryStoreBackfill.cs", Here());
        var write = text.IndexOf("await _runner.WriteBackfillBatchAsync(", System.StringComparison.Ordinal);
        var guard = text.IndexOf("if (written > 0)", write, System.StringComparison.Ordinal);
        var invoke = text.IndexOf("_onBackdatedBatchWritten?.Invoke(server.ServerId)", write, System.StringComparison.Ordinal);
        Assert.True(write > 0 && guard > write && invoke > guard, "the callback must follow the write and sit under written > 0");
        Assert.True(invoke - guard < 120, "the invoke must be the guarded statement");
    }

    [Fact]
    public void TheWorker_PassesTheAlertAdaptersInvalidationToTheBackfill_AndKeepsTheAdapterItBuilt()
    {
        var text = Read("DarlingWorker.cs", Here());
        Assert.Contains("serverId => _alertReadAdapter?.InvalidateForcePlanFailures(serverId)", text, System.StringComparison.Ordinal);
        Assert.Contains("_alertReadAdapter = readAdapter;", text, System.StringComparison.Ordinal);
        Assert.Contains("readAdapter,", text, System.StringComparison.Ordinal);
    }

    private static string Here([CallerFilePath] string thisFile = "") => thisFile;
}
