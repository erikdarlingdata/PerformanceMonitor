/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

public sealed class JobHistoryLastSuccessColumnTests
{
    /// <summary>#6651 (#5644 W21(a)): the job's newest success repeated on every row (also beside runs from earlier minutes, and "Never"
    /// for a job that retries) is gone from the row the grid binds; the web page removed the column, and the Darling Viewer did too.</summary>
    [Fact]
    public void JobHistoryRow_HasNoLastSuccessColumnValue()
        => Assert.Null(typeof(JobHistoryRow).GetProperty("LastSuccessfulRunLocal"));
}
