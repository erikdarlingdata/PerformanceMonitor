/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

public class CheckDbErrorLogLineTests
{
    [Theory]
    [InlineData("DBCC CHECKDB (Sales) WITH all_errormsgs, no_infomsgs, physical_only executed by DOM\\svc found 0 errors and repaired 0 errors. Elapsed time: 0 hours 3 minutes 2 seconds.", "physical_only", 0)]
    [InlineData("DBCC CHECKDB (Sales) WITH all_errormsgs, no_infomsgs, data_purity executed by DOM\\svc found 0 errors and repaired 0 errors. Elapsed time: 0 hours 9 minutes 0 seconds.", "full,data_purity", 0)]
    [InlineData("DBCC CHECKDB (Sales) executed by sa found 0 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 1 seconds.", "full", 0)]
    [InlineData("DBCC CHECKDB (Sales) WITH extended_logical_checks, data_purity executed by sa found 12 errors and repaired 0 errors. Elapsed time: 0 hours 1 minutes 0 seconds.", "full,data_purity,extended_logical_checks", 12)]
    [InlineData("DBCC CHECKDB (My [Odd] (Name)) WITH physical_only executed by sa found 3 errors and repaired 3 errors. Elapsed time: 0 hours 0 minutes 2 seconds.", "physical_only", 3)]
    public void ALine_GivesItsOptionsAndErrorCount(string text, string options, int errors)
    {
        Assert.True(CheckDbErrorLogLine.TryParse(text, out var o, out var e));
        Assert.Equal(options, o);
        Assert.Equal(errors, e);
    }

    [Fact]
    public void ALineWhoseCountCannotBeRead_IsNotParsed() =>
        Assert.False(CheckDbErrorLogLine.TryParse("DBCC CHECKDB (Sales) WITH physical_only ausgeführt von sa hat 0 Fehler gefunden", out _, out _));
}
