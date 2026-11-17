/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
    [InlineData("DBCC CHECKDB (master, noindex) WITH no_infomsgs executed by sa found 0 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 0 seconds.", "full,noindex", 0)]
    [InlineData("DBCC CHECKDB (Sales, noindex) executed by sa found 1 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 0 seconds.", "full,noindex", 1)]
    [InlineData("DBCC CHECKDB (Sales, repair_rebuild) WITH physical_only executed by sa found 0 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 0 seconds.", "physical_only", 0)]
    public void ALine_GivesItsOptionsAndErrorCount(string text, string options, int errors)
    {
        Assert.True(CheckDbErrorLogLine.TryParse(text, out var o, out var e));
        Assert.Equal(options, o);
        Assert.Equal(errors, e);
    }

    [Fact]
    public void ALine_NamesTheDatabasesItCanBeAbout_ExactlyAndWithASecondArgument()
    {
        var names = new HashSet<string>(StringComparer.Ordinal) { "Sales", "Sales old", "Sales, Inc", "Acme", "My) odd", "My", "sales" };

        Assert.Equal(new[] { "Sales" }, CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Sales) WITH no_infomsgs executed by sa found 0 errors", names));
        Assert.Equal(new[] { "Sales" }, CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Sales, noindex) WITH no_infomsgs executed by sa found 0 errors", names));
        Assert.Equal(new[] { "Sales" }, CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Sales, NOINDEX) executed by sa found 0 errors", names));
        Assert.Equal(new[] { "Sales" }, CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Sales, repair_allow_data_loss) WITH no_infomsgs executed by sa found 0 errors", names));
        Assert.Equal(new[] { "Sales old" }, CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Sales old) executed by sa found 0 errors", names));
        /* #8865: a name holding ", " is not also credited to the shorter name before it; only a check option may follow that comma. */
        Assert.Equal(new[] { "Sales, Inc" }, CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Sales, Inc) WITH no_infomsgs executed by sa found 0 errors", names));
        Assert.Equal(new[] { "Sales, Inc" }, CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Sales, Inc, noindex) executed by sa found 0 errors", names));
        Assert.Empty(CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Acme, Archive 2019) WITH no_infomsgs executed by sa found 4 errors", names));
        /* A name holding ")" is found, and the shorter name it starts with is not: that ")" is not followed by WITH or "executed by". */
        Assert.Equal(new[] { "My) odd" }, CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (My) odd) executed by sa found 0 errors", names));
        Assert.Empty(CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Unknown) executed by sa found 0 errors", names));
        /* Message 5235: the check ended abnormally, so the line says nothing about the database's last check. */
        Assert.Empty(CheckDbErrorLogLine.DatabaseNames("DBCC CHECKDB (Sales) WITH no_infomsgs executed by sa terminated abnormally due to error state 5. Elapsed time: 0 hours 0 minutes 0 seconds.", names));
    }

    [Fact]
    public void ALineWhoseCountCannotBeRead_IsNotParsed() =>
        Assert.False(CheckDbErrorLogLine.TryParse("DBCC CHECKDB (Sales) WITH physical_only ausgeführt von sa hat 0 Fehler gefunden", out _, out _));
}
