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
        var names = new HashSet<string>(StringComparer.Ordinal) { "Sales", "My [Odd] (Name)", "master" };
        var name = CheckDbErrorLogLine.DatabaseName(text, names);
        Assert.NotNull(name);
        Assert.True(CheckDbErrorLogLine.TryParse(text, name, out var o, out var e));
        Assert.Equal(options, o);
        Assert.Equal(errors, e);
    }

    [Fact]
    public void ALine_NamesTheOneDatabaseItIsAbout_ExactlyAndWithASecondArgument()
    {
        var names = new HashSet<string>(StringComparer.Ordinal) { "Sales", "Sales old", "Sales, Inc", "Acme", "My) odd", "My", "sales" };

        Assert.Equal("Sales", CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Sales) WITH no_infomsgs executed by sa found 0 errors", names));
        Assert.Equal("Sales", CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Sales, noindex) WITH no_infomsgs executed by sa found 0 errors", names));
        Assert.Equal("Sales", CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Sales, NOINDEX) executed by sa found 0 errors", names));
        Assert.Equal("Sales", CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Sales, repair_allow_data_loss) WITH no_infomsgs executed by sa found 0 errors", names));
        Assert.Equal("Sales old", CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Sales old) executed by sa found 0 errors", names));
        /* #8865: a name holding ", " is not also credited to the shorter name before it; only a check option may follow that comma. */
        Assert.Equal("Sales, Inc", CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Sales, Inc) WITH no_infomsgs executed by sa found 0 errors", names));
        Assert.Equal("Sales, Inc", CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Sales, Inc, noindex) executed by sa found 0 errors", names));
        Assert.Null(CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Acme, Archive 2019) WITH no_infomsgs executed by sa found 4 errors", names));
        /* A name holding ")" is found, and the shorter name it starts with is not: that ")" is not followed by WITH or "executed by". */
        Assert.Equal("My) odd", CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (My) odd) executed by sa found 0 errors", names));
        Assert.Null(CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Unknown) executed by sa found 0 errors", names));
        /* Message 5235: the check ended abnormally, so the line says nothing about the database's last check. */
        Assert.Null(CheckDbErrorLogLine.DatabaseName("DBCC CHECKDB (Sales) WITH no_infomsgs executed by sa terminated abnormally due to error state 5. Elapsed time: 0 hours 0 minutes 0 seconds.", names));
    }

    [Theory]
    [InlineData("Sales) WITH physical_only", "DBCC CHECKDB (Sales) WITH physical_only) WITH no_infomsgs executed by sa found 0 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 0 seconds.")]
    [InlineData("Sales) executed by sa", "DBCC CHECKDB (Sales) executed by sa) WITH no_infomsgs executed by sa found 0 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 0 seconds.")]
    [InlineData("Sales, noindex", "DBCC CHECKDB (Sales, noindex) WITH no_infomsgs executed by sa found 0 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 0 seconds.")]
    public void ANameThatHoldsAShorterNameAndAnEnd_CreditsOnlyTheLongerName(string longer, string text)
    {
        /* #8865: a line is about one database. A name may hold ") WITH ", ") executed by " or ", noindex" after a shorter database's
           name, and options never hold ")", so when the longer name matches the line is about it alone. */
        var names = new HashSet<string>(StringComparer.Ordinal) { "Sales", longer };

        Assert.Equal(longer, CheckDbErrorLogLine.DatabaseName(text, names));
        Assert.True(CheckDbErrorLogLine.TryParse(text, longer, out var options, out var errors));
        Assert.Equal("full", options);
        Assert.Equal(0, errors);
    }

    [Fact]
    public void TheCount_IsTheChecksOwn_NotOneInsideTheNameOrTheLogin()
    {
        /* #8865: the first "found N errors" can sit inside the name; the count is the last full "found N errors and repaired M errors". */
        const string name = "X found 0 errors";
        const string text = "DBCC CHECKDB (X found 0 errors) WITH no_infomsgs executed by sa found 7 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 1 seconds.";
        var names = new HashSet<string>(StringComparer.Ordinal) { "X", name };

        Assert.Equal(name, CheckDbErrorLogLine.DatabaseName(text, names));
        Assert.True(CheckDbErrorLogLine.TryParse(text, name, out _, out var errors));
        Assert.Equal(7, errors);

        const string login = "DBCC CHECKDB (Sales) executed by DOM\found 0 errors and repaired 0 errors found 5 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 1 seconds.";
        Assert.True(CheckDbErrorLogLine.TryParse(login, "Sales", out _, out var loginErrors));
        Assert.Equal(5, loginErrors);
    }

    [Fact]
    public void ALineWhoseCountCannotBeRead_IsNotParsed() =>
        Assert.False(CheckDbErrorLogLine.TryParse("DBCC CHECKDB (Sales) WITH physical_only ausgeführt von sa hat 0 Fehler gefunden", "Sales", out _, out _));

    [Theory]
    [InlineData("DBCC CHECKDB (Sales) WITH physical_only wurde von sa ausgeführt und hat 0 Fehler gefunden und 0 Fehler repariert.", true)]
    [InlineData("DBCC CHECKDB (Ventes) a été exécuté par sa et a trouvé 0 erreurs et réparé 0 erreurs.", true)]
    [InlineData("DBCC CHECKDB (Sales) WITH no_infomsgs executed by sa found 0 errors and repaired 0 errors.", false)]
    [InlineData("DBCC CHECKDB (Sales) WITH no_infomsgs executed by sa terminated abnormally due to error state 5. Elapsed time: 0 hours 0 minutes 0 seconds.", false)]
    [InlineData("Some job said DBCC CHECKDB (Sales) ran.", false)]
    public void ACheckDbLineWithoutTheEnglishMarkers_IsACompletionThatCannotBeRead(string text, bool unreadable)
    {
        /* #8865: on an instance whose error log is not English, the 8957 line still starts with the command, which is never translated,
           but has neither " executed by " nor the English 5235 wording. */
        Assert.Equal(unreadable, CheckDbErrorLogLine.IsUnreadableCompletion(text));
    }
}
