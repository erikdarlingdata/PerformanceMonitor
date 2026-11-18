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

        /* A login may hold the same words: read left to right, the first count here would be the login's 0. */
        const string login = "DBCC CHECKDB (Sales) executed by found 0 errors and repaired 0 errors found 5 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 1 seconds.";
        Assert.True(CheckDbErrorLogLine.TryParse(login, "Sales", out _, out var loginErrors));
        Assert.Equal(5, loginErrors);
    }

    [Fact]
    public void ALineWhoseCountCannotBeRead_IsNotParsed() =>
        Assert.False(CheckDbErrorLogLine.TryParse("DBCC CHECKDB (Sales) WITH physical_only ausgeführt von sa hat 0 Fehler gefunden", "Sales", out _, out _));

    /* #8865: message 8957 as the 2022 instance's sys.messages writes it in the languages whose line does not start with the command, plus
       two that do. The command is never translated, but some languages put a word or the login before it, Korean writes no space before
       the parenthesis and Turkish puts the login between the command and the parenthesised name. */
    [Theory]
    [InlineData("pt", "O DBCC CHECKDB (Sales) WITH no_infomsgs executado por sa encontrou 0 erros e reparou 0 erros. Tempo decorrido: 0 horas 0 minutos 0 segundos.")]
    [InlineData("pl", "Polecenie DBCC CHECKDB (Sales) WITH no_infomsgs wykonane przez sa spowodowało znalezienie 0 i naprawienie błędów 0. Upłynęło czasu: 0 godz. 0 min 0 s.")]
    [InlineData("hu", "A(z) sa felhasználó által végrehajtott DBCC CHECKDB (Sales) WITH no_infomsgs utasítás 0 hibát talált, és 0 hibát kijavított. Eltelt idő: 0 óra, 0 perc, 0 másodperc.")]
    [InlineData("ja", "sa から実行された DBCC CHECKDB (Sales) WITH no_infomsgs では、0 個のエラーが見つかり、0 個のエラーが修復されました。経過時間: 0 時間 0 分 0 秒。")]
    [InlineData("ko", "sa이(가) 실행한 DBCC CHECKDB(Sales) WITH no_infomsgs에서 0개의 오류를 찾아 0개의 오류를 복구했습니다. 경과 시간: 0시간 0분 0초.")]
    [InlineData("zh-TW", "sa 執行的 DBCC CHECKDB (Sales) WITH no_infomsgs 發現 0 個錯誤並修復了 0 個錯誤。經過時間: 0 小時 0 分 0 秒。")]
    [InlineData("zh-CN", "由 sa 执行的 DBCC CHECKDB (Sales) WITH no_infomsgs 发现 0 个错误，已修复 0 个错误。占用时间: 0 小时 0 分钟 0 秒。")]
    [InlineData("tr", "DBCC CHECKDB sa tarafından yürütülen (Sales) WITH no_infomsgs 0 hata buldu ve 0 hatayı onardı. Geçen süre: 0 saat 0 dakika 0 saniye.")]
    [InlineData("de", "DBCC CHECKDB (Sales) wurde von sa ausgeführt, hat 0 Fehler gefunden und 0 Fehler repariert. Verstrichene Zeit: 0 Stunden, 0 Minuten, 0 Sekunden.")]
    [InlineData("ru", "DBCC CHECKDB (Sales, noindex), выполненная sa, обнаружила ошибки (0) и исправила ошибки (0). Затраченное время: 0 ч 0 мин 0 с.")]
    public void ACheckDbCompletionInAnotherLanguage_IsUnreadable(string language, string text)
    {
        var names = new HashSet<string>(StringComparer.Ordinal) { "Sales" };

        Assert.True(CheckDbErrorLogLine.IsUnreadableCompletion(text, names), language);
        /* About a database the collector does not list, it is not ours to count. */
        Assert.False(CheckDbErrorLogLine.IsUnreadableCompletion(text.Replace("Sales", "Other", StringComparison.Ordinal), names), language);
    }

    /* #8865: English lines that name the command but are not a completion: corruption advisories (602, 823, 1522, 8646, 9100, as the
       2022 instance's sys.messages writes them, filled in), the English 8957 line itself and message 5235. None is counted. */
    [Theory]
    [InlineData("Could not find an entry for table or index with partition ID 72057594038321152 in database 5. This error can occur if a stored procedure references a dropped table, or metadata is corrupted. Drop and re-create the stored procedure, or execute DBCC CHECKDB.")]
    [InlineData("The operating system returned error 21(The device is not ready.) to SQL Server during a read at offset 0x00000000012000 in file 'D:\\Data\\Sales.mdf'. Additional messages in the SQL Server error log and operating system error log may provide more detail. This is a severe system-level error condition that threatens database integrity and must be corrected immediately. Complete a full database consistency check (DBCC CHECKDB). This error can be caused by many factors; for more information, see SQL Server Books Online.")]
    [InlineData("Sort operation failed during an index build. The overwriting of the allocation page in database 'Sales' was prevented by terminating the sort. Run DBCC CHECKDB to check for allocation and consistency errors. It may be necessary restore the database from backup.")]
    [InlineData("Unable to find index entry in index ID 2, of table 5, in database 'Sales'. The indicated index is corrupt or there is a problem with the current update plan. Run DBCC CHECKDB or DBCC CHECKTABLE. If the problem persists, contact product support.")]
    [InlineData("Possible index corruption detected. Run DBCC CHECKDB.")]
    [InlineData("DBCC CHECKDB (Sales) WITH no_infomsgs executed by sa found 0 errors and repaired 0 errors.")]
    [InlineData("DBCC CHECKDB (Sales) WITH no_infomsgs executed by sa terminated abnormally due to error state 5. Elapsed time: 0 hours 0 minutes 0 seconds.")]
    public void AnEnglishLineNamingTheCommand_IsNotAnUnreadableCompletion(string text)
    {
        var names = new HashSet<string>(StringComparer.Ordinal) { "Sales", "5" };

        Assert.False(CheckDbErrorLogLine.IsUnreadableCompletion(text, names));
    }
}
