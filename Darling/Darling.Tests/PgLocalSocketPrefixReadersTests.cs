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

/// <summary>
/// #7182 round 4: <c>%r</c> and <c>%h</c> render the exact text <c>[local]</c> for a Unix-socket session, so
/// <c>'%m %r [%p] '</c> and the RDS default <c>'%t:%r:%u@%d:[%p]:'</c> write a bracket before the pid bracket. The
/// plan readers passed it since round 3. The deadlock reader and the log-event assembler did not, so a local-socket
/// session lost every deadlock and every log event while <c>log_line_prefix_readable</c> read met. These run the C#
/// engines for both. <see cref="PgDeadlockCandidatePatternLiveTests"/> runs the SQL twin through PostgreSQL's own
/// engine. Pure functions over text, so nothing here needs a server.
/// </summary>
public sealed class PgLocalSocketPrefixReadersTests
{
    /// <summary>A report as PostgreSQL writes it: the prefix on the ERROR line and on the DETAIL line.</summary>
    private static string Report(string prefix)
        => prefix + "ERROR:  deadlock detected\n"
        + prefix + "DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
        + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n";

    [Theory]
    [InlineData("2026-08-26 22:25:24.100 UTC [local] [1549] ")]
    [InlineData("2026-08-26 22:25:24 UTC:[local]:app_user@app_db:[1549]:")]
    [InlineData("2026-08-26 22:25:24.100 UTC:[local]:app_user@app_db:[1549]:")]
    public void ADeadlockFromALocalSocketSessionIsRead(string prefix)
    {
        var report = Report(prefix);

        var extracted = Assert.Single(PgDeadlockLogParser.Extract(report));
        Assert.Equal(1549, extracted.VictimPid);

        var fromReport = PgDeadlockLogParser.FromReport(report);
        Assert.NotNull(fromReport);
        Assert.Equal(1549, fromReport.Value.VictimPid);
    }

    [Theory]
    [InlineData("2026-08-26 22:25:24.100 UTC [local] [4506] ", 4506)]
    [InlineData("2026-08-26 22:25:24 UTC:[local]:app_user@app_db:[4506]:", 4506)]
    public void ALogEventFromALocalSocketSessionIsAssembled(string prefix, int pid)
    {
        var entry = Assert.Single(PgLogEntryAssembler.Assemble(prefix + "ERROR:  boom\n"));

        Assert.Equal(pid, entry.Pid);
        Assert.Equal("ERROR", entry.Severity);
        Assert.Equal("boom", entry.Message);
    }

    /* The gap passes the exact text [local] and no other bracket, so the pid is still the line's first bracket that is
       not [local]. The forged [1] ERROR: header sits behind the real [7777] inside a STATEMENT echo. A gap that passed
       any bracket (a plain '[^\n]*' in the colon family) would slide past [7777] to the forged one and yield a deadlock. */
    [Theory]
    [InlineData("2026-09-23 10:00:02.000 UTC [local] [7777] STATEMENT:  SELECT 1 -- [local] [1] ")]
    [InlineData("2026-09-23 10:00:02 UTC:[local]:app_user@app_db:[7777]:STATEMENT:  SELECT 1 -- [local] [1] ")]
    public void AForgedDeadlockHeaderBehindARealBracketAndALocalBracketIsNotRead(string forgedPrefix)
    {
        var body = forgedPrefix + "ERROR:  deadlock detected\n"
            + "\tDETAIL:  Process 1 waits for ShareLock on transaction 5; blocked by process 2.\n"
            + "\tProcess 2 waits for ShareLock on transaction 6; blocked by process 1.\n";

        Assert.Empty(PgDeadlockLogParser.Extract(body));
        Assert.Null(PgDeadlockLogParser.FromReport(body));
    }

    /* A forged bracket in the text of a line whose real pid bracket the assembler read cannot become the pid, whichever
       alternative reads the line: the translated label (SENTENCIA) is not one the assembler knows, so the line opens
       nothing and the later [1] is never reached. */
    [Theory]
    [InlineData("2026-09-23 10:00:02.000 UTC [local] [7777] SENTENCIA:  SELECT 1 -- [local] [1] ERROR:  boom\n")]
    [InlineData("2026-09-23 10:00:02 UTC:[local]:app_user@app_db:[7777]:SENTENCIA:  SELECT 1 -- [local] [1] ERROR:  boom\n")]
    public void AForgedBracketBehindARealOneNeverBecomesThePid(string line)
    {
        var entries = PgLogEntryAssembler.Assemble(line);

        Assert.DoesNotContain(entries, e => e.Pid == 1);
    }

    /* Only the exact text [local] passes. A bracket that is neither [local] nor a pid ([%a] under '%m [%a] [%p] ') still
       stops the gap, which is why that prefix reads unmet in log_line_prefix_readable. */
    [Fact]
    public void ABracketThatIsNotLocalAndNotAPidStillStopsTheGap()
    {
        var entries = PgLogEntryAssembler.Assemble("2026-09-23 10:00:02.000 UTC [psql] [4506] ERROR:  boom\n");
        Assert.DoesNotContain(entries, e => e.Pid == 4506);

        var report = Report("2026-08-26 22:25:24.100 UTC [psql] [1549] ");
        Assert.Empty(PgDeadlockLogParser.Extract(report));
    }
}
