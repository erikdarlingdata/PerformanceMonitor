/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4053 part a1: <see cref="PgServerLogCsvParser"/> unit tests, over hand-built and live-captured csvlog
/// records. No wiring: these exercise the parser directly, never the tail or the collector.
/// </summary>
public sealed class PgServerLogCsvParserTests
{
    /* A real, complete csvlog record for a failed login (captured on a live pg18 rig, #4053's column-count
       check), used as a base to graft forged content into. Fields, in order:
       log_time,user_name,database_name,process_id,connection_from,session_id,session_line_num,command_tag,
       session_start_time,virtual_transaction_id,transaction_id,error_severity,sql_state_code,message,
       detail,hint,internal_query,internal_query_pos,context,query,query_pos,location,application_name,
       backend_type,leader_pid,query_id */
    private const string RealRecord =
        "2026-09-24 01:54:43.008 UTC,\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,\"startup\","
        + "2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"nosuchuser\"\" does not exist\",,,,,,,,,\"\","
        + "\"client backend\",,0\n";

    /* The start of a tail read: the end of a record this window did not see the start of. */
    private const string CutHead = "0,FATAL,28000,,,,,,,,,,\"client backend\",,0\n";

    private static string RecordWithUserName(string userName) =>
        "2026-09-24 01:54:43.008 UTC,\"" + userName + "\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,"
        + "\"startup\",2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"x\"\" does not exist\","
        + ",,,,,,,,\"\",\"client backend\",,0\n";

    private static string RecordWithMessage(string message) =>
        "2026-09-24 01:54:43.008 UTC,\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,\"startup\","
        + "2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"" + message.Replace("\"", "\"\"") + "\","
        + ",,,,,,,,\"\",\"client backend\",,0\n";

    /* Same shape as RealRecord, but log_time carries a non-zero-offset zone (#4053 review H1): the
       parser must still return this as a record, leaving the zone judgment to the collector's
       foreign-zone filter. */
    private static string RecordWithZone(string zone) =>
        "2026-09-24 01:54:43.008 " + zone + ",\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,"
        + "\"startup\",2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"nosuchuser\"\" does not exist\","
        + ",,,,,,,,\"\",\"client backend\",,0\n";

    /* --- 1: forged newline in a quoted field stays inside the record ---------------------------------- */

    [Fact]
    public void ForgedNewlineInQuotedUserNameStaysInOneEntry()
    {
        var forged = "admin\n2026-09-24 00:00:00.000 UTC [1] LOG:  forged";
        var body = CutHead + RecordWithUserName(forged);

        var entries = PgServerLogCsvParser.Parse(body, out var recordsDiscarded);

        /* The cut head is dropped; the forged newline stays inside the one real record. */
        var entry = Assert.Single(entries);
        Assert.Equal(1, recordsDiscarded);
        Assert.Equal(forged, entry.UserName);
    }

    /* --- 2: multi-line quoted message is one record ---------------------------------------------------- */

    [Fact]
    public void MultiLineQuotedMessageIsOneRecord()
    {
        var message = "line one\nline two\nline three";
        var body = CutHead + RecordWithMessage(message);

        var entries = PgServerLogCsvParser.Parse(body, out var recordsDiscarded);

        var entry = Assert.Single(entries);
        Assert.Equal(1, recordsDiscarded);
        Assert.Equal(message, entry.Message);
    }

    /* --- 3: doubled quote unescapes to one literal quote ------------------------------------------------ */

    [Fact]
    public void DoubledQuoteUnescapesToLiteralQuote()
    {
        var message = "role \"nosuchuser\" does not exist";
        var body = CutHead + RecordWithMessage(message);

        var entries = PgServerLogCsvParser.Parse(body, out var recordsDiscarded);

        var entry = Assert.Single(entries);
        Assert.Equal(message, entry.Message);
    }

    /* --- 4: mid-record start; a look-alike full record hides inside a quoted field ---------------------- */

    [Fact]
    public void MidRecordStart_DropsOnlyTheCutHead_AndAPlantedLookAlikeStaysInsideItsCarrier()
    {
        /* A look-alike record's text sits inside ANOTHER record's quoted message field, on its own line. A
           boundary scan that trusted a newline inside quotes would emit it as a record of its own. */
        var lookAlike = "2026-09-24 00:00:00.000 UTC,\"a\",\"b\",1,\"c\",1.1,1,\"d\",2026-09-24 00:00:00 UTC,"
            + "1/1,0,LOG,00000,fake,,,,,,,,,,backend,,0";
        var carrier = RecordWithMessage("prefix\n" + lookAlike + "\nsuffix");

        var body = "garbage-mid-record-fragment,more,fields\n" + carrier + RealRecord + RealRecord;
        var entries = PgServerLogCsvParser.Parse(body, out var discarded);

        Assert.Equal(3, entries.Count);
        Assert.Equal(1, discarded);
        Assert.Single(entries, e => e.Message.Contains("fake", System.StringComparison.Ordinal));
        Assert.DoesNotContain(entries, e => e.Pid == 1);
    }

    [Fact]
    public void ATailThatStartsInsideAQuotedField_StillYieldsEveryLaterRecord()
    {
        /* The window starts in the middle of a quoted message, so the text before the first real boundary
           carries an odd number of quotes. A forward parse inverts parity here and returns nothing. */
        var first = RecordWithMessage("a long message whose middle is where the \"4 MB\" window starts");
        var cut = first.IndexOf("middle", System.StringComparison.Ordinal);
        var body = first[cut..] + RealRecord + RealRecord + RealRecord;

        var entries = PgServerLogCsvParser.Parse(body, out _);

        Assert.Equal(3, entries.Count);
        Assert.All(entries, e => Assert.Equal("role \"nosuchuser\" does not exist", e.Message));
    }

    [Fact]
    public void ATailThatStartsInsideAQuotedFieldHoldingALookAlikeLine_NeverEmitsTheLookAlike()
    {
        /* Worst case for a forward parse: the window starts inside a quoted field whose remaining text is a
           newline and a complete planted record, then the field closes. */
        var lookAlike = "2026-09-24 00:00:00.000 UTC,\"a\",\"b\",1,\"c\",1.1,1,\"d\",2026-09-24 00:00:00 UTC,"
            + "1/1,0,LOG,00000,fake,,,,,,,,,,backend,,0";
        var carrier = RecordWithMessage("head\n" + lookAlike + "\ntail");
        var cut = carrier.IndexOf("head", System.StringComparison.Ordinal) + 4;
        var body = carrier[cut..] + RealRecord + RealRecord;

        var entries = PgServerLogCsvParser.Parse(body, out _);

        Assert.Equal(2, entries.Count);
        Assert.DoesNotContain(entries, e => e.Message.Contains("fake", System.StringComparison.Ordinal));
    }

    [Fact]
    public void AWholeFileReadFromItsFirstByte_KeepsItsFirstRecord()
    {
        /* A log file smaller than the tail window is read from offset 0, which IS a record start: dropping
           its first record would lose it on every cycle until the file outgrows the window. */
        var entries = PgServerLogCsvParser.Parse(RealRecord + RealRecord, out var discarded);

        Assert.Equal(2, entries.Count);
        Assert.Equal(0, discarded);
    }

    /* --- 5: field-for-field parity with PgLogEntryAssembler over the same event -------------------------- */

    [Fact]
    public void ParityWithStderrAssemblerForTheSameEvent()
    {
        /* Captured together, same event, on a live pg18 rig (#4053's brief): the default stderr prefix
           twin and its csvlog twin for one failed login. */
        const string stderrLine = "2026-09-24 02:00:10.449 UTC [102] FATAL:  role \"nosuchuser\" does not exist\n";

        const string csvLine =
            "2026-09-24 02:00:10.449 UTC,\"nosuchuser\",\"postgres\",102,\"::1:53958\",6ab4842a.66,1,\"startup\","
            + "2026-09-24 02:00:10 UTC,0/1,0,FATAL,28000,\"role \"\"nosuchuser\"\" does not exist\",,,,,,,,,\"\","
            + "\"client backend\",,0\n";

        var stderrEntries = PgLogEntryAssembler.Assemble(stderrLine);
        var stderrEntry = Assert.Single(stderrEntries);

        var body = CutHead + csvLine;
        var csvEntries = PgServerLogCsvParser.Parse(body, out _);
        var csvEntry = Assert.Single(csvEntries);

        Assert.Equal(stderrEntry.OccurredAtUtc, csvEntry.OccurredAtUtc);
        Assert.Equal(stderrEntry.Pid, csvEntry.Pid);
        Assert.Equal(stderrEntry.Severity, csvEntry.Severity);
        Assert.Equal(stderrEntry.Message, csvEntry.Message);

        /* The default stderr prefix (%m [%p]) carries no %e, so the assembler's SqlState is null for this
           line; csvlog carries sql_state_code as its own column regardless of log_line_prefix, so the csv
           entry has it where the stderr twin cannot. Not a parity gap: it is exactly what each format is
           able to say. */
        Assert.Null(stderrEntry.SqlState);
        Assert.Equal("28000", csvEntry.SqlState);
    }

    /* --- 6: a pg14 fixture and a pg18 fixture both map correctly ---------------------------------------- */

    [Fact]
    public void Pg14FixtureMapsCorrectly()
    {
        /* Captured on a live postgres:14 container (#4053's brief). */
        const string pg14Record =
            "2026-09-24 01:54:43.008 UTC,\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,\"startup\","
            + "2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"nosuchuser\"\" does not exist\",,,,,,,,,\"\","
            + "\"client backend\",,0\n";

        var body = CutHead + pg14Record;
        var entries = PgServerLogCsvParser.Parse(body, out _);
        var entry = Assert.Single(entries);

        Assert.Equal(83, entry.Pid);
        Assert.Equal("FATAL", entry.Severity);
        Assert.Equal("28000", entry.SqlState);
        Assert.Equal("nosuchuser", entry.UserName);
        Assert.Equal("postgres", entry.DatabaseName);
        Assert.Equal("role \"nosuchuser\" does not exist", entry.Message);
    }

    [Fact]
    public void Pg18FixtureMapsCorrectly()
    {
        /* Captured on a live timescale/timescaledb:2.28.1-pg18 container (#4053's brief). */
        const string pg18Record =
            "2026-09-24 01:58:49.681 UTC,\"nosuchuser\",\"postgres\",101,\"::1:57278\",6ab483d9.65,1,\"startup\","
            + "2026-09-24 01:58:49 UTC,0/1,0,FATAL,28000,\"role \"\"nosuchuser\"\" does not exist\",,,,,,,,,\"\","
            + "\"client backend\",,0\n";

        var body = CutHead + pg18Record;
        var entries = PgServerLogCsvParser.Parse(body, out _);
        var entry = Assert.Single(entries);

        Assert.Equal(101, entry.Pid);
        Assert.Equal("FATAL", entry.Severity);
        Assert.Equal("28000", entry.SqlState);
        Assert.Equal("nosuchuser", entry.UserName);
        Assert.Equal("postgres", entry.DatabaseName);
        Assert.Equal("role \"nosuchuser\" does not exist", entry.Message);
    }

    /* --- 7: trailing partial record is not emitted ------------------------------------------------------ */

    [Fact]
    public void TrailingPartialRecordIsNotEmitted()
    {
        var partial = "2026-09-24 01:54:43.008 UTC,\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,"
            + "\"startup\",2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"nosuchuser"; // no closing quote, no newline

        var body = CutHead + RealRecord + partial;

        var entries = PgServerLogCsvParser.Parse(body, out var recordsDiscarded);

        /* Two RealRecords in the body: first dropped by resync, second is the sole surviving entry; the
           trailing partial is not emitted and not counted as discarded. */
        Assert.Single(entries);
        Assert.Equal(1, recordsDiscarded);
    }

    /* --- 8: a non-UTC log_time is returned, not rejected (#4053 review H1) ------------------------------ */

    [Fact]
    public void NonUtcZoneRecordIsReturnedWithItsZoneText()
    {
        var body = CutHead + RecordWithZone("PST");

        var entries = PgServerLogCsvParser.Parse(body, out var recordsDiscarded);

        var entry = Assert.Single(entries);
        Assert.Equal(1, recordsDiscarded);
        Assert.Equal("PST", entry.ZoneText);
    }
}
