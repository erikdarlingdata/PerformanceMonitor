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

    private static string RecordWithUserName(string userName) =>
        "2026-09-24 01:54:43.008 UTC,\"" + userName + "\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,"
        + "\"startup\",2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"x\"\" does not exist\","
        + ",,,,,,,,\"\",\"client backend\",,0\n";

    private static string RecordWithMessage(string message) =>
        "2026-09-24 01:54:43.008 UTC,\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,\"startup\","
        + "2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"" + message.Replace("\"", "\"\"") + "\","
        + ",,,,,,,,\"\",\"client backend\",,0\n";

    /* --- 1: forged newline in a quoted field stays inside the record ---------------------------------- */

    [Fact]
    public void ForgedNewlineInQuotedUserNameStaysInOneEntry()
    {
        var forged = "admin\n2026-09-24 00:00:00.000 UTC [1] LOG:  forged";
        var body = RealRecord + RecordWithUserName(forged);

        var entries = PgServerLogCsvParser.Parse(body, out var recordsDiscarded);

        /* Resync drops the first complete record (RealRecord) by rule; only the forged one remains. */
        var entry = Assert.Single(entries);
        Assert.Equal(1, recordsDiscarded);
        Assert.Equal(forged, entry.UserName);
    }

    /* --- 2: multi-line quoted message is one record ---------------------------------------------------- */

    [Fact]
    public void MultiLineQuotedMessageIsOneRecord()
    {
        var message = "line one\nline two\nline three";
        var body = RealRecord + RecordWithMessage(message);

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
        var body = RealRecord + RecordWithMessage(message);

        var entries = PgServerLogCsvParser.Parse(body, out var recordsDiscarded);

        var entry = Assert.Single(entries);
        Assert.Equal(message, entry.Message);
    }

    /* --- 4: mid-record start; a look-alike full record hides inside a quoted field ---------------------- */

    [Fact]
    public void MidRecordStartDropsPartialAndLookAlikeThenResyncsCorrectly()
    {
        /* A look-alike record's text (up to 26 commas) sits inside ANOTHER record's quoted message field,
           so a boundary-blind scan could mistake it for a real record if it started reading mid-field. */
        var lookAlike = "2026-09-24 00:00:00.000 UTC,\"a\",\"b\",1,\"c\",1.1,1,\"d\",2026-09-24 00:00:00 UTC,"
            + "1/1,0,LOG,00000,fake,,,,,,,,,,backend,,0";

        var carrier = RecordWithMessage("prefix " + lookAlike + " suffix");

        /* Body starts mid-record: a fragment with no leading newline (the cut head), then a newline, then
           the look-alike carrier (the first "complete" record, dropped by rule), then two more real
           records that must survive. */
        var cutHead = "garbage-mid-record-fragment,more,fields";
        var body = cutHead + "\n" + carrier + RealRecord + RealRecord;

        var entries = PgServerLogCsvParser.Parse(body, out var recordsDiscarded);

        /* carrier is dropped by the resync rule (first complete record); cutHead is never even a candidate
           because it lacks 26 fields, but it is counted only if it is tested — it precedes the first
           newline and is not itself between newlines as a candidate record at all, since the split
           produces cutHead as record 0. It fails the shape check (wrong field count) and is discarded;
           carrier is the next candidate and is the one dropped by the resync rule. */
        Assert.Equal(2, entries.Count);
        Assert.True(recordsDiscarded >= 2);
        Assert.DoesNotContain(entries, e => e.Message.Contains("fake"));
        Assert.All(entries, e => Assert.Equal("role \"nosuchuser\" does not exist", e.Message));
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

        var body = RealRecord + csvLine;
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

        var body = RealRecord + pg14Record;
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

        var body = RealRecord + pg18Record;
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

        var body = RealRecord + RealRecord + partial;

        var entries = PgServerLogCsvParser.Parse(body, out var recordsDiscarded);

        /* Two RealRecords in the body: first dropped by resync, second is the sole surviving entry; the
           trailing partial is not emitted and not counted as discarded. */
        Assert.Single(entries);
        Assert.Equal(1, recordsDiscarded);
    }
}
