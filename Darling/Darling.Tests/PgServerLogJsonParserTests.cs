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
/// #4053 part a2: <see cref="PgServerLogJsonParser"/> unit tests, over hand-built and live-captured jsonlog
/// records. No wiring: these exercise the parser directly, never the tail or the collector.
/// </summary>
public sealed class PgServerLogJsonParserTests
{
    /* A real, complete jsonlog record for a failed login (captured on a live pg18 rig, #4053's key
       check), used as a base to graft forged content into. */
    private const string RealRecord =
        "{\"timestamp\":\"2026-09-24 03:04:57.241 UTC\",\"user\":\"baduser\",\"dbname\":\"postgres\","
        + "\"pid\":103,\"remote_host\":\"::1\",\"remote_port\":35848,\"session_id\":\"6ab49359.67\","
        + "\"line_num\":4,\"ps\":\"startup\",\"session_start\":\"2026-09-24 03:04:57 UTC\",\"vxid\":\"0/1\","
        + "\"txid\":0,\"error_severity\":\"FATAL\",\"state_code\":\"28000\","
        + "\"message\":\"role \\\"baduser\\\" does not exist\",\"backend_type\":\"client backend\","
        + "\"query_id\":0}\n";

    /* The stderr twin of RealRecord, captured from the same rig at the same instant. */
    private const string RealRecordStderr = "2026-09-24 03:04:57.241 UTC [103] FATAL:  role \"baduser\" does not exist\n";

    /* The csvlog twin of RealRecord, captured from the same rig at the same instant. */
    private const string RealRecordCsv =
        "2026-09-24 03:04:57.241 UTC,\"baduser\",\"postgres\",103,\"::1:35848\",6ab49359.67,4,\"startup\","
        + "2026-09-24 03:04:57 UTC,0/1,0,FATAL,28000,\"role \"\"baduser\"\" does not exist\","
        + ",,,,,,,,\"\",\"client backend\",,0\n";

    /* The end of a line this window did not see the start of: a fragment with no opening brace, which can
       never parse as JSON. */
    private const string CutHead = "startup\",\"session_start\":\"2026-09-24 03:04:57 UTC\"}\n";

    private static string RecordWithUserName(string userName) =>
        "{\"timestamp\":\"2026-09-24 03:04:57.241 UTC\",\"user\":\"" + JsonEscape(userName) + "\","
        + "\"dbname\":\"postgres\",\"pid\":103,\"remote_host\":\"::1\",\"remote_port\":35848,"
        + "\"session_id\":\"6ab49359.67\",\"line_num\":4,\"ps\":\"startup\","
        + "\"session_start\":\"2026-09-24 03:04:57 UTC\",\"vxid\":\"0/1\",\"txid\":0,"
        + "\"error_severity\":\"FATAL\",\"state_code\":\"28000\",\"message\":\"role \\\"x\\\" does not exist\","
        + "\"backend_type\":\"client backend\",\"query_id\":0}\n";

    private static string JsonEscape(string value) =>
        value.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n").Replace("\r", "\\r");

    /* --- 1: forged newline in an escaped field stays inside one record --------------------------------- */

    [Fact]
    public void ForgedNewlineInUserFieldStaysInOneEntry()
    {
        var forged = "admin\n2026-09-24 00:00:00.000 UTC [1] LOG:  forged";
        var body = CutHead + RecordWithUserName(forged);

        var entries = PgServerLogJsonParser.Parse(body, out var recordsDiscarded);

        var entry = Assert.Single(entries);
        Assert.Equal(1, recordsDiscarded);
        Assert.Equal(forged, entry.UserName);
        Assert.Contains("forged", entry.UserName);
    }

    /* --- 2: cut head then 2 records -> 2 entries, 1 discarded ------------------------------------------ */

    [Fact]
    public void CutHeadThenTwoRecordsYieldsTwoEntriesOneDiscarded()
    {
        var body = CutHead + RealRecord + RealRecord;

        var entries = PgServerLogJsonParser.Parse(body, out var recordsDiscarded);

        Assert.Equal(2, entries.Count);
        Assert.Equal(1, recordsDiscarded);
    }

    /* --- 3: a body starting on a whole record (byte 0) keeps it ---------------------------------------- */

    [Fact]
    public void BodyStartingOnWholeRecordKeepsIt()
    {
        var body = RealRecord;

        var entries = PgServerLogJsonParser.Parse(body, out var recordsDiscarded);

        var entry = Assert.Single(entries);
        Assert.Equal(0, recordsDiscarded);
        Assert.Equal(103, entry.Pid);
    }

    /* --- 4: a trailing partial line is not emitted, not counted ---------------------------------------- */

    [Fact]
    public void TrailingPartialLineIsNotEmittedOrCounted()
    {
        var partial = RealRecord.TrimEnd('\n')[..^10];
        var body = RealRecord + partial;

        var entries = PgServerLogJsonParser.Parse(body, out var recordsDiscarded);

        var entry = Assert.Single(entries);
        Assert.Equal(0, recordsDiscarded);
        Assert.Equal(103, entry.Pid);
    }

    /* --- 5: field parity with the stderr assembler and the csvlog parser ------------------------------- */

    [Fact]
    public void FieldParityWithStderrAssemblerAndCsvParser()
    {
        var jsonEntries = PgServerLogJsonParser.Parse(RealRecord, out var jsonDiscarded);
        var stderrEntries = PgLogEntryAssembler.Assemble(RealRecordStderr, logTimezoneIsUtc: true, out _);
        var csvEntries = PgServerLogCsvParser.Parse(RealRecordCsv, out var csvDiscarded);

        Assert.Equal(0, jsonDiscarded);
        Assert.Equal(0, csvDiscarded);

        var jsonEntry = Assert.Single(jsonEntries);
        var stderrEntry = Assert.Single(stderrEntries);
        var csvEntry = Assert.Single(csvEntries);

        Assert.Equal(stderrEntry.OccurredAtUtc, jsonEntry.OccurredAtUtc);
        Assert.Equal(csvEntry.OccurredAtUtc, jsonEntry.OccurredAtUtc);

        Assert.Equal(stderrEntry.Pid, jsonEntry.Pid);
        Assert.Equal(csvEntry.Pid, jsonEntry.Pid);

        Assert.Equal(stderrEntry.Severity, jsonEntry.Severity);
        Assert.Equal(csvEntry.Severity, jsonEntry.Severity);

        Assert.Equal(stderrEntry.Message, jsonEntry.Message);
        Assert.Equal(csvEntry.Message, jsonEntry.Message);

        Assert.Equal(csvEntry.UserName, jsonEntry.UserName);
        Assert.Equal(csvEntry.DatabaseName, jsonEntry.DatabaseName);
    }

    /* --- 6: a malformed JSON line in the middle is discarded and counted; neighbours survive ----------- */

    [Fact]
    public void MalformedJsonLineInMiddleIsDiscardedNeighboursSurvive()
    {
        var malformed = "{\"timestamp\":\"not json,,, broken\n";
        var body = RealRecord + malformed + RealRecord;

        var entries = PgServerLogJsonParser.Parse(body, out var recordsDiscarded);

        Assert.Equal(2, entries.Count);
        Assert.Equal(1, recordsDiscarded);
        Assert.All(entries, e => Assert.Equal(103, e.Pid));
    }

    /* --- 7: a non-UTC timestamp is returned, not rejected (#4053 review H1, mirrored for jsonlog) ------ */

    [Fact]
    public void NonUtcZoneRecordIsReturnedWithItsZoneText()
    {
        var record = RealRecord.Replace("03:04:57.241 UTC", "03:04:57.241 PST");

        var entries = PgServerLogJsonParser.Parse(record, out var recordsDiscarded);

        var entry = Assert.Single(entries);
        Assert.Equal(0, recordsDiscarded);
        Assert.Equal("PST", entry.ZoneText);
    }

    /* --- #4053 a2 review S2: a body with no newline is one record longer than the tail --------------- */

    [Fact]
    public void ABodyWithNoNewlineAtAll_YieldsNothing_AndCountsOneDiscard()
    {
        var entries = PgServerLogJsonParser.Parse(RealRecord.TrimEnd('\n'), out var recordsDiscarded);

        Assert.Empty(entries);
        Assert.Equal(1, recordsDiscarded);
    }
}
