/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The row values PostgreSQL writes into its own fixed formats are masked in their slot, in every catalogue: a
/// unique or foreign-key violation's key values, a NOT NULL or CHECK failure's failing row, a COPY or JSON input
/// error's CONTEXT, and an input function's rejected value. Column, constraint and table names stay. A field whose
/// SQLSTATE puts it in the class, or that opens like one of the forms, and matches no form whole is withheld.
/// Every other line of prose is kept as written.
/// </summary>
public sealed class PgLogValueSlotTests
{
    private const string W = PgLogTextRedactor.WithheldValues;

    /* A catalogue form filled: %s, %d and %llu in order, or %n$s where the catalogue reorders them. */
    private static string Fill(string format, params string[] args)
    {
        var next = 0;
        return Regex.Replace(format, @"%(?:(?<position>[0-9])\$)?(?:llu|d|s)", m =>
            args[(m.Groups["position"].Success ? m.Groups["position"].Value[0] - '0' : ++next) - 1]);
    }

    [Fact]
    public void AUniqueViolationsDetail_KeepsItsColumnsAndMasksItsValues()
    {
        Assert.Equal("Key (email)=(?) already exists.", PgLogTextRedactor.RedactDetail("Key (email)=(alice@example.com) already exists."));
        Assert.Equal("Key (a, b)=(?) already exists.", PgLogTextRedactor.RedactDetail("Key (a, b)=(1, x, y) already exists."));
        Assert.Equal("Key (email)=(?) is duplicated.", PgLogTextRedactor.RedactDetail("Key (email)=(alice@example.com) is duplicated."));

        /* A value holding the form's own closing text is masked whole; a value holding a newline too. */
        Assert.Equal("Key (note)=(?) already exists.", PgLogTextRedactor.RedactDetail("Key (note)=(x) already exists. y) already exists."));
        Assert.Equal("Key (note)=(?) already exists.", PgLogTextRedactor.RedactDetail("Key (note)=(line one\n\tline two) already exists."));

        /* Idempotent. */
        Assert.Equal("Key (email)=(?) already exists.", PgLogTextRedactor.RedactDetail("Key (email)=(?) already exists."));
    }

    [Fact]
    public void AForeignKeyViolationsDetail_KeepsItsColumnsAndTableAndMasksItsValues()
    {
        Assert.Equal(
            "Key (customer_id)=(?) is not present in table \"customers\".",
            PgLogTextRedactor.RedactDetail("Key (customer_id)=(98765) is not present in table \"customers\"."));
        Assert.Equal(
            "Key (id)=(?) is still referenced from table \"orders\".",
            PgLogTextRedactor.RedactDetail("Key (id)=(42) is still referenced from table \"orders\"."));

        /* The form written when the role may not read the key's columns carries no value and is kept. */
        Assert.Equal("Key is not present in table \"customers\".", PgLogTextRedactor.RedactDetail("Key is not present in table \"customers\"."));
    }

    [Fact]
    public void AFailingRow_IsMaskedWhole_AndAPartitionKeyKeepsItsColumns()
    {
        Assert.Equal("Failing row contains (?).", PgLogTextRedactor.RedactDetail("Failing row contains (42, bob@example.com, 123-45-6789, null)."));
        Assert.Equal("Failing row contains (?).", PgLogTextRedactor.RedactDetail("Failing row contains (42, Steps:\nProcess 1: mix, null)."));
        Assert.Equal(
            "Partition key of the failing row contains (tenant_email) = (?).",
            PgLogTextRedactor.RedactDetail("Partition key of the failing row contains (tenant_email) = (bob@example.com)."));
    }

    [Fact]
    public void AnExclusionViolationsDetail_MasksBothKeys_AndKeysOfDifferentColumnsAreWithheld()
    {
        Assert.Equal(
            "Key (room, during)=(?) conflicts with existing key (room, during)=(?).",
            PgLogTextRedactor.RedactDetail("Key (room, during)=(101, [\"2026-01-01\",\"2026-01-02\")) conflicts with existing key (room, during)=(101, [\"2026-01-01\",\"2026-01-03\"))."));
        Assert.Equal(
            "Key (a)=(?) conflicts with key (a)=(?).",
            PgLogTextRedactor.RedactDetail("Key (a)=(1) conflicts with key (a)=(1)."));

        /* Two keys of one index name the same columns; a text where they do not is not what PostgreSQL wrote. */
        Assert.Equal(W, PgLogTextRedactor.RedactDetail("Key (a)=(x) conflicts with existing key (Kept)=(y)."));
    }

    [Fact]
    public void ACopyFrame_MasksItsInput_AndOneCutBeforeItsCloseIsWithheldWithTheRest()
    {
        Assert.Equal(
            "COPY customers, line 3: \"?\"\n\tPL/pgSQL function f() line 3 at SQL statement",
            PgLogTextRedactor.RedactContext("COPY customers, line 3: \"43,carol@example.com,987-65-4321\"\n\tPL/pgSQL function f() line 3 at SQL statement"));
        Assert.Equal(
            "COPY customers, line 3, column ssn: \"?\"",
            PgLogTextRedactor.RedactContext("COPY customers, line 3, column ssn: \"987-65-4321\""));

        /* The frames that quote no input are kept. */
        foreach (var bare in new[] { "COPY customers", "COPY customers, line 3", "COPY customers, line 3, column ssn", "COPY customers, line 3, column ssn: null input" })
        {
            Assert.Equal(bare, PgLogTextRedactor.RedactContext(bare));
        }

        /* A CSV value holding a newline runs onto the next line: nothing after the frame's head is trusted. */
        Assert.Equal(W, PgLogTextRedactor.RedactContext("COPY t, line 1: \"4111,Jane\n\tSQL statement \"SELECT 1\""));
        Assert.Equal(
            "PL/pgSQL function g() line 2 at PERFORM\n\t" + W,
            PgLogTextRedactor.RedactContext("PL/pgSQL function g() line 2 at PERFORM\n\tCOPY t, line 1: \"4111,Jane\n\tSQL statement \"SELECT 1\""));
    }

    [Fact]
    public void AJsonFrame_MasksTheInputAroundTheError()
    {
        Assert.Equal(
            "JSON data, line 1: ?\n\tPL/pgSQL function g() line 2 at PERFORM",
            PgLogTextRedactor.RedactContext("JSON data, line 1: {\"card\": 4111111111111111, \"cvv\": 123, \"x\": }\n\tPL/pgSQL function g() line 2 at PERFORM"));
        Assert.Equal("JSON data, line 1: ?", PgLogTextRedactor.RedactContext("JSON data, line 1: ...{\"ssn\": \"123-45-6789\"..."));
    }

    [Fact]
    public void AnInputSyntaxMessage_KeepsTheTypeAndMasksTheValue_AfterAnyPositionAndVerboseState()
    {
        Assert.Equal(
            "invalid input syntax for type integer: \"?\"",
            PgLogTextRedactor.RedactMessage("invalid input syntax for type integer: \"4111111111111111\""));
        Assert.Equal(
            "invalid input syntax for type integer: \"?\" at character 8",
            PgLogTextRedactor.RedactMessage("invalid input syntax for type integer: \"abc\" at character 8"));
        Assert.Equal(
            "22P02: invalid input syntax for type timestamp with time zone: \"?\"",
            PgLogTextRedactor.RedactMessage("22P02: invalid input syntax for type timestamp with time zone: \"2026-13-45 x\""));
        Assert.Equal(
            "invalid input syntax for type integer: \"?\"",
            PgLogTextRedactor.RedactMessage("invalid input syntax for type integer: \"a\n\tb\""));

        /* The JSON parser's message names no value, and is kept. */
        Assert.Equal("invalid input syntax for type json", PgLogTextRedactor.RedactMessage("invalid input syntax for type json"));
    }

    /// <summary>Every catalogue's form masks its value slot and keeps its names, and a second pass changes nothing.</summary>
    [Fact]
    public void EveryCataloguesForm_MasksItsValueSlotAndKeepsItsNames()
    {
        const string kept = "Kept-S1";
        var checks = new List<(string Format, string Filled, string Expected, Func<string, string?> Redact)>();
        foreach (var f in PgLogTextRedactor.KeyExistsDetailFormats)
        {
            checks.Add((f, Fill(f, "(email)=(" + kept + ")"), Fill(f, "(email)=(?)"), d => PgLogTextRedactor.RedactDetail(d)));
        }

        foreach (var f in PgLogTextRedactor.KeyConflictDetailFormats)
        {
            checks.Add((f, Fill(f, "(a)=(" + kept + "1)", "(a)=(" + kept + "2)"), Fill(f, "(a)=(?)", "(a)=(?)"), d => PgLogTextRedactor.RedactDetail(d)));
        }

        foreach (var f in PgLogTextRedactor.KeyReferenceDetailFormats)
        {
            checks.Add((f, Fill(f, "id", kept, "orders"), Fill(f, "id", "?", "orders"), d => PgLogTextRedactor.RedactDetail(d)));
        }

        foreach (var f in PgLogTextRedactor.KeyReferenceUnreadableDetailFormats)
        {
            checks.Add((f, Fill(f, "orders"), Fill(f, "orders"), d => PgLogTextRedactor.RedactDetail(d)));
        }

        foreach (var f in PgLogTextRedactor.FailingRowDetailFormats)
        {
            checks.Add((f, Fill(f, "(1, " + kept + ", null)"), Fill(f, "(?)"), d => PgLogTextRedactor.RedactDetail(d)));
        }

        foreach (var f in PgLogTextRedactor.PartitionKeyDetailFormats)
        {
            checks.Add((f, Fill(f, "(tenant) = (" + kept + ")"), Fill(f, "(tenant) = (?)"), d => PgLogTextRedactor.RedactDetail(d)));
        }

        foreach (var f in PgLogTextRedactor.CopyLineContextFormats)
        {
            checks.Add((f, Fill(f, "t", "3", kept), Fill(f, "t", "3", "?"), c => PgLogTextRedactor.RedactContext(c)));
        }

        foreach (var f in PgLogTextRedactor.CopyColumnContextFormats)
        {
            checks.Add((f, Fill(f, "t", "3", "ssn", kept), Fill(f, "t", "3", "ssn", "?"), c => PgLogTextRedactor.RedactContext(c)));
        }

        foreach (var f in PgLogTextRedactor.CopyBareContextFormats)
        {
            checks.Add((f, Fill(f, "t", "3", "ssn"), Fill(f, "t", "3", "ssn"), c => PgLogTextRedactor.RedactContext(c)));
        }

        foreach (var f in PgLogTextRedactor.JsonContextFormats)
        {
            checks.Add((f, Fill(f, "1", "...", kept, "..."), Fill(f, "1", "?", string.Empty, string.Empty), c => PgLogTextRedactor.RedactContext(c)));
        }

        foreach (var f in PgLogTextRedactor.InputSyntaxMessageFormats)
        {
            checks.Add((f, Fill(f, "integer", kept), Fill(f, "integer", "?"), m => PgLogTextRedactor.RedactMessage(m)));
            checks.Add((f, Fill(f, "integer", kept) + " at character 8", Fill(f, "integer", "?") + " at character 8", m => PgLogTextRedactor.RedactMessage(m)));
        }

        foreach (var f in PgLogTextRedactor.InputSyntaxBareMessageFormats)
        {
            checks.Add((f, Fill(f, "json"), Fill(f, "json"), m => PgLogTextRedactor.RedactMessage(m)));
        }

        foreach (var (format, filled, expected, redact) in checks)
        {
            var once = redact(filled);
            Assert.True(expected == once, $"{format}: expected [{expected}] got [{once}]");
            Assert.Equal(once, redact(once));
        }
    }

    /// <summary>The SQLSTATE keys the class: a field in it that matches no form is withheld, an application's
    /// DETAIL under a RAISE with that errcode included. Without one, prose that opens like a form and matches none
    /// whole is withheld, and every other line of prose is kept as written.</summary>
    [Fact]
    public void TheClass_WithholdsAShapeThisBuildDoesNotRead_AndOtherProseIsKept()
    {
        foreach (var state in PgLogTextRedactor.ValueDetailStates)
        {
            Assert.Equal(W, PgLogTextRedactor.RedactDetail("Customer Kept-S1 already has an account.", sqlState: state));
            Assert.Equal(W, PgLogTextRedactor.RedactDetail(W, sqlState: state));
        }

        Assert.Equal(W, PgLogTextRedactor.RedactMessage("malformed array literal: \"{Kept-S1\"", sqlState: "22P02"));
        Assert.Equal("Key (email)=(?) already exists.", PgLogTextRedactor.RedactDetail("Key (email)=(Kept-S1) already exists.", sqlState: "23505"));
        Assert.Equal("Failing row contains (?).", PgLogTextRedactor.RedactDetail("Failing row contains (Kept-S1).", sqlState: "23514"));

        /* Not in the class: prose is kept, unless it opens like a form without being one. */
        Assert.Equal("Customer Kept-S1 already has an account.", PgLogTextRedactor.RedactDetail("Customer Kept-S1 already has an account.", sqlState: "P0001"));
        Assert.Equal("Customer Kept-S1 already has an account.", PgLogTextRedactor.RedactDetail("Customer Kept-S1 already has an account."));
        Assert.Equal("malformed array literal: \"{x\"", PgLogTextRedactor.RedactMessage("malformed array literal: \"{x\""));
        Assert.Equal(W, PgLogTextRedactor.RedactDetail("Key (email)=(Kept-S1) already exists"));
        Assert.Equal(W, PgLogTextRedactor.RedactDetail("Failing row contains Kept-S1."));
        Assert.Equal(W, PgLogTextRedactor.RedactMessage("invalid input syntax for type integer: Kept-S1"));
        Assert.Equal(W, PgLogTextRedactor.RedactMessage("22P02: invalid input syntax for type integer: Kept-S1"));

        /* The other DETAIL shapes are untouched: a deadlock report's queries are still read as SQL. */
        Assert.Equal(
            "Process 1 waits for ShareLock on transaction 2; blocked by process 3.\nProcess 3 waits for ShareLock on transaction 4; blocked by process 1.\nProcess 1: UPDATE t SET a = '?'\nProcess 3: UPDATE t SET b = ?",
            PgLogTextRedactor.RedactDetail(
                "Process 1 waits for ShareLock on transaction 2; blocked by process 3.\nProcess 3 waits for ShareLock on transaction 4; blocked by process 1.\nProcess 1: UPDATE t SET a = 'x'\nProcess 3: UPDATE t SET b = 1",
                complete: true));
    }

    /// <summary>The entry path: a csvlog or jsonlog entry, or a stderr entry under a <c>%e</c> prefix, carries its
    /// SQLSTATE, which keys the class; a stderr entry under the default prefix carries none, and the forms are
    /// matched by template alone.</summary>
    [Fact]
    public void TheEntryPath_KeysTheClassOffTheEntrysSqlState_AndMatchesTemplatesWithoutOne()
    {
        static PgLogEntry Entry(string? sqlState, string message, string? detail, string? context) => new(
            TimestampText: "2026-09-29 04:00:00.000", ZoneText: "UTC", OccurredAtUtc: new DateTime(2026, 9, 29, 4, 0, 0, DateTimeKind.Utc),
            Pid: 4102, PrefixRest: string.Empty, Severity: "ERROR", Message: message, Detail: detail, Hint: null, Statement: null,
            Context: context, UserName: null, DatabaseName: null, SqlState: sqlState, RawText: message, DetailComplete: true);

        /* csvlog: field 12 carried the SQLSTATE, and an application's DETAIL under that errcode is withheld. */
        var raised = PgLogEvent.From(Entry("23505", "customer exists", "Customer Kept-S1 already has an account.", null), PgLogFamilies.Error);
        Assert.Equal(W, raised.Detail);
        Assert.Equal("customer exists", raised.Message);

        var duplicate = PgLogEvent.From(Entry("23505", "duplicate key value violates unique constraint \"customers_email_key\"", "Key (email)=(Kept-S1@example.com) already exists.", null), PgLogFamilies.Error);
        Assert.Equal("Key (email)=(?) already exists.", duplicate.Detail);

        /* stderr under the default prefix: no SQLSTATE, the template alone. */
        var stderr = PgLogEvent.From(Entry(null, "invalid input syntax for type integer: \"Kept-S1\"", null, "COPY customers, line 3, column id: \"Kept-S1\""), PgLogFamilies.Error);
        Assert.Equal("invalid input syntax for type integer: \"?\"", stderr.Message);
        Assert.Equal("COPY customers, line 3, column id: \"?\"", stderr.Context);
        var prose = PgLogEvent.From(Entry(null, "customer exists", "Customer Kept-S1 already has an account.", null), PgLogFamilies.Error);
        Assert.Equal("Customer Kept-S1 already has an account.", prose.Detail);
    }

    /// <summary>A store-log row captured under the earlier rules is brought to these by the hourly re-mask, which
    /// rewrites any row whose masked form changed; a second pass changes nothing.</summary>
    [Fact]
    public void AStoreLogRowStoredUnderTheEarlierRules_IsBroughtToThese()
    {
        const string prefix = "2026-09-05 14:03:02.551 UTC [5288] ";
        var sample = prefix + "ERROR:  duplicate key value violates unique constraint \"customers_email_key\"\n"
            + prefix + "DETAIL:  Key (email)=(Kept-S1@example.com) already exists.\n"
            + prefix + "STATEMENT:  INSERT INTO customers (email) VALUES ('Kept-S1@example.com')";
        var (message, masked) = StoreLogClassifier.MaskStoredEvent(StoreLogClassifier.UnclassifiedClass, "duplicate key value violates unique constraint \"customers_email_key\"", sample);
        Assert.DoesNotContain("Kept-S1", masked, StringComparison.Ordinal);
        /* The store log withholds a key tuple whole (StoreLogClassifier.MaskRowValues), after the slot is masked. */
        Assert.Contains("DETAIL:  Key (email)=(...) already exists.", masked, StringComparison.Ordinal);
        Assert.Equal((message, masked), StoreLogClassifier.MaskStoredEvent(StoreLogClassifier.UnclassifiedClass, message, masked));

        var invalid = prefix + "ERROR:  invalid input syntax for type integer: \"Kept-S1\"";
        var (_, maskedInvalid) = StoreLogClassifier.MaskStoredEvent(StoreLogClassifier.UnclassifiedClass, "invalid input syntax for type integer: \"Kept-S1\"", invalid);
        Assert.Equal(prefix + "ERROR:  invalid input syntax for type integer: \"?\"", maskedInvalid);
    }
}
