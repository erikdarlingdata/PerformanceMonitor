/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The relation names Darling prints into commands it invites an operator to RUN, and the two levels of
/// escaping that keeps honest. Added with #3234 after a PR review found the index-bloat command
/// concatenating raw catalog text into a single-quoted literal, and the table-bloat command already
/// shipping that way.
///
/// <para><b>Why this is not merely tidiness.</b> These strings are handed to functions taking
/// <c>regclass</c>, whose TEXT input parses like an identifier — so an unquoted part folds to lower case
/// and a camelCase index resolves to a different object or to none. And an index created through a
/// double-quoted <c>CREATE INDEX</c> may contain a single quote, which closes the literal and appends
/// whatever follows to a command an operator is being asked to paste into a privileged session. A role
/// that can create an index therefore chooses part of what the operator runs.</para>
/// </summary>
public class PgIdentifierQuotingTests
{
    [Fact]
    public void OrdinaryNames_AreStillQuoted_RatherThanQuotedOnlyWhenTheyLookNecessary()
        => Assert.Equal("\"public\".\"ix_orders\"", PgIdentifier.Qualify("public", "ix_orders"));

    /// <summary>
    /// The correctness half. Unquoted regclass text folds to lower case, so this is the case that
    /// resolves to the wrong object rather than erroring.
    /// </summary>
    [Fact]
    public void MixedCaseSurvives_BecauseUnquotedRegclassTextWouldBeFolded()
        => Assert.Equal("\"Sales\".\"IX_Orders\"", PgIdentifier.Qualify("Sales", "IX_Orders"));

    [Fact]
    public void AnEmbeddedDoubleQuote_IsDoubled_WhichIsQuoteIdentsRule()
        => Assert.Equal("\"public\".\"ix\"\"weird\"", PgIdentifier.Qualify("public", "ix\"weird"));

    /// <summary>
    /// The injection half. A single quote in an identifier would otherwise close the literal the name is
    /// embedded in; doubling it keeps the whole name inside the literal where it belongs.
    /// </summary>
    [Fact]
    public void AnEmbeddedSingleQuote_IsDoubled_SoItCannotCloseTheLiteral()
    {
        var qualified = PgIdentifier.Qualify("public", "ix'); DROP TABLE orders; --");

        Assert.Equal("\"public\".\"ix''); DROP TABLE orders; --\"", qualified);

        /* The property that matters is not the exact text but that no SINGLE quote survives unpaired -- an
           odd count is what lets the literal close early. */
        var singles = 0;
        foreach (var c in qualified)
        {
            if (c == '\'') { singles++; }
        }

        Assert.Equal(0, singles % 2);
    }

    [Fact]
    public void MissingParts_DoNotProduceAnUnquotedFragment()
    {
        Assert.Equal("\"public\".\"\"", PgIdentifier.Qualify(null, null));
        Assert.Equal("\"public\".\"ix\"", PgIdentifier.Qualify(null, "ix"));
    }

    /// <summary>
    /// The row's command, end to end, because that is the string an operator actually pastes. The
    /// reviewer's specific objection was that this code path had no coverage of its own text.
    /// </summary>
    [Fact]
    public void TheRowsCommand_QuotesTheIdentifier_AndPrefixesCreateExtensionOnlyWhenAbsent()
    {
        var present = Row("Sales", "IX_Orders", pgstattuple: true).ExactMeasurementCommand;
        Assert.Equal("SELECT * FROM pgstatindex('\"Sales\".\"IX_Orders\"');", present);

        var absent = Row("Sales", "IX_Orders", pgstattuple: false).ExactMeasurementCommand;
        Assert.Equal(
            "CREATE EXTENSION pgstattuple; SELECT * FROM pgstatindex('\"Sales\".\"IX_Orders\"');",
            absent);

        /* A NULL availability is not a known absence, so it must not print a CREATE EXTENSION an operator
           would then run against a database that already has it. */
        var unknown = Row("Sales", "IX_Orders", pgstattuple: null).ExactMeasurementCommand;
        Assert.DoesNotContain("CREATE EXTENSION", unknown, StringComparison.Ordinal);
    }

    private static DarlingPgIndexBloatReader.PgIndexBloatRow Row(
        string? schema, string? index, bool? pgstattuple) =>
        new(
            DatabaseName: "app",
            SchemaName: schema,
            TableName: "orders",
            IndexName: index,
            IndexBytes: 8192,
            TreeLevel: null,
            EmptyPages: null,
            DeletedPages: null,
            AvgLeafDensity: null,
            LeafFragmentation: null,
            EstimatedReclaimableBytes: 0,
            SkippedReason: null,
            MeasurementKind: "estimated",
            EstBloatPct: 0,
            IndexPages: 1,
            TableRows: 10,
            Fillfactor: 90,
            EstTupleBytes: 16,
            EstLeafPages: 1,
            PgstattupleAvailable: pgstattuple,
            CaptureTime: new DateTime(2026, 9, 9, 12, 0, 0, DateTimeKind.Utc));
}
