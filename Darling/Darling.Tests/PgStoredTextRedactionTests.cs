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
/// Text already stored is brought to the current rules: a setting value that an earlier rule ended at a backslash-escaped
/// space is masked whole, and a statement or row-value detail whose patterns cannot finish in time is withheld rather than
/// kept as written or thrown into the caller.
/// </summary>
public sealed class PgStoredTextRedactionTests
{
    private const string Mask = "********";

    [Fact]
    public void AStoredPasswordKeywordFollowedByAWordOfItsValue_IsMaskedWhole()
    {
        Assert.Equal(Mask, PgSettingRedactor.RedactStored("primary_conninfo", "host=h password=******** def user=x"));
        Assert.Equal(Mask, PgSettingRedactor.RedactStored("primary_conninfo", "password=******** def"));
        Assert.Equal(Mask, PgSettingRedactor.RedactStored("primary_conninfo", "host=h sslpassword=******** tail"));

        /* A mask followed by the next keyword, an option or nothing is kept, and the call is idempotent. */
        const string fine = "host=h password=******** user=x";
        Assert.Equal(fine, PgSettingRedactor.RedactStored("primary_conninfo", fine));
        Assert.Equal("host=h password=********", PgSettingRedactor.RedactStored("primary_conninfo", "host=h password=********"));
        Assert.Equal("host=h password=********", PgSettingRedactor.RedactStored("primary_conninfo", "host=h password=abc\\ def"));
        Assert.Equal(Mask, PgSettingRedactor.RedactStored("primary_conninfo", Mask));
        Assert.Null(PgSettingRedactor.RedactStored("primary_conninfo", null));
    }

    [Fact]
    public void AStatementWhosePatternsTimeOut_IsWithheld_NotKeptOrThrown()
    {
        var statement = "SELECT '" + new string('a', 4_000_000) + "'";
        var before = PgSettingRedactor.MatchTimeoutForTest;
        PgSettingRedactor.MatchTimeoutForTest = TimeSpan.FromMilliseconds(1);
        try
        {
            Assert.Equal(PgLogTextRedactor.WithheldValues, PgLogTextRedactor.RedactStatement(statement));
        }
        finally
        {
            PgSettingRedactor.MatchTimeoutForTest = before;
        }

        Assert.Equal("SELECT '?' WHERE a = ?", PgLogTextRedactor.RedactStatement("SELECT 'x' WHERE a = 5"));
    }

    [Fact]
    public void ARowValueDetailWhosePatternsTimeOut_IsWithheld_NotKeptOrThrown()
    {
        var detail = "Key (" + new string((char)97, 4_000_000);
        var before = StoreLogClassifier.MatchTimeoutForTest;
        StoreLogClassifier.MatchTimeoutForTest = TimeSpan.FromMilliseconds(1);
        try
        {
            Assert.Equal(PgLogTextRedactor.WithheldValues, StoreLogClassifier.MaskRowValues(detail));
        }
        finally
        {
            StoreLogClassifier.MatchTimeoutForTest = before;
        }

        Assert.Equal("Failing row contains (...).", StoreLogClassifier.MaskRowValues("Failing row contains (1, x)."));
    }
}
