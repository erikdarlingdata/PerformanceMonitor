/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6183: SqlClient's native network library failing to load (typically a path near 260 characters) made every
/// SQL Server connection fail with "The type initializer for 'Microsoft.Data.SqlClient.TdsParser' threw an
/// exception". <see cref="SqlClientNativeLoadFailure"/> turns that into one explanation for every surface.
/// </summary>
public class SqlClientNativeLoadFailureTests
{
    private const string ShortBase = @"C:\Darling";

    private static TypeInitializationException TdsParserFailure(Exception inner) =>
        new("Microsoft.Data.SqlClient.TdsParser", inner);

    private static string LongBase() => @"C:\" + new string('d', 230);

    [Fact]
    public void TdsParserInitializerFailure_ShortPath_ExplainsLibraryAndGivesReasonButNoPathAdvice()
    {
        var ex = TdsParserFailure(new DllNotFoundException(
            "Unable to load DLL 'Microsoft.Data.SqlClient.SNI.x64.dll' or one of its dependencies.\r\nSecond line."));

        var message = SqlClientNativeLoadFailure.Describe(ex, ShortBase);

        Assert.NotNull(message);
        Assert.Contains("native network library did not load", message);
        Assert.Contains("no SQL Server connection can work from this install", message);
        Assert.Contains("Unable to load DLL 'Microsoft.Data.SqlClient.SNI.x64.dll' or one of its dependencies.", message);
        Assert.DoesNotContain("Second line", message);
        Assert.DoesNotContain("shorter folder path", message);
    }

    [Fact]
    public void TdsParserInitializerFailure_LongPath_NamesPathLengthAndSaysToMoveTheInstall()
    {
        var baseDirectory = LongBase();
        var ex = TdsParserFailure(new DllNotFoundException("Unable to load DLL 'Microsoft.Data.SqlClient.SNI.x64.dll'."));

        var message = SqlClientNativeLoadFailure.Describe(ex, baseDirectory + @"\");

        Assert.NotNull(message);
        Assert.Contains("native network library did not load", message);
        Assert.Contains($"{baseDirectory.Length} characters", message);
        Assert.Contains("likely cause", message);
        Assert.Contains("Move the install to a shorter folder path", message);
    }

    [Fact]
    public void PathJustUnderTheThreshold_DoesNotBlameThePath()
    {
        var baseDirectory = new string('p', SqlClientNativeLoadFailure.LongPathThreshold - 1);
        var ex = TdsParserFailure(new DllNotFoundException("Unable to load DLL 'Microsoft.Data.SqlClient.SNI.x64.dll'."));

        var message = SqlClientNativeLoadFailure.Describe(ex, baseDirectory);

        Assert.NotNull(message);
        Assert.DoesNotContain("shorter folder path", message);
    }

    [Fact]
    public void BareSniDllNotFound_IsRecognisedWithoutTheTypeInitializerWrapper()
    {
        var ex = new DllNotFoundException("Unable to load DLL 'Microsoft.Data.SqlClient.SNI.x86.dll'.");

        Assert.NotNull(SqlClientNativeLoadFailure.Describe(ex, ShortBase));
    }

    [Fact]
    public void BadImageFormatNamingSni_IsRecognisedInsideAnotherException()
    {
        var ex = new InvalidOperationException(
            "outer", new BadImageFormatException("Could not load file or assembly 'Microsoft.Data.SqlClient.SNI'."));

        Assert.NotNull(SqlClientNativeLoadFailure.Describe(ex, ShortBase));
    }

    [Fact]
    public void TdsParserInitializerFailure_WithNoInnerException_StillExplainsWithoutRepeatingTheWrapper()
    {
        var message = SqlClientNativeLoadFailure.Describe(
            new TypeInitializationException("Microsoft.Data.SqlClient.TdsParser", null), ShortBase);

        Assert.NotNull(message);
        Assert.DoesNotContain("Reason:", message);
        Assert.DoesNotContain("type initializer", message);
    }

    [Fact]
    public void OrdinaryConnectFailure_IsNotClassified_SoTheCallerKeepsItsOwnMessage()
    {
        var ordinary = new InvalidOperationException("A network-related or instance-specific error occurred.");

        Assert.False(SqlClientNativeLoadFailure.IsMatch(ordinary));
        Assert.Null(SqlClientNativeLoadFailure.Describe(ordinary, LongBase()));
        Assert.Null(SqlClientNativeLoadFailure.Describe(null, LongBase()));
    }

    [Fact]
    public void OtherTypeInitializerAndOtherMissingDll_AreNotClassified()
    {
        var otherType = new TypeInitializationException("Some.Other.Type", new InvalidOperationException("boom"));
        var otherDll = new DllNotFoundException("Unable to load DLL 'duckdb.dll'.");

        Assert.Null(SqlClientNativeLoadFailure.Describe(otherType, ShortBase));
        Assert.Null(SqlClientNativeLoadFailure.Describe(otherDll, ShortBase));
    }
}
