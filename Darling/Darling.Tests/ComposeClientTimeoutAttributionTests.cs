// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: on the web compose path the client <c>CommandTimeout</c> used to equal the role's server-side
/// <c>statement_timeout</c> (both 60 s), so the two raced. When Npgsql's client timer won, the run threw an
/// <see cref="NpgsqlException"/> wrapping a <see cref="TimeoutException"/> that landed in the generic arm
/// and recorded Error. The client deadline now sits above the server's, and that exception shape is
/// classified as a timeout with the same text 57014 gives.
/// </summary>
public class ComposeClientTimeoutAttributionTests
{
    [Fact]
    public void WebComposeClientDeadline_IsStrictlyAboveTheServerStatementTimeout()
    {
        const int server = 60;
        var client = DarlingWebEndpoints.ComposeClientDeadlineSeconds(server, DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds);

        Assert.True(client > server, $"web compose client deadline {client}s must exceed the server statement_timeout {server}s so 57014 wins the race");
        Assert.True(DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds > 0);
    }

    [Fact]
    public void McpComposeCaller_KeepsItsDeadline_NoHeadroom()
    {
        Assert.Equal(60, DarlingWebEndpoints.ComposeClientDeadlineSeconds(60, 0));
    }

    [Fact]
    public void ClientTimeout_NpgsqlExceptionWrappingTimeoutException_MapsToTimeoutWithTheActionableText()
    {
        var ex = new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"));

        var outcome = DarlingWebEndpoints.FromRunException(ex);

        Assert.False(outcome.IsServerError);
        Assert.Null(outcome.Fault);
        Assert.Equal("57014", outcome.AuthorSqlState);
        Assert.Equal(DarlingWebEndpoints.StatementTimeoutText, outcome.Error);
        Assert.Equal(
            DarlingWebEndpoints.FromPostgresException(new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014")).Error,
            outcome.Error);
    }

    [Fact]
    public void ClientTimeout_OnTheMcpPath_KeepsTheGenericServerError()
    {
        var ex = new NpgsqlException("x", new TimeoutException("t"));
        var outcome = DarlingWebEndpoints.FromRunException(ex, webPath: false);
        Assert.True(outcome.IsServerError);
        Assert.StartsWith("Error running query:", outcome.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void OtherExceptions_StillMapToTheGenericServerError()
    {
        var plain = DarlingWebEndpoints.FromRunException(new InvalidOperationException("boom"));
        Assert.True(plain.IsServerError);
        Assert.Equal("Error running query: boom", plain.Error);
        Assert.Null(plain.AuthorSqlState);

        var npgsqlOther = DarlingWebEndpoints.FromRunException(new NpgsqlException("connection refused", new System.IO.IOException("x")));
        Assert.True(npgsqlOther.IsServerError);
    }
}
