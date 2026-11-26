/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Authentication;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9692: Lite's connection tests (<c>--add-server</c>, the MCP server tools, the Add Server window) take their failure text from
/// <see cref="ServerEntryRules.FailureText"/>. A server certificate this machine rejects gets the shared sentence that says so,
/// and any other failure keeps the driver's text.
/// </summary>
public sealed class ServerCertificateTrustFailureTests
{
    private const string Handshake =
        "A connection was successfully established with the server, but then an error occurred during the pre-login handshake. "
        + "(provider: TCP Provider, error: 35 - An internal exception was caught)";

    [Fact]
    public void FailureText_UntrustedCertificateChain_AddsTheSentence()
    {
        var ex = new InvalidOperationException(Handshake, new AuthenticationException(
            "Certificate failed chain validation. Error(s): 'The certificate was not trusted., [Status: UntrustedRoot]\n'."));

        var text = ServerEntryRules.FailureText(ex, "Mandatory");

        Assert.StartsWith(Handshake + " ", text, StringComparison.Ordinal);
        Assert.Contains("add the issuing CA to the system trust store", text, StringComparison.Ordinal);
        Assert.Contains("set trustServerCertificate for this server knowing that it skips the check", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Strict ignores trustServerCertificate, so a Strict entry's sentence does not offer it. Lite connects with Strict only for the
    /// exact word, the way the Strict line reads it, so "strict" builds Mandatory and keeps the offer.
    /// </summary>
    [Theory]
    [InlineData("Strict", false)]
    [InlineData("strict", true)]
    [InlineData("Mandatory", true)]
    public void FailureText_OffersTrustServerCertificate_OnlyWhenTheEntryIsNotStrict(string encryptMode, bool offered)
    {
        var ex = new InvalidOperationException(Handshake, new AuthenticationException(
            "Certificate failed chain validation. Error(s): 'The certificate was not trusted., [Status: UntrustedRoot]\n'."));

        var text = ServerEntryRules.FailureText(ex, encryptMode);

        Assert.Contains("add the issuing CA to the system trust store", text, StringComparison.Ordinal);
        Assert.Equal(offered, text.Contains("trustServerCertificate", StringComparison.Ordinal));
        Assert.Equal(!offered, ServerEntryRules.UsesStrict(encryptMode));
    }

    [Fact]
    public void FailureText_AnotherHandshakeFailure_KeepsTheDriversText()
    {
        var ex = new InvalidOperationException(Handshake, new AuthenticationException("The client and server cannot communicate, because they do not possess a common algorithm."));

        Assert.Equal(Handshake, ServerEntryRules.FailureText(ex, "Mandatory"));
    }
}
