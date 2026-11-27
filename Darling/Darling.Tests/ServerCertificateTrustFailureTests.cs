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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9692: a SQL Server whose certificate this machine rejects fails with "error occurred during the pre-login handshake ...
/// error: 35 - An internal exception was caught", which reads as a network fault. <see cref="ServerCertificateTrustFailure"/> says
/// what happened and what fixes it. The messages below are the ones Microsoft.Data.SqlClient 7.1.2 produced against a SQL Server 2022
/// with its self-signed certificate, reached by IP address, and the ones its source and Windows' native layer use for the other shapes.
/// </summary>
public sealed class ServerCertificateTrustFailureTests
{
    private const string Handshake =
        "A connection was successfully established with the server, but then an error occurred during the pre-login handshake. "
        + "(provider: TCP Provider, error: 35 - An internal exception was caught)";

    private const string ChainAndName =
        "Certificate failed chain validation. Error(s): 'The certificate was not trusted., [Status: UntrustedRoot]\n'. "
        + "Certificate name mismatch. The provided 'DataSource' or 'HostNameInCertificate' does not match the name in the certificate.\n";

    private const string ChainOnly =
        "Certificate failed chain validation. Error(s): 'The certificate was not trusted., [Status: UntrustedRoot]\n'.";

    private const string NameOnly =
        "Certificate name mismatch. The provided 'DataSource' or 'HostNameInCertificate' does not match the name in the certificate.";

    private const string TrustStoreText = "add the issuing CA to the system trust store";
    private const string TrustServerCertificateText = "set trustServerCertificate for this server knowing that it skips the check";

    /// <summary>The shape SqlClient throws: a connection exception whose inner exception carries the certificate verdict.</summary>
    private static Exception ConnectFailure(string certificateMessage) =>
        new InvalidOperationException(Handshake, new AuthenticationException(certificateMessage));

    [Fact]
    public void UntrustedChain_NestedTwoDeep_SaysTheChainIsNotTrusted_AndNamesBothFixes()
    {
        var ex = new InvalidOperationException("outer", ConnectFailure(ChainOnly));

        var text = ServerCertificateTrustFailure.Append(Handshake, ex);

        Assert.StartsWith(Handshake + " ", text, StringComparison.Ordinal);
        Assert.Contains("certificate chain is not trusted by this machine", text, StringComparison.Ordinal);
        Assert.Contains(TrustStoreText, text, StringComparison.Ordinal);
        Assert.Contains(TrustServerCertificateText, text, StringComparison.Ordinal);
        Assert.DoesNotContain("name used to connect", text, StringComparison.Ordinal);
    }

    [Fact]
    public void UntrustedChainWithNameMismatch_SaysBoth()
    {
        var description = ServerCertificateTrustFailure.Describe(ConnectFailure(ChainAndName));

        Assert.NotNull(description);
        Assert.Contains("not trusted by this machine", description, StringComparison.Ordinal);
        Assert.Contains("name used to connect", description, StringComparison.Ordinal);
        Assert.Contains(TrustStoreText, description, StringComparison.Ordinal);
        Assert.Contains(TrustServerCertificateText, description, StringComparison.Ordinal);
    }

    [Fact]
    public void NameMismatchAlone_SaysTheNameIsWrong_NotThatTheChainIsUntrusted()
    {
        var description = ServerCertificateTrustFailure.Describe(ConnectFailure(NameOnly));

        Assert.NotNull(description);
        Assert.Contains("not issued to the name used to connect", description, StringComparison.Ordinal);
        Assert.DoesNotContain("trust store", description, StringComparison.Ordinal);
        Assert.Contains(TrustServerCertificateText, description, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCallbackRejectionWording_YieldsToANameMismatch_AndStandsAloneAsARejection()
    {
        const string callbackRejected = "The remote certificate was rejected by the provided RemoteCertificateValidationCallback.";
        var withName = new InvalidOperationException(Handshake, new AuthenticationException(callbackRejected, new AuthenticationException(NameOnly)));

        var nameDescription = ServerCertificateTrustFailure.Describe(withName);
        var aloneDescription = ServerCertificateTrustFailure.Describe(ConnectFailure(callbackRejected));

        Assert.NotNull(nameDescription);
        Assert.Contains("not issued to the name used to connect", nameDescription, StringComparison.Ordinal);
        Assert.DoesNotContain("check that it is current", nameDescription, StringComparison.Ordinal);
        Assert.NotNull(aloneDescription);
        Assert.Contains("rejected the server's certificate", aloneDescription, StringComparison.Ordinal);
    }

    [Fact]
    public void AChainFailureThatIsNotAboutTrust_DoesNotSendTheOperatorToTheTrustStoreAsTheOnlyCause()
    {
        var expired = ConnectFailure("Certificate failed chain validation. Error(s): 'The certificate has expired., [Status: NotTimeValid]\n'.");

        var description = ServerCertificateTrustFailure.Describe(expired);

        Assert.NotNull(description);
        Assert.DoesNotContain("is not trusted", description, StringComparison.Ordinal);
        Assert.Contains("check that it is current", description, StringComparison.Ordinal);
    }

    [Fact]
    public void AChainFailureThatIsNotAboutTrust_WithANameMismatch_NamesBoth()
    {
        var both = ConnectFailure(
            "Certificate failed chain validation. Error(s): 'The certificate has expired., [Status: NotTimeValid]\n'. " + NameOnly);

        var description = ServerCertificateTrustFailure.Describe(both);

        Assert.NotNull(description);
        Assert.Contains("check that it is current", description, StringComparison.Ordinal);
        Assert.Contains("not issued to the name used to connect", description, StringComparison.Ordinal);
        Assert.Contains("connect with a name the certificate covers", description, StringComparison.Ordinal);
    }

    /// <summary>Strict ignores trustServerCertificate, so no sentence offers it to a Strict entry, and each keeps its own fixes.</summary>
    [Theory]
    [InlineData(ChainOnly, TrustStoreText)]
    [InlineData(NameOnly, "connect with a name the certificate covers")]
    [InlineData(ChainAndName, TrustStoreText)]
    [InlineData("Certificate failed chain validation. Error(s): 'The certificate has expired., [Status: NotTimeValid]\n'.", "check that it is current")]
    public void AStrictEntry_IsNotOfferedTrustServerCertificate(string certificateMessage, string fix)
    {
        var strict = ServerCertificateTrustFailure.Describe(ConnectFailure(certificateMessage), strict: true);
        var mandatory = ServerCertificateTrustFailure.Describe(ConnectFailure(certificateMessage));

        Assert.NotNull(strict);
        Assert.Contains(fix, strict, StringComparison.Ordinal);
        Assert.DoesNotContain("trustServerCertificate", strict, StringComparison.Ordinal);
        Assert.EndsWith(".", strict, StringComparison.Ordinal);
        Assert.Contains(TrustServerCertificateText, mandatory, StringComparison.Ordinal);
    }

    /// <summary>The service reads Strict the way its connection builders and the Strict line do: any casing, trimmed, SQL Server only.</summary>
    [Theory]
    [InlineData("Strict", "sqlserver", true)]
    [InlineData(" strict ", "sqlserver", true)]
    [InlineData("Mandatory", "sqlserver", false)]
    [InlineData(null, "sqlserver", false)]
    [InlineData("Strict", "postgres", false)]
    public void TheServiceReadsStrict_AsTheStrictLineDoes(string? encryptMode, string engine, bool expected)
    {
        var server = new MonitoredServer { Name = "srv-a", Host = "srv-a", EncryptMode = encryptMode!, Engine = engine };

        Assert.Equal(expected, DarlingServerConnector.ConnectsStrict(server));
        Assert.Equal(StrictEncryptionHint.IsStrict(encryptMode), StrictEncryptionHint.For(encryptMode, transportFailure: true) is not null);
    }

    [Fact]
    public void AggregateChildren_AreSearched()
    {
        var aggregate = new AggregateException(
            new TimeoutException("first attempt timed out"),
            new InvalidOperationException("second attempt", new AuthenticationException(ChainOnly)));

        Assert.True(ServerCertificateTrustFailure.IsMatch(new InvalidOperationException("outer", aggregate)));
    }

    [Theory]
    [InlineData("The remote certificate is invalid because of errors in the certificate chain: UntrustedRoot")]
    [InlineData("The certificate chain was issued by an authority that is not trusted.")]
    [InlineData("A certificate chain processed, but terminated in a root certificate which is not trusted by the trust provider.")]
    public void UntrustedChainWordings_FromTheDotNetStackAndWindows_AreRecognised(string certificateMessage)
    {
        var message = Handshake.Replace("TCP Provider, error: 35 - An internal exception was caught", "SSL Provider, error: 0 - " + certificateMessage);

        var description = ServerCertificateTrustFailure.Describe(new InvalidOperationException(message));

        Assert.NotNull(description);
        Assert.Contains(TrustStoreText, description, StringComparison.Ordinal);
    }

    [Fact]
    public void WindowsNameMismatch_IsRecognised_ButAKerberosFailureThatSharesTheWordsIsNot()
    {
        var windows = new InvalidOperationException("... (provider: SSL Provider, error: 0 - The target principal name is incorrect.)");
        var kerberos = new InvalidOperationException("The target principal name is incorrect.  Cannot generate SSPI context.");

        Assert.Contains("not issued to the name used to connect", ServerCertificateTrustFailure.Describe(windows), StringComparison.Ordinal);
        Assert.Null(ServerCertificateTrustFailure.Describe(kerberos));
    }

    [Fact]
    public void UnrelatedFailures_KeepTheirText()
    {
        var tlsVersion = new InvalidOperationException(Handshake, new AuthenticationException("The client and server cannot communicate, because they do not possess a common algorithm."));
        var refused = new InvalidOperationException("A network-related or instance-specific error occurred while establishing a connection to SQL Server.", new TimeoutException("timed out"));
        var login = new InvalidOperationException("Login failed for user 'monitor'.");

        foreach (var ex in new[] { tlsVersion, refused, login })
        {
            Assert.False(ServerCertificateTrustFailure.IsMatch(ex));
            Assert.Null(ServerCertificateTrustFailure.Describe(ex));
            Assert.Equal(ex.Message, ServerCertificateTrustFailure.Append(ex.Message, ex));
        }

        Assert.Equal("text", ServerCertificateTrustFailure.Append("text", null));
    }

    private sealed class Lines : ILogger<DarlingWorker>
    {
        public List<string> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Add(formatter(state, exception));
    }

    /// <summary>The service's connect-failure log line, and the failure text it hands to the registry and the alert, carry the sentence.</summary>
    [Fact]
    public void ConnectFailureLog_AndItsReturnedText_CarryTheSentenceForACertificateFailure_AndNotForOtherFailures()
    {
        var worker = (DarlingWorker)RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        var log = new Lines();
        typeof(DarlingWorker).GetField("_logger", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(worker, log);
        var server = new DarlingWorker.ServerLoopState { Config = new MonitoredServer { Name = "srv-a", Host = "srv-a" } };

        var certificate = worker.LogConnectFailure(server, ConnectFailure(ChainOnly), 60);
        var other = worker.LogConnectFailure(new DarlingWorker.ServerLoopState { Config = server.Config }, new InvalidOperationException("connection refused"), 60);

        Assert.Contains(TrustStoreText, certificate, StringComparison.Ordinal);
        Assert.Contains(log.Entries, e => e.Contains(Handshake, StringComparison.Ordinal) && e.Contains(TrustStoreText, StringComparison.Ordinal));
        Assert.Equal("connection refused", other);

        var strictServer = new DarlingWorker.ServerLoopState { Config = new MonitoredServer { Name = "srv-b", Host = "srv-b", EncryptMode = "Strict" } };
        var strict = worker.LogConnectFailure(strictServer, ConnectFailure(ChainOnly), 60);

        Assert.Contains(TrustStoreText, strict, StringComparison.Ordinal);
        Assert.DoesNotContain("trustServerCertificate", strict, StringComparison.Ordinal);
    }
}
