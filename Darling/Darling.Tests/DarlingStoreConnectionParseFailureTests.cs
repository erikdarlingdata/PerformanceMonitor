/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store connection strings every test in this file feeds through a parse or a construction step. All values are fake. The
/// first two make Npgsql 10.0.3 report a keyword it cannot set, and the .NET parser lowercases that keyword:
/// <c>Password=P4SS;W0RD;...</c> makes <c>w0rd;database</c> the "keyword" (an unquoted ';' in the password), and
/// <c>Password==Zq7Pw;...</c> makes <c>password=zq7pw;database</c> the "keyword" (a password that starts with '=').
/// The last two parse, and fail later, when Npgsql builds a data source from them.
/// </summary>
internal static class StoreParseFailureFixtures
{
    internal const string SemicolonInPassword =
        "Host=127.0.0.1;Port=5432;Username=darling;Password=P4SS;W0RD;Database=darlingtest";

    internal const string LeadingEqualsInPassword =
        "Host=127.0.0.1;Port=5432;Username=darling;Password==Zq7Pw;Database=darlingtest";

    /// <summary>
    /// A password with an unquoted ';' whose tail is a <c>Host=</c> pair with a colon suffix. Npgsql's string parser
    /// accepts it, and its construction step (a data source or a connection) then splits the Host value on the last ':'
    /// and throws a <see cref="FormatException"/> whose message quotes the text after the colon ('W0rd').
    /// </summary>
    internal const string PortTailInHost =
        "Host=127.0.0.1;Port=5432;Username=darling;Password=Zq7;Host=S3cret:W0rd;Database=darlingtest";

    /// <summary>The same shape with a port that is a number outside the range: the construction step throws an
    /// <see cref="ArgumentOutOfRangeException"/> whose message quotes '-4401'.</summary>
    internal const string NegativePortInHost =
        "Host=127.0.0.1;Username=darling;Password=Zq7;Host=S3cret:-4401;Database=darlingtest";

    /// <summary>The lowercased pieces of the strings above that Npgsql's message quotes. Compared with
    /// <see cref="StringComparison.OrdinalIgnoreCase"/>, because the parser changes their case.</summary>
    internal static readonly string[] Fragments = ["w0rd", "zq7pw", "s3cret", "-4401"];

    internal static void AssertNoFragment(string text, string where)
    {
        foreach (var fragment in Fragments)
        {
            Assert.False(
                text.Contains(fragment, StringComparison.OrdinalIgnoreCase),
                $"{where} carries the password fragment '{fragment}': {text}");
        }
    }

    internal static int GetClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

/// <summary>
/// A store connection string that Npgsql cannot parse must never put part of a value into text that is printed or
/// logged. <see cref="DarlingStoreConnection.DescribeParseFailure"/> is the one formatter, and every site that prints
/// or logs such a failure (the CLI, the service's worker and its two Kestrel hosts, and the viewer) uses it. Every
/// test here is parse-only: no connection is opened and no store is needed.
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingStoreConnectionParseFailureTests
{
    private const string FixedHintStart = "Npgsql rejected the string. A value that contains ';' or '=' must be quoted";

    private static Exception Parse(string connectionString) =>
        Assert.ThrowsAny<Exception>(() => new NpgsqlConnectionStringBuilder(connectionString));

    /* ---------------------------------------------------------------------------------------------------
       The formatter
       --------------------------------------------------------------------------------------------------- */

    /// <summary>Every shape of parse failure whose keyword is not one Npgsql knows: the formatter prints its one
    /// fixed hint, which is the same for every secret and holds no text from the exception.</summary>
    [Theory]
    [InlineData("an unquoted ';' in the password", StoreParseFailureFixtures.SemicolonInPassword)]
    [InlineData("a password that starts with '='", StoreParseFailureFixtures.LeadingEqualsInPassword)]
    [InlineData("a '=' after a ';' in the password", "Host=h;Username=u;Password=Zq7Pw;Cd=Ef;Database=db")]
    [InlineData("a ';' in the password, then a real key", "Host=h;Username=u;Password=P4SS;W0RD;Port=5432")]
    [InlineData("two ';' in the password", "Host=h;Username=u;Password=P4;SS;W0RD;Database=db")]
    [InlineData("an unterminated quote", "Host=h;Username=u;Password='P4SS;W0RD;Database=db")]
    [InlineData("a ';' in the last pair", "Host=h;Username=u;Database=db;Password=P4SS;W0RD")]
    [InlineData("only '=' characters", "=====")]
    [InlineData("a keyword that is misspelled", "Host=h;Usrname=u;Password=pw")]
    public void AKeywordNpgsqlDoesNotKnow_GetsTheFixedHint_AndNothingFromTheException(string shape, string connectionString)
    {
        var ex = Parse(connectionString);

        var text = DarlingStoreConnection.DescribeParseFailure(ex);

        Assert.StartsWith(FixedHintStart, text, StringComparison.Ordinal);
        StoreParseFailureFixtures.AssertNoFragment(text, shape);
        Assert.DoesNotContain(ex.Message, text, StringComparison.Ordinal);
        for (var inner = ex.InnerException; inner is not null; inner = inner.InnerException)
        {
            Assert.DoesNotContain(inner.Message, text, StringComparison.Ordinal);
        }

        if (ex is ArgumentException { ParamName: { Length: > 0 } paramName })
        {
            Assert.DoesNotContain(paramName, text, StringComparison.OrdinalIgnoreCase);
        }
    }

    /// <summary>Two different secrets give one text: nothing about the secret reaches the output.</summary>
    [Fact]
    public void TheHint_IsTheSameForEverySecret()
    {
        var first = DarlingStoreConnection.DescribeParseFailure(Parse(StoreParseFailureFixtures.SemicolonInPassword));
        var second = DarlingStoreConnection.DescribeParseFailure(
            Parse("Host=elsewhere;Username=other;Password=Ab1;Y8XV;Database=x"));
        var third = DarlingStoreConnection.DescribeParseFailure(Parse(StoreParseFailureFixtures.LeadingEqualsInPassword));

        Assert.Equal(first, second);
        Assert.Equal(first, third);
    }

    /// <summary>A bad value for a keyword Npgsql knows names the key, in Npgsql's canonical spelling, and never the
    /// value. The value sits in the inner exception, and the message names only the key.</summary>
    [Theory]
    [InlineData("Host=h;Port=Abc;Username=u;Password=pw;Database=db", "Port", "Abc")]
    [InlineData("Host=h;Username=u;Password=pw;Database=db;SSL Mode=NotARealSslMode", "SSL Mode", "NotARealSslMode")]
    [InlineData("Host=h;Username=u;Password=pw;Timeout=Xyz", "Timeout", "Xyz")]
    [InlineData("Host=h;Username=u;Password=Ab;Port=S3cret;Database=db", "Port", "S3cret")]
    public void ARealKeywordWithABadValue_NamesTheKey_AndNotTheValue(string connectionString, string key, string value)
    {
        var ex = Parse(connectionString);

        var text = DarlingStoreConnection.DescribeParseFailure(ex);

        Assert.Contains($"'{key}'", text, StringComparison.Ordinal);
        Assert.DoesNotContain(value, text, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(ex.InnerException);
        Assert.DoesNotContain(ex.InnerException!.Message, text, StringComparison.Ordinal);
        Assert.DoesNotContain(FixedHintStart, text, StringComparison.Ordinal);
    }

    /// <summary>The formatter reads only <see cref="ArgumentException.ParamName"/> and only when Npgsql lists it as a
    /// keyword, so a hand-built exception whose message and inner exception are full of secrets prints none of it.</summary>
    [Fact]
    public void AnExceptionBuiltByHand_ContributesOnlyAKnownKeyword()
    {
        var secretInner = new KeyNotFoundException("inner Zq7Pw");

        var unknown = DarlingStoreConnection.DescribeParseFailure(
            new ArgumentException("Couldn't set zq7pw;database", "zq7pw;database", secretInner));
        var known = DarlingStoreConnection.DescribeParseFailure(
            new ArgumentException("Couldn't set port for Zq7Pw", "port", secretInner));
        var other = DarlingStoreConnection.DescribeParseFailure(new FormatException("The input string 'Zq7Pw' was not in a correct format."));
        var keyNotFound = DarlingStoreConnection.DescribeParseFailure(new KeyNotFoundException("The given key 'Zq7Pw' was not present."));

        Assert.StartsWith(FixedHintStart, unknown, StringComparison.Ordinal);
        Assert.StartsWith(FixedHintStart, other, StringComparison.Ordinal);
        Assert.StartsWith(FixedHintStart, keyNotFound, StringComparison.Ordinal);
        Assert.Contains("'Port'", known, StringComparison.Ordinal);
        foreach (var text in new[] { unknown, known, other, keyNotFound })
        {
            StoreParseFailureFixtures.AssertNoFragment(text, "a hand-built exception");
        }

        Assert.Throws<ArgumentNullException>(() => DarlingStoreConnection.DescribeParseFailure(null!));
    }

    [Fact]
    public void TryDescribeParseFailure_IsNullForAStringThatParses_AndTheFormattersTextForOneThatDoesNot()
    {
        Assert.Null(DarlingStoreConnection.TryDescribeParseFailure("Host=h;Username=u;Password=pw;Database=db"));
        Assert.Null(DarlingStoreConnection.TryDescribeParseFailure("Host=h;Username=u;Password='P4SS;W0RD';Database=db"));
        Assert.Null(DarlingStoreConnection.TryDescribeParseFailure("Host=h;Username=u;Password=P4SS=W0RD;Database=db"));

        var semicolon = DarlingStoreConnection.TryDescribeParseFailure(StoreParseFailureFixtures.SemicolonInPassword);
        var leadingEquals = DarlingStoreConnection.TryDescribeParseFailure(StoreParseFailureFixtures.LeadingEqualsInPassword);
        var typed = DarlingStoreConnection.TryDescribeParseFailure("Host=h;Username=u;Password=Ab;Port=S3cret;Database=db");

        Assert.NotNull(semicolon);
        Assert.NotNull(leadingEquals);
        Assert.NotNull(typed);
        StoreParseFailureFixtures.AssertNoFragment(semicolon!, "TryDescribeParseFailure");
        StoreParseFailureFixtures.AssertNoFragment(leadingEquals!, "TryDescribeParseFailure");
        Assert.Contains("'Port'", typed, StringComparison.Ordinal);
        Assert.DoesNotContain("S3cret", typed, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The string parser accepts both strings, and Npgsql's construction step (what a data source runs) is what throws:
    /// it splits the Host value on its last ':' and the exception's message quotes the text after it. The guard runs
    /// that step, so it returns the formatter's text and the text holds nothing from the exception.
    /// </summary>
    [Theory]
    [InlineData(StoreParseFailureFixtures.PortTailInHost, "W0rd")]
    [InlineData(StoreParseFailureFixtures.NegativePortInHost, "-4401")]
    public void TheGuard_RunsNpgsqlsConstructionStep_SoAPasswordTailInHostIsNotQuoted(string connectionString, string quoted)
    {
        _ = new NpgsqlConnectionStringBuilder(connectionString);
        var thrown = Assert.ThrowsAny<Exception>(() => { using var dataSource = NpgsqlDataSource.Create(connectionString); });
        Assert.Contains(quoted, thrown.Message, StringComparison.Ordinal);

        var text = DarlingStoreConnection.TryDescribeParseFailure(connectionString);

        Assert.NotNull(text);
        Assert.StartsWith(FixedHintStart, text, StringComparison.Ordinal);
        StoreParseFailureFixtures.AssertNoFragment(text!, "the guard's text");
        Assert.DoesNotContain(thrown.Message, text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Npgsql's own setting conflicts are fixed text with no operator input. The formatter maps each to a sentence of
    /// its own that names the conflict, instead of the hint about quoting a value, and does not pass Npgsql's message
    /// through. The exception comes from the real Npgsql (both the data source and the connection constructor, which
    /// the CLI proves with), so a Npgsql that rewords a message makes this test fail.
    /// </summary>
    [Theory]
    [InlineData("Host=127.0.0.1;Username=u;Password=pw;Multiplexing=true;Pooling=false", "Multiplexing needs Pooling to be on.")]
    [InlineData("Host=127.0.0.1;Username=u;Password=pw;SslNegotiation=direct;SSL Mode=Prefer", "SSL Negotiation=Direct needs SSL Mode Require, Verify CA or Verify Full.")]
    [InlineData("Host=127.0.0.1;Username=u;Password=pw;Target Session Attributes=primary", "Target Session Attributes other than Any needs more than one host in Host.")]
    [InlineData("Host=127.0.0.1;Username=u;Password=pw;Minimum Pool Size=10;Maximum Pool Size=5", "Maximum Pool Size is smaller than Minimum Pool Size.")]
    public void ASettingConflictNpgsqlRejects_GetsASentenceThatNamesIt_NotTheHintAboutQuoting(string connectionString, string sentence)
    {
        var expected = "Npgsql rejected the settings: " + sentence;
        var fromDataSource = Assert.ThrowsAny<Exception>(() => { using var dataSource = NpgsqlDataSource.Create(connectionString); });
        var fromConnection = Assert.ThrowsAny<Exception>(() => { using var connection = new NpgsqlConnection(connectionString); });

        Assert.Equal(expected, DarlingStoreConnection.DescribeParseFailure(fromDataSource));
        Assert.Equal(expected, DarlingStoreConnection.DescribeParseFailure(fromConnection));
        Assert.Equal(expected, DarlingStoreConnection.TryDescribeParseFailure(connectionString));
        Assert.DoesNotContain(FixedHintStart, expected, StringComparison.Ordinal);
        Assert.DoesNotContain(fromDataSource.Message, expected, StringComparison.Ordinal);
    }

    /// <summary>
    /// Npgsql's keyword lookup folds some non-ASCII characters into ASCII ones (U+017F, LATIN SMALL LETTER LONG S,
    /// upper-cases to 'S'), so <c>ContainsKey</c> accepts a keyword spelled with one. The formatter prints Npgsql's
    /// canonical spelling from its own list and never the spelling in the exception, which here came from the
    /// password.
    /// </summary>
    [Fact]
    public void AKeywordSpelledWithAFoldingCharacter_PrintsNothingOfTheSecretsOwnSpelling()
    {
        var ex = Parse("Host=h;Username=u;Password=Zq7;\u017Fslmode=W0rd;Database=db");
        var paramName = Assert.IsAssignableFrom<ArgumentException>(ex).ParamName!;
        Assert.Contains('\u017F', paramName);
        Assert.True(new NpgsqlConnectionStringBuilder().ContainsKey(paramName), "Npgsql accepts the folded spelling");

        var text = DarlingStoreConnection.DescribeParseFailure(ex);

        Assert.DoesNotContain('\u017F', text);
        Assert.All(text, c => Assert.True(c < 128, $"a non-ASCII character reached the output: {text}"));
        StoreParseFailureFixtures.AssertNoFragment(text, "the folded spelling");

        /* An ASCII spelling that differs from Npgsql's own only in case or spacing prints the canonical one. */
        foreach (var spelling in new[] { "sslmode", "SSLMODE", "ssl mode", "Ssl Mode" })
        {
            var canonical = DarlingStoreConnection.DescribeParseFailure(new ArgumentException("x", spelling));
            Assert.Contains("'SSL Mode'", canonical, StringComparison.Ordinal);
            Assert.DoesNotContain(spelling, canonical.Replace("'SSL Mode'", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        }
    }

    /* ---------------------------------------------------------------------------------------------------
       The service: the worker, and the MCP and web hosts
       --------------------------------------------------------------------------------------------------- */

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        private readonly List<string> _lines = new();
        private readonly List<Exception> _exceptions = new();

        public string Joined
        {
            get
            {
                lock (_lines)
                {
                    return string.Join(Environment.NewLine, _lines);
                }
            }
        }

        public IReadOnlyList<Exception> Exceptions
        {
            get
            {
                lock (_lines)
                {
                    return _exceptions.ToList();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Add($"{logLevel}: {formatter(state, exception)}");
                if (exception is not null)
                {
                    _exceptions.Add(exception);
                }
            }
        }
    }

    /// <summary>
    /// The worker parses the store string first thing in <c>RunCollectionLoopAsync</c>, and the failure that leaves the
    /// method is what the host logs: the exception is the formatter's text with no inner exception. The test then
    /// logs it the way the host does (the exception object, on the real file provider) and checks the file, and it
    /// checks <c>ToString()</c>, which is what the console and the Event Log print.
    /// </summary>
    [Theory]
    [InlineData(StoreParseFailureFixtures.SemicolonInPassword)]
    [InlineData(StoreParseFailureFixtures.LeadingEqualsInPassword)]
    [InlineData(StoreParseFailureFixtures.PortTailInHost)]
    [InlineData(StoreParseFailureFixtures.NegativePortInHost)]
    public async Task TheWorker_FailsWithTheFormattersText_AndTheHostsLogLineHoldsNoFragment(string connectionString)
    {
        var worker = new DarlingWorker(
            new CapturingLogger<DarlingWorker>(),
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator());
        var method = typeof(DarlingWorker).GetMethod("RunCollectionLoopAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var config = new DarlingConfig { Postgres = new PostgresConfig { ConnectionString = connectionString } };

        var task = (Task)method.Invoke(
            worker, [config, connectionString, null, null, null, null, false, null, null, CancellationToken.None])!;
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => task);

        Assert.StartsWith("postgres.connectionString could not be used: " + FixedHintStart, ex.Message, StringComparison.Ordinal);
        Assert.Null(ex.InnerException);
        StoreParseFailureFixtures.AssertNoFragment(ex.ToString(), "the exception the host logs (console and Event Log print ToString())");

        var logDir = Directory.CreateTempSubdirectory("darling-parse-worker-").FullName;
        try
        {
            using var provider = new DarlingFileLoggerProvider(logDir);
            provider.CreateLogger("Microsoft.Extensions.Hosting.Internal.Host").LogCritical(ex, "BackgroundService failed");
            provider.Flush();

            var written = File.ReadAllText(provider.CurrentLogFile());
            Assert.Contains("could not be used", written, StringComparison.Ordinal);
            StoreParseFailureFixtures.AssertNoFragment(written, "the service's file log");
        }
        finally
        {
            Directory.Delete(logDir, recursive: true);
        }
    }

    private static DarlingConfig HostConfig(string configuredSurfaceString, bool web) => new()
    {
        Postgres = new PostgresConfig
        {
            ConnectionString = "Host=127.0.0.1;Port=5432;Username=darling;Password=x;Database=darlingtest",
            McpConnectionString = web ? null : configuredSurfaceString,
            WebConnectionString = web ? configuredSurfaceString : null,
        },
        /* No token here: the opt-out lets the start reach the store string these tests are about. */
        Web = new WebConfig { AllowTokenlessLoopback = true },
        Mcp = new McpConfig { AllowTokenlessLoopback = true },
    };

    /// <summary>
    /// A start attempt of the MCP server whose configured store login cannot be parsed logs the formatter's text,
    /// returns false, and logs no exception object (a logger appends the exception's message, and the console and the
    /// Event Log append its inner exceptions).
    /// </summary>
    [Theory]
    [InlineData(StoreParseFailureFixtures.SemicolonInPassword)]
    [InlineData(StoreParseFailureFixtures.LeadingEqualsInPassword)]
    [InlineData(StoreParseFailureFixtures.PortTailInHost)]
    [InlineData(StoreParseFailureFixtures.NegativePortInHost)]
    public async Task TheMcpHost_LogsTheFormattersText_WhenItsStoreStringDoesNotParse(string connectionString)
    {
        var logger = new CapturingLogger<DarlingMcpHostService>();
        var host = new DarlingMcpHostService(logger, new McpRuntimeState(), new MonitoredServerRegistryState());
        var method = typeof(DarlingMcpHostService).GetMethod("TryStartServerAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var toggle = DarlingHostBinding.ResolveEndpointToggle(null, true, StoreParseFailureFixtures.GetClosedPort());

        var started = await (Task<bool>)method.Invoke(host, [HostConfig(connectionString, web: false), toggle, CancellationToken.None])!;

        Assert.False(started);
        Assert.Contains("is unusable: " + FixedHintStart, logger.Joined, StringComparison.Ordinal);
        StoreParseFailureFixtures.AssertNoFragment(logger.Joined, "the MCP host's log");
        Assert.Empty(logger.Exceptions);
    }

    [Theory]
    [InlineData(StoreParseFailureFixtures.SemicolonInPassword)]
    [InlineData(StoreParseFailureFixtures.LeadingEqualsInPassword)]
    [InlineData(StoreParseFailureFixtures.PortTailInHost)]
    [InlineData(StoreParseFailureFixtures.NegativePortInHost)]
    public async Task TheWebHost_LogsTheFormattersText_WhenItsStoreStringDoesNotParse(string connectionString)
    {
        var logger = new CapturingLogger<DarlingWebHostService>();
        var host = new DarlingWebHostService(
            logger, new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache());
        var method = typeof(DarlingWebHostService).GetMethod("TryStartServerAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var toggle = DarlingHostBinding.ResolveEndpointToggle(null, true, StoreParseFailureFixtures.GetClosedPort());

        var started = await (Task<bool>)method.Invoke(host, [HostConfig(connectionString, web: true), toggle, CancellationToken.None])!;

        Assert.False(started);
        Assert.Contains("is unusable: " + FixedHintStart, logger.Joined, StringComparison.Ordinal);
        StoreParseFailureFixtures.AssertNoFragment(logger.Joined, "the web host's log");
        Assert.Empty(logger.Exceptions);
    }

    /* ---------------------------------------------------------------------------------------------------
       The viewer
       --------------------------------------------------------------------------------------------------- */

    /// <summary>The self-test's config layer is the first thing it runs, and a string that does not parse stops
    /// there, so this needs no network. Its detail goes into the failure overlay and the viewer log.</summary>
    [Theory]
    [InlineData(StoreParseFailureFixtures.SemicolonInPassword)]
    [InlineData(StoreParseFailureFixtures.LeadingEqualsInPassword)]
    public async Task TheSelfTestsConfigLayer_ReportsTheFormattersText_NotTheExceptionMessage(string connectionString)
    {
        var results = await StoreConnectionSelfTest.RunAsync(connectionString);

        var config = results[0];
        Assert.Equal(StoreConnectionSelfTest.ConfigLayer, config.Layer);
        Assert.Equal(StoreConnectionSelfTest.LayerOutcome.Failed, config.Outcome);
        Assert.Contains("connection string did not parse: " + FixedHintStart, config.Detail, StringComparison.Ordinal);
        Assert.All(results.Skip(1), r => Assert.Equal(StoreConnectionSelfTest.LayerOutcome.NotReached, r.Outcome));

        var report = StoreConnectionSelfTest.FormatReport(results);
        StoreParseFailureFixtures.AssertNoFragment(config.Detail, "the self-test's config layer detail");
        StoreParseFailureFixtures.AssertNoFragment(report, "the self-test report");
    }

    /// <summary>A string that parses but that Npgsql's construction step rejects stops at the config layer with the
    /// formatter's text. It used to pass the layer and print the password tail as the endpoint.</summary>
    [Theory]
    [InlineData(StoreParseFailureFixtures.PortTailInHost)]
    [InlineData(StoreParseFailureFixtures.NegativePortInHost)]
    public async Task TheSelfTestsConfigLayer_ProvesTheStringWithNpgsqlsConstructionStep(string connectionString)
    {
        var results = await StoreConnectionSelfTest.RunAsync(connectionString);

        var config = results[0];
        Assert.Equal(StoreConnectionSelfTest.LayerOutcome.Failed, config.Outcome);
        Assert.Contains("connection string parsed but cannot be used: " + FixedHintStart, config.Detail, StringComparison.Ordinal);
        Assert.All(results.Skip(1), r => Assert.Equal(StoreConnectionSelfTest.LayerOutcome.NotReached, r.Outcome));
        StoreParseFailureFixtures.AssertNoFragment(StoreConnectionSelfTest.FormatReport(results), "the self-test report");
    }

    [Fact]
    public async Task TheSelfTestsConfigLayer_NamesARealKeyword_AndNotItsValue()
    {
        var results = await StoreConnectionSelfTest.RunAsync(
            "Host=127.0.0.1;Username=u;Password=pw;Database=db;SSL Mode=NotARealSslMode");

        Assert.Equal(StoreConnectionSelfTest.LayerOutcome.Failed, results[0].Outcome);
        Assert.Contains("'SSL Mode'", results[0].Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("NotARealSslMode", StoreConnectionSelfTest.FormatReport(results), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// <c>MainWindow.OnLoaded</c> asks <see cref="ViewerDataService.TryCreate"/> for the data service and shows and logs
    /// what it says (<c>ShowConnectionFailure</c> writes its argument to the viewer log). It used to construct the
    /// service directly, so a string that did not parse escaped an <c>async void</c> handler into the dispatcher's
    /// unhandled-exception log, which writes the exception's message and its inner exception.
    /// </summary>
    [Theory]
    [InlineData(StoreParseFailureFixtures.SemicolonInPassword)]
    [InlineData(StoreParseFailureFixtures.LeadingEqualsInPassword)]
    [InlineData(StoreParseFailureFixtures.PortTailInHost)]
    [InlineData(StoreParseFailureFixtures.NegativePortInHost)]
    public void TheViewersDataService_SaysWhyAStringCannotBeUsed_WithoutTheFragment(string connectionString)
    {
        var created = ViewerDataService.TryCreate(connectionString, 5, out var service, out var unusable);

        Assert.False(created);
        Assert.Null(service);
        Assert.StartsWith("postgres.connectionString could not be used: " + FixedHintStart, unusable, StringComparison.Ordinal);
        StoreParseFailureFixtures.AssertNoFragment(unusable, "the text the viewer shows and logs");
    }

    [Fact]
    public void TheViewersMainWindow_BuildsItsDataServiceThroughTryCreate_AndShowsWhatItSays()
    {
        var code = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs");

        Assert.Contains(
            "ViewerDataService.TryCreate(settings.ConnectionString, _connectionTimeoutSeconds, out var dataService, out var unusable)",
            code, StringComparison.Ordinal);
        Assert.Contains("ShowConnectionFailure(unusable);", code, StringComparison.Ordinal);
        Assert.DoesNotContain("new ViewerDataService(", code, StringComparison.Ordinal);
    }

    /// <summary>The three service sites and the CLI each parse first or catch, and none passes the exception itself to
    /// a logger or prints its message for this failure.</summary>
    [Fact]
    public void EverySitePrintsTheFormattersText_NotTheParseExceptionsMessage()
    {
        string Read(params string[] segments) => RepoFile.ReadRepoFile(segments);

        var cli = Read("Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs");
        Assert.Contains(
            "\"postgres.connectionString could not be used: \" + DarlingStoreConnection.DescribeParseFailure(ex)",
            cli, StringComparison.Ordinal);
        Assert.DoesNotContain("\"postgres.connectionString could not be used: \" + ex.Message", cli, StringComparison.Ordinal);

        foreach (var file in new[]
        {
            new[] { "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs" },
            new[] { "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs" },
            new[] { "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs" },
            new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs" },
        })
        {
            var text = Read(file);
            Assert.Contains("DarlingStoreConnection.TryDescribeParseFailure(", text, StringComparison.Ordinal);
        }

        /* The viewer's connection summary prints the host, so it asks the formatter first. */
        var diagnostics = Read("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerConfigDiagnostics.cs");
        Assert.Contains("DarlingStoreConnection.TryDescribeParseFailure(connectionString)", diagnostics, StringComparison.Ordinal);
        Assert.True(
            diagnostics.IndexOf("DarlingStoreConnection.TryDescribeParseFailure(", StringComparison.Ordinal)
                < diagnostics.IndexOf("Line(\"Host\"", StringComparison.Ordinal),
            "the summary must ask the formatter before it prints the Host.");

        var selfTest = Read("Darling", "PerformanceMonitor.Darling.Storage", "StoreConnectionSelfTest.cs");
        Assert.Contains("connection string did not parse: {DarlingStoreConnection.DescribeParseFailure(ex)}", selfTest, StringComparison.Ordinal);
        Assert.DoesNotContain("connection string did not parse: {ex.Message}", selfTest, StringComparison.Ordinal);
        Assert.Contains("DarlingStoreConnection.TryDescribeParseFailure(connectionString)", selfTest, StringComparison.Ordinal);
        Assert.Contains("connection string parsed but cannot be used: {unusable}", selfTest, StringComparison.Ordinal);
    }

    /// <summary>The viewer's connection summary (the log and the failure overlay) prints the Host of a string that
    /// parses. For each fixture, whether it fails at the parse or later at construction, neither the summary nor the
    /// whole block carries a fragment of the password.</summary>
    [Theory]
    [InlineData(StoreParseFailureFixtures.SemicolonInPassword)]
    [InlineData(StoreParseFailureFixtures.LeadingEqualsInPassword)]
    [InlineData(StoreParseFailureFixtures.PortTailInHost)]
    [InlineData(StoreParseFailureFixtures.NegativePortInHost)]
    public void TheViewerConnectionSummary_CarriesNoFragmentOfTheString(string connectionString)
    {
        var summary = string.Join(Environment.NewLine, ViewerConfigDiagnostics.DescribeConnection(connectionString, managed: false));
        StoreParseFailureFixtures.AssertNoFragment(summary, "the viewer's connection summary");

        var location = ViewerSettings.ResolveConfigLocation(@"C:\Darling\darling.json");
        var details = ViewerConfigDiagnostics.BuildDetails(location, connectionString, managed: false);
        StoreParseFailureFixtures.AssertNoFragment(details, "the viewer's connection details");
    }

    /// <summary>A string that parses and fails at construction prints the formatter's text in the summary, and no Host line.</summary>
    [Fact]
    public void TheViewerConnectionSummary_SaysThePortTailStringCannotBeUsed_WithoutAHostLine()
    {
        var lines = ViewerConfigDiagnostics.DescribeConnection(StoreParseFailureFixtures.PortTailInHost, managed: false);
        var summary = string.Join(Environment.NewLine, lines);

        Assert.Contains("COULD NOT BE USED", summary, StringComparison.Ordinal);
        Assert.DoesNotContain(lines, line => line.TrimStart().StartsWith("Host", StringComparison.Ordinal));
    }

    /// <summary>A string with no Host gets its own sentence, not the quoting hint, and the sentence holds no text of the string.</summary>
    [Theory]
    [InlineData("Username=darling;Password=Zq7Pw;Database=darlingtest")]
    [InlineData("Host=;Username=darling;Password=Zq7Pw;Database=darlingtest")]
    public void AStringWithNoHost_GetsItsOwnSentence_NotTheQuotingHint(string connectionString)
    {
        var text = DarlingStoreConnection.TryDescribeParseFailure(connectionString);

        Assert.NotNull(text);
        Assert.Contains("has no Host", text, StringComparison.Ordinal);
        Assert.DoesNotContain("must be quoted", text, StringComparison.Ordinal);
        StoreParseFailureFixtures.AssertNoFragment(text!, "the missing-Host sentence");
        Assert.DoesNotContain("Zq7Pw", text!, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("darlingtest", text!, StringComparison.OrdinalIgnoreCase);
    }
}
