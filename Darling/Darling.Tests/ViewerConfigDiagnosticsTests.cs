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
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The definition behind <c>[Collection("darling-config-env")]</c>: the classes that must set the
/// process-wide <c>DARLING_CONFIG</c> environment variable to exercise the resolver that reads it.
///
/// <para>Same hazard, same remedy as <see cref="ViewerTimeStaticsCollection"/>. An environment variable is
/// process state, and xUnit runs separate collections in PARALLEL — so a test that sets DARLING_CONFIG to a
/// temp file can be mid-assertion while another test asks the resolver what it would pick, and the second
/// one fails on the first one's variable. Every member here saves and restores the previous value in a
/// <c>finally</c>, which makes them safe against each OTHER inside the collection; the
/// <see cref="CollectionDefinitionAttribute.DisableParallelization"/> is what makes them safe against the
/// rest of the assembly.</para>
/// </summary>
[CollectionDefinition("darling-config-env", DisableParallelization = true)]
public sealed class DarlingConfigEnvironmentCollection
{
}

/// <summary>
/// The viewer's startup self-description (#1954): which darling.json it read, and the non-secret summary
/// of what it parsed.
///
/// <para><b>The redaction test is the point of this file.</b> The summary is emitted to a log file and
/// rendered in the connection-failure overlay — two places an operator copies into a bug report — and the
/// connection string it summarizes carries a live database password. "We were careful not to print it" is
/// not a guarantee; a test that feeds a real password through and asserts the output cannot contain it is.
/// It goes red if the allowlist in <see cref="ViewerConfigDiagnostics"/> is ever replaced by anything that
/// copies the caller's string through.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class ViewerConfigDiagnosticsTests
{
    /// <summary>A value distinctive enough that its presence anywhere in the output is unambiguous.</summary>
    private const string LivePassword = "pw-9f3c1a7e-must-never-be-logged";

    private static string ByoConnectionString(string rootCertificate = "server.crt") =>
        $"Host=store.example.com;Port=5641;Username=viewer;Password={LivePassword};Database=darling;" +
        $"Search Path=collect,config,public;SSL Mode=VerifyFull;Root Certificate={rootCertificate}";

    [Fact]
    public void DescribeConnection_NeverEchoesThePassword()
    {
        var lines = ViewerConfigDiagnostics.DescribeConnection(ByoConnectionString(), managed: false);
        var text = string.Join(Environment.NewLine, lines);

        Assert.DoesNotContain(LivePassword, text, StringComparison.Ordinal);
        /* The keyword too: a summary that named "Password" without its value would still invite someone to
           "just add the value" later, and there is no diagnostic reason to mention it at all. */
        Assert.DoesNotContain("password", text, StringComparison.OrdinalIgnoreCase);

        /* A redaction assertion passes trivially against empty output, so pin that a real summary was in
           fact produced — otherwise this test would keep passing after the summary stopped working. */
        Assert.Contains("store.example.com", text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildDetails_TheBlockShownInTheUiAndWrittenToTheLog_NeverEchoesThePassword()
    {
        /* The composed block, not just the connection half — this is the exact string that reaches
           MessageDetailsText and ViewerLogger, so it is the string the guarantee has to hold for. */
        var location = ViewerSettings.ResolveConfigLocation(@"C:\Darling\darling.json");
        var details = ViewerConfigDiagnostics.BuildDetails(location, ByoConnectionString(), managed: false);

        Assert.DoesNotContain(LivePassword, details, StringComparison.Ordinal);
        Assert.DoesNotContain("password", details, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(@"C:\Darling\darling.json", details, StringComparison.Ordinal);
        Assert.Contains("store.example.com", details, StringComparison.Ordinal);
    }

    [Fact]
    public void DescribeConnection_MalformedString_ReportsTheFailureWithoutEchoingTheString()
    {
        /* Npgsql's parse errors can quote the fragment they choked on, and that fragment can be the
           credential half — so an unparseable string reports the exception TYPE and nothing else. */
        var lines = ViewerConfigDiagnostics.DescribeConnection(
            $"Host=store.example.com;Port=NOT-A-NUMBER;Password={LivePassword}", managed: false);
        var text = string.Join(Environment.NewLine, lines);

        Assert.DoesNotContain(LivePassword, text, StringComparison.Ordinal);
        Assert.Contains("COULD NOT BE PARSED", text, StringComparison.Ordinal);
    }

    [Fact]
    public void DescribeConnection_ReportsEveryNonSecretFieldAnOperatorWouldCheck()
    {
        var lines = ViewerConfigDiagnostics.DescribeConnection(ByoConnectionString(), managed: false);

        Assert.Equal("store.example.com", ValueFor(lines, "Host"));
        Assert.Equal("5641", ValueFor(lines, "Port"));
        Assert.Equal("viewer", ValueFor(lines, "Username"));
        Assert.Equal("darling", ValueFor(lines, "Database"));
        Assert.Equal("VerifyFull", ValueFor(lines, "SSL Mode"));
        Assert.Equal("collect,config,public", ValueFor(lines, "Search Path"));
        Assert.Equal("server.crt", ValueFor(lines, "Root Certificate"));
    }

    [Fact]
    public void DescribeConnection_ManagedMode_SaysTheConnectionStringInTheFileIsNotRead()
    {
        /* The managed flag decides whether postgres.connectionString is consulted AT ALL — an operator
           editing it on a managed install is editing something nothing reads, which is exactly the kind of
           "right file, wrong value" confusion this summary exists to end. */
        var managed = ValueFor(
            ViewerConfigDiagnostics.DescribeConnection("Host=127.0.0.1;Port=5641;Username=admin;Database=darling", managed: true),
            "postgres.managed");
        Assert.StartsWith("true", managed, StringComparison.Ordinal);
        Assert.Contains("not read", managed, StringComparison.Ordinal);

        var byo = ValueFor(ViewerConfigDiagnostics.DescribeConnection(ByoConnectionString(), managed: false), "postgres.managed");
        Assert.StartsWith("false", byo, StringComparison.Ordinal);
    }

    [Fact]
    public void DescribeConnection_NoConnectionString_SaysNothingWasParsed_RatherThanInventingFields()
    {
        var text = string.Join(Environment.NewLine, ViewerConfigDiagnostics.DescribeConnection(null, managed: false));

        Assert.Contains("not loaded", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Host:", text, StringComparison.Ordinal);
        /* Nor a managed verdict — there was no file to read one out of. */
        Assert.DoesNotContain("postgres.managed", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The sharpest case in the whole feature: the documented bring-your-own string carries a BARE
    /// <c>Root Certificate=server.crt</c>. Npgsql resolves a relative path against the process working
    /// directory, so the same viewer launched from a shortcut and from a shell used to look for the
    /// certificate in different places. #1970 moved the anchor to darling.json's own directory, and this
    /// pins that the block reports THAT anchor — the directory it names has to be the one the connection
    /// string was rewritten against, or the block is confidently wrong.
    /// </summary>
    [Fact]
    public void DescribeConnection_RelativeRootCertificate_ResolvesAgainstTheConfigDirectory_AndReportsExistence()
    {
        var root = Directory.CreateTempSubdirectory("darling-viewer-cert-");
        try
        {
            var expected = Path.Combine(root.FullName, "server.crt");

            var missing = ViewerConfigDiagnostics.DescribeConnection(
                ByoConnectionString(), managed: false, configDirectory: root.FullName);
            Assert.Equal(expected, ValueFor(missing, "resolves to"));
            Assert.Equal(root.FullName, ValueFor(missing, "relative to"));
            Assert.Equal("NO", ValueFor(missing, "exists"));

            File.WriteAllText(expected, "-----BEGIN CERTIFICATE-----");
            var present = ViewerConfigDiagnostics.DescribeConnection(
                ByoConnectionString(), managed: false, configDirectory: root.FullName);
            Assert.Equal("yes", ValueFor(present, "exists"));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void DescribeConnection_AbsoluteRootCertificate_ReportsItAsGiven_WithNoRelativeToLine()
    {
        var root = Directory.CreateTempSubdirectory("darling-viewer-abscert-");
        try
        {
            var certificate = Path.Combine(root.FullName, "pinned.crt");
            File.WriteAllText(certificate, "-----BEGIN CERTIFICATE-----");

            /* A DIFFERENT config directory than the certificate's, so an anchor wrongly applied to an
               absolute path would move it somewhere this assertion can see. */
            var elsewhere = Directory.CreateTempSubdirectory("darling-viewer-abscert-config-");
            try
            {
                var lines = ViewerConfigDiagnostics.DescribeConnection(
                    ByoConnectionString(certificate), managed: false, configDirectory: elsewhere.FullName);

                Assert.Equal(certificate, ValueFor(lines, "resolves to"));
                Assert.Equal("yes", ValueFor(lines, "exists"));
                /* An absolute path has no anchor dependency, so claiming one would be noise. */
                Assert.DoesNotContain(lines, l => l.TrimStart().StartsWith("relative to:", StringComparison.Ordinal));
            }
            finally
            {
                elsewhere.Delete(recursive: true);
            }
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void DescribeConnection_ManagedLoopback_ReportsNoCertificate()
    {
        var lines = ViewerConfigDiagnostics.DescribeConnection(
            "Host=127.0.0.1;Port=5641;Username=admin;Database=darling", managed: true);

        Assert.Equal("(not set)", ValueFor(lines, "Root Certificate"));
    }

    // ── Which darling.json won ────────────────────────────────────────────────────────────

    [Fact]
    public void ResolveConfigLocation_CommandLineArgument_IsReportedAsSuch_WithItsExistence()
    {
        var missing = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName(), "darling.json");

        var location = ViewerSettings.ResolveConfigLocation(missing);

        Assert.Equal(ViewerConfigSource.CommandLine, location.Source);
        Assert.Equal(missing, location.Path);
        Assert.False(location.Exists);
    }

    /// <summary>
    /// The failure this feature was reported for: DARLING_CONFIG is set AND a darling.json sits beside the
    /// binary, and the operator cannot tell which one is live. The variable wins, and now says so.
    /// </summary>
    [Fact]
    public void ResolveConfigLocation_EnvironmentVariable_OutranksAFileBesideTheBinary_AndIsReportedAsSuch()
    {
        var saved = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        var root = Directory.CreateTempSubdirectory("darling-viewer-envsource-");
        try
        {
            var viewerDirectory = Path.Combine(root.FullName, "viewer");
            Directory.CreateDirectory(viewerDirectory);
            File.WriteAllText(Path.Combine(viewerDirectory, "darling.json"), "{}");

            var fromEnvironment = Path.Combine(root.FullName, "elsewhere.json");
            File.WriteAllText(fromEnvironment, "{}");
            Environment.SetEnvironmentVariable("DARLING_CONFIG", fromEnvironment);

            var location = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerDirectory);

            Assert.Equal(ViewerConfigSource.EnvironmentVariable, location.Source);
            Assert.Equal(fromEnvironment, location.Path);
            Assert.True(location.Exists);

            var text = string.Join(Environment.NewLine, ViewerConfigDiagnostics.DescribeConfigLocation(location));
            Assert.Contains("DARLING_CONFIG", text, StringComparison.Ordinal);
            Assert.Contains(fromEnvironment, text, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", saved);
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void ResolveConfigLocation_ProbedLocations_DistinguishBesideTheViewerFromTheServiceRoot()
    {
        var saved = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        Environment.SetEnvironmentVariable("DARLING_CONFIG", null);
        var root = Directory.CreateTempSubdirectory("darling-viewer-probesource-");
        try
        {
            var viewerDirectory = Path.Combine(root.FullName, "viewer");
            Directory.CreateDirectory(viewerDirectory);

            /* Nothing anywhere: report the viewer's own directory, and say it is not there. */
            var nothing = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerDirectory, installedServiceFolder: () => null);
            Assert.Equal(ViewerConfigSource.BesideViewer, nothing.Source);
            Assert.False(nothing.Exists);

            /* The shipped-zip layout: viewer\ under the service root, darling.json beside the SERVICE. */
            var atServiceRoot = Path.Combine(root.FullName, "darling.json");
            File.WriteAllText(atServiceRoot, "{}");
            var fromServiceRoot = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerDirectory, installedServiceFolder: () => null);
            Assert.Equal(ViewerConfigSource.ServiceRoot, fromServiceRoot.Source);
            Assert.Equal(atServiceRoot, fromServiceRoot.Path);
            Assert.True(fromServiceRoot.Exists);

            /* Beside the viewer still wins when both exist. */
            var besideViewer = Path.Combine(viewerDirectory, "darling.json");
            File.WriteAllText(besideViewer, "{}");
            var fromBesideViewer = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerDirectory, installedServiceFolder: () => null);
            Assert.Equal(ViewerConfigSource.BesideViewer, fromBesideViewer.Source);
            Assert.Equal(besideViewer, fromBesideViewer.Path);
            Assert.True(fromBesideViewer.Exists);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", saved);
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The diagnostics and the load must never describe different files. <c>ResolveConfigPath</c> is a
    /// projection of <c>ResolveConfigLocation</c> rather than a second copy of the rules, and this pins it —
    /// a re-implementation that drifted would make the whole feature actively misleading.
    /// </summary>
    [Fact]
    public void ResolveConfigLocation_AndResolveConfigPath_CannotDisagree()
    {
        var saved = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        Environment.SetEnvironmentVariable("DARLING_CONFIG", null);
        var root = Directory.CreateTempSubdirectory("darling-viewer-agree-");
        try
        {
            var viewerDirectory = Path.Combine(root.FullName, "viewer");
            Directory.CreateDirectory(viewerDirectory);

            Assert.Equal(
                ViewerSettings.ResolveConfigPath(baseDirectory: viewerDirectory, installedServiceFolder: () => null),
                ViewerSettings.ResolveConfigLocation(baseDirectory: viewerDirectory, installedServiceFolder: () => null).Path);

            File.WriteAllText(Path.Combine(root.FullName, "darling.json"), "{}");
            Assert.Equal(
                ViewerSettings.ResolveConfigPath(baseDirectory: viewerDirectory, installedServiceFolder: () => null),
                ViewerSettings.ResolveConfigLocation(baseDirectory: viewerDirectory, installedServiceFolder: () => null).Path);

            Environment.SetEnvironmentVariable("DARLING_CONFIG", @"C:\from\env\darling.json");
            Assert.Equal(
                ViewerSettings.ResolveConfigPath(baseDirectory: viewerDirectory, installedServiceFolder: () => null),
                ViewerSettings.ResolveConfigLocation(baseDirectory: viewerDirectory, installedServiceFolder: () => null).Path);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", saved);
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void DescribeConfigLocation_RelativeConfiguredPath_ReportsTheAbsolutePathItActuallyOpens()
    {
        /* A relative DARLING_CONFIG or command-line path is precisely the case where the operator's idea of
           the path and the viewer's are not the same string, so both are reported. */
        var location = ViewerSettings.ResolveConfigLocation("conf/darling.json");
        var lines = ViewerConfigDiagnostics.DescribeConfigLocation(location);

        Assert.Equal(Path.GetFullPath("conf/darling.json"), ValueFor(lines, "darling.json path"));
        Assert.Equal("conf/darling.json", ValueFor(lines, "as configured"));
        Assert.Contains("command-line", ValueFor(lines, "darling.json source"), StringComparison.Ordinal);
        Assert.Equal("NO", ValueFor(lines, "darling.json exists"));
    }

    // ── The seam: the UI failure path cannot ship without the diagnostics ─────────────────

    /// <summary>
    /// Every connection/config failure in the viewer shell renders through <c>ShowConnectionFailure</c>,
    /// which attaches the diagnostics block; the raw <c>ShowMessage</c> renderer has exactly one caller,
    /// which is that helper. The compiler already forces a details argument at every call site, but it
    /// cannot stop a new branch from passing null — so the invariant that matters is structural: nothing in
    /// the shell calls the renderer directly. Source-parsed, like the repo's other seam pins, because the
    /// alternative is standing up a WPF window on an STA thread to assert a text property.
    /// </summary>
    [Fact]
    public void EveryFailureSurfaceInTheShellGoesThroughTheDiagnosticsCarryingHelper()
    {
        /* MainWindow is a PARTIAL class across several files, and partial members are visible from every
           part - so a failure branch added in ANY part file could call the raw renderer directly. Scan the
           whole partial set, derived by glob so a new part file joins the pin the day it exists (review on
           #1966 caught the single-file version guarding less than it documented). */
        var partFiles = Directory.GetFiles(ViewerDirectory(), "MainWindow*.cs");
        Assert.True(partFiles.Length >= 5, $"expected the MainWindow partial family, found {partFiles.Length} file(s)");
        var source = string.Concat(partFiles.Select(File.ReadAllText));

        /* The definition reads "private void ShowMessage(", so exclude a preceding "void ". */
        var directCalls = Regex.Matches(source, @"(?<!void )ShowMessage\(").Count;
        Assert.True(
            directCalls == 1,
            $"the MainWindow partial family calls ShowMessage directly {directCalls} time(s); exactly one is expected " +
            "(inside ShowConnectionFailure). A failure surface that calls ShowMessage itself shows the " +
            "operator a message with no configuration context — route it through ShowConnectionFailure, or " +
            "update this pin deliberately if a message genuinely has none.");

        var helperUses = Regex.Matches(source, @"ShowConnectionFailure\(").Count;
        Assert.True(
            helperUses >= 6,
            $"Expected the diagnostics-carrying helper at the config-read, config-missing, schema-gate, " +
            $"store-unreachable, connect-failed and store-read failure surfaces (plus its definition); found {helperUses}.");
    }

    // ── The certificate anchor (#1970) ────────────────────────────────────────────────────

    /// <summary>The <c>Root Certificate</c> value out of a connection string, as Npgsql reads it.</summary>
    private static string? CertificateIn(string connectionString) =>
        new NpgsqlConnectionStringBuilder(connectionString).RootCertificate;

    [Fact]
    public void Resolve_AbsolutePath_IsTheAnswerRegardlessOfTheAnchor()
    {
        Assert.Equal(
            @"C:\Pinned\server.crt",
            ViewerCertificateAnchor.Resolve(@"C:\Pinned\server.crt", @"C:\Darling"));
    }

    [Fact]
    public void Resolve_BareNameAndRelativeSubpath_BothAnchorToTheConfigDirectory()
    {
        Assert.Equal(
            @"C:\Darling\server.crt",
            ViewerCertificateAnchor.Resolve("server.crt", @"C:\Darling"));
        Assert.Equal(
            @"C:\Darling\certs\server.crt",
            ViewerCertificateAnchor.Resolve(@"certs\server.crt", @"C:\Darling"));
    }

    /// <summary>
    /// No anchor means no darling.json directory to measure from, so the answer is the process working
    /// directory — what Npgsql itself would do. The diagnostics need this branch to stay honest rather than
    /// invent a path; nothing on the connection path takes it (a rewrite without an anchor is skipped).
    /// </summary>
    [Fact]
    public void Resolve_WithoutAnAnchor_FallsBackToTheWorkingDirectory_AndNullValueYieldsNull()
    {
        Assert.Equal(Path.GetFullPath("server.crt"), ViewerCertificateAnchor.Resolve("server.crt", null));
        Assert.Null(ViewerCertificateAnchor.Resolve(null, @"C:\Darling"));
        Assert.Null(ViewerCertificateAnchor.Resolve("   ", @"C:\Darling"));
    }

    /// <summary>
    /// The rewrite never ADDS <c>Root Certificate</c>: a connection that was not pinning must not be made to
    /// pin by a path-handling fix. Managed loopback is the shipped default and carries no certificate at all.
    /// </summary>
    [Fact]
    public void Anchor_NoRootCertificate_ReturnsTheStringUntouched()
    {
        const string managedLoopback = "Host=127.0.0.1;Port=5641;Username=admin;Database=darling";

        var anchored = ViewerCertificateAnchor.Anchor(managedLoopback, @"C:\Darling");

        Assert.Equal(managedLoopback, anchored);
        Assert.True(string.IsNullOrEmpty(CertificateIn(anchored)));
    }

    [Fact]
    public void Anchor_AbsoluteRootCertificate_ReturnsTheStringUntouched()
    {
        var absolute = ByoConnectionString(@"C:\Pinned\server.crt");

        Assert.Equal(absolute, ViewerCertificateAnchor.Anchor(absolute, @"C:\Darling"));
    }

    /// <summary>
    /// The rewrite itself, and the two things it must not disturb: the verify-full mode that makes the
    /// certificate load-bearing at all, and the credential. Everything else in the string is the operator's.
    /// </summary>
    [Fact]
    public void Anchor_RelativeRootCertificate_RewritesOnlyTheCertificate()
    {
        var anchored = ViewerCertificateAnchor.Anchor(ByoConnectionString(), @"C:\Darling");

        Assert.Equal(@"C:\Darling\server.crt", CertificateIn(anchored));

        var builder = new NpgsqlConnectionStringBuilder(anchored);
        Assert.Equal(SslMode.VerifyFull, builder.SslMode);
        Assert.Equal("store.example.com", builder.Host);
        Assert.Equal(5641, builder.Port);
        Assert.Equal("viewer", builder.Username);
        Assert.Equal("darling", builder.Database);
        Assert.Equal("collect,config,public", builder.SearchPath);
        Assert.Equal(LivePassword, builder.Password);
    }

    [Fact]
    public void Anchor_WithoutAnAnchor_OrOnAnUnparseableString_ReturnsItUntouched()
    {
        Assert.Equal(ByoConnectionString(), ViewerCertificateAnchor.Anchor(ByoConnectionString(), null));

        /* Npgsql owns rejecting a malformed string, with its own message, at connect time — the anchor must
           not turn that into a startup exception. */
        const string malformed = "Host=store.example.com;Port=NOT-A-NUMBER";
        Assert.Equal(malformed, ViewerCertificateAnchor.Anchor(malformed, @"C:\Darling"));
    }

    /// <summary>
    /// The whole point, end to end: the certificate the viewer pins is the one beside the darling.json it
    /// READ, not the one beside whatever directory the process happened to start in. Before #1970 the string
    /// came back verbatim and Npgsql resolved <c>server.crt</c> against the test host's working directory —
    /// so this assertion is exactly the behavior change.
    /// </summary>
    [Fact]
    public void TryLoad_AnchorsARelativeCertificateToTheDirectoryOfTheFileItActuallyRead()
    {
        var root = Directory.CreateTempSubdirectory("darling-viewer-tryload-anchor-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            File.WriteAllText(
                configPath,
                $$"""{ "postgres": { "managed": false, "connectionString": {{JsonSerializer.Serialize(ByoConnectionString())}} } }""");

            var settings = Assert.IsType<ViewerSettings>(ViewerSettings.TryLoad(configPath));

            Assert.Equal(Path.Combine(root.FullName, "server.crt"), CertificateIn(settings.ConnectionString));
            Assert.NotEqual(Path.GetFullPath("server.crt"), CertificateIn(settings.ConnectionString));

            /* And the as-written value survives for the diagnostics to show alongside it. */
            Assert.Equal("server.crt", CertificateIn(settings.ConfiguredConnectionString));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The seam this refactor exists to close: the connection path and the diagnostics must never name
    /// different certificate files. They are given the same two inputs and pinned to the same answer — a
    /// functional pin, so it goes red for ANY divergence, not only for the ones a source scan would spot.
    /// </summary>
    [Fact]
    public void TheDiagnosticsCertificateLine_IsThePathTheConnectionStringActuallyCarries()
    {
        var root = Directory.CreateTempSubdirectory("darling-viewer-anchor-agree-");
        try
        {
            foreach (var configured in new[] { "server.crt", @"certs\server.crt", @"C:\Pinned\server.crt" })
            {
                var settings = ViewerSettings.Parse(
                    $$"""{ "postgres": { "connectionString": {{JsonSerializer.Serialize(ByoConnectionString(configured))}} } }""",
                    root.FullName);

                var reported = ValueFor(
                    ViewerConfigDiagnostics.DescribeConnection(
                        settings.ConfiguredConnectionString, settings.Managed, root.FullName),
                    "resolves to");

                Assert.Equal(CertificateIn(settings.ConnectionString), reported);
            }
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// Structural half of the same guarantee: neither consumer may grow its own copy of the rule. The
    /// diagnostics must ASK the resolver (and must not resolve a path itself), and the settings must route
    /// the connection string through it. Source-parsed, like the shell pin above.
    /// </summary>
    [Fact]
    public void BothConsumersOfTheCertificatePath_RouteThroughTheOneResolver()
    {
        var diagnostics = File.ReadAllText(Path.Combine(ViewerDirectory(), "ViewerConfigDiagnostics.cs"));
        var settings = File.ReadAllText(Path.Combine(ViewerDirectory(), "ViewerSettings.cs"));

        Assert.Contains("ViewerCertificateAnchor.Resolve(", diagnostics, StringComparison.Ordinal);
        Assert.Contains("ViewerCertificateAnchor.Anchor(", settings, StringComparison.Ordinal);

        Assert.DoesNotContain(
            "Path.GetFullPath(",
            diagnostics,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// And the shell has to HAND the diagnostics that anchor. A call that omits it silently falls back to the
    /// process working directory — the block would then print a path the connection is no longer using, which
    /// is the failure mode #1954 exists to prevent, reintroduced by an omitted argument. Scanned across the
    /// MainWindow partial family by glob, like the shell pin above, so a new part file joins the day it exists.
    /// </summary>
    [Fact]
    public void EveryDiagnosticsCallInTheShellCarriesTheConfigDirectoryAsItsAnchor()
    {
        var partFiles = Directory.GetFiles(ViewerDirectory(), "MainWindow*.cs");
        Assert.True(partFiles.Length >= 5, $"expected the MainWindow partial family, found {partFiles.Length} file(s)");
        var source = string.Concat(partFiles.Select(File.ReadAllText));

        var diagnosticsCalls = Regex.Matches(
            source, @"ViewerConfigDiagnostics\.(BuildDetails|DescribeConnection)\(").Count;
        Assert.True(diagnosticsCalls >= 3, $"expected the pre-load, log and overlay diagnostics calls; found {diagnosticsCalls}");

        var anchorArguments = Regex.Matches(source, @"configLocation\.Directory").Count;
        Assert.True(
            anchorArguments >= diagnosticsCalls,
            $"the MainWindow partial family makes {diagnosticsCalls} diagnostics call(s) but passes the config " +
            $"directory {anchorArguments} time(s). A call without it reports the certificate against the process " +
            "working directory while the connection uses darling.json's folder (#1970).");
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────

    /// <summary>The value on the single line whose label is <paramref name="label"/> (labels are padded, so
    /// this trims rather than splitting on a fixed column).</summary>
    private static string ValueFor(IReadOnlyList<string> lines, string label)
    {
        var prefix = label + ":";
        var line = Assert.Single(lines, l => l.TrimStart().StartsWith(prefix, StringComparison.Ordinal));
        return line.Trim()[prefix.Length..].Trim();
    }

    /// <summary>The Viewer project directory, resolved from this test file's compile-time path.</summary>
    private static string ViewerDirectory([CallerFilePath] string thisFile = "")
    {
        var testDirectory = Path.GetDirectoryName(thisFile)!;
        return Path.GetFullPath(Path.Combine(testDirectory, "..", "PerformanceMonitor.Darling.Viewer"));
    }
}
