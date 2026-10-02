/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The bundled-runtime version pin (#1706). The bundle is supposed to ship PostgreSQL 18, and for a
/// while everyone believed it did — but the artifact deployed to the field booted 17.10, because
/// <c>fetch-pg-runtime.ps1</c> had its pins bumped to 18.4 AFTER the zip beside it was assembled from
/// the 17.10 download, and nothing anywhere compared the two. A stale artifact is indistinguishable
/// from a fresh one by inspection: same name, same layout, same size to the nearest tens of MB. The
/// only honest witness is the binary itself, so these tests ask it.
///
/// Two layers, because the drift can enter from either side:
///   1. UNGATED (runs on every build, no artifact required): the script's own pins must agree with each
///      other. A <c>$pgVersion</c> bumped without its <c>$pgUrl</c> — or a TimescaleDB URL still aimed at
///      the previous major — is caught here, on the pull request that introduces it.
///   2. GATED on DARLING_TEST_PGRUNTIME (CI's darling-pg job and nightly both set it, pointed at the
///      extracted bundle): the ASSEMBLED runtime's real version, as reported by pg_ctl.exe itself, must
///      equal what the script pins. This is the assertion that would have failed loudly on the stale
///      artifact instead of letting it ship and be discovered in the field.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")], and NOT for the reason a sweep might assume.
   This class never reads DARLING_TEST_PG at all — it reads DARLING_TEST_PGRUNTIME, which merely shares that
   prefix, and it stands up its OWN throwaway cluster from the bundled runtime. A substring search for
   "DARLING_TEST_PG" matches it anyway (that is how #1776's original sweep came to list it), so this note is here to
   stop the next one serializing a class that touches no shared store. */
public sealed class DarlingPgRuntimeVersionPinTests
{
    /// <summary>
    /// The PostgreSQL major the bundle is intended to ship. Bumping the runtime to a new major is a
    /// deliberate act with field consequences (an existing store's data directory is NOT compatible
    /// across majors), so it changes here, in a diff a reviewer sees, rather than drifting in silently.
    /// </summary>
    private const int ExpectedPostgresMajor = 18;

    /* Internal, not private: DarlingLiveStoreExtensionParityTests (#1787) reads the same pins, so both
       guards can never disagree on what "the pinned version" means. */
    internal static string FetchScriptText =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "fetch-pg-runtime.ps1"));

    internal static string PinnedValue(string script, string variableName)
    {
        var match = Regex.Match(script, @"^\$" + Regex.Escape(variableName) + @"\s*=\s*'([^']+)'",
            RegexOptions.Multiline);
        Assert.True(match.Success, $"fetch-pg-runtime.ps1 no longer defines ${variableName} as a single-quoted literal.");
        return match.Groups[1].Value;
    }

    [Fact]
    public void FetchScript_PinsAreInternallyConsistent()
    {
        var script = FetchScriptText;

        var pgVersion = PinnedValue(script, "pgVersion");
        var pgUrl = PinnedValue(script, "pgUrl");
        var tsVersion = PinnedValue(script, "tsVersion");
        var tsUrl = PinnedValue(script, "tsUrl");

        var pgMajor = int.Parse(pgVersion.Split('.')[0], System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(ExpectedPostgresMajor, pgMajor);

        /* The URL is what actually gets downloaded; $pgVersion only names the cache file. They drifted
           apart once already, which is how a "18.4" script fed a 17.10 bundle. */
        Assert.Contains($"postgresql-{pgVersion}-", pgUrl, StringComparison.Ordinal);

        /* TimescaleDB ships a SEPARATE artifact per PostgreSQL major; the wrong one produces a runtime
           whose extension will not load at all (#1705's class of drift). */
        Assert.Contains($"postgresql-{ExpectedPostgresMajor}-windows-amd64", tsUrl, StringComparison.Ordinal);
        Assert.Contains($"/{tsVersion}/", tsUrl, StringComparison.Ordinal);

        /* #3908: the carried builds. Each is an older release's PG18 build, never the bundle's own version, and
           the list is never empty while any field store may be on a version other than the bundle's. */
        var carried = CarriedTimescaleBuilds(script);
        Assert.NotEmpty(carried);
        Assert.Contains(carried, c => c.Version == "2.28.1");
        foreach (var (version, url, sha256) in carried)
        {
            Assert.NotEqual(tsVersion, version);
            Assert.Contains($"/{version}/", url, StringComparison.Ordinal);
            Assert.Contains($"postgresql-{ExpectedPostgresMajor}-windows-amd64", url, StringComparison.Ordinal);
            Assert.Matches("^[0-9A-F]{64}$", sha256);
        }
    }

    [Fact]
    public void BundledRuntime_ReportsTheVersionTheScriptPins_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing " +
            "pgsql\\bin\\pg_ctl.exe; Darling\\tools\\fetch-pg-runtime.ps1 -KeepWork leaves one under " +
            "artifacts\\pg-runtime-work\\assemble\\pg-runtime) to verify the bundled runtime's version.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");

        var pgCtl = Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe");
        Assert.SkipUnless(File.Exists(pgCtl),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var script = FetchScriptText;
        var pinnedPgVersion = PinnedValue(script, "pgVersion");
        var pinnedTsVersion = PinnedValue(script, "tsVersion");

        /* --version, not a boot: this has to be cheap enough to run unconditionally wherever the runtime
           is extracted, and the executable's own answer is the thing that was wrong before. */
        var reported = RunForOutput(pgCtl, "--version");

        /* "pg_ctl (PostgreSQL) 18.4" */
        var match = Regex.Match(reported, @"(\d+)\.(\d+)");
        Assert.True(match.Success, $"Could not parse a version out of pg_ctl --version output: {reported}");
        var actualMajor = int.Parse(match.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);

        Assert.Equal(ExpectedPostgresMajor, actualMajor);
        Assert.Contains(pinnedPgVersion, reported, StringComparison.Ordinal);

        /* The TimescaleDB payload has to match its pin too. The extension files are copied in by name, so a
           stale assembly shows up as a different version's install script and control default. Older versions'
           LIBRARIES are expected since #3908 (the carried builds), so staleness is read from the script and the
           control file, not from which DLLs are present. */
        var extensionDirectory = Path.Combine(runtimeRoot!, "pgsql", "share", "extension");
        Assert.True(File.Exists(Path.Combine(extensionDirectory, $"timescaledb--{pinnedTsVersion}.sql")),
            $"The assembled runtime has no timescaledb--{pinnedTsVersion}.sql — it was built from a different " +
            "TimescaleDB archive than fetch-pg-runtime.ps1 currently pins.");
        Assert.Equal(pinnedTsVersion, DarlingStoreUpgrade.TryReadInstalledTimescaleVersion(Path.Combine(runtimeRoot!, "pgsql", "bin")));
        Assert.True(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "lib", $"timescaledb-{pinnedTsVersion}.dll")),
            $"The assembled runtime has no timescaledb-{pinnedTsVersion}.dll — same staleness, library side.");

        /* Every carried build is present as both libraries, and as nothing else: its install script would let a
           new database be created at the old, vulnerable version, and pg_upgrade does not need it (measured). */
        foreach (var (version, _, _) in CarriedTimescaleBuilds(script))
        {
            Assert.True(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "lib", $"timescaledb-{version}.dll")),
                $"The carried TimescaleDB {version} library is missing, so a store on {version} cannot be pg_upgraded (#3908).");
            Assert.True(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "lib", $"timescaledb-tsl-{version}.dll")),
                $"The carried TimescaleDB {version} TSL library is missing. pg_upgrade --check does not test it, so nothing else would notice.");
            Assert.False(File.Exists(Path.Combine(extensionDirectory, $"timescaledb--{version}.sql")),
                $"timescaledb--{version}.sql should not be carried: only the libraries are.");
        }
    }

    /// <summary>The <c>$tsCarried</c> entries of fetch-pg-runtime.ps1, one per line.</summary>
    internal static (string Version, string Url, string Sha256)[] CarriedTimescaleBuilds(string script)
        => Regex.Matches(script, @"@\{\s*Version\s*=\s*'(?<version>[^']+)';\s*Url\s*=\s*'(?<url>[^']+)';\s*Sha256\s*=\s*'(?<sha>[^']+)'\s*\}")
            .Select(m => (m.Groups["version"].Value, m.Groups["url"].Value, m.Groups["sha"].Value))
            .ToArray();

    /// <summary>
    /// #3906: an unstamped host is adopted only when its full PostgreSQL version equals the package's, and
    /// the two sides come from different places. The host side is the extracted <c>pg_ctl --version</c>
    /// line; the package side is the zip's <c>pg_ctl.exe</c> version resource, whose numeric block EDB
    /// encodes as 18.0.4 for 18.4. So the seam is checked on the real binary: both readings must come out
    /// as the pinned major.minor. If the resource stopped carrying the minor, every unstamped host would
    /// be compared on TimescaleDB alone again, and a minor-only security release would never reach it.
    /// </summary>
    [Fact]
    public void BundledRuntime_ZipResourceAndVersionLine_BothReadThePinnedMinor_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory to compare the zip's and the binary's version readings.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");

        var pgCtl = Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe");
        Assert.SkipUnless(File.Exists(pgCtl),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var pinned = Version.Parse(PinnedValue(FetchScriptText, "pgVersion"));

        /* A zip laid out the way the shipped one is, holding only the entry the package side reads. */
        var zip = Path.Combine(Path.GetTempPath(), $"pm-runtime-pin-{Guid.NewGuid():N}.zip");
        try
        {
            using (var archive = ZipFile.Open(zip, ZipArchiveMode.Create))
            {
                archive.CreateEntryFromFile(pgCtl, "pgsql/bin/pg_ctl.exe");
            }

            Assert.Equal(ExpectedPostgresMajor, DarlingStoreUpgrade.TryReadZipPostgresMajor(zip));
            Assert.Equal(pinned, DarlingStoreUpgrade.TryReadZipPostgresVersion(zip));
            Assert.Equal(pinned, DarlingStoreUpgrade.ParsePostgresVersion(RunForOutput(pgCtl, "--version")));
        }
        finally
        {
            File.Delete(zip);
        }
    }

    private static string RunForOutput(string fileName, string arguments)
    {
        using var process = Process.Start(new ProcessStartInfo(fileName, arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        });
        Assert.NotNull(process);

        var stdout = process!.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit(30_000);
        return string.Concat(stdout, stderr).Trim();
    }
}
