/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>darling-managed.conf</c> (#4215 Release A, part 1): the render, the body hash, hand-edit detection, the
/// preload merge, the include-line heal and the atomic replace. Ungated — every test here is pure logic or
/// plain file I/O in a throwaway temp directory, never a real PostgreSQL binary (that is
/// <c>DarlingManagedPostgresTests</c>, gated on <c>DARLING_TEST_PGRUNTIME</c>, and the live tests this PR adds
/// there for the <c>postgres -C</c> validation path).
/// </summary>
public sealed class ManagedConfFileTests
{
    private static ManagedConfFile.RenderInputs SampleInputs(
        bool ramAuthoritative = true,
        bool dataVolumeAuthoritative = true,
        string? effectivePreloadList = null,
        int port = 55432)
        => new(
            FormulaVersion: ManagedConfFile.CurrentFormulaVersion,
            Platform: "Windows",
            RamBytes: 17_179_869_184L, // 16 GiB
            RamAuthoritative: ramAuthoritative,
            ProcessorCount: 8,
            HypertableCount: 42,
            PostgresMajor: 18,
            DataVolumeFreeBytes: 100_000_000_000L,
            DataVolumeTotalBytes: 500_000_000_000L,
            DataVolumeAuthoritative: dataVolumeAuthoritative,
            Port: port,
            EffectivePreloadList: effectivePreloadList);

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    [Fact]
    public void Render_SameInputs_IsByteIdentical()
    {
        var inputs = SampleInputs();
        var first = ManagedConfFile.Render(inputs);
        var second = ManagedConfFile.Render(inputs);
        Assert.Equal(first, second);
    }

    [Fact]
    public void RenderHeader_CarriesEveryDesignInput_AndNoTimestamp()
    {
        var text = ManagedConfFile.Render(SampleInputs());
        Assert.Contains("# formula-version=1\n", text, StringComparison.Ordinal);
        Assert.Contains("# platform=Windows\n", text, StringComparison.Ordinal);
        Assert.Contains("# ram-bytes=17179869184\n", text, StringComparison.Ordinal);
        Assert.Contains("# cpus=8\n", text, StringComparison.Ordinal);
        Assert.Contains("# hypertables=42\n", text, StringComparison.Ordinal);
        Assert.Contains("# postgres-major=18\n", text, StringComparison.Ordinal);
        Assert.Contains("# data-volume-free-gib=93\n", text, StringComparison.Ordinal);
        Assert.Contains("# data-volume-total-gib=466\n", text, StringComparison.Ordinal);

        /* No timestamp anywhere: two renders of the same inputs must be byte-identical, which a wall-clock
           field would break on every single start. */
        Assert.DoesNotContain(DateTime.UtcNow.Year.ToString(System.Globalization.CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
    }

    [Fact]
    public void Render_BodyHashLine_MatchesComputeBodyHashOfTheActualBody()
    {
        var rendered = ManagedConfFile.Render(SampleInputs());
        var parsed = ManagedConfFile.ParseExisting(rendered);

        Assert.True(parsed.IsWellFormed);
        Assert.Equal(ManagedConfFile.ComputeBodyHash(parsed.Body), parsed.DeclaredHash);

        /* The body itself is exactly what RenderBody produces — the header carries nothing the hash needs to
           cover. */
        var expectedBody = ManagedConfFile.RenderBody(SampleInputs());
        Assert.Equal(expectedBody, parsed.Body);
    }

    [Fact]
    public void ComputeBodyHash_IsDeterministic_AndSensitiveToASingleCharacter()
    {
        var hashA = ManagedConfFile.ComputeBodyHash("shared_buffers = '2048MB'\n");
        var hashB = ManagedConfFile.ComputeBodyHash("shared_buffers = '2048MB'\n");
        var hashC = ManagedConfFile.ComputeBodyHash("shared_buffers = '2049MB'\n");

        Assert.Equal(hashA, hashB);
        Assert.NotEqual(hashA, hashC);
        Assert.Equal(64, hashA.Length); // hex SHA-256
        Assert.Equal(hashA, hashA.ToLowerInvariant()); // lower-case, so a plain string compare is enough
    }

    [Fact]
    public void IsHandEdited_FreshRender_IsFalse()
    {
        var rendered = ManagedConfFile.Render(SampleInputs());
        Assert.False(ManagedConfFile.IsHandEdited(rendered));
    }

    [Fact]
    public void IsHandEdited_ChangedBodyValue_IsTrue()
    {
        var rendered = ManagedConfFile.Render(SampleInputs());
        var parsed = ManagedConfFile.ParseExisting(rendered);
        var header = rendered[..^parsed.Body.Length];
        var editedBody = parsed.Body.Replace("= '", "= 'EDITED-", StringComparison.Ordinal);

        Assert.True(ManagedConfFile.IsHandEdited(header + editedBody));
    }

    [Fact]
    public void IsHandEdited_NoBodyHashLine_IsTrue()
    {
        Assert.True(ManagedConfFile.IsHandEdited("# some other tool wrote this\nshared_buffers = '128MB'\n"));
    }

    [Fact]
    public void DiffBodyKeys_ChangedValue_NamesTheKeyAndBothValues()
    {
        var diffs = ManagedConfFile.DiffBodyKeys(
            "shared_buffers = '2048MB'\nwork_mem = '16MB'\n",
            "shared_buffers = '4096MB'\nwork_mem = '16MB'\n");

        var diff = Assert.Single(diffs);
        Assert.Equal("shared_buffers", diff.Key);
        Assert.Equal("2048MB", diff.FileValue);
        Assert.Equal("4096MB", diff.RenderedValue);
    }

    [Fact]
    public void RenderBody_PreloadMerge_KeepsAnOperatorLibraryInForce()
    {
        /* An operator's own library, already in force when this render runs (review M5): the merge must not
           shrink the list down to a hard-coded literal. */
        var body = ManagedConfFile.RenderBody(SampleInputs(effectivePreloadList: "auto_explain"));

        var diffs = ManagedConfFile.DiffBodyKeys(string.Empty, body);
        var preload = Assert.Single(diffs, d => d.Key == "shared_preload_libraries");

        Assert.Contains("auto_explain", preload.RenderedValue, StringComparison.Ordinal);
        Assert.Contains("pg_stat_statements", preload.RenderedValue, StringComparison.Ordinal);
    }

    [Fact]
    public void RenderBody_NonAuthoritativeRam_SkipsHardwareSizingBlock_ButStillSetsSharedBuffers()
    {
        /* Mirrors v8's own discipline: without an authoritative RAM reading, do not mint a fresh hardware
           derivation — but v3/v5's values (from the always-available reading) still land in the body. */
        var body = ManagedConfFile.RenderBody(SampleInputs(ramAuthoritative: false));
        var diffs = ManagedConfFile.DiffBodyKeys(string.Empty, body);

        Assert.Contains(diffs, d => d.Key == "shared_buffers");
    }

    [Fact]
    public void HasManagedInclude_NoIncludeLine_ReturnsFalse()
    {
        Assert.False(ManagedConfFile.HasManagedInclude("port = 5432\n"));
    }

    [Theory]
    [InlineData("include 'darling-managed.conf'")]
    [InlineData("include darling-managed.conf")]
    public void HasManagedInclude_AnyFormPostgresAccepts_ReturnsTrue(string includeLine)
    {
        Assert.True(ManagedConfFile.HasManagedInclude($"port = 5432\n{includeLine}\n"));
    }

    [Fact]
    public void TryReplaceAtomic_NoExistingFile_WritesContentAndLeavesNoTempFile()
    {
        var dir = Directory.CreateTempSubdirectory("darling-4215-replace-");
        try
        {
            var destPath = Path.Combine(dir.FullName, ManagedConfFile.FileName);
            var succeeded = ManagedConfFile.TryReplaceAtomic(
                destPath, "shared_buffers = '2048MB'\n", ManagedConfFile.DefaultMaxReplaceAttempts, ManagedConfFile.DefaultReplaceRetryDelay, out var error);

            Assert.True(succeeded);
            Assert.Null(error);
            Assert.Equal("shared_buffers = '2048MB'\n", File.ReadAllText(destPath));
            Assert.False(File.Exists(Path.Combine(dir.FullName, ManagedConfFile.TempFileName)));
        }
        finally
        {
            Directory.Delete(dir.FullName, recursive: true);
        }
    }

    [Fact]
    public async Task TryReplaceAtomic_RetriesWhileAnotherHandleHoldsTheFile()
    {
        var dir = Directory.CreateTempSubdirectory("darling-4215-replace-locked-");
        try
        {
            var destPath = Path.Combine(dir.FullName, ManagedConfFile.FileName);
            File.WriteAllText(destPath, "old\n");

            var blocker = new FileStream(destPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            var releaseThread = new Thread(() =>
            {
                Thread.Sleep(200);
                blocker.Dispose();
            })
            { IsBackground = true };
            releaseThread.Start();

            var succeeded = ManagedConfFile.TryReplaceAtomic(
                destPath, "new\n", maxAttempts: 200, retryDelay: TimeSpan.FromMilliseconds(50), out var error);

            releaseThread.Join();

            Assert.True(succeeded, error?.Message);
            Assert.Equal("new\n", File.ReadAllText(destPath));
        }
        finally
        {
            Directory.Delete(dir.FullName, recursive: true);
        }
    }

    [Fact]
    public void WriteManagedConfFile_FreshDirectory_WritesFileAndAppendsIncludeLine()
    {
        var dir = Directory.CreateTempSubdirectory("darling-4215-write-");
        try
        {
            var confPath = Path.Combine(dir.FullName, "postgresql.conf");
            File.WriteAllText(confPath, "# base conf\n");
            var logger = new CapturingTestLogger();
            var cluster = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, Port = 55432, DataDirectory = dir.FullName }, logger);

            var result = cluster.WriteManagedConfFile(dir.FullName, postgresMajor: 18);

            Assert.True(result.Written);
            Assert.False(result.HandEdited);
            var managedPath = Path.Combine(dir.FullName, ManagedConfFile.FileName);
            Assert.Equal(result.RenderedText, File.ReadAllText(managedPath));
            Assert.Equal(1, CountOccurrences(File.ReadAllText(confPath), "include 'darling-managed.conf'"));

            /* A second call with nothing changed must be a no-op write, and must not touch the include line
               again — "appended once, never duplicated". */
            var second = cluster.WriteManagedConfFile(dir.FullName, postgresMajor: 18);
            Assert.False(second.Written);
            Assert.Equal(1, CountOccurrences(File.ReadAllText(confPath), "include 'darling-managed.conf'"));
        }
        finally
        {
            Directory.Delete(dir.FullName, recursive: true);
        }
    }

    [Fact]
    public void WriteManagedConfFile_HandEditedFile_IsNeverOverwritten()
    {
        var dir = Directory.CreateTempSubdirectory("darling-4215-handedit-");
        try
        {
            var confPath = Path.Combine(dir.FullName, "postgresql.conf");
            File.WriteAllText(confPath, "# base conf\n");
            var logger = new CapturingTestLogger();
            var cluster = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, Port = 55432, DataDirectory = dir.FullName }, logger);

            var first = cluster.WriteManagedConfFile(dir.FullName, postgresMajor: 18);
            Assert.True(first.Written);

            var managedPath = Path.Combine(dir.FullName, ManagedConfFile.FileName);
            var parsed = ManagedConfFile.ParseExisting(first.RenderedText);
            var header = first.RenderedText[..^parsed.Body.Length];
            var handEditedText = header + parsed.Body.Replace("= '", "= 'HANDEDITED-", StringComparison.Ordinal);
            File.WriteAllText(managedPath, handEditedText);

            var second = cluster.WriteManagedConfFile(dir.FullName, postgresMajor: 18);

            Assert.False(second.Written);
            Assert.True(second.HandEdited);
            Assert.NotEmpty(second.ChangedKeys);
            Assert.Equal(handEditedText, File.ReadAllText(managedPath));
            Assert.True(logger.CountAtLevel(LogLevel.Warning) > 0, logger.Joined);
        }
        finally
        {
            Directory.Delete(dir.FullName, recursive: true);
        }
    }

    [Fact]
    public void EnsureManagedIncludeLine_MissingLine_AppendsOnceAndWarnsOnlyOnce()
    {
        var dir = Directory.CreateTempSubdirectory("darling-4215-include-");
        try
        {
            var confPath = Path.Combine(dir.FullName, "postgresql.conf");
            File.WriteAllText(confPath, "port = 5432\n");
            var logger = new CapturingTestLogger();
            var cluster = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, Port = 55432, DataDirectory = dir.FullName }, logger);

            cluster.EnsureManagedIncludeLine(dir.FullName);
            Assert.Equal(1, CountOccurrences(File.ReadAllText(confPath), "include 'darling-managed.conf'"));
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));

            cluster.EnsureManagedIncludeLine(dir.FullName);
            Assert.Equal(1, CountOccurrences(File.ReadAllText(confPath), "include 'darling-managed.conf'"));
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            Directory.Delete(dir.FullName, recursive: true);
        }
    }
}
