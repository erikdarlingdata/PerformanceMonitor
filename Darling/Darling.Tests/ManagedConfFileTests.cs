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
        /* An operator's own library, already in force when this render runs: the merge must not
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

    /// <summary>#4336: a snapshot value replaces the derived one.</summary>
    [Fact]
    public void RenderWithValues_SnapshotValue_ReplacesTheDerivedOne()
    {
        var inputs = SampleInputs();
        var derivedBody = ManagedConfFile.RenderBody(inputs);
        var derivedDiffs = ManagedConfFile.DiffBodyKeys(string.Empty, derivedBody);
        var derivedSharedBuffers = Assert.Single(derivedDiffs, d => d.Key == "shared_buffers").RenderedValue!;

        var snapshotValues = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var diff in derivedDiffs)
        {
            snapshotValues[diff.Key] = diff.RenderedValue!;
        }

        snapshotValues["shared_buffers"] = "9999MB";
        Assert.NotEqual(derivedSharedBuffers, snapshotValues["shared_buffers"]);

        var rendered = ManagedConfFile.RenderWithValues(inputs, snapshotValues);
        var renderedDiffs = ManagedConfFile.DiffBodyKeys(string.Empty, ManagedConfFile.ParseExisting(rendered).Body);

        Assert.Contains(renderedDiffs, d => d.Key == "shared_buffers" && d.RenderedValue == "9999MB");
    }

    /// <summary>#4336: <see cref="ManagedConfFile.IsHandEdited"/> is false on the
    /// result — the hash is recomputed over the snapshot-valued body, so Step A's own write reads as its own
    /// write, never as an edit.</summary>
    [Fact]
    public void RenderWithValues_ResultIsNotHandEdited()
    {
        var inputs = SampleInputs();
        var derivedBody = ManagedConfFile.RenderBody(inputs);
        var (_, values) = ExtractValues(derivedBody);

        var rendered = ManagedConfFile.RenderWithValues(inputs, values);

        Assert.False(ManagedConfFile.IsHandEdited(rendered));
    }

    /// <summary>#4336: an owned key missing from the map is ABSENT from the body — it
    /// stays at whatever default is already in force; Step A never invents a value the snapshot did not
    /// report.</summary>
    [Fact]
    public void RenderWithValues_OwnedKeyMissingFromMap_IsAbsentFromBody()
    {
        var inputs = SampleInputs();
        var derivedBody = ManagedConfFile.RenderBody(inputs);
        var (_, values) = ExtractValues(derivedBody);
        values.Remove("work_mem");

        var rendered = ManagedConfFile.RenderWithValues(inputs, values);
        var body = ManagedConfFile.ParseExisting(rendered).Body;

        Assert.DoesNotMatch(@"(?m)^work_mem\s*=", body);
        Assert.Contains("maintenance_work_mem", body, StringComparison.Ordinal);
    }

    /// <summary>#4336: a value containing a quote and a backslash
    /// round-trips exactly through <c>EscapeConfValue</c> and re-parsing.</summary>
    [Fact]
    public void RenderWithValues_QuoteAndBackslashValue_RoundTripsExactly()
    {
        var inputs = SampleInputs();
        var derivedBody = ManagedConfFile.RenderBody(inputs);
        var (_, values) = ExtractValues(derivedBody);
        const string trickyValue = @"C:\pgdata\it's ""quoted""";
        values["work_mem"] = trickyValue;

        var rendered = ManagedConfFile.RenderWithValues(inputs, values);
        var body = ManagedConfFile.ParseExisting(rendered).Body;
        var (_, roundTripped) = ExtractValues(body);

        Assert.Equal(trickyValue, roundTripped["work_mem"]);
    }

    /// <summary>#4215 fixes commit: an <c>extraKeys</c> entry is appended AFTER every owned key, from the
    /// same snapshot-value map, and the result still reads as this render's own write (not a hand edit).</summary>
    [Fact]
    public void RenderWithValues_ExtraKey_AppendedAfterOwnedKeys_ResultIsNotHandEdited()
    {
        var inputs = SampleInputs(dataVolumeAuthoritative: false); // v12 skipped: min_wal_size is not owned this render
        var derivedBody = ManagedConfFile.RenderBody(inputs);
        var (order, values) = ExtractValues(derivedBody);
        Assert.DoesNotContain("min_wal_size", order);

        values["min_wal_size"] = "512MB"; // the BEFORE snapshot's value for a key this render does not own
        var rendered = ManagedConfFile.RenderWithValues(inputs, values, extraKeys: new List<string> { "min_wal_size" });
        var parsed = ManagedConfFile.ParseExisting(rendered);
        var body = parsed.Body;
        var (renderedOrder, renderedValues) = ExtractValues(body);

        Assert.Equal("min_wal_size", renderedOrder[^1]); // after every owned key
        Assert.Equal("512MB", renderedValues["min_wal_size"]);
        Assert.False(ManagedConfFile.IsHandEdited(rendered));
    }

    private static (List<string> Order, Dictionary<string, string> Values) ExtractValues(string body)
    {
        var order = new List<string>();
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(body))
        {
            if (values.TryAdd(name, value))
            {
                order.Add(name);
            }
            else
            {
                values[name] = value;
            }
        }

        return (order, values);
    }

    /// <summary>
    /// Carries #4342's heal ("Heal the v8 sizing block on stores resized before #4225 (#4207)") into
    /// <c>darling-managed.conf</c>: unlike the v1-v15 postgresql.conf blocks #4342 heals, <see
    /// cref="ManagedConfFile.RenderBody"/> never reads or reuses an existing v8 block's text — every start
    /// re-derives <c>work_mem</c> straight from <see cref="DarlingManagedPostgres.DeriveMemorySettings"/> and
    /// the CURRENT inputs, with nothing on disk to go stale in the first place. So a store whose LAST
    /// postgresql.conf write happened before #4225 (or even before #4207, when work_mem was not re-derived at
    /// all) still gets today's formula the moment darling-managed.conf starts rendering it, with no separate
    /// heal step needed: the current value is the only value this method is capable of writing.
    /// </summary>
    [Fact]
    public void RenderBody_AuthoritativeRam_WorkMemIsAlwaysTheCurrentDerivation_NeverAStaleV8Value()
    {
        // 15.6 GiB: v8's quantization (round-to-nearest-GiB, #2845) rounds this UP to 16 GiB, which
        // DeriveMemorySettings turns into work_mem=32MB. The un-quantized raw byte count derives 31MB
        // instead — a real divergence the 16-64MB clamp does not mask, so this pins that the render takes
        // the CURRENT (quantized) derivation rather than a stale (pre-quantization/pre-#4225) one.
        const long ramBytes = 16_749_363_609L; // ~15.6 GiB
        var body = ManagedConfFile.RenderBody(SampleInputs(ramAuthoritative: true) with { RamBytes = ramBytes });
        var (_, values) = ReduceToLastOccurrenceForTest(body);

        var quantized = DarlingManagedPostgres.QuantizeRam(ramBytes);
        var expected = DarlingManagedPostgres.DeriveMemorySettings(quantized);
        var stale = DarlingManagedPostgres.DeriveMemorySettings(ramBytes); // un-quantized: the stale story

        Assert.True(values.TryGetValue("work_mem", out var workMem));
        Assert.NotEqual(stale.WorkMemMb, expected.WorkMemMb); // sanity: this RAM figure actually diverges
        Assert.Equal($"{expected.WorkMemMb}MB", workMem);
        Assert.NotEqual($"{stale.WorkMemMb}MB", workMem);
    }

    /// <summary>
    /// The other half of #4342's heal: a non-authoritative RAM reading must not carry a stale hardware-sizing
    /// value forward. Because <see cref="ManagedConfFile.RenderBody"/> renders fresh every time rather than
    /// healing a persisted block, "not carrying it forward" here means the v8 block is skipped outright (as
    /// #4342's own <c>ShouldAppendHardwareSizing</c> requires an authoritative reading before acting at all)
    /// and <c>work_mem</c> falls back to v3's always-available derivation — never a value minted from a
    /// reading this render could not trust.
    /// </summary>
    [Fact]
    public void RenderBody_NonAuthoritativeRam_WorkMemFallsBackToV3_NotAStaleHardwareValue()
    {
        var body = ManagedConfFile.RenderBody(SampleInputs(ramAuthoritative: false));
        var (_, values) = ReduceToLastOccurrenceForTest(body);

        var v3Expected = DarlingManagedPostgres.DeriveMemorySettings(17_179_869_184L); // SampleInputs' RamBytes
        Assert.True(values.TryGetValue("work_mem", out var workMem));
        Assert.Equal($"{v3Expected.WorkMemMb}MB", workMem);
    }

    private static (System.Collections.Generic.List<string> Order, System.Collections.Generic.Dictionary<string, string> Values) ReduceToLastOccurrenceForTest(string body)
    {
        var diffs = ManagedConfFile.DiffBodyKeys(string.Empty, body);
        var order = new System.Collections.Generic.List<string>();
        var values = new System.Collections.Generic.Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var diff in diffs)
        {
            if (diff.RenderedValue is not null)
            {
                order.Add(diff.Key);
                values[diff.Key] = diff.RenderedValue;
            }
        }

        return (order, values);
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
