/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Keeps <c>Lite\config\settings.sample.json</c> honest against the code that actually reads
/// settings.json, in both directions: every key a loader reads is documented, and every key the
/// sample documents is really read.
///
/// <para><b>Why this exists.</b> The file this replaces (#2418) shipped nowhere, seeded nothing and
/// was never read, so nothing could tell it had gone stale — and it had: four of its eight keys were
/// read by no code anywhere in the repo, including <c>"theme"</c>, which the loader has always spelled
/// <c>color_theme</c>. A stale reference is worse than none, because it is what someone finds when they
/// go looking for what settings.json can hold, and it teaches wrong keys with nothing to signal that it
/// is wrong. The only thing that makes a reference file worth keeping is a check that fails when it
/// drifts, so this is that check.</para>
///
/// <para><b>Derived from the shipped source, not a copy.</b> The key list is regexed out of the real
/// <c>App.xaml.cs</c> and <c>Mcp\McpSettings.cs</c>, copied beside the test binary by the csproj. A
/// hand-maintained list here would be a third thing to keep in sync and would rot the same way the
/// sample did.</para>
///
/// <para><b>Scoping.</b> Each <c>TryGetProperty</c> is attributed to the most recent <c>*.json</c>
/// string literal above it, which is the shape every loader in these files has: open the file, then
/// read keys out of it. That means a loader added later for a DIFFERENT json file is excluded
/// automatically instead of needing an exemption — which matters, because App.xaml.cs already reads
/// servers.json, ignored_wait_types.json and collection_schedule.json elsewhere.</para>
/// </summary>
public sealed class SettingsSampleTests
{
    /// <summary>
    /// The two source files that read settings.json. Both are copied to <c>Fixtures\</c> by the csproj.
    /// If a THIRD loader appears, add it here — a reader this list does not know about is a key the
    /// sample can omit forever without failing anything.
    /// </summary>
    private static readonly string[] ReaderSources = { "App.xaml.cs", "McpSettings.cs" };

    /// <summary>
    /// Keys deliberately documented in the sample that no loader reads. EMPTY, and it should stay that
    /// way: a key with no reader is the exact defect this test exists to catch. The seam is here so that
    /// adding one has to be a deliberate, commented act rather than a silent omission — the old file's
    /// dead <c>"theme"</c> is discussed in a sample COMMENT, which carries the warning without claiming
    /// to be a live key.
    /// </summary>
    private static readonly HashSet<string> SampleOnlyKeys = new(StringComparer.Ordinal);

    /// <summary>
    /// Keys a loader reads that the sample deliberately leaves undocumented. EMPTY. If a key is ever
    /// too dangerous to publish, exempt it here WITH the reason — do not quietly drop it from the
    /// sample, because an undocumented knob is how #2418 started.
    /// </summary>
    private static readonly HashSet<string> LoaderOnlyKeys = new(StringComparer.Ordinal);

    [Fact]
    public void Sample_DocumentsEveryKeyTheLoadersRead()
    {
        var read = ReadKeysFromLoaders();
        var documented = SampleKeys();

        var missing = UndocumentedKeys(read, documented);

        Assert.True(
            missing.Count == 0,
            "settings.json keys read by Lite but absent from config\\settings.sample.json: "
                + string.Join(", ", missing.Select(k => $"{k} (in {read[k]})"))
                + ". Document them in the sample, or exempt them in LoaderOnlyKeys with a reason.");
    }

    [Fact]
    public void Sample_DocumentsNoKeyTheLoadersIgnore()
    {
        var read = ReadKeysFromLoaders();
        var documented = SampleKeys();

        var dead = documented
            .Where(k => !read.ContainsKey(k) && !SampleOnlyKeys.Contains(k))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            dead.Count == 0,
            "config\\settings.sample.json documents keys nothing reads: " + string.Join(", ", dead)
                + ". Either the loader lost them or the sample invented them; a reader who copies one "
                + "gets a setting that silently does nothing.");
    }

    [Fact]
    public void Sample_ParsesAsCommentedJson_WithNoDuplicateKeys()
    {
        using var doc = ParseSample();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var duplicates = doc.RootElement
            .EnumerateObject()
            .Where(p => !seen.Add(p.Name))
            .Select(p => p.Name)
            .ToList();

        Assert.True(
            duplicates.Count == 0,
            "config\\settings.sample.json repeats keys: " + string.Join(", ", duplicates)
                + ". A duplicate silently documents two different defaults for one setting.");
    }

    /// <summary>
    /// Guards the guard: the regex above is the whole test, so a restructured loader that stops matching
    /// would make BOTH symmetry checks pass on an empty set. Lite reads dozens of keys and always will;
    /// the floor is deliberately far below today's count so it pins "the extraction still works" rather
    /// than becoming a second thing to bump on every new setting.
    /// </summary>
    [Fact]
    public void KeyExtraction_StillFindsTheLoaders()
    {
        var read = ReadKeysFromLoaders();

        Assert.True(
            read.Count >= 50,
            $"Only {read.Count} settings.json keys were found across {string.Join(", ", ReaderSources)}. "
                + "The loaders were restructured and the extraction no longer sees them, which would make "
                + "the symmetry tests pass vacuously.");

        /* Anchors: one no-UI key (the reason the sample is kept at all) and one from each source file,
           so a fixture that silently stopped being copied fails here rather than passing on the other. */
        Assert.Contains("analysis_timeout_seconds", read.Keys);
        Assert.Contains("alerts_enabled", read.Keys);
        Assert.Contains("mcp_port", read.Keys);
    }

    /// <summary>
    /// Alternation, evaluated left to right in one pass, so the "which file is open" state and the key hits
    /// stay in source order.
    /// <para>The <c>TryGetProperty</c> half keys on a METHOD NAME, which is the constraint every refactor of
    /// the loaders runs into — see <c>TheReadHelpersName_IsWhatThisExtractionKeysOn</c> for why the #2444
    /// read helper is spelled the way it is.</para>
    /// </summary>
    private static readonly Regex Scanner = new(
        "\"(?<file>[A-Za-z0-9_.\\-]+\\.json)\"|TryGetProperty\\(\"(?<key>[^\"]+)\"",
        RegexOptions.CultureInvariant);

    /// <summary>
    /// The self-test #2444 owed this guard. The loaders' reads moved onto a helper
    /// (<see cref="SettingsReader"/>), and the reason they still work is that the helper's method carries the
    /// same NAME the extraction keys on — which is a fact about a string, held together by nothing the
    /// compiler checks. So it is checked here, against a source this test writes, in both shapes: the one the
    /// loaders used before #2444 and the one they use now.
    ///
    /// <para>And it does not stop at "the keys are found". A guard that extracts keys but no longer COMPARES
    /// them is the same vacuous green as one that extracts nothing, so this runs the real
    /// <see cref="UndocumentedKeys"/> over a sample that is deliberately missing one key and asserts that it
    /// is named. That is the property the two symmetry tests exist for, proved on a fixture rather than on
    /// the tree — where it can only ever be proved by breaking the tree.</para>
    /// </summary>
    [Fact]
    public void KeyExtraction_SeesBothReaderShapes_AndStillCatchesAnUndocumentedKey()
    {
        const string source = """
            var settings = SettingsFileGuard.Read(Path.Combine(configDirectory, "settings.json"));
            if (root.TryGetProperty("old_shape_key", out var a)) A = a.GetBoolean();
            if (read.TryGetProperty("new_shape_key", out var b)) B = b.Bool(B);
            if (read.TryGetProperty("undocumented_key", out var c)) C = c.Int(C);
            """;

        var found = new Dictionary<string, string>(StringComparer.Ordinal);
        ExtractKeys(source, "synthetic.cs", found);

        Assert.Contains("old_shape_key", found.Keys);
        Assert.Contains("new_shape_key", found.Keys);

        /* The guard still bites: one key is left out of the "sample", and it is the one named. */
        var documented = new HashSet<string>(StringComparer.Ordinal) { "old_shape_key", "new_shape_key" };
        Assert.Equal(new[] { "undocumented_key" }, UndocumentedKeys(found, documented));

        /* ...and stays silent when the sample really does document everything. */
        documented.Add("undocumented_key");
        Assert.Empty(UndocumentedKeys(found, documented));

        /* The trap itself, so the paragraph above is a measurement rather than a warning. This is the shape
           #2428 tried — a read helper that takes the key as an ordinary argument — and every key written that
           way is invisible here. That is what makes the helper's NAME load-bearing rather than incidental. */
        const string hidden = """
            var settings = SettingsFileGuard.Read(Path.Combine(configDirectory, "settings.json"));
            ReadInt(root, "invisible_key", ref X);
            """;

        var missed = new Dictionary<string, string>(StringComparer.Ordinal);
        ExtractKeys(hidden, "synthetic.cs", missed);
        Assert.Empty(missed);
    }

    /// <summary>
    /// The other half of the scoping contract, pinned for the same reason: a read attributed to the WRONG
    /// file would make the sample look as though it were missing keys it has no business documenting. Each
    /// hit belongs to the most recent <c>*.json</c> literal above it, which is how App.xaml.cs's
    /// servers.json and collection_schedule.json loaders stay out of this set without needing an exemption.
    /// </summary>
    [Fact]
    public void KeyExtraction_AttributesEachReadToTheFileOpenedAboveIt()
    {
        const string source = """
            var servers = Path.Combine(dir, "servers.json");
            if (root.TryGetProperty("not_a_settings_key", out var a)) A = a.GetBoolean();
            var settings = SettingsFileGuard.Read(Path.Combine(dir, "settings.json"));
            if (read.TryGetProperty("a_settings_key", out var b)) B = b.Bool(B);
            """;

        var found = new Dictionary<string, string>(StringComparer.Ordinal);
        ExtractKeys(source, "synthetic.cs", found);

        Assert.Equal(new[] { "a_settings_key" }, found.Keys.ToArray());
    }

    /// <summary>
    /// Names the dependency this guard acquired in #2444 so a rename fails HERE, with the reason, rather
    /// than as an unexplained collapse of the key count somewhere else.
    ///
    /// <para><c>SettingsReader</c>'s read method is called <c>TryGetProperty</c> because that is the literal
    /// this extraction matches. Spell it <c>TryRead</c> and all eighty-seven of Lite's keys vanish from the
    /// extracted set, both symmetry tests pass on what is left, and settings.sample.json is free to drift
    /// exactly the way #2418 was filed about. PR #2428 hit this and had to read back through
    /// <c>JsonDocument</c>; #2444 kept the name instead, and this is the note that says so out loud.</para>
    /// </summary>
    [Fact]
    public void TheReadHelpersName_IsWhatThisExtractionKeysOn()
    {
        Assert.Contains("TryGetProperty", Scanner.ToString(), StringComparison.Ordinal);

        Assert.True(
            typeof(SettingsReader).GetMethod("TryGetProperty") is not null,
            "SettingsReader no longer has a public TryGetProperty. That name is what this file's extraction "
                + "matches on, so renaming it removes every Lite settings key from the extracted set and lets "
                + "settings.sample.json drift undetected (#2418, #2428, #2444). If it really must be renamed, "
                + "teach the Scanner regex the new name in the SAME change and prove it here.");
    }

    /// <summary>
    /// key -> the source file it was found in, for a failure message that names where to look.
    /// </summary>
    private static Dictionary<string, string> ReadKeysFromLoaders()
    {
        var keys = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var name in ReaderSources)
        {
            var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", name);
            Assert.True(
                File.Exists(path),
                $"{name} was not copied beside the test binary — check the csproj None/Link item.");

            ExtractKeys(File.ReadAllText(path), name, keys);
        }

        return keys;
    }

    /// <summary>
    /// The extraction itself, over TEXT rather than over a path, so the self-tests below can run it against a
    /// source they control instead of only against whatever the tree happens to contain today. Split out
    /// during #2444: the loaders' read shape changed, and a guard whose only exercise is the real file cannot
    /// show that it still SEES the new shape — it can only fail later, obliquely, on a count.
    /// </summary>
    private static void ExtractKeys(string source, string sourceName, Dictionary<string, string> into)
    {
        var openFile = string.Empty;
        foreach (Match match in Scanner.Matches(source))
        {
            if (match.Groups["file"].Success)
            {
                openFile = match.Groups["file"].Value;
            }
            else if (openFile == "settings.json")
            {
                into.TryAdd(match.Groups["key"].Value, sourceName);
            }
        }
    }

    /// <summary>
    /// The comparison <see cref="Sample_DocumentsEveryKeyTheLoadersRead"/> makes, so the self-test below
    /// exercises the REAL check rather than a re-implementation of it that could drift away from it.
    /// </summary>
    private static List<string> UndocumentedKeys(Dictionary<string, string> read, HashSet<string> documented) =>
        read.Keys
            .Where(k => !documented.Contains(k) && !LoaderOnlyKeys.Contains(k))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

    private static HashSet<string> SampleKeys()
    {
        using var doc = ParseSample();
        return doc.RootElement.EnumerateObject().Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
    }

    private static JsonDocument ParseSample()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "settings.sample.json");
        Assert.True(
            File.Exists(path),
            "settings.sample.json was not copied beside the test binary — check the csproj None/Link item.");

        /* The sample is JSONC on purpose: the comments ARE the documentation. Lite's own loader parses
           settings.json with default options and would reject them, which is why the sample's header
           says to copy keys out of it rather than the file itself. */
        return JsonDocument.Parse(
            File.ReadAllText(path),
            new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = false });
    }
}
