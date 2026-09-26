/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The cross-SKU source-scan pins of the <c>get_collection_health</c> parity families (#3013's
/// <c>alert_read_health</c> block, #3453's <c>service</c> block), gathered into ONE file that BOTH test
/// projects compile: <c>Lite.Tests</c> by its default glob, <c>Darling.Tests</c> through a linked
/// <c>Compile</c> item (#3938).
///
/// <para><b>Why the move.</b> Every fact here reads source text from both SKUs' files and touches no Lite,
/// DuckDB or WPF type, yet while they lived in <c>Lite.Tests</c> they could only execute in CI's Windows
/// <c>build</c> job, because that project references the WPF app and cannot be built or run on a Mac. So a
/// Darling-side change that broke one of them (a field added to one SKU's block, a description edited on one
/// side, a fleet-scoped read added to the Darling evaluator) went red for the first time in CI, on lanes cut
/// from a Mac. Linked, the same bytes run in <c>Darling.Tests</c> too, where a local harness reaches them.</para>
///
/// <para><b>Why a link and not a mirror.</b> One file is one authority: there is no second copy to drift and
/// therefore no parity pin to keep the copies honest, which is the argument <c>CSharpSourceWalker.cs</c>'s
/// link the other way already made. The facts' names and assertions moved here byte-for-byte from
/// <c>AlertReadFailureSurfaceTests</c> and <c>ServiceBuildSurfaceTests</c>; what stayed behind in those two
/// classes is exactly the set that needs a Lite type or reads only Lite's own source.</para>
///
/// <para><b>What a linked file may use.</b> Only what both projects carry: the BCL, xunit, and the shared
/// <c>PerformanceMonitor.Alerting</c> library. No helper from either test project, no <c>cref</c> to a class
/// only one of them compiles, and a repo root found by walking up from this file's OWN path — which is the
/// same path whichever project compiles it. The namespace stays <c>Lite.Tests</c> in both assemblies, the
/// way the walker keeps <c>Darling.Tests</c> when Lite compiles it.</para>
/// </summary>
public sealed class CrossSkuSurfaceSourceTests
{
    /// <summary>
    /// Every file in the tree that DEFINES a <c>get_collection_health</c> MCP tool. Discovered rather than
    /// listed: a positive check over the two files this change touched would confirm its own work and see
    /// nothing else, which is how two older sites survived a parity sweep earlier in this backlog.
    /// </summary>
    private static IReadOnlyList<string> CollectionHealthToolFiles()
    {
        var root = RepoRoot();

        var found = Directory
            .EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
            .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !p.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !p.Contains($"{Path.DirectorySeparatorChar}deprecated{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !p.Contains($"{Path.DirectorySeparatorChar}.git{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(p => File.ReadAllText(p).Contains(
                "[McpServerTool(Name = \"get_collection_health\")", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();

        /* The control for the sweep above, through the identical enumeration: it must find MORE than one
           file, or the whole-tree walk is silently reading nothing and every assertion built on it is
           vacuous. Two is today's answer and the count is asserted at the call site; this is the floor. */
        Assert.True(
            found.Count >= 2,
            $"the whole-tree walk for get_collection_health tool definitions found {found.Count} file(s) "
            + "under " + root + " — a walk that reaches one file or none cannot make a parity claim");

        return found;
    }

    [Fact]
    public void EverySkusCollectionHealthTool_CarriesTheAlertReadBlock()
    {
        var files = CollectionHealthToolFiles();

        Assert.Equal(2, files.Count);
        Assert.Contains(files, f => f.EndsWith("McpHealthTools.cs", StringComparison.Ordinal));
        Assert.Contains(files, f => f.EndsWith("DarlingMcpDataTools.cs", StringComparison.Ordinal));

        foreach (var file in files)
        {
            var text = File.ReadAllText(file);

            Assert.Contains("alert_read_health = new", text, StringComparison.Ordinal);
            Assert.Contains("AlertReadFailureCounter.Shared.ReadFor(", text, StringComparison.Ordinal);
            Assert.Contains("AlertReadFailureCounter.FormatFinding(", text, StringComparison.Ordinal);
            Assert.Contains("note = AlertReadFailureCounter.WindowNote", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BothSkusBlocks_CarryTheIdenticalFieldSet_AndEveryFieldOfTheReading()
    {
        /* The parity claim proper. Compared as SETS of field names extracted from each SKU's own
           initializer, so a field added to one and forgotten on the other fails here — and cross-checked
           against the Reading record by REFLECTION, so a field added to the record and rendered by neither
           SKU also fails. Two directions, because a payload nobody renders and a payload one SKU renders
           are different defects with the same cause. */
        var fieldsBySku = CollectionHealthToolFiles()
            .ToDictionary(
                f => Path.GetFileName(f),
                f => AlertReadFieldNames(File.ReadAllText(f)),
                StringComparer.Ordinal);

        var sets = fieldsBySku.Values.ToList();
        Assert.Equal(2, sets.Count);
        Assert.Equal(sets[0], sets[1]);

        var rendered = sets[0];

        /* Reflected off the record so a seventh member cannot be added without a surface for it. */
        var readingMembers = typeof(AlertReadFailureCounter.Reading)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name)
            .Where(n => n != "EqualityContract")
            .ToList();

        Assert.Equal(16, readingMembers.Count);

        /* Sixteen from the record plus the two composed values. The record's members are three failure
           counts and three newest-failure trios, the pass denominator, counting_since, and #3848's two
           retry counts — so the set below reads as five groups: this server's, the fleet-scoped
           conditions', the instance-wide newest, the two figures that frame them, and the retry pair.

           The retry pair carries no stamp, read name or elapsed, unlike each failure count, and that
           asymmetry is asserted by this exact-set equality rather than merely stated: a trio added for it
           would red here. It exists because those three date and attribute a condition that went BLIND,
           and a retried read did not — it was judged on evidence that arrived late. */
        Assert.Equal(18, rendered.Count);
        Assert.Equal(
            new[]
            {
                "counting_since", "finding", "fleet_last_failure_at", "fleet_last_failure_elapsed_ms",
                "fleet_last_failure_read", "fleet_read_failures", "instance_last_failure_at",
                "instance_last_failure_elapsed_ms", "instance_last_failure_read",
                "instance_read_failures", "instance_retried_reads", "last_failure_at",
                "last_failure_elapsed_ms", "last_failure_read", "note", "retried_reads",
                "server_alert_passes", "server_read_failures",
            },
            rendered.OrderBy(f => f, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void BothSkusToolDescriptions_StayByteIdentical()
    {
        /* One tool, one contract: the two SKUs' descriptions of get_collection_health are byte-identical on
           origin/dev and must stay so, or a client learns different things about the same tool depending on
           which SKU answered. This change appended to both; the pin is that it appended the SAME bytes. */
        var descriptions = CollectionHealthToolFiles()
            .Select(f => ToolDescription(File.ReadAllText(f)))
            .ToList();

        Assert.Equal(2, descriptions.Count);
        Assert.Equal(descriptions[0], descriptions[1]);

        /* And that the appended paragraph is actually in there, so the equality above is not two copies of
           an unchanged string agreeing with each other. */
        Assert.Contains("alert_read_health", descriptions[0], StringComparison.Ordinal);
        Assert.Contains("counting_since", descriptions[0], StringComparison.Ordinal);
        Assert.Contains("failed to DELIVER", descriptions[0], StringComparison.Ordinal);
    }

    /// <summary>
    /// The fleet-scoped inventory is what the counter actually records, in both directions.
    ///
    /// <para>The hand-maintained version of this list was wrong twice over: it named store DISK PRESSURE,
    /// whose two feed reads are both exempt so it can never contribute a failure, and it omitted the
    /// collector-cost regression self-alert, which does. Both errors point the same way for an operator —
    /// a nonzero instance total, every server at zero, and a documented list that does not name the cause.</para>
    ///
    /// <para>So the set is derived from SOURCE (every <c>RecordReadFailure(null, ...)</c> call, matched
    /// across line breaks because those calls are wrapped) and each one must be represented in the single
    /// constant every surface now concatenates. A site beyond the count asserted below fails here until
    /// the constant names it — stated that way rather than as its own numeral, because the numeral here
    /// was wrong from the day it was written (it said "a sixth" beside an asserted count of two) and a
    /// second copy of a pinned number has nothing keeping it honest.</para>
    ///
    /// <para>Derived from the LITERAL, which is why the call sites spell the read name inline rather than
    /// through a constant: a named constant at the site would leave this regex finding nothing there, and
    /// the pin would report the set complete while an un-inventoried fleet-scoped read shipped.</para>
    /// </summary>
    [Fact]
    public void TheFleetScopedInventory_MatchesWhatTheCounterActuallyRecords()
    {
        var root = RepoRoot();
        var nullKeyReads = new List<string>();

        foreach (var relative in new[]
        {
            Path.Combine("PerformanceMonitor.Alerting", "AlertEngine.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"),
            /* Lite's own recording site. The constant is concatenated into BOTH SKUs' descriptions, so a
               derivation that read only Darling's files could not see a Lite fleet-scoped read at all — and
               the reverse gap is the one that bites: a read named here that the reading SKU cannot
               increment is this counter's own defect, a confident zero, written into its documentation. */
            Path.Combine("Lite", "MainWindow.xaml.cs"),
        })
        {
            var src = File.ReadAllText(Path.Combine(root, relative));

            /* Singleline, because both of these calls are wrapped across lines — a line-bound pattern found
               one of the two and would have "proved" a single fleet-scoped site. */
            foreach (Match m in Regex.Matches(src, @"RecordReadFailure\(\s*null\s*,\s*""([^""]+)""", RegexOptions.Singleline))
            {
                nullKeyReads.Add(m.Groups[1].Value);
            }
        }

        /* Seventh since #3466: the fleet-sweep rollup read, Darling-only like the store self-alerts. Eighth
           since #3580: the daily documents' delivery-stamp read — ONE site gating both the digest and the
           rollup on delivered-today, so one name — Darling-only for the same reason. Ninth since #3712: the
           analysis singles digest read, the third daily document's span read, Darling-only like the others.
           Tenth and eleventh since #3826: the plan dimension's TOAST slack read and the store checkpointer
           pressure read, the two informational store self-alerts that PR added — Darling-only for the same
           reason, and the pair that shipped un-inventoried because that PR's own build check was cancelled
           rather than red. Twelfth since #4215: the store settings self-alert's managed-conf verdicts read,
           Darling-only like the other store self-alerts. */
        Assert.Equal(12, nullKeyReads.Count);

        var inventory = AlertReadFailureCounter.FleetScopedReads;

        /* Each recorded site is represented. Keyed on the distinguishing word rather than the whole read
           name, because the constant is prose for an operator and the read name is a label for a log. */
        Assert.Contains(nullKeyReads, r => r.Contains("collector-cost regression", StringComparison.Ordinal));
        /* #3443: the regression read grew two fleet-scoped companions in the same evaluator pass — the
           census read that supplies the paging-versus-digest routing denominator, and the digest's movers
           read. Asserted as three DISTINCT reads rather than one "collector-cost" match, because they blind
           different stages of the same condition and a shared name would make last_failure_read ambiguous
           exactly when one of the three is the one that went quiet. */
        Assert.Contains(nullKeyReads, r => r.Contains("collector-cost census", StringComparison.Ordinal));
        Assert.Contains(nullKeyReads, r => r.Contains("collector-cost digest", StringComparison.Ordinal));
        Assert.Contains(nullKeyReads, r => r.Contains("background-job health", StringComparison.Ordinal));
        Assert.Contains(nullKeyReads, r => r.Contains("fleet-sweep rollup", StringComparison.Ordinal));
        Assert.Contains(nullKeyReads, r => r.Contains("delivery-stamp", StringComparison.Ordinal));
        Assert.Contains(nullKeyReads, r => r.Contains("analysis singles digest", StringComparison.Ordinal));
        Assert.Contains(nullKeyReads, r => r.Contains("TOAST slack", StringComparison.Ordinal));
        Assert.Contains(nullKeyReads, r => r.Contains("checkpointer pressure", StringComparison.Ordinal));
        Assert.Contains(nullKeyReads, r => r.Contains("managed-conf verdicts", StringComparison.Ordinal));
        /* #3354: config_mute_rules belongs to the store, not to any monitored server, so its failed read
           lands in the instance total and in no server's count — exactly the case a per-server-only
           surface would have given no home. Recorded TWICE across the tree, once per SKU, and that is the
           point rather than a duplicate: both SKUs perform this read, the counters are per-process, and a
           SKU that named it without recording it would promise a reading it cannot produce. The other ten
           entries are Darling-only with no Lite equivalent — nine store self-alerts and the background-job
           health read. */
        Assert.Equal(
            2,
            nullKeyReads.Count(r => r.Contains("mute-rule reload", StringComparison.Ordinal)));
        Assert.Contains("collector-cost regression", inventory, StringComparison.Ordinal);
        Assert.Contains("collector-cost census", inventory, StringComparison.Ordinal);
        Assert.Contains("collector-cost digest", inventory, StringComparison.Ordinal);
        Assert.Contains("background-job health", inventory, StringComparison.Ordinal);
        Assert.Contains("mute-rule reload", inventory, StringComparison.Ordinal);
        Assert.Contains("fleet-sweep rollup", inventory, StringComparison.Ordinal);
        Assert.Contains("delivery-stamp", inventory, StringComparison.Ordinal);
        Assert.Contains("analysis singles digest", inventory, StringComparison.Ordinal);
        Assert.Contains("TOAST slack", inventory, StringComparison.Ordinal);
        Assert.Contains("checkpointer pressure", inventory, StringComparison.Ordinal);
        Assert.Contains("managed-conf verdicts", inventory, StringComparison.Ordinal);

        /* And the phantom stays gone. Disk pressure's feed reads are exempt — a local filesystem read and a
           recorded-store-size lookup that is context for the alert text — so naming it here would send an
           operator after a read that cannot fail into this number. */
        Assert.DoesNotContain("disk pressure", inventory, StringComparison.OrdinalIgnoreCase);

        /* The constant is what every surface concatenates, so this is also the cross-surface tie. */
        foreach (var file in CollectionHealthToolFiles())
        {
            Assert.Contains(
                "AlertReadFailureCounter.FleetScopedReads",
                File.ReadAllText(file),
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EverySkusCollectionHealthTool_CarriesTheServiceBlock_WithTheIdenticalFieldSet()
    {
        /* Same shape as the #3013 census one file over: extracted from each SKU's own initializer and
           compared as SETS, so a field added to one SKU and forgotten on the other fails here rather than
           shipping as a payload-shape fork of one tool. */
        var files = CollectionHealthToolFiles();

        Assert.Equal(2, files.Count);

        var sets = files
            .Select(f => ServiceFieldNames(File.ReadAllText(f)))
            .ToList();

        Assert.Equal(sets[0], sets[1]);

        /* Three facts and deliberately no more: what build (version), since when (started_at), and what
           the build expects of its store (compiled_schema_version). A fourth field arriving here should
           have to argue for itself the way these three did in #3453. */
        Assert.Equal(
            new[] { "compiled_schema_version", "started_at", "version" },
            sets[0].ToArray());
    }

    [Fact]
    public void StartedAt_IsTheSameInstantAsCountingSince_OnBothSkus()
    {
        /* The decision #3453 asked to be made and then documented: started_at reuses the counter's own
           CountingSinceUtc rather than taking a stamp of its own. Both mean "when this process came up",
           and two clocks for one fact would put two near-identical stamps on one payload whose skew a
           reader has to explain away — so the pin is that BOTH fields render the IDENTICAL expression,
           which is a stronger claim than two stamps that happen to agree tonight. */
        foreach (var file in CollectionHealthToolFiles())
        {
            var text = File.ReadAllText(file);

            Assert.Contains(
                "started_at = alertReads.CountingSinceUtc.ToString(\"o\")",
                text,
                StringComparison.Ordinal);
            Assert.Contains(
                "counting_since = alertReads.CountingSinceUtc.ToString(\"o\")",
                text,
                StringComparison.Ordinal);
        }

        /* And the rendering that expression produces: ISO-8601, UTC-marked, round-trippable to the
           construction instant. Unspecified kind would make "since when" a function of the reader's local
           zone, which for the restart detector's own instant is a wrong answer by up to a day. */
        var started = new DateTime(2026, 9, 15, 1, 2, 3, 456, DateTimeKind.Utc);
        var counter = new AlertReadFailureCounter(() => started);

        var rendered = counter.ReadFor("never-seen").CountingSinceUtc.ToString("o");

        Assert.EndsWith("Z", rendered, StringComparison.Ordinal);

        var parsed = DateTime.Parse(rendered, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
        Assert.Equal(started, parsed);
    }

    [Fact]
    public void TheSharedDescription_DocumentsTheServiceBlock()
    {
        /* The description is THE build-attribution contract now — an MCP caller learns what the block is
           for from this text and nothing else — so its claims are censused the way the #3013 paragraph's
           are. Byte-identity across the SKUs is the sibling family's pin; this one holds that the shared
           bytes actually say the load-bearing things: what question version answers, that counting_since
           keeps its old job, and what the rung is NOT (a store read). Asserted inside the Description
           attribute's own span rather than against the whole file, so a sentence demoted to a code comment
           cannot keep this green. */
        foreach (var file in CollectionHealthToolFiles())
        {
            var description = DescriptionSpan(File.ReadAllText(file));

            foreach (var phrase in new[]
            {
                "is the running service the build that carries fix X",
                "counting_since remains the restart detector",
                "started_at is the SAME instant",
                "compiled_schema_version is the schema rung this BUILD expects",
                "not a read of the store's migrated rung",
            })
            {
                Assert.Contains(phrase, description, StringComparison.Ordinal);
            }

            /* The control: the identical Contains form finds a plausible-but-absent claim nowhere, so the
               silences above are absences rather than a matcher that never matches. */
            Assert.DoesNotContain(
                "the store's migrated rung is reported beside it",
                description,
                StringComparison.Ordinal);
        }
    }

    /* ---------------- helpers ---------------- */

    /// <summary>The field names inside a tool's <c>alert_read_health = new { … }</c> initializer.</summary>
    private static SortedSet<string> AlertReadFieldNames(string source)
    {
        var at = source.IndexOf("alert_read_health = new", StringComparison.Ordinal);
        Assert.True(at > 0, "a get_collection_health tool no longer builds an alert_read_health block");

        var open = source.IndexOf('{', at);
        Assert.True(open > 0, "alert_read_health has no initializer");

        var depth = 0;
        var end = -1;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    end = i;
                    break;
                }
            }
        }

        Assert.True(end > open, "alert_read_health's initializer never closes");

        var body = source[open..end];

        /* Assignments only, and only at the initializer's own level — the block contains explanatory
           comments with '=' in prose, so the pattern requires an identifier at the start of a line. */
        var names = new SortedSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(body, @"^\s*([a-z_][a-z0-9_]*)\s*=\s*[^=]", RegexOptions.Multiline))
        {
            names.Add(m.Groups[1].Value);
        }

        Assert.True(names.Count > 0, "no fields were extracted from an alert_read_health block");

        return names;
    }

    /// <summary>
    /// The tool's description as the CLIENT sees it, reassembled from however the literal is spelled.
    ///
    /// <para>It is no longer one literal: both descriptions concatenate
    /// <see cref="AlertReadFailureCounter.FleetScopedReads"/> so the fleet-scoped set cannot drift
    /// between them. A pattern that captured a single quoted run would simply stop matching, which is at
    /// least loud — but it would also stop comparing the halves either side of the constant, so the
    /// segments are concatenated and the constant substituted in its place.</para>
    /// </summary>
    private static string ToolDescription(string source)
    {
        var call = Regex.Match(
            source,
            @"\[McpServerTool\(Name = ""get_collection_health""\), Description\((.*?)\)\]",
            RegexOptions.Singleline);

        Assert.True(call.Success, "a get_collection_health tool has no Description attribute in the expected shape");

        var assembled = new System.Text.StringBuilder();
        foreach (var piece in Regex.Split(call.Groups[1].Value, @"\s*\+\s*"))
        {
            var trimmed = piece.Trim();

            if (trimmed.StartsWith("\"", StringComparison.Ordinal) && trimmed.EndsWith("\"", StringComparison.Ordinal))
            {
                assembled.Append(trimmed[1..^1]);
            }
            else if (trimmed.EndsWith("FleetScopedReads", StringComparison.Ordinal))
            {
                assembled.Append(AlertReadFailureCounter.FleetScopedReads);
            }
            else
            {
                Assert.Fail($"unrecognised piece in the Description concatenation: {trimmed}");
            }
        }

        var text = assembled.ToString();
        Assert.True(text.Length > 5000, $"the reassembled description is only {text.Length} chars — the split lost content");

        return text;
    }

    /// <summary>The field names inside a tool's <c>service = new { … }</c> initializer, extracted the way
    /// the sibling census extracts <c>alert_read_health</c>'s: brace-balanced from the initializer's own
    /// opening, assignments matched only at line starts so prose in comments cannot join the set.</summary>
    private static SortedSet<string> ServiceFieldNames(string source)
    {
        var at = source.IndexOf("service = new", StringComparison.Ordinal);
        Assert.True(at > 0, "a get_collection_health tool no longer builds a service block");

        var open = source.IndexOf('{', at);
        Assert.True(open > 0, "the service block has no initializer");

        var depth = 0;
        var end = -1;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    end = i;
                    break;
                }
            }
        }

        Assert.True(end > open, "the service block's initializer never closes");

        var body = source[open..end];

        var names = new SortedSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(body, @"^\s*([a-z_][a-z0-9_]*)\s*=\s*[^=]", RegexOptions.Multiline))
        {
            names.Add(m.Groups[1].Value);
        }

        Assert.True(names.Count > 0, "no fields were extracted from a service block");

        return names;
    }

    /// <summary>The raw span of the tool's <c>Description(...)</c> attribute argument — quoted literals and
    /// the concatenated constant's NAME, which is enough for phrases that live in the literals. The full
    /// client-visible reassembly (with the constant substituted) belongs to the sibling family's
    /// byte-identity pin; this lighter read exists so a claim census cannot be satisfied by the same words
    /// in a code comment.</summary>
    private static string DescriptionSpan(string source)
    {
        var call = Regex.Match(
            source,
            @"\[McpServerTool\(Name = ""get_collection_health""\), Description\((.*?)\)\]",
            RegexOptions.Singleline);

        Assert.True(call.Success, "a get_collection_health tool has no Description attribute in the expected shape");

        return call.Groups[1].Value;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
