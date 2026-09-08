/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3161: a collector DEFINITION can contribute to <c>collection_log.error_message</c>, and what it
/// contributes is COUNTS.
///
/// <para><b>The defect.</b> Every value <c>CollectorRunResult.Note</c> could take was runner-authored, so a
/// collector that could work out why it returned nothing — cheaply, on the target, inside the round trip it
/// had already made — had nowhere to put it, and the run recorded SUCCESS with a NULL note. Five issues in a
/// row were that shape (#3030, #3109, #3114, #3153, #3154) and every one was repaired at READ time, because
/// read time was the only seam that existed.</para>
///
/// <para><b>Why counts and not verdicts, which is what these pins are mostly about.</b>
/// <c>CollectorRuntimePrecondition</c> (#2546) argues that a MUTABLE, ACTIONABLE condition has to be
/// re-derived on every read, and it is right: a stored verdict "would silently ignore the fix we ourselves
/// recommended, which is the worst possible direction to be wrong in". A stored COUNT has no such problem —
/// <c>candidates=1264 visible=0</c> is a fact about one instant and stays true, and the read draws the
/// conclusion fresh every time it looks.</para>
///
/// <para><b>The guard is STRUCTURAL, and deliberately not a keyword check.</b> A blocklist over free prose
/// passes on the wording nobody thought of, and it is the kind of pin that reports discrimination it stopped
/// providing. <see cref="CollectorMeasurement.Value"/> being a <see cref="long"/> is what makes a verdict
/// unrepresentable, and the label grammar is what stops the sentence relocating one field left. Those two
/// are pinned separately here, and for the reason PR #3159 arrived at independently one collector over:
/// <c>PgColumnStatsCoverage</c> keeps its <c>Census</c> free of verdicts and its <c>Cause</c> free of
/// counts as SEPARATE pins, because a distinctness check over the combined message would pass on the
/// numbers rather than on the verdicts.</para>
/// </summary>
public class CollectorMeasurementSeamTests
{
    /* A rendered note may contain NOTHING but label=value tokens separated by single spaces. Anchored at
       both ends, so an appended clause fails rather than being tolerated as a suffix. */
    private static readonly Regex s_countsOnly =
        new(@"^[a-z][a-z0-9_]*=-?[0-9]+( [a-z][a-z0-9_]*=-?[0-9]+)*$", RegexOptions.Compiled);

    /* ── the structural half: a verdict is unrepresentable, rather than discouraged ── */

    [Fact]
    public void TheMeasuredValueIsAnInteger_SoAStoredVerdictIsUnrepresentable()
    {
        /* The pin that carries the whole design. If this type ever widens its value to string or object —
           the obvious "let a collector explain itself properly" change — then every other guard in this file
           becomes advisory, because prose would have a first-class home again and #2546's stale-gate defect
           would be back with our own blessing. Asserted by reflection on the TYPE rather than by trying to
           store a sentence, because the failure being prevented is a compile-time widening that no runtime
           attempt can reach. */
        var value = typeof(CollectorMeasurement).GetProperty(nameof(CollectorMeasurement.Value));

        Assert.NotNull(value);
        Assert.Equal(typeof(long), value!.PropertyType);
    }

    [Fact]
    public void TheMeasurementTypeCarriesOnlyANameAndACount()
    {
        /* The escape route the grammar pins CANNOT see, found by mutation: adding a third member —
           `string? Detail`, "counts are great but let the collector add one clarifying sentence" — routes a
           verdict through a seam whose every other guard still passes, because those guards check the
           inputs the TEST chose and a new field is populated by the DEFINITION. Freezing the member set is
           what catches it, and it catches it at the type rather than at whichever collector happens to have
           a pin. Adding a count to a measurement is not a thing anyone needs to do; adding prose is, and
           this is the pin that says no.

           Asserted as an exact set rather than a count, so renaming a member is caught too — a numeral
           would keep passing while the shape changed underneath it. */
        var members = typeof(CollectorMeasurement)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name + ":" + p.PropertyType.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(new[] { "Label:String", "Value:Int64" }, members);
    }

    [Fact]
    public void ADefinitionSuppliedNoteCarriesNoRenderedConclusion_EvenWhenHandedOne()
    {
        /* The adversarial direction, and the one that makes this pin discriminate what it claims rather than
           something adjacent: the caller tries to put a real verdict through the seam. Not an invented
           sentence — PgColumnStatsCoverage.PrivilegeRemedy is the actual shipped remedy text for the actual
           collector #3154 was filed about, so if this seam can carry a conclusion, THAT is the conclusion it
           would carry. */
        var context = Context();
        context.Measure("candidates", 1264);
        context.Measure("visible", 0);

        var note = CollectorMeasurementNote.Render(context.Measurements);

        Assert.NotNull(note);
        Assert.Equal("candidates=1264 visible=0", note);
        Assert.Matches(s_countsOnly, note!);

        /* And the same seam handed the verdict directly renders no part of it. */
        var handFed = new[]
        {
            new CollectorMeasurement("candidates", 1264),
            new CollectorMeasurement(PgColumnStatsCoverage.PrivilegeRemedy, 1),
            new CollectorMeasurement("statistics not visible to this login; GRANT SELECT", 1),
        };

        var rendered = CollectorMeasurementNote.Render(handFed);

        Assert.NotNull(rendered);
        Assert.Matches(s_countsOnly, rendered!);
        Assert.DoesNotContain("GRANT", rendered!, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("not visible", rendered!, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(PgColumnStatsCoverage.PrivilegeRemedy, rendered!, StringComparison.Ordinal);

        /* The two it refused are COUNTED, not swallowed. A silent drop would be the same invisibility this
           whole seam exists to end, one level up. */
        Assert.Equal(
            "candidates=1264 " + CollectorMeasurementNote.RejectedLabelCount + "=2",
            rendered);
    }

    [Fact]
    public async Task WhatARealDefinitionProducesIsCountsOnlyToo()
    {
        /* The pin above chooses its own measurements, so it can only prove the RENDERER adds no conclusion —
           not that a definition cannot introduce one by another route. This one asks the same question of
           what a shipped collector actually put in the context, driving its real read. Mutation found the
           gap: a third member on CollectorMeasurement, populated by the definition, left every
           hand-built-input pin green. */
        var context = Context();

        await BlockedProcessReportCollector.Instance.ReadAsync(
            MakeReader(new[] { "<not-a-blocked-process-report/>" }), context, CancellationToken.None);

        var note = CollectorMeasurementNote.Render(context.Measurements);

        Assert.NotNull(note);
        Assert.Matches(s_countsOnly, note!);

        /* Every measurement it recorded is a legal count name and nothing else. */
        Assert.All(context.Measurements, m => Assert.True(
            CollectorMeasurementNote.IsValidLabel(m.Label), m.Label));
    }

    [Theory]
    /* Every spelling of a sentence, and each is rejected by more than one clause of the grammar. */
    [InlineData("statistics not visible to this login")]
    [InlineData("GRANT SELECT")]
    [InlineData("Remedy: grant the monitoring login read access")]
    [InlineData("candidates=1264 visible=0")]
    [InlineData("events read")]
    [InlineData("events-read")]
    [InlineData("events.read")]
    [InlineData("EventsRead")]
    [InlineData("Events_Read")]
    [InlineData("_events_read")]
    [InlineData("1events")]
    [InlineData("")]
    [InlineData(null)]
    /* Long enough that no clause fits is part of the grammar, so the ceiling is pinned as grammar. */
    [InlineData("this_label_is_far_too_long_to_be_a_count_name_and_is_really_a_sentence")]
    public void TheLabelGrammarRejectsAnythingThatCouldHoldAClause(string? label)
    {
        Assert.False(CollectorMeasurementNote.IsValidLabel(label));

        /* And Measure REFUSES it rather than storing it — the chokepoint a definition actually goes
           through. Throwing is right here and only here: labels are first-party constants in definition
           source, so this is a build-and-test-time failure, and
           EveryMeasurementLabelInTheCollectorsIsLegal below is what keeps it out of a release. */
        Assert.Throws<ArgumentException>(() => Context().Measure(label!, 1));
    }

    [Theory]
    [InlineData("events_read")]
    [InlineData("candidates")]
    [InlineData("visible")]
    [InlineData("a")]
    [InlineData("tables_above_floor_24h")]
    public void TheLabelGrammarAcceptsACountName(string label)
    {
        Assert.True(CollectorMeasurementNote.IsValidLabel(label));

        var context = Context();
        context.Measure(label, 7);

        Assert.Equal(label + "=7", CollectorMeasurementNote.Render(context.Measurements));
    }

    [Fact]
    public void TheCeilingIsTheGrammarsOwnConstant_NotARetypedNumber()
    {
        /* At the ceiling is legal and one past it is not, asserted against the constant rather than against
           40 — a pin that retyped the number would keep passing while the two disagreed. */
        var atCeiling = "a" + new string('b', CollectorMeasurementNote.MaxLabelLength - 1);
        Assert.Equal(CollectorMeasurementNote.MaxLabelLength, atCeiling.Length);
        Assert.True(CollectorMeasurementNote.IsValidLabel(atCeiling));
        Assert.False(CollectorMeasurementNote.IsValidLabel(atCeiling + "b"));
    }

    /* ── counts, so repeats sum: the property that makes the fan-out shapes correct ── */

    [Fact]
    public void MeasureAccumulatesARepeatedLabel_SoAPerDatabaseCycleReportsItsSum()
    {
        /* The host calls ReadAsync once per database against ONE context on the Azure per-database shape
           (blocked_process_report.RunsPerDatabase is true there), and once per item on the enumerated shape.
           Appending instead of accumulating would render "events_read=3 events_read=0 events_read=11", which
           is not a count of anything — and a reader summing it by eye would get the right answer for the
           wrong reason on the day one database is missing. */
        var context = Context();
        context.Measure("events_read", 3);
        context.Measure("events_read", 0);
        context.Measure("events_read", 11);
        context.Measure("events_stored", 2);

        Assert.Equal(2, context.Measurements.Count);
        Assert.Equal("events_read=14 events_stored=2", CollectorMeasurementNote.Render(context.Measurements));

        /* First-measured order is kept, so the pairs read in the order the definition declared them rather
           than in whatever order the last cycle happened to touch them. */
        Assert.Equal("events_read", context.Measurements[0].Label);
    }

    /* ── composition: the definition half is exactly the counts, and nothing else ── */

    [Fact]
    public void AnOrdinaryRunLeavesTheColumnNull_ExactlyAsBefore()
    {
        /* 69 collectors measure nothing. None of them may start writing a note. */
        Assert.Null(CollectorMeasurementNote.Render(CollectorContext.NoMeasurements));
        Assert.Null(CollectorMeasurementNote.Compose(null, CollectorContext.NoMeasurements));
        Assert.Null(new CollectorRunResult(12, 34, 56, CollectorContext.NoMeasurements).Note);
    }

    [Fact]
    public void TheDefinitionHalfOfAComposedNoteIsExactlyTheRenderedCounts()
    {
        /* The complement of the no-conclusion pin: the HOST cannot wrap the counts in a sentence on the way
           past either. With no host note the composed value IS the render, character for character, and with
           one the render survives intact at the tail. */
        var context = Context();
        context.Measure("events_read", 40);
        context.Measure("events_stored", 0);

        var rendered = CollectorMeasurementNote.Render(context.Measurements)!;

        Assert.Equal(rendered, CollectorMeasurementNote.Compose(null, context.Measurements));
        Assert.Equal(rendered, CollectorMeasurementNote.Compose("   ", context.Measurements));

        var composed = CollectorMeasurementNote.Compose(
            EnumeratedCollectorDriver.EmptyEnumerationMessage, context.Measurements);

        Assert.Equal(
            EnumeratedCollectorDriver.EmptyEnumerationMessage + "; " + rendered,
            composed);

        /* Host note FIRST, because both hosts truncate this column and the classified half is the one that
           has to survive a cut. */
        Assert.StartsWith(EnumeratedCollectorDriver.EmptyEnumerationMessage, composed, StringComparison.Ordinal);
        Assert.EndsWith(rendered, composed, StringComparison.Ordinal);
    }

    /* ── the discrimination claim: the COUNTS separate the outcomes, the NOTE concludes nothing ── */

    [Fact]
    public void TheCountsSeparateAQuietServerFromAFault_WhileTheNoteJudgesNeither()
    {
        /* Both halves of the claim, in one place, because either alone is satisfiable by the wrong thing:
           notes that never differ would pass a no-verdict check, and notes that name their verdict would
           pass a distinctness check. This is the pair PR #3159 pinned separately for the same reason. */
        var quiet = Measured(("events_read", 0), ("report_xml_empty", 0), ("report_xml_unparsed", 0), ("events_stored", 0));
        var fault = Measured(("events_read", 40), ("report_xml_empty", 0), ("report_xml_unparsed", 40), ("events_stored", 0));
        var healthy = Measured(("events_read", 40), ("report_xml_empty", 0), ("report_xml_unparsed", 0), ("events_stored", 40));

        /* They discriminate: three outcomes that used to be one SUCCESS-with-a-NULL-note are now three
           distinct rows. */
        Assert.NotEqual(quiet, fault);
        Assert.NotEqual(fault, healthy);
        Assert.NotEqual(quiet, healthy);

        /* And not one of them says WHICH it is. The verdict is the read's job, every time it reads. */
        foreach (var note in new[] { quiet, fault, healthy })
        {
            Assert.Matches(s_countsOnly, note);

            foreach (var verdict in new[]
                     {
                         "fault", "quiet", "idle", "healthy", "broken", "cannot", "unable", "failed",
                         "grant", "privilege", "permission", "remedy", "should", "expected", "not measured",
                     })
            {
                Assert.DoesNotContain(verdict, note, StringComparison.OrdinalIgnoreCase);
            }
        }
    }

    /* ── the consumer, run for real ── */

    [Fact]
    public async Task BlockedProcessReportMeasuresWhatItReadAndWhatItDropped()
    {
        /* Against the collector's own ReadAsync and its real 5-column projection, not a description of it.
           A quiet server and a server whose every captured report we failed to parse both arrived as
           SUCCESS / zero rows / NULL note before this, and only the second is a fault. */
        var quiet = Context();
        var quietRows = await BlockedProcessReportCollector.Instance.ReadAsync(
            MakeReader(Array.Empty<string?>()), quiet, CancellationToken.None);

        Assert.Empty(quietRows);
        Assert.Equal(
            "events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0",
            CollectorMeasurementNote.Render(quiet.Measurements));

        /* Three captured events: one with no report XML at all, one whose XML will not parse, one good. */
        var mixed = Context();
        var mixedRows = await BlockedProcessReportCollector.Instance.ReadAsync(
            MakeReader(new[] { null, "<not-a-blocked-process-report/>", ReportXml }), mixed, CancellationToken.None);

        Assert.Single(mixedRows);
        Assert.Equal(
            "events_read=3 report_xml_empty=1 report_xml_unparsed=1 events_stored=1",
            CollectorMeasurementNote.Render(mixed.Measurements));

        /* The one that matters: the ring buffer delivered events and NOT ONE became a row. Indistinguishable
           from the quiet server above until this seam existed. */
        var allDropped = Context();
        var droppedRows = await BlockedProcessReportCollector.Instance.ReadAsync(
            MakeReader(new[] { "<not-a-blocked-process-report/>", "<not-a-blocked-process-report/>" }),
            allDropped,
            CancellationToken.None);

        Assert.Empty(droppedRows);
        Assert.Equal(
            "events_read=2 report_xml_empty=0 report_xml_unparsed=2 events_stored=0",
            CollectorMeasurementNote.Render(allDropped.Measurements));
    }

    [Fact]
    public void TheConsumersLabelsAreLegalAndDistinct()
    {
        var labels = new[]
        {
            BlockedProcessReportCollector.EventsReadMeasurement,
            BlockedProcessReportCollector.EmptyReportMeasurement,
            BlockedProcessReportCollector.UnparsedReportMeasurement,
            BlockedProcessReportCollector.EventsStoredMeasurement,
        };

        Assert.All(labels, label => Assert.True(CollectorMeasurementNote.IsValidLabel(label), label));

        /* Distinct, or Measure would silently ACCUMULATE two different readings into one figure — the
           accumulation that is correct across a fan-out is wrong between two different counts. */
        Assert.Equal(labels.Length, labels.Distinct(StringComparer.Ordinal).Count());
    }

    /* ── the labels that actually ship ── */

    [Fact]
    public void EveryMeasurementLabelInTheCollectorsIsLegal()
    {
        /* Measure THROWS on an illegal label, which would turn a first-party typo into a FAILING collector
           on a healthy server — the loud unrelated defect this seam is supposed to avoid trading the quiet
           one for. This is the gate that means it cannot get that far.

           Read through CSharpSourceWalker rather than a bare regex, because a label named in a comment or
           a doc-comment must not count as a call — three hand-rolled maskers in this repo produced three
           different wrong answers, and two of them agreed. It resolves BOTH argument spellings: a literal
           at the call site, and the `const string` a collector declares for it (which is the form the one
           shipped consumer uses, so a literal-only scan would have checked nothing while passing).

           An argument it CANNOT resolve FAILS rather than being skipped. That is the direction that costs
           least to be wrong in: a future author passing a computed label is told to make it a const, where
           a skip would silently exempt exactly the case a scanner cannot reason about. */
        var directory = Path.Combine(RepoFile.Root, "PerformanceMonitor.Collectors");
        var files = Directory.EnumerateFiles(directory, "*.cs", SearchOption.TopDirectoryOnly).ToList();

        /* The sweep is only evidence if it read the tree. An empty enumeration would pass this test while
           checking nothing, which is the shape of a guard that cannot fail. */
        Assert.True(files.Count > 90, $"the sweep found only {files.Count} .cs files under {directory}");

        var callSites = 0;
        var resolved = new List<string>();

        foreach (var file in files)
        {
            var source = File.ReadAllText(file);
            var code = CSharpSourceWalker.StripCommentsAndStrings(source);
            var literals = CSharpSourceWalker.StringLiteralBodies(source)
                .ToDictionary(l => l.Start, l => l.Text);

            /* `const string NAME = "..."` — the const map, keyed by the literal that follows the
               declaration in the CODE stream (so a declaration inside a comment contributes nothing). */
            var constants = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var (offset, text) in literals)
            {
                var declaration = Regex.Match(
                    code[..Math.Max(0, offset - 1)],
                    @"const\s+string\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*$");

                if (declaration.Success)
                {
                    constants[declaration.Groups[1].Value] = text;
                }
            }

            foreach (var call in Regex.Matches(code, @"\.Measure\(").Cast<Match>())
            {
                callSites++;

                var argumentStart = call.Index + call.Length;
                var comma = code.IndexOf(',', argumentStart);
                Assert.True(comma > argumentStart, $"unparsable Measure( call in {Path.GetFileName(file)}");

                var argument = source[argumentStart..comma].Trim();
                string? label;

                if (argument.StartsWith('"'))
                {
                    /* A literal at the call site: its body starts one character past the opening quote. */
                    var quote = source.IndexOf('"', argumentStart);
                    Assert.True(literals.TryGetValue(quote + 1, out label),
                        $"could not read the literal label at {Path.GetFileName(file)}:{quote}");
                }
                else
                {
                    /* An identifier, optionally type-qualified. The declaring collector is the one that
                       calls it, so the last segment is the const name in this same file. */
                    var name = argument[(argument.LastIndexOf('.') + 1)..];
                    Assert.True(
                        constants.TryGetValue(name, out label),
                        $"measurement label argument \"{argument}\" in {Path.GetFileName(file)} resolves to "
                        + "no `const string` in that file — declare the label as a const beside the "
                        + "collector so this gate can read it");
                }

                Assert.True(
                    CollectorMeasurementNote.IsValidLabel(label),
                    $"illegal measurement label \"{label}\" in {Path.GetFileName(file)} — Measure would "
                    + "throw at runtime and fail an otherwise healthy collector");

                resolved.Add(label!);
            }
        }

        /* And the scan REACHED the calls that ship, or it proved nothing about them. Compared against the
           consumer's own constants rather than a literal count, so a fifth measurement fails here until
           this expectation is updated instead of passing on a stale numeral. */
        Assert.True(callSites > 0, "the scan found no Measure( call sites at all");

        Assert.Equal(
            new[]
            {
                BlockedProcessReportCollector.EventsReadMeasurement,
                BlockedProcessReportCollector.EmptyReportMeasurement,
                BlockedProcessReportCollector.UnparsedReportMeasurement,
                BlockedProcessReportCollector.EventsStoredMeasurement,
            }.OrderBy(l => l, StringComparer.Ordinal).ToList(),
            resolved.Distinct(StringComparer.Ordinal).OrderBy(l => l, StringComparer.Ordinal).ToList());
    }

    /* ── both SKUs, or the member reads as permanently empty in one of them ── */

    [Fact]
    public void DarlingsRunNoteIsComputed_SoTheHostCannotReadOneHalfWithoutTheOther()
    {
        var note = typeof(CollectorRunResult).GetProperty(nameof(CollectorRunResult.Note));

        Assert.NotNull(note);

        /* No setter, and no init accessor either — there is no spelling of "the host note alone" for a
           caller to reach for. This is the parity guarantee: a shared seam wired into one runner reads as a
           permanently-empty value in the other SKU and NOTHING FAILS TO BUILD, which is the failure mode
           CONTRIBUTING's two-store parity rules name. */
        Assert.Null(note!.SetMethod);

        /* And the definition's half is REQUIRED on the way in, so the compiler names every construction site
           rather than a grep doing it. This is what turned "roughly the success return plus the early
           outs" into an enumerated 22. */
        var constructor = typeof(CollectorRunResult).GetConstructors().Single();
        var measurements = constructor.GetParameters()
            .Single(p => p.Name == nameof(CollectorRunResult.Measurements));

        Assert.False(measurements.IsOptional);
        Assert.Equal(typeof(IReadOnlyList<CollectorMeasurement>), measurements.ParameterType);

        /* Both halves reach the column, in that order. */
        var result = new CollectorRunResult(
            0, 5, 0,
            new[] { new CollectorMeasurement("events_read", 0) },
            EnumeratedCollectorDriver.EmptyEnumerationMessage);

        Assert.Equal(
            EnumeratedCollectorDriver.EmptyEnumerationMessage + "; events_read=0",
            result.Note);
    }

    [Fact]
    public void LitesRunNoteIsComputedTheSameWay_ThroughTheSameSharedCompose()
    {
        /* Lite's WPF assembly is not referenced here, so this half is a source pin; Lite.Tests asserts the
           same thing against the live object. Both are needed: the source pin is what fails in THIS
           project's CI when Darling gains a note channel Lite never learned about. */
        var source = RepoFile.ReadRepoFile("Lite", "Services", "RemoteCollectorService.cs");

        Assert.Contains(
            "public string? Note => CollectorMeasurementNote.Compose(HostNote, Measurements);",
            source);

        /* The settable Note is GONE rather than merely unused — while it existed, the read site could pick
           up the host half and drop the definition half without anything failing to build. */
        Assert.DoesNotContain("public string? Note { get; set; }", source);

        /* And the bridge to the column still reads the composed property. */
        Assert.Contains("errorMessage = telemetry.Note;", source);

        /* Darling's own runner passes the LIVE list at its success return and the empty one everywhere else;
           the argument being required is what makes that a choice each site had to state. */
        var runner = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        var definitionRunner = RepoFile.ReadRepoFile(
            "Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs");

        Assert.Contains("storageMs, context.Measurements, collectionNote", Regex.Replace(runner, @"\s+", " "));
        Assert.Contains("telemetry.Measurements.AddRange(context.Measurements);", definitionRunner);
    }

    /* ── helpers ── */

    private static string Measured(params (string Label, long Value)[] counts)
    {
        var context = Context();
        foreach (var (label, value) in counts)
        {
            context.Measure(label, value);
        }

        return CollectorMeasurementNote.Render(context.Measurements)!;
    }

    private static CollectorContext Context() => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 9, 7, 12, 0, 0, DateTimeKind.Utc),
        Deltas = null!,
        Target = new CollectorTargetInfo(),
    };

    private const string ReportXml =
        "<blocked-process-report monitorLoop=\"7\">" +
        "<blocked-process><process spid=\"55\" ecid=\"0\" waittime=\"9000\" " +
        "waitresource=\"PAGE: 6:5:2093167\" currentdbname=\"target_database\">" +
        "<inputbuf>UPDATE dbo.some_table SET x = 1;</inputbuf></process></blocked-process>" +
        "<blocking-process><process spid=\"66\" ecid=\"0\"><inputbuf>BEGIN TRAN;</inputbuf></process></blocking-process>" +
        "</blocked-process-report>";

    /// <summary>
    /// The collector's real 5-column payload projection, one row per supplied report XML (null renders as
    /// SQL NULL — the "event captured, no report" case). CapturePlanXml stays false, which is the shape that
    /// omits the two trailing plan ordinals entirely.
    /// </summary>
    private static DataTableReader MakeReader(string?[] reports)
    {
        var payload = new DataTable("payload");
        payload.Columns.Add("event_time", typeof(DateTime));
        payload.Columns.Add("blocked_process_report_xml", typeof(string));
        payload.Columns.Add("object_id", typeof(int));
        payload.Columns.Add("database_id", typeof(int));
        payload.Columns.Add("contentious_object", typeof(string));

        foreach (var report in reports)
        {
            payload.Rows.Add(
                new DateTime(2026, 9, 7, 11, 59, 0, DateTimeKind.Utc),
                (object?)report ?? DBNull.Value,
                DBNull.Value,
                6,
                DBNull.Value);
        }

        var dataSet = new DataSet();
        dataSet.Tables.Add(payload);
        return dataSet.CreateDataReader();
    }
}
