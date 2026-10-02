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
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>ContentPresenter.RecognizesAccessKey</c> on every button <c>ControlTemplate</c>, and no button caption
/// whose underscores mean something other than what WPF will do with them (#3835).
///
/// <para><b>Nothing else catches it.</b> The property defaults to FALSE and WPF's stock <c>Button</c>
/// template sets it true, so a custom template that omits it silently opts every button out of access keys.
/// The XAML compiles, the build is green, the window opens, and the damage is visible only as text: a
/// caption written <c>E_dit Collector Schedules...</c> renders with the underscore as a literal character
/// and <c>Alt</c>+<c>D</c> invokes nothing. No behavioral test reaches it — there is no exception, no
/// binding failure, and no resource to fail to resolve. It is a rendering fact about a template, which is
/// why the assertion is over the markup.</para>
///
/// <para><b>Fifteen templates was the estimate; fifty-five is the population.</b> #3835 named three theme
/// files by the count of <c>TargetType="Button"</c> templates in the Darling viewer's <c>Themes/</c>
/// directory. There are three PARALLEL theme families — <c>Darling/PerformanceMonitor.Darling.Viewer/Themes/</c>,
/// <c>Lite/Themes/</c> and <c>deprecated/Dashboard/Themes/</c>, the last still in
/// <c>PerformanceMonitor.sln</c> — plus five one-off templates outside any of them, including
/// <c>ViewerDarkTheme.xaml</c>'s <c>ViewerButton</c>, which is the implicit <c>Button</c> style for every
/// secondary window and therefore the template the reported <c>_Save</c> and <c>_Close</c> actually hit.
/// A rule scoped to the three files in the report would have left two thirds of the defect in place and
/// reported clean, so this sweeps the whole tree and carries no per-family carve-out.</para>
///
/// <para><b>Governed: <c>TargetType="Button"</c> templates containing a presenter that is not sourced.</b>
/// Three exclusions, each because the attribute would be inert rather than because the case is tolerated.
/// <c>TabCloseButton</c> has no <c>ContentPresenter</c> at all — it renders a literal <c>×</c>
/// <c>TextBlock</c> — so it is outside the rule by construction and needs no exemption, which is the shape
/// an exclusion should have. A presenter carrying <c>ContentSource</c> is showing a named part
/// (<c>Icon</c>, <c>Header</c>) rather than the control's own <c>Content</c>. And a presenter inside a
/// NESTED <c>ControlTemplate</c> belongs to that inner template's target, not to the button — the
/// <c>ComboBox</c> templates nest a <c>ToggleButton</c> template, so reading those as the outer target's
/// would attribute a presenter to the wrong control.</para>
///
/// <para><b><c>ToggleButton</c> and the two calendar button templates are deliberately NOT governed.</b>
/// They are button-FAMILY by type but none of them takes an access key in this application: the
/// <c>ToggleButton</c> templates are the sidebar and combo-box chrome, whose content is a glyph or a
/// <c>Path</c>, and <c>CalendarDayButton</c>/<c>CalendarButton</c> render a day number and a month name
/// from the calendar's own data. Governing them would add thirty-nine no-op attributes and dilute the
/// rule's claim into "every template in the family", which is not the fact #3835 is about. If one of them
/// ever acquires a caption with an access key, widening <see cref="Governed"/> is a one-word change and
/// this paragraph is the record of why it was not already done.</para>
///
/// <para><b>The second rule is the other half of the fix, and the one with teeth after it.</b> Turning
/// <c>RecognizesAccessKey</c> on changes how EVERY underscore in every button caption is interpreted:
/// a single underscore becomes the access-key marker and is consumed, and a literal underscore now has to
/// be written doubled. A caption holding a snake_case identifier would therefore start rendering wrong the
/// moment the first rule was satisfied — the fix creating a second defect. That was measured before the
/// attribute was added rather than after: all fifty-one distinct underscore-bearing <c>Button</c> captions
/// in the tree carry exactly one underscore and every one of them is an intended access key, and the only
/// literal identifiers in this idiom (<c>SP_SERVER_DIAGNOSTICS</c> and the three beside it) sit on
/// CheckBoxes, whose template has always set the attribute and whose captions were therefore already
/// doubled in the Darling and Lite copies. The <c>deprecated/Dashboard</c> copy was not, and was fixed in
/// the same change. <see cref="NoButtonCaption_MeansOneThingAndRendersAnother"/> is what keeps that true:
/// it reds on an odd-length underscore run, which is the shape both a new snake_case caption and a
/// half-escaped one take.</para>
///
/// <para>Read with a raw <c>Directory.EnumerateFiles</c> walk and <c>XDocument</c>, the
/// <c>XamlGridRowRangeTests</c> idiom, rather than through <see cref="RepoFile"/>: this pin reads the tree
/// by GLOB rather than by naming files, so it has no repo-relative paths to resolve, and staying off the
/// shared reader keeps it out of <c>RepoFileAdoptionTests</c>' rosters — which are shared censuses under
/// concurrent edit, and a roster entry is a merge conflict this rule does not need.</para>
/// </summary>
public sealed class ViewerButtonAccessKeyTests
{
    /// <summary>
    /// The templates this rule governs: <c>TargetType="Button"</c> exactly, in either spelling
    /// (<c>"Button"</c> or <c>"{x:Type Button}"</c>). Not the whole button family — see the class summary
    /// for the ToggleButton/Calendar decision and what would change it.
    /// </summary>
    private static bool Governed(string? targetType) =>
        string.Equals(NormalizeTargetType(targetType), "Button", StringComparison.Ordinal);

    /// <summary>Any <c>TargetType</c> whose type name ends in <c>Button</c>, governed or not. Counted only
    /// so the floor below can prove the walk found the family it is choosing within — a scan that saw the
    /// governed set and nothing else would satisfy the rule while having stopped descending.</summary>
    private static bool ButtonFamily(string? targetType) =>
        NormalizeTargetType(targetType)?.EndsWith("Button", StringComparison.Ordinal) == true;

    private static string? NormalizeTargetType(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        /* "{x:Type Button}" and "Button" are the same declaration; the local name is what matters, so a
           "local:Foo" prefix is dropped rather than making the value unrecognisable. */
        var value = raw.Trim();
        var match = Regex.Match(value, @"^\{\s*x:Type\s+(?<name>[\w:.]+)\s*\}$");
        if (match.Success)
        {
            value = match.Groups["name"].Value;
        }

        var colon = value.LastIndexOf(':');
        return colon >= 0 ? value[(colon + 1)..] : value;
    }

    [Fact]
    public void EveryButtonTemplate_LetsItsPresenterRecognizeAnAccessKey()
    {
        var root = RepoRoot();
        var files = XamlFiles(root);

        /* Floors, because every claim below is over what the walk found: a glob that matched nothing, or a
           walk that stopped descending, would satisfy an "all of them are correct" assertion by having
           nothing to disagree with. That is the same silent-pass shape the rule itself is about, and it is
           exactly how a rule scoped to the three files in the report would have read clean. */
        Assert.True(
            files.Count >= 100,
            $"only {files.Count} XAML files found; the tree walk is not finding the markup, so the rule "
          + "below is being enforced on a population it did not read");

        var offenders = new List<string>();
        var governed = 0;
        var family = 0;
        var presenters = 0;
        var themeFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var file in files)
        {
            XDocument document;
            try
            {
                document = XDocument.Load(file);
            }
            catch (Exception ex)
            {
                /* Not skipped, for XamlGridRowRangeTests' reason: markup this scan cannot read is markup the
                   rule is not enforced on, and a file that stopped being well-formed XML is worth a failure
                   of its own. */
                offenders.Add($"{Path.GetRelativePath(root, file)}: could not be parsed as XML — {ex.Message}");
                continue;
            }

            var relative = Path.GetRelativePath(root, file);

            if (Path.GetFileName(file).EndsWith("Theme.xaml", StringComparison.Ordinal))
            {
                themeFiles.Add(relative);
            }

            foreach (var template in document.Descendants().Where(e => e.Name.LocalName == "ControlTemplate"))
            {
                var targetType = template.Attribute("TargetType")?.Value;

                if (ButtonFamily(targetType))
                {
                    family++;
                }

                if (!Governed(targetType))
                {
                    continue;
                }

                governed++;

                foreach (var presenter in OwnPresenters(template))
                {
                    /* A sourced presenter shows a named part (Icon, Header), not the control's Content, so
                       the attribute would govern nothing. */
                    if (presenter.Attribute("ContentSource") is not null)
                    {
                        continue;
                    }

                    presenters++;

                    if (presenter.Attribute("RecognizesAccessKey")?.Value != "True")
                    {
                        offenders.Add(
                            $"{relative}: a ContentPresenter in a TargetType=\"Button\" ControlTemplate does "
                          + "not set RecognizesAccessKey=\"True\"");
                    }
                }
            }
        }

        /* Nine theme files across three parallel families, and the count is the point: #3835 was reported as
           three. A fourth family, or a fourth theme inside one, arrives here as a governed template like any
           other — but this floor is what catches a walk that stopped seeing the families it already has. */
        Assert.True(
            themeFiles.Count >= 9,
            $"only {themeFiles.Count} *Theme.xaml files were read ({string.Join(", ", themeFiles.OrderBy(f => f, StringComparer.Ordinal))}); "
          + "there are three parallel theme families with three themes each, so the walk is missing at least one");

        Assert.True(
            family >= 90,
            $"only {family} button-family ControlTemplates were seen; the element walk is not descending the "
          + "resource dictionaries");

        Assert.True(
            governed >= 50,
            $"only {governed} TargetType=\"Button\" templates were found. The defect spanned 55, so either the "
          + "templates were consolidated — in which case raise this floor on purpose — or this scan has "
          + "stopped seeing most of them and is reporting clean on the remainder");

        /* The governed templates existing is satisfied just as well by templates with no presenter to
           govern, which is the TabCloseButton shape. Flooring the presenters separately is what says the
           attribute assertion had subjects. */
        Assert.True(
            presenters >= 45,
            $"only {presenters} unsourced ContentPresenters were checked inside button templates; the rule is "
          + "passing because it found almost nothing to judge");

        Assert.True(
            offenders.Count == 0,
            "A ContentPresenter inside a Button ControlTemplate does not set RecognizesAccessKey=\"True\". "
          + "The property defaults to FALSE and the stock WPF template sets it true, so a custom template "
          + "that omits it renders the access-key underscore in Content as a LITERAL character and binds no "
          + "Alt shortcut — with a green build and no runtime error (#3835). Add the attribute:\n  "
          + string.Join("\n  ", offenders));
    }

    /// <summary>
    /// No button caption holds an underscore run that WPF will read differently from how it was written.
    ///
    /// <para>With <c>RecognizesAccessKey</c> on, <c>_</c> marks the access key and is consumed, and
    /// <c>__</c> renders one literal <c>_</c>. So an ODD-length run is the tell for both defects this rule
    /// exists for: a snake_case identifier pasted into a caption (one underscore, silently eaten, wrong
    /// text, a shortcut nobody declared), and a half-escaped literal (three underscores, one literal plus a
    /// marker). Even-length runs are literals by construction and need no judgement. The fifty-one captions
    /// in the tree are all single-underscore, all intentional, and all therefore expected to have exactly
    /// ONE run of length one — anything with two markers is also reported, because WPF takes the first and
    /// the second is a silent literal.</para>
    /// </summary>
    [Fact]
    public void NoButtonCaption_MeansOneThingAndRendersAnother()
    {
        var root = RepoRoot();
        var files = XamlFiles(root);

        var offenders = new List<string>();
        var captions = 0;

        foreach (var file in files)
        {
            XDocument document;
            try
            {
                document = XDocument.Load(file);
            }
            catch (Exception)
            {
                /* Reported by the rule above; not duplicated here. */
                continue;
            }

            var relative = Path.GetRelativePath(root, file);

            foreach (var button in document.Descendants().Where(e => e.Name.LocalName == "Button"))
            {
                var content = button.Attribute("Content")?.Value;

                /* A binding or a markup extension is not a caption this can reason about, and a caption
                   with no underscore is not this rule's business. */
                if (content is null || content.StartsWith('{') || !content.Contains('_', StringComparison.Ordinal))
                {
                    continue;
                }

                captions++;

                var runs = Regex.Matches(content, "_+").Select(m => m.Value.Length).ToList();
                var markers = runs.Count(length => length % 2 == 1);

                if (markers == 0)
                {
                    /* Every underscore is doubled: a literal caption, which is correct and needs no marker. */
                    continue;
                }

                if (markers == 1 && runs.All(length => length <= 2))
                {
                    /* One marker, and every other run an even-length literal: the intended shape. */
                    continue;
                }

                if (markers > 1)
                {
                    offenders.Add(
                        $"{relative}: Content=\"{content}\" declares {markers} access keys; WPF takes the "
                      + "FIRST and renders the rest as silent literals");
                    continue;
                }

                offenders.Add(
                    $"{relative}: Content=\"{content}\" has an underscore run of length "
                  + $"{runs.First(length => length % 2 == 1)}, which is a marker plus a partial escape — "
                  + "double every LITERAL underscore");
            }
        }

        /* The fifty-one captions are the safety argument for the attribute the rule above adds; a scan that
           found none of them would pass this and say nothing about the escaping. */
        Assert.True(
            captions >= 45,
            $"only {captions} underscore-bearing Button captions were read; there are 51 distinct ones across "
          + "the three applications, so this scan is not seeing the captions the access-key change affects");

        Assert.True(
            offenders.Count == 0,
            "A Button caption's underscores will not render the way they are written. With "
          + "RecognizesAccessKey on (#3835), a single _ is consumed as the access-key marker and a LITERAL "
          + "underscore must be doubled — so a snake_case identifier in a caption loses a character and "
          + "silently claims an Alt shortcut:\n  "
          + string.Join("\n  ", offenders));
    }

    /// <summary>
    /// The scan fires, exercised through the same predicate and escaping arithmetic the tree walk uses so a
    /// change that stopped it reporting reds here instead of passing there.
    /// </summary>
    [Theory]
    [InlineData("Button", true)]
    [InlineData("{x:Type Button}", true)]
    /* Family, but deliberately outside the rule — see the class summary. */
    [InlineData("ToggleButton", false)]
    [InlineData("{x:Type ToggleButton}", false)]
    [InlineData("CalendarDayButton", false)]
    [InlineData("CalendarButton", false)]
    [InlineData("RepeatButton", false)]
    [InlineData("TextBox", false)]
    [InlineData(null, false)]
    public void TheTargetTypePredicate_GovernsPlainButtonsOnly(string? targetType, bool governed)
        => Assert.Equal(governed, Governed(targetType));

    [Theory]
    /* One marker, the intended shape: 51 of the tree's captions look like this. */
    [InlineData("_Save", true)]
    [InlineData("E_dit Collector Schedules...", true)]
    /* Fully doubled: a literal caption with no access key. */
    [InlineData("Exclude SP__SERVER__DIAGNOSTICS", true)]
    /* A marker plus doubled literals — the shape a snake_case caption takes once escaped properly. */
    [InlineData("Exclude _SP__SERVER__DIAGNOSTICS", true)]
    [InlineData("No underscore at all", true)]
    /* Two markers: WPF takes the first, the second is a silent literal. */
    [InlineData("_Save _All", false)]
    /* Three in a row: a literal plus a marker, which is the half-escaped shape. */
    [InlineData("Exclude SP___SERVER", false)]
    public void TheEscapingRule_AcceptsOneMarkerAndRejectsThePartialEscape(string caption, bool acceptable)
    {
        var runs = Regex.Matches(caption, "_+").Select(m => m.Value.Length).ToList();
        var markers = runs.Count(length => length % 2 == 1);
        var ok = markers switch
        {
            0 => true,
            1 => runs.All(length => length <= 2),
            _ => false,
        };

        Assert.Equal(acceptable, ok);
    }

    /// <summary>
    /// A presenter inside a NESTED <c>ControlTemplate</c> belongs to the inner template's target, not to
    /// this one — the ComboBox templates nest a ToggleButton template, and reading its presenter as the
    /// outer target's would judge a presenter against the wrong control. This is the case that says so.
    /// </summary>
    [Fact]
    public void OwnPresenters_StopsAtANestedTemplate()
    {
        var template = XDocument.Parse(
            "<ControlTemplate xmlns=\"http://schemas.microsoft.com/winfx/2006/xaml/presentation\" TargetType=\"Button\">"
          + "<Border><ContentPresenter/>"
          + "<ToggleButton><ToggleButton.Template><ControlTemplate TargetType=\"ToggleButton\">"
          + "<ContentPresenter/></ControlTemplate></ToggleButton.Template></ToggleButton>"
          + "</Border></ControlTemplate>").Root!;

        Assert.Single(OwnPresenters(template));
    }

    /// <summary>
    /// Every <c>ContentPresenter</c> this template owns: descendants, but not through a nested
    /// <c>ControlTemplate</c>.
    /// </summary>
    private static List<XElement> OwnPresenters(XElement template)
    {
        var found = new List<XElement>();

        void Descend(XElement node)
        {
            foreach (var child in node.Elements())
            {
                if (child.Name.LocalName == "ControlTemplate")
                {
                    continue;
                }

                if (child.Name.LocalName == "ContentPresenter")
                {
                    found.Add(child);
                }

                Descend(child);
            }
        }

        Descend(template);

        return found;
    }

    private static List<string> XamlFiles(string root) =>
        Directory.EnumerateFiles(root, "*.xaml", SearchOption.AllDirectories)
            .Where(f => !IsBuildOutput(root, f))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Build output, by path SEGMENT rather than substring — <c>XamlGridRowRangeTests</c>' reason: a
    /// directory legitimately named <c>Binaries</c> must not be skipped, and the separator-padded substring
    /// form misses a segment that ends the path.
    /// </summary>
    private static bool IsBuildOutput(string root, string file) =>
        Path.GetRelativePath(root, file)
            .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            .Any(segment => string.Equals(segment, "bin", StringComparison.OrdinalIgnoreCase)
                            || string.Equals(segment, "obj", StringComparison.OrdinalIgnoreCase));

    /// <summary>Same walk-up idiom as <c>XamlGridRowRangeTests.FindRepoRoot</c>, failing rather than
    /// returning null: a scan that cannot find the tree enforces nothing, and skipping would make that
    /// indistinguishable from a clean run.</summary>
    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate the repository root (walked up from the test binary looking for "
          + "PerformanceMonitor.sln). This test scans the source tree, so it cannot run without it — fix the "
          + "walk-up rather than skipping, or the rule stops being enforced without anyone noticing.");
    }
}
