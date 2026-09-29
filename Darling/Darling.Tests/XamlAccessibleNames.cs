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
using System.Xml;
using System.Xml.Linq;

namespace Darling.Tests;

/// <summary>
/// The scan behind the "every Button, ToggleButton, MenuItem and TabItem has a UI Automation name source"
/// pins (<c>ViewerXamlAccessibleNameTests</c> here, <c>XamlAccessibleNameTests</c> in Lite.Tests, which compiles
/// this file by link the way it compiles <c>CSharpSourceWalker</c>). One implementation, so the two apps
/// cannot drift into different definitions of "named".
///
/// <para><b>What a name source is.</b> UI Automation reads a control's Name from
/// <c>AutomationProperties.Name</c>, then <c>LabeledBy</c>, then the control's own text. A <c>Content</c> or
/// <c>Header</c> that is a STRING (or a binding or resource that resolves to one) supplies that text. Content
/// that is an element tree (a <c>StackPanel</c> holding a glyph and a label, a <c>Path</c>) does not: UIA
/// falls back to <c>ToString()</c>, which is how the plan sub-tabs came to read
/// "System.Windows.Controls.TabItem Header:..." in the UIA pass behind #4684. A string with no letter or digit
/// (a Segoe MDL2 private-use glyph, a "+", an "X", a chevron) is a name UIA can read out but no person can
/// hear, so it does not count either. Both need an explicit <c>AutomationProperties.Name</c>.</para>
///
/// <para><b>Instances only.</b> An element inside a <c>ControlTemplate</c> is a template part (the toggle
/// inside a ComboBox chrome), not a control a user meets, so the walk skips it. Buttons and menu items in a
/// <c>DataTemplate</c>, a <c>ContextMenu</c> or a <c>Style</c> setter's value are real and are checked.</para>
/// </summary>
internal static class XamlAccessibleNames
{
    private static readonly XNamespace X = "http://schemas.microsoft.com/winfx/2006/xaml";

    private static readonly HashSet<string> Governed = new(StringComparer.Ordinal)
    {
        "Button", "ToggleButton", "MenuItem", "TabItem",
    };

    /// <summary>One governed element that carries no name source.</summary>
    internal sealed record Unnamed(string File, int Line, string Type, string Key)
    {
        /// <summary>The allowlist identity: file, element type and the handler or name that tells two
        /// elements in one file apart. No line number, so an unrelated edit above it does not stale the entry.</summary>
        public string Identity => $"{File}|{Type}|{Key}";

        public override string ToString() => $"{File}:{Line} <{Type}> [{Key}]";
    }

    internal sealed record ScanResult(
        int FileCount,
        IReadOnlyDictionary<string, int> CheckedByType,
        IReadOnlyList<Unnamed> Unnamed);

    internal static ScanResult Run(string repoRoot, params string[] relativeRoots)
    {
        var checkedByType = new Dictionary<string, int>(StringComparer.Ordinal);
        var unnamed = new List<Unnamed>();
        var fileCount = 0;

        foreach (var relativeRoot in relativeRoots)
        {
            var directory = Path.Combine(repoRoot, relativeRoot.Replace('/', Path.DirectorySeparatorChar));
            var files = Directory.EnumerateFiles(directory, "*.xaml", SearchOption.AllDirectories)
                .Where(f => !IsBuildOutput(repoRoot, f))
                .OrderBy(f => f, StringComparer.Ordinal);

            foreach (var file in files)
            {
                fileCount++;
                var document = XDocument.Load(file, LoadOptions.SetLineInfo);
                var relative = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');

                foreach (var element in document.Descendants())
                {
                    var type = element.Name.LocalName;
                    if (!Governed.Contains(type))
                    {
                        continue;
                    }

                    if (element.Ancestors().Any(a => a.Name.LocalName == "ControlTemplate"))
                    {
                        continue;
                    }

                    checkedByType[type] = checkedByType.GetValueOrDefault(type) + 1;

                    if (!HasNameSource(element, type))
                    {
                        unnamed.Add(new Unnamed(relative, ((IXmlLineInfo)element).LineNumber, type, KeyOf(element)));
                    }
                }
            }
        }

        return new ScanResult(fileCount, checkedByType, unnamed);
    }

    private static bool HasNameSource(XElement element, string type)
    {
        var explicitName = element.Attribute("AutomationProperties.Name");
        if (explicitName is not null && explicitName.Value.Trim().Length > 0)
        {
            return true;
        }

        if (element.Attribute("AutomationProperties.LabeledBy") is not null)
        {
            return true;
        }

        var property = type is "MenuItem" or "TabItem" ? "Header" : "Content";

        var attribute = element.Attribute(property);
        if (attribute is not null)
        {
            return IsNameText(attribute.Value);
        }

        var propertyElement = element.Elements().FirstOrDefault(c => c.Name.LocalName == type + "." + property);
        if (propertyElement is not null)
        {
            return !propertyElement.HasElements && IsNameText(propertyElement.Value);
        }

        /* Plain text between the tags is the Content of a button; a MenuItem or TabItem body is not its name. */
        if (property == "Content" && !element.Elements().Any(c => !c.Name.LocalName.Contains('.')))
        {
            return IsNameText(string.Concat(element.Nodes().OfType<XText>().Select(t => t.Value)));
        }

        return false;
    }

    private static bool IsNameText(string value)
    {
        var text = value.Trim();
        if (text.Length == 0)
        {
            return false;
        }

        if (text.StartsWith("{}", StringComparison.Ordinal))
        {
            text = text[2..];
        }
        else if (text.StartsWith('{'))
        {
            /* A binding, StaticResource, DynamicResource or x:Static resolves to text at run time; only an
               explicit null is not a name. */
            return !text.StartsWith("{x:Null", StringComparison.OrdinalIgnoreCase);
        }

        return text.Any(char.IsLetterOrDigit);
    }

    private static string KeyOf(XElement element)
    {
        foreach (var candidate in new[] { element.Attribute("Click")?.Value, element.Attribute("Command")?.Value })
        {
            if (!string.IsNullOrEmpty(candidate))
            {
                return candidate;
            }
        }

        return element.Attribute(X + "Name")?.Value ?? element.Attribute("Name")?.Value ?? "(anonymous)";
    }

    private static bool IsBuildOutput(string root, string file) =>
        Path.GetRelativePath(root, file)
            .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            .Any(segment => string.Equals(segment, "bin", StringComparison.OrdinalIgnoreCase)
                            || string.Equals(segment, "obj", StringComparison.OrdinalIgnoreCase));

    /// <summary>Walk up from the test binary to the folder holding <c>PerformanceMonitor.sln</c>, failing
    /// rather than skipping: a scan that cannot find the tree enforces nothing.</summary>
    internal static string RepoRoot()
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
          + "PerformanceMonitor.sln). This test scans the source tree, so it cannot run without it.");
    }

    /// <summary>The comparison both pins make: the elements found with no name source must be EXACTLY the
    /// allowlist (identity and count), so a new unnamed control fails the test and a fixed one forces its
    /// allowlist entry to be deleted.</summary>
    internal static string? Diff(IReadOnlyList<Unnamed> found, IEnumerable<(string File, string Type, string Key, string Issue)> allowed)
    {
        var allowedList = allowed.ToList();
        var bad = allowedList.Where(a => !a.Issue.StartsWith('#')).Select(a => a.File + "|" + a.Type + "|" + a.Key).ToList();
        if (bad.Count > 0)
        {
            return "allowlist entries with no issue number: " + string.Join("; ", bad);
        }

        var foundCounts = found.GroupBy(u => u.Identity, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        var allowedCounts = allowedList.GroupBy(a => a.File + "|" + a.Type + "|" + a.Key, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);

        var lines = new List<string>();
        foreach (var (identity, count) in foundCounts)
        {
            var allowedCount = allowedCounts.GetValueOrDefault(identity);
            if (count > allowedCount)
            {
                var where = string.Join(", ", found.Where(u => u.Identity == identity).Select(u => "line " + u.Line));
                lines.Add($"NO NAME SOURCE ({count} found, {allowedCount} allowed): {identity} ({where})");
            }
        }

        foreach (var (identity, count) in allowedCounts)
        {
            if (foundCounts.GetValueOrDefault(identity) < count)
            {
                lines.Add($"STALE ALLOWLIST ENTRY (now named, delete it): {identity}");
            }
        }

        return lines.Count == 0 ? null : string.Join(Environment.NewLine, lines);
    }
}
