/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Automation;
using System.Windows.Automation.Peers;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;

/// <summary>
/// #6138: finds the Settings inputs a screen reader cannot name. Lives in Darling.Tests and is compiled into Lite.Tests by a link (one probe, two
/// suites); global namespace so both suites use it without a <c>using</c>.
/// </summary>
internal static class InputNamesProbe
{
    /// <summary>The text a label element shows, or null when the element is not a plain text label.</summary>
    private static string? LabelText(object? label) => label switch
    {
        TextBlock t => t.Text,
        Label { Content: string s } => s,
        ToggleButton { Content: string s } => s,
        _ => null
    };

    private static string Describe(FrameworkElement input, string problem)
    {
        var id = string.IsNullOrEmpty(input.Name) ? "(no x:Name)" : input.Name;
        return $"{input.GetType().Name} {id}: {problem}";
    }

    /// <summary>
    /// Every input on every tab that has no name, or whose name is not the text of its label. A box, drop-down or slider
    /// is named by the label element it points at (<c>AutomationProperties.LabeledBy</c>), so its name is that label's
    /// text; a check box or radio button is its own label, so its name is its own content.
    /// </summary>
    internal static List<string> Unnamed(Window window, TabControl tabs)
    {
        var problems = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < tabs.Items.Count; i++)
        {
            tabs.SelectedIndex = i;
            ClippedTextProbe.Settle(window);
            var tab = (TabItem)tabs.Items[i];
            foreach (var node in ClippedTextProbe.Descendants((DependencyObject)tab.Content))
            {
                if (node is not FrameworkElement input
                    || input.TemplatedParent is not null
                    || input is not (TextBox or PasswordBox or ComboBox or Slider or CheckBox or RadioButton))
                {
                    continue;
                }

                var name = UIElementAutomationPeer.CreatePeerForElement(input)?.GetName() ?? string.Empty;
                string? problem = null;
                if (string.IsNullOrWhiteSpace(name))
                {
                    problem = "has no automation name";
                }
                else if (input is ToggleButton)
                {
                    /* Named from its own content; a named label here would be a second answer. */
                }
                else if (AutomationProperties.GetLabeledBy(input) is { } target)
                {
                    /* Named by pointing at its label: the name is that label's text, so it cannot drift from what is on screen. */
                    var label = LabelText(target);
                    if (!string.Equals(name, label, StringComparison.Ordinal))
                    {
                        problem = $"is named \"{name}\" but its label reads \"{label}\"";
                    }
                }
                else if (!string.Equals(AutomationProperties.GetName(input), name, StringComparison.Ordinal))
                {
                    problem = $"is named \"{name}\" by neither its label nor an explicit name";
                }

                if (problem is not null)
                {
                    problems.Add($"[{tab.Header}] {Describe(input, problem)}");
                }
                else
                {
                    var key = $"[{tab.Header}] {name} ({UIElementAutomationPeer.CreatePeerForElement(input)?.GetAutomationControlType()})";
                    if (!seen.Add(key))
                    {
                        problems.Add($"{key}: a second input has the same name, so a screen reader cannot tell them apart");
                    }
                }
            }
        }

        return problems;
    }
}
