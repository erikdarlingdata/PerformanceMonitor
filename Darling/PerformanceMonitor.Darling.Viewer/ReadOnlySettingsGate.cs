using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8115: on a read-only seat the Settings dialog's monitoring fields are shown for reference, as its banner says, so the
/// fields themselves refuse edits. Text boxes become read-only (their text still selects and copies); check boxes, combo
/// boxes, radio buttons, sliders and password boxes are disabled. Buttons are left alone: the ones that write guard
/// themselves, and the rest open windows that have their own read-only view.
/// </summary>
public static class ReadOnlySettingsGate
{
    /// <summary>Gates every input under <paramref name="root"/> in the logical tree, and returns how many it gated.</summary>
    /// <param name="skipNames">Named elements whose subtree is left alone: the viewer-local preferences that a read-only seat still saves.</param>
    public static int Apply(DependencyObject root, string tooltip, params string[] skipNames)
    {
        ArgumentNullException.ThrowIfNull(root);
        var gated = 0;
        foreach (var child in LogicalTreeHelper.GetChildren(root))
        {
            if (child is not DependencyObject node)
            {
                continue;
            }

            if (node is FrameworkElement { Name: { Length: > 0 } name } && Array.IndexOf(skipNames, name) >= 0)
            {
                continue;
            }

            switch (node)
            {
                case TextBox box:
                    box.IsReadOnly = true;
                    box.ToolTip ??= tooltip;
                    gated++;
                    break;
                case PasswordBox or ToggleButton or ComboBox or RangeBase:
                    var control = (Control)node;
                    control.IsEnabled = false;
                    control.ToolTip ??= tooltip;
                    gated++;
                    break;
            }

            gated += Apply(node, tooltip, skipNames);
        }

        return gated;
    }
}
