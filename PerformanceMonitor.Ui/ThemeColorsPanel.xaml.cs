/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The Colors section of the Settings window (#3577, arm B): the twelve adjustable palette colors of the
/// theme currently applied, each with a swatch, its hex, a picker and the measured WCAG contrast ratio
/// against the surface it renders on; Apply, Reset to default and Open file underneath.
///
/// <para><b>It edits the CURRENT theme.</b> The theme combo beside it live-previews a theme through
/// <see cref="ThemeManager.Apply"/>, and this control follows <see cref="ThemeManager.ThemeChanged"/>, so
/// picking Cool Breeze in the combo turns these into Cool Breeze's rows. Overrides are stored per theme
/// (the base palettes differ), so that is the only sensible thing for the rows to mean.</para>
///
/// <para><b>Nothing is applied until Apply.</b> Typing a hex or moving a slider updates the row's swatch
/// and every readout that depends on it — a new page background re-measures every text and status row —
/// but the app keeps its current colors until the button, because each Apply is a regeneration of the
/// theme dictionary and the operator should choose when that happens. Apply writes the file and
/// re-applies live; Reset removes this theme's block and re-applies; Open file hands the JSON to the
/// shell's editor, and <see cref="ThemeManager.WatchOverridesFile"/> applies whatever is saved there.</para>
///
/// <para><b>The readout warns and never refuses.</b> A ratio under 4.5:1 is shown in amber (3–4.5) or red
/// (under 3) with a one-line note beneath the rows, and Apply still works. The operator asked for the
/// ability to maintain their own colors; an app that overrules them on a threshold it measured is not
/// giving them that. A value that is not a color at all is the one thing Apply declines, because there is
/// nothing to apply.</para>
/// </summary>
public partial class ThemeColorsPanel : UserControl
{
    private sealed class Row
    {
        public required ThemeColorSlot Slot { get; init; }
        public required Border Swatch { get; init; }
        public required TextBox Hex { get; init; }
        public required TextBlock Readout { get; init; }

        /// <summary>The last hex the row held that parsed. A row mid-edit measures with this.</summary>
        public Color Current { get; set; }

        public bool IsValid { get; set; } = true;
    }

    private readonly List<Row> _rows = new();
    private readonly Dictionary<string, Row> _rowsByKey = new(StringComparer.Ordinal);
    private bool _suppressTextChanged;
    private bool _suppressSliders;
    private Row? _pickerRow;

    public ThemeColorsPanel()
    {
        InitializeComponent();
        BuildRows();
        Loaded += (_, _) =>
        {
            ThemeManager.ThemeChanged += OnThemeChanged;
            Reload();
        };
        Unloaded += (_, _) => ThemeManager.ThemeChanged -= OnThemeChanged;
    }

    /* ------------------------------------------------------------------------------------------------
       Rows
       ------------------------------------------------------------------------------------------------ */

    private void BuildRows()
    {
        var rowIndex = 0;
        string? lastGroup = null;

        foreach (var slot in ThemeColorOverrides.Slots)
        {
            if (slot.Group != lastGroup)
            {
                RowsGrid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
                var header = new TextBlock
                {
                    Text = slot.Group,
                    FontSize = 11,
                    FontWeight = FontWeights.SemiBold,
                    Margin = new Thickness(0, lastGroup is null ? 0 : 8, 0, 2),
                };
                header.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundDimBrush");
                Grid.SetRow(header, rowIndex);
                Grid.SetColumnSpan(header, 5);
                RowsGrid.Children.Add(header);
                rowIndex++;
                lastGroup = slot.Group;
            }

            RowsGrid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

            var swatch = new Border
            {
                Width = 18,
                Height = 18,
                CornerRadius = new CornerRadius(3),
                BorderThickness = new Thickness(1),
                Margin = new Thickness(0, 2, 8, 2),
                VerticalAlignment = VerticalAlignment.Center,
            };
            swatch.SetResourceReference(Border.BorderBrushProperty, "BorderLightBrush");

            var label = new TextBlock
            {
                Text = slot.Label,
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(0, 0, 8, 0),
                MinWidth = 110,
                ToolTip = $"{slot.Key} — {slot.Meaning}",
            };
            label.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundBrush");

            var hex = new TextBox
            {
                Width = 84,
                FontFamily = new FontFamily("Consolas"),
                VerticalAlignment = VerticalAlignment.Center,
                VerticalContentAlignment = VerticalAlignment.Center,
                Margin = new Thickness(0, 1, 4, 1),
                ToolTip = "#RRGGBB (or #AARRGGBB)",
            };

            var pick = new Button
            {
                Content = "\u2026",
                Padding = new Thickness(6, 0, 6, 0),
                MinWidth = 24,
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(0, 1, 10, 1),
                ToolTip = "Pick a color",
            };

            var readout = new TextBlock
            {
                FontSize = 11,
                VerticalAlignment = VerticalAlignment.Center,
            };

            Grid.SetRow(swatch, rowIndex);
            Grid.SetColumn(swatch, 0);
            Grid.SetRow(label, rowIndex);
            Grid.SetColumn(label, 1);
            Grid.SetRow(hex, rowIndex);
            Grid.SetColumn(hex, 2);
            Grid.SetRow(pick, rowIndex);
            Grid.SetColumn(pick, 3);
            Grid.SetRow(readout, rowIndex);
            Grid.SetColumn(readout, 4);

            RowsGrid.Children.Add(swatch);
            RowsGrid.Children.Add(label);
            RowsGrid.Children.Add(hex);
            RowsGrid.Children.Add(pick);
            RowsGrid.Children.Add(readout);

            var row = new Row { Slot = slot, Swatch = swatch, Hex = hex, Readout = readout };
            hex.TextChanged += (_, _) => OnHexChanged(row);
            pick.Click += (_, _) => OpenPicker(row, pick);

            _rows.Add(row);
            _rowsByKey[slot.Key] = row;
            rowIndex++;
        }
    }

    /// <summary>Re-seeds every row from the current theme's stock palette plus its loaded overrides.</summary>
    private void Reload()
    {
        var theme = ThemeManager.CurrentTheme;
        var stock = ThemeManager.StockPalette(theme);
        var overrides = ThemeManager.Overrides.For(theme);

        ThemeNameText.Text = $"{ThemeDisplayName(theme)} theme — " +
                             (overrides.Count > 0 ? $"{overrides.Count} override{(overrides.Count == 1 ? "" : "s")} in effect" : "stock palette");

        _suppressTextChanged = true;
        try
        {
            foreach (var row in _rows)
            {
                var color = overrides.TryGetValue(row.Slot.Key, out var o)
                    ? o
                    : stock.TryGetValue(row.Slot.Key, out var s) ? s : Colors.Transparent;

                row.Current = color;
                row.IsValid = true;
                row.Hex.Text = ThemeColorOverrides.ToHex(color);
                row.Hex.FontWeight = overrides.ContainsKey(row.Slot.Key) ? FontWeights.SemiBold : FontWeights.Normal;
                row.Swatch.Background = new SolidColorBrush(color);
                row.Swatch.SetResourceReference(Border.BorderBrushProperty, "BorderLightBrush");
            }
        }
        finally
        {
            _suppressTextChanged = false;
        }

        RefreshReadouts();

        var enabled = ThemeManager.OverridesFilePath is not null;
        ApplyButton.IsEnabled = enabled;
        ResetButton.IsEnabled = enabled;
        OpenFileButton.IsEnabled = enabled;
        PathText.Text = enabled
            ? $"Stored per user in {ThemeManager.OverridesFilePath}"
            : "This app has not configured a color-overrides file.";
        StatusText.Text = string.Empty;
    }

    private void OnThemeChanged(string _)
    {
        Reload();
    }

    private void OnHexChanged(Row row)
    {
        if (_suppressTextChanged)
        {
            return;
        }

        if (ThemeColorOverrides.TryParseHex(row.Hex.Text, out var color))
        {
            row.Current = color;
            row.IsValid = true;
            row.Swatch.Background = new SolidColorBrush(color);
            row.Swatch.SetResourceReference(Border.BorderBrushProperty, "BorderLightBrush");
        }
        else
        {
            /* The swatch's border goes red and the readout says so; the text box itself is left alone,
               because the theme's TextBox template paints the focused border in the accent and the
               operator is, at this moment, focused on it. */
            row.IsValid = false;
            row.Swatch.SetResourceReference(Border.BorderBrushProperty, "ErrorBrush");
        }

        /* Every readout, not just this row's: a new page background changes what every text and status
           row measures against, and a new text color changes what every background row does. */
        RefreshReadouts();
        StatusText.Text = string.Empty;
    }

    private void RefreshReadouts()
    {
        var below = new List<string>();

        foreach (var row in _rows)
        {
            if (!row.IsValid)
            {
                row.Readout.Text = "not a color — use #RRGGBB";
                row.Readout.SetResourceReference(TextBlock.ForegroundProperty, "ErrorBrush");
                continue;
            }

            if (!_rowsByKey.TryGetValue(row.Slot.ContrastAgainst, out var partner))
            {
                row.Readout.Text = string.Empty;
                continue;
            }

            var ratio = WcagContrast.Ratio(row.Current, partner.Current);
            var band = WcagContrast.Band(ratio);
            row.Readout.Text = $"{WcagContrast.Format(ratio)} vs {partner.Slot.Label.ToLowerInvariant()}";
            row.Readout.SetResourceReference(TextBlock.ForegroundProperty, band switch
            {
                ContrastBand.Pass => "SuccessBrush",
                ContrastBand.Marginal => "WarningTextBrush",
                _ => "ErrorBrush",
            });

            if (band != ContrastBand.Pass)
            {
                below.Add(row.Slot.Label);
            }
        }

        if (below.Count == 0)
        {
            ContrastNote.Visibility = Visibility.Collapsed;
        }
        else
        {
            ContrastNote.Text = $"Below 4.50:1 — harder to read as text (WCAG AA): {string.Join(", ", below)}. " +
                                "Apply still works; this is a measurement, not a gate.";
            ContrastNote.Visibility = Visibility.Visible;
        }
    }

    private static string ThemeDisplayName(string theme) => theme switch
    {
        "CoolBreeze" => "Cool Breeze",
        _ => theme,
    };

    /* ------------------------------------------------------------------------------------------------
       Buttons
       ------------------------------------------------------------------------------------------------ */

    private void ApplyButton_Click(object sender, RoutedEventArgs e)
    {
        var invalid = _rows.Where(r => !r.IsValid).Select(r => r.Slot.Label).ToList();
        if (invalid.Count > 0)
        {
            StatusText.Text = $"Not applied — not a color: {string.Join(", ", invalid)}.";
            return;
        }

        var theme = ThemeManager.CurrentTheme;
        var stock = ThemeManager.StockPalette(theme);

        /* Only what differs from the stock palette is an override. A row typed back to its default is not
           stored, so Reset and "type the default back in" leave the same file behind, and the file only
           ever says what the operator changed. */
        var overrides = new Dictionary<string, Color>(StringComparer.Ordinal);
        foreach (var row in _rows)
        {
            if (!stock.TryGetValue(row.Slot.Key, out var stockColor) || stockColor != row.Current)
            {
                overrides[row.Slot.Key] = row.Current;
            }
        }

        if (ThemeManager.SaveOverridesAndApply(theme, overrides))
        {
            /* Reload has already run via ThemeChanged; the status line is set after it so it survives. */
            StatusText.Text = overrides.Count == 0
                ? "Applied — this theme is back on its stock palette."
                : $"Applied — {overrides.Count} override{(overrides.Count == 1 ? "" : "s")} saved.";
        }
        else
        {
            StatusText.Text = "Not saved — see the log for why.";
        }
    }

    private void ResetButton_Click(object sender, RoutedEventArgs e)
    {
        var theme = ThemeManager.CurrentTheme;
        if (ThemeManager.SaveOverridesAndApply(theme, new Dictionary<string, Color>(StringComparer.Ordinal)))
        {
            StatusText.Text = $"{ThemeDisplayName(theme)} is back on its stock palette.";
        }
        else
        {
            StatusText.Text = "Not reset — see the log for why.";
        }
    }

    private void OpenFileButton_Click(object sender, RoutedEventArgs e)
    {
        var path = ThemeManager.EnsureOverridesFileExists(ThemeManager.CurrentTheme);
        if (path is null)
        {
            StatusText.Text = "The file could not be created — see the log.";
            return;
        }

        try
        {
            Process.Start(new ProcessStartInfo { FileName = path, UseShellExecute = true });
            StatusText.Text = "Opened in your editor — saved edits apply live.";
        }
        catch (Exception ex)
        {
            StatusText.Text = $"Could not open an editor ({ex.Message}). The file is at the path below.";
        }
    }

    /* ------------------------------------------------------------------------------------------------
       Picker
       ------------------------------------------------------------------------------------------------ */

    private void OpenPicker(Row row, Button anchor)
    {
        _pickerRow = row;
        var hsv = HsvColor.FromColor(row.Current);

        _suppressSliders = true;
        try
        {
            HueSlider.Value = hsv.Hue;
            SaturationSlider.Value = hsv.Saturation * 100;
            BrightnessSlider.Value = hsv.Value * 100;
        }
        finally
        {
            _suppressSliders = false;
        }

        UpdatePickerPreview(row.Current);
        PickerPopup.PlacementTarget = anchor;
        PickerPopup.IsOpen = true;
    }

    private void PickerSlider_ValueChanged(object sender, RoutedPropertyChangedEventArgs<double> e)
    {
        if (_suppressSliders || _pickerRow is null)
        {
            return;
        }

        var color = new HsvColor(HueSlider.Value, SaturationSlider.Value / 100, BrightnessSlider.Value / 100).ToColor();
        UpdatePickerPreview(color);

        /* Through the text box, so the one TextChanged path owns the swatch, the validity and the readouts. */
        _pickerRow.Hex.Text = ThemeColorOverrides.ToHex(color);
    }

    private void UpdatePickerPreview(Color color)
    {
        PickerPreview.Background = new SolidColorBrush(color);
        PickerHexText.Text = ThemeColorOverrides.ToHex(color);
    }
}
