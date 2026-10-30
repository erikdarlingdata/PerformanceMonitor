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
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The add and edit form for one custom alert rule (#6390, S4). Validate and Save build the definition through
/// <see cref="AlertRuleEditorModel.TryBuild"/> (which ends in <see cref="CustomAlertRuleDefinition.TryParse"/>, the web's validator) and the
/// expiry through <see cref="CustomAlertExpiry"/>, and show the refusal in place, leaving the dialog open. The window does the store write.
/// </summary>
public partial class AlertRuleEditDialog : Window
{
    private readonly CustomAlertRule? _existing;
    private readonly string? _baseJson;
    private readonly IReadOnlyList<DarlingTag> _tags;
    private bool _loading = true;

    public string RuleName { get; private set; } = "";

    public string? Description { get; private set; }

    public bool RuleEnabled { get; private set; }

    public string DefinitionJson { get; private set; } = "";

    public DateTime? ExpiresAtUtc { get; private set; }

    /// <summary>The rule being edited, or null for a new one. The window saves under <c>Rule.Version</c>, the version this dialog loaded.</summary>
    public CustomAlertRule? Rule => _existing;

    public AlertRuleEditDialog(
        IReadOnlyList<string> serverNames, IReadOnlyList<DarlingTag> tags, CustomAlertRule? existing = null, CustomAlertTemplate? template = null)
    {
        InitializeComponent();
        _existing = existing;
        _tags = tags;
        _baseJson = existing?.DefinitionJson ?? template?.DefinitionJson;

        MeasureCombo.ItemsSource = AlertRuleEditorModel.Measures.Select(AlertRuleEditorModel.MeasureLabel).ToList();
        OpCombo.ItemsSource = AlertRuleEditorModel.Ops;
        ServerList.ItemsSource = serverNames;
        TagCombo.ItemsSource = tags;
        OpCombo.SelectedItem = "ge";

        if (existing is not null)
        {
            Title = "Edit Alert Rule";
            HeaderText.Text = "Edit Alert Rule";
            NameBox.Text = existing.Name;
            DescriptionBox.Text = existing.Description ?? "";
            EnabledCheck.IsChecked = existing.Enabled;
            ExpiresBox.Text = AlertRuleEditorModel.ExpiryText(existing.ExpiresAtUtc);
        }
        else if (template is not null)
        {
            NameBox.Text = template.Name;
            DescriptionBox.Text = template.Description;
        }

        if (_baseJson is not null && AlertRuleEditorModel.Load(_baseJson) is { } form)
        {
            Apply(form);
        }
        else
        {
            MeasureCombo.SelectedIndex = 0;
        }

        _loading = false;
        RefreshOpFields();
    }

    private ComposeMeasure? SelectedMeasure =>
        MeasureCombo.SelectedIndex >= 0 ? AlertRuleEditorModel.Measures[MeasureCombo.SelectedIndex] : null;

    private void Apply(AlertRuleForm form)
    {
        var index = AlertRuleEditorModel.Measures.ToList().FindIndex(m => string.Equals(m.Key, form.MeasureKey, StringComparison.Ordinal));
        MeasureCombo.SelectedIndex = index >= 0 ? index : 0;
        FillAggregateAndUnit(form.Aggregate, form.Unit);
        HoursBox.Text = form.Hours;
        OpCombo.SelectedItem = AlertRuleEditorModel.Ops.Contains(form.Op) ? form.Op : "ge";
        WarnBox.Text = form.Warn;
        CriticalBox.Text = form.Critical;
        LowerBox.Text = form.Lower;
        UpperBox.Text = form.Upper;
        CriticalLowerBox.Text = form.CriticalLower;
        CriticalUpperBox.Text = form.CriticalUpper;
        BreachBox.Text = form.BreachSamples;
        ClearBox.Text = form.ClearSamples;
        switch (form.Scope)
        {
            case AlertRuleScopeKind.Servers:
                ScopeServers.IsChecked = true;
                foreach (var server in form.Servers)
                {
                    ServerList.SelectedItems.Add(server);
                }

                break;
            case AlertRuleScopeKind.Tag:
                ScopeTag.IsChecked = true;
                TagCombo.SelectedItem = _tags.FirstOrDefault(t => t.Id == form.TagId);
                break;
            default:
                ScopeAll.IsChecked = true;
                break;
        }
    }

    private void FillAggregateAndUnit(string? aggregate, string? unit)
    {
        var measure = SelectedMeasure;
        if (measure is null)
        {
            return;
        }

        var aggregates = AlertRuleEditorModel.AggregatesFor(measure);
        AggregateCombo.ItemsSource = aggregates;
        AggregateCombo.IsEnabled = aggregates.Count > 0;
        AggregateCombo.SelectedItem = aggregates.Contains(aggregate ?? "") ? aggregate : AlertRuleEditorModel.DefaultAggregateFor(measure);
        FillUnits(measure, unit);
    }

    private void FillUnits(ComposeMeasure measure, string? unit)
    {
        var units = AlertRuleEditorModel.UnitsFor(measure, AggregateCombo.SelectedItem as string);
        UnitCombo.ItemsSource = units;
        UnitCombo.SelectedItem = unit is not null && units.Contains(unit) ? unit : units.Contains(measure.DefaultUnit) ? measure.DefaultUnit : units.Count > 0 ? units[0] : null;
    }

    private void MeasureCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!_loading)
        {
            FillAggregateAndUnit(null, null);
        }
    }

    private void AggregateCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!_loading && SelectedMeasure is { } measure)
        {
            FillUnits(measure, UnitCombo.SelectedItem as string);
        }
    }

    private void OpCombo_SelectionChanged(object sender, SelectionChangedEventArgs e) => RefreshOpFields();

    /// <summary>A scalar operator takes the warning and critical bars; a band operator takes the bounds. The unused boxes are disabled so they cannot be mistaken for set.</summary>
    private void RefreshOpFields()
    {
        if (_loading)
        {
            return;
        }

        var range = AlertRuleEditorModel.IsRangeOp(OpCombo.SelectedItem as string ?? "ge");
        WarnBox.IsEnabled = CriticalBox.IsEnabled = !range;
        LowerBox.IsEnabled = UpperBox.IsEnabled = CriticalLowerBox.IsEnabled = CriticalUpperBox.IsEnabled = range;
    }

    private void Scope_Changed(object sender, RoutedEventArgs e)
    {
        if (ServerList is null || TagCombo is null)
        {
            return;
        }

        ServerList.Visibility = ScopeServers.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
        TagCombo.Visibility = ScopeTag.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
    }

    private AlertRuleForm ReadForm() => new(
        SelectedMeasure?.Key ?? "",
        AggregateCombo.SelectedItem as string,
        UnitCombo.SelectedItem as string,
        HoursBox.Text,
        OpCombo.SelectedItem as string ?? "ge",
        WarnBox.IsEnabled ? WarnBox.Text : "",
        CriticalBox.IsEnabled ? CriticalBox.Text : "",
        LowerBox.IsEnabled ? LowerBox.Text : "",
        UpperBox.IsEnabled ? UpperBox.Text : "",
        CriticalLowerBox.IsEnabled ? CriticalLowerBox.Text : "",
        CriticalUpperBox.IsEnabled ? CriticalUpperBox.Text : "",
        BreachBox.Text,
        ClearBox.Text,
        ScopeServers.IsChecked == true ? AlertRuleScopeKind.Servers : ScopeTag.IsChecked == true ? AlertRuleScopeKind.Tag : AlertRuleScopeKind.All,
        ServerList.SelectedItems.Cast<string>().ToList(),
        (TagCombo.SelectedItem as DarlingTag)?.Id);

    /// <summary>Builds and validates the whole form; null when it is acceptable, else the text to show.</summary>
    private string? Check(out string definitionJson, out DateTime? expiry)
    {
        expiry = null;
        definitionJson = "";
        if (string.IsNullOrWhiteSpace(NameBox.Text))
        {
            return "Name is required.";
        }

        var refusal = AlertRuleEditorModel.TryBuild(_baseJson, ReadForm(), out definitionJson);
        if (refusal is not null)
        {
            return refusal;
        }

        var (resolved, expiryError) = AlertRuleEditorModel.ResolveExpiry(ExpiresBox.Text, _existing is not null, _existing?.ExpiresAtUtc);
        expiry = resolved;
        return expiryError;
    }

    private void Validate_Click(object sender, RoutedEventArgs e)
    {
        var refusal = Check(out _, out _);
        StatusText.Text = refusal ?? "Valid: this definition parses.";
        StatusText.Foreground = (System.Windows.Media.Brush)FindResource(refusal is null ? "ForegroundMutedBrush" : "WarningTextBrush");
    }

    private void Save_Click(object sender, RoutedEventArgs e)
    {
        var refusal = Check(out var definitionJson, out var expiry);
        if (refusal is not null)
        {
            MessageBox.Show(refusal, "Alert Rule", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        RuleName = NameBox.Text.Trim();
        Description = string.IsNullOrWhiteSpace(DescriptionBox.Text) ? null : DescriptionBox.Text.Trim();
        RuleEnabled = EnabledCheck.IsChecked == true;
        DefinitionJson = definitionJson;
        ExpiresAtUtc = expiry;
        DialogResult = true;
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => DialogResult = false;
}
