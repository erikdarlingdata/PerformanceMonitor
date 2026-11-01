/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Add/Edit one notification route (#3598). The match combo is filled from <see cref="AlertFamily.All"/> plus
/// one "Exact metric name…" entry, so the family list on screen IS the closed taxonomy the resolver matches
/// against and cannot drift from it. Validation is the store's own
/// (<see cref="ViewerDataService.ValidateNotificationRoute"/>), so a route this dialog accepts is one the
/// store accepts. Since #5750 the dialog also picks the route's scope (all servers, one server, or a tag) and offers the
/// every-metric match only while a scope is chosen.
/// </summary>
public partial class NotificationRouteEditDialog : Window
{
    private const string ExactMetricChoice = "Exact metric name…";
    private const string AgentInheritChoice = "Inherit";

    /* #5825: the route's override of the channels' "send resolutions" setting. */
    internal const string ResolutionsChannelChoice = "Use channel setting";
    internal const string ResolutionsAlwaysChoice = "Always";
    internal const string ResolutionsNeverChoice = "Never";

    /* #6928: the severity filter's three choices, in the order they are offered. */
    internal const string SeveritiesBothChoice = "Both (Warning and Critical)";
    internal const string SeveritiesCriticalChoice = "Critical only";
    internal const string SeveritiesWarningChoice = "Warning only";

    /* #6979: the report notice setting's three choices, in the order they are offered. */
    internal const string ReportNoticeOffChoice = "Off";
    internal const string ReportNoticeNeedsPersonOrChangeChoice = "Needs a person or made a change";
    internal const string ReportNoticeAllChoice = "All reports";
    private const string AgentOffChoice = "Off";
    /* #5912: shown only on a route already stored at Level 2, and never chosen from here (the write path for Act is a later slice). */
    private const string AgentActChoice = "Act (Level 2: investigate and change alert settings)";
    private const string AgentInvestigateChoice = "Investigate";
    private const string AgentServerActChoice = "Act on servers (Level 3: also needs each server's switch and remediation credential)";

    /// <summary>The "Every metric (*)" entry of the Matches list. It is in the list only while a server or a tag is chosen
    /// (the V181 CHECK refuses a fleet-wide <c>*</c>); its Tag is the stored match text.</summary>
    private readonly ComboBoxItem _wildcardItem =
        new() { Content = ViewerNotificationRouteScope.WildcardChoiceLabel, Tag = NotificationRoute.WildcardMatch };

    private readonly bool _built;

    /// <summary>The route as edited — the caller persists it.</summary>
    public NotificationRouteRow Route { get; private set; }

    /// <summary>The route as stored, for the sealed values a blank box keeps (#5366).</summary>
    private readonly NotificationRouteRow? _stored;

    private readonly ViewerDataService? _dataService;

    /// <summary>The route's signing key columns as last read (#5998). Null for a new route, a read-only seat or a failed read.</summary>
    private ViewerSigningKeyState? _signingState;

    /// <param name="scopeInputs">The servers and tags to offer; null when they could not be read (the picker then offers All servers
    /// and the route's own scope).</param>
    /// <param name="existing">The route to edit, or null for a new one.</param>
    /// <param name="dataService">The store, for the signing key of an existing route (#5998); null leaves the key block off.</param>
    private const string FallbackNoneChoice = "None (this route's own channels)";

    public NotificationRouteEditDialog(ViewerRouteScopeInputs? scopeInputs, NotificationRouteRow? existing = null, ViewerDataService? dataService = null,
        System.Collections.Generic.IEnumerable<NotificationRouteRow>? allRoutes = null)
    {
        InitializeComponent();
        _stored = existing;
        _dataService = dataService;

        foreach (var name in AlertFamily.All)
        {
            FamilyCombo.Items.Add(new ComboBoxItem { Content = name, Tag = name });
        }

        FamilyCombo.Items.Add(new ComboBoxItem { Content = ExactMetricChoice, Tag = null });

        /* #5632: the agent channel, Inherit / Off / Investigate, and its four numbers. */
        AgentLevelCombo.Items.Add(new ComboBoxItem { Content = AgentInheritChoice, Tag = null });
        AgentLevelCombo.Items.Add(new ComboBoxItem { Content = AgentOffChoice, Tag = NotificationRoute.AgentLevelOff });
        AgentLevelCombo.Items.Add(new ComboBoxItem { Content = AgentInvestigateChoice, Tag = NotificationRoute.AgentLevelInvestigate });
        /* #5915: Level 2 is offered. An agent holding a live claim may mute one metric on the request's server and change custom alert rules scoped to it; every change is journaled. */
        AgentLevelCombo.Items.Add(new ComboBoxItem { Content = AgentActChoice, Tag = NotificationRoute.AgentLevelAct });
        /* #5632 step 5: Level 3 is offered. It includes everything Level 2 allows. A server action also needs that server's own switch (all off, and no
           server action exists yet), the server's remediation login given as an env: or file: reference, and agent_actions_enabled. */
        AgentLevelCombo.Items.Add(new ComboBoxItem { Content = AgentServerActChoice, Tag = NotificationRoute.AgentLevelServerAct });
        ShowAgent(existing ?? new NotificationRouteRow());

        SendResolutionsCombo.Items.Add(new ComboBoxItem { Content = ResolutionsChannelChoice, Tag = null });
        SendResolutionsCombo.Items.Add(new ComboBoxItem { Content = ResolutionsAlwaysChoice, Tag = true });
        SendResolutionsCombo.Items.Add(new ComboBoxItem { Content = ResolutionsNeverChoice, Tag = false });
        var storedResolutions = existing?.SendResolutions;
        SendResolutionsCombo.SelectedItem = SendResolutionsCombo.Items.Cast<ComboBoxItem>().First(i => Equals(i.Tag, storedResolutions));

        SeveritiesCombo.Items.Add(new ComboBoxItem { Content = SeveritiesBothChoice, Tag = NotificationRouteKinds.SeveritiesBoth });
        SeveritiesCombo.Items.Add(new ComboBoxItem { Content = SeveritiesCriticalChoice, Tag = NotificationRouteKinds.SeveritiesCritical });
        SeveritiesCombo.Items.Add(new ComboBoxItem { Content = SeveritiesWarningChoice, Tag = NotificationRouteKinds.SeveritiesWarning });
        var storedSeverities = NotificationRouteKinds.NormalizeSeverities(existing?.Severities);
        SeveritiesCombo.SelectedItem = SeveritiesCombo.Items.Cast<ComboBoxItem>().First(i => Equals(i.Tag, storedSeverities));

        ReportNoticeCombo.Items.Add(new ComboBoxItem { Content = ReportNoticeOffChoice, Tag = NotificationRouteKinds.ReportNoticeOff });
        ReportNoticeCombo.Items.Add(new ComboBoxItem { Content = ReportNoticeNeedsPersonOrChangeChoice, Tag = NotificationRouteKinds.ReportNoticeNeedsPersonOrChange });
        ReportNoticeCombo.Items.Add(new ComboBoxItem { Content = ReportNoticeAllChoice, Tag = NotificationRouteKinds.ReportNoticeAll });
        var storedReportNotice = NotificationRouteKinds.NormalizeReportNotice(existing?.ReportNotice);
        ReportNoticeCombo.SelectedItem = ReportNoticeCombo.Items.Cast<ComboBoxItem>().First(i => Equals(i.Tag, storedReportNotice));

        /* #6036: the fallback picker lists every OTHER route, with a none choice; the route being edited is never offered. */
        FallbackRouteCombo.Items.Add(new ComboBoxItem { Content = FallbackNoneChoice, Tag = null });
        foreach (var other in (allRoutes ?? System.Linq.Enumerable.Empty<NotificationRouteRow>()).Where(r => r.RouteId != (existing?.RouteId ?? 0)))
        {
            FallbackRouteCombo.Items.Add(new ComboBoxItem { Content = $"#{other.RouteId} {other.MetricMatch}", Tag = other.RouteId });
        }

        FallbackRouteCombo.SelectedItem = FallbackRouteCombo.Items.Cast<ComboBoxItem>().FirstOrDefault(i => Equals(i.Tag, existing?.FallbackRouteId))
            ?? FallbackRouteCombo.Items[0];

        var scopeOptions = ViewerNotificationRouteScope.ScopeOptions(scopeInputs);
        var picked = existing is null ? scopeOptions[0] : ViewerNotificationRouteScope.OptionFor(existing, scopeOptions);
        ScopeCombo.ItemsSource = scopeOptions;
        ScopeCombo.SelectedItem = picked;
        _built = true;
        UpdateWildcardOffer();

        if (existing is null)
        {
            Route = new NotificationRouteRow();
            FamilyCombo.SelectedIndex = System.Linq.Enumerable.ToList(AlertFamily.All).IndexOf(AlertFamily.Performance); /* the pages family, the likeliest first route */
            SigningKeyButton.IsEnabled = false;
            SigningKeyRemoveButton.IsEnabled = false;
            SigningKeyStatusText.Text = "Save the route first. A key is tied to the saved URL.";
            return;
        }

        Route = existing.Clone();
        Title = "Edit Notification Route";
        HeaderText.Text = "Edit Notification Route";

        var family = Route.Family;
        if (Route.MetricMatch.Trim() == NotificationRoute.WildcardMatch && FamilyCombo.Items.Contains(_wildcardItem))
        {
            FamilyCombo.SelectedItem = _wildcardItem;
        }
        else if (family is null)
        {
            FamilyCombo.SelectedIndex = FamilyCombo.Items.IndexOf(FamilyCombo.Items.Cast<ComboBoxItem>().First(i => i.Tag is null));
            ExactMetricBox.Text = Route.MetricMatch;
        }
        else
        {
            FamilyCombo.SelectedIndex = FamilyIndex(family);
        }

        TeamsUrlBox.Text = ViewerWebhookSealing.ShownText(Route.TeamsUrl);
        SlackUrlBox.Text = ViewerWebhookSealing.ShownText(Route.SlackUrl);
        GenericUrlBox.Text = ViewerWebhookSealing.ShownText(Route.GenericUrl);
        PagerDutyKeyBox.Text = ViewerWebhookSealing.ShownText(Route.PagerDutyRoutingKey);
        MarkSaved(TeamsUrlBox, Route.TeamsUrl);
        MarkSaved(SlackUrlBox, Route.SlackUrl);
        MarkSaved(GenericUrlBox, Route.GenericUrl);
        MarkSaved(PagerDutyKeyBox, Route.PagerDutyRoutingKey);
        SmtpRecipientsBox.Text = Route.SmtpRecipients;
        EnabledCheckBox.IsChecked = Route.Enabled;
        Loaded += async (_, _) => await RefreshSigningKeyAsync();
    }

    private void FamilyCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        var isExact = SelectedMatch() is null;
        ExactMetricBox.IsEnabled = isExact;
        FamilyDescriptionText.Text = SelectedMatch() switch
        {
            NotificationRoute.WildcardMatch => "Every alert for this scope that no more specific route of the same scope matches. Offered only with a server or a tag.",
            AlertFamily.SelfMonitor => "Alerts about the monitoring tool itself: its store, collectors, jobs and certificates (Compression Job Stuck, Collection Stopped, Store Disk Pressure, Retention Held, …).",
            AlertFamily.Reports => "Scheduled prose — the Collector Cost Digest, the Fleet Sweep Rollup, the Daily Digest (metric Analysis Singles Digest) and the Daily Health Report. Reports to read, not pages.",
            AlertFamily.AgentJobs => "SQL Server Agent: Failed Agent Job, Long-Running Job, Agent Not Running, Agent Alert Gaps.",
            AlertFamily.Performance => "A monitored server's health: blocking, deadlocks, CPU, long-running queries, space, availability groups, connection loss, custom rules and analysis findings — and any alert the taxonomy does not name.",
            _ => "One alert by its exact metric_name (as shown in Alert History). Wins over the family route; a recovery (Server Restored, AG Replica Reconnected) routes as the alert it clears.",
        };
    }

    private static int FamilyIndex(string family)
    {
        for (var i = 0; i < AlertFamily.All.Count; i++)
        {
            if (string.Equals(AlertFamily.All[i], family, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return 0;
    }

    /// <summary>The chosen family, the wildcard <c>*</c>, or null for "Exact metric name…".</summary>
    private string? SelectedMatch() => (FamilyCombo.SelectedItem as ComboBoxItem)?.Tag as string;

    private RouteScopeOption? SelectedScope() => ScopeCombo.SelectedItem as RouteScopeOption;

    private void ScopeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_built)
        {
            UpdateWildcardOffer();
        }
    }

    /// <summary>Adds "Every metric (*)" while a server or a tag is chosen and removes it on All servers; a selection that was
    /// on it moves to the Performance family.</summary>
    private void UpdateWildcardOffer()
    {
        var offered = ViewerNotificationRouteScope.WildcardOffered(SelectedScope());
        var present = FamilyCombo.Items.Contains(_wildcardItem);
        if (offered && !present)
        {
            FamilyCombo.Items.Add(_wildcardItem);
        }
        else if (!offered && present)
        {
            var wasSelected = ReferenceEquals(FamilyCombo.SelectedItem, _wildcardItem);
            FamilyCombo.Items.Remove(_wildcardItem);
            if (wasSelected)
            {
                FamilyCombo.SelectedIndex = FamilyIndex(AlertFamily.Performance);
            }
        }
    }

    private void Save_Click(object sender, RoutedEventArgs e)
    {
        Route.MetricMatch = SelectedMatch() ?? ExactMetricBox.Text.Trim();
        (Route.ScopeServerId, Route.ScopeTagId) = ViewerNotificationRouteScope.ScopeIds(SelectedScope());
        /* A blank box keeps the sealed value stored for it; the data service seals anything typed (#5366). */
        Route.TeamsUrl = ViewerWebhookSealing.CarryKept(TeamsUrlBox.Text, _stored?.TeamsUrl);
        Route.SlackUrl = ViewerWebhookSealing.CarryKept(SlackUrlBox.Text, _stored?.SlackUrl);
        Route.GenericUrl = ViewerWebhookSealing.CarryKept(GenericUrlBox.Text, _stored?.GenericUrl);
        Route.PagerDutyRoutingKey = ViewerWebhookSealing.CarryKept(PagerDutyKeyBox.Text, _stored?.PagerDutyRoutingKey);
        Route.SmtpRecipients = SmtpRecipientsBox.Text.Trim();
        Route.Enabled = EnabledCheckBox.IsChecked == true;
        Route.Severities = NotificationRouteKinds.NormalizeSeverities((SeveritiesCombo.SelectedItem as ComboBoxItem)?.Tag as string);
        Route.ReportNotice = NotificationRouteKinds.NormalizeReportNotice((ReportNoticeCombo.SelectedItem as ComboBoxItem)?.Tag as string);

        var error = ReadAgent();
        error ??= ViewerDataService.ValidateNotificationRoute(Route);
        if (error is not null)
        {
            ValidationText.Text = error;
            ValidationText.Visibility = Visibility.Visible;
            return;
        }

        ValidationText.Visibility = Visibility.Collapsed;

        /* An exact name that is really a family spelled by hand is normalised to the family, so the grid and
           the resolver agree about what kind of route this is. */
        if (AlertFamily.NormalizeFamily(Route.MetricMatch) is { } family)
        {
            Route.MetricMatch = family;
        }

        DialogResult = true;
    }

    // ── The route's own generic URL signing key (#5998) ──

    /// <summary>Reads the route's key columns and puts the block in line with them. Only whether a sealed key is saved and when the
    /// previous key stops being sent: the key is never read in the clear.</summary>
    private async Task RefreshSigningKeyAsync()
    {
        _signingState = null;
        if (_stored is null || _stored.RouteId == 0)
        {
            return;
        }

        var readOnly = _dataService is null || _dataService.IsReadOnly;
        if (!readOnly)
        {
            try
            {
                _signingState = await _dataService!.GetRouteSigningKeyStateAsync(_stored.RouteId);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                ViewerLogger.Warn("NotificationRouteEditDialog", "The route signing key state could not be read: " + ex.Message);
            }
        }

        SigningKeyButton.Content = ViewerWebhookSigningKeys.ActionText(_signingState);
        SigningKeyButton.IsEnabled = !readOnly;
        SigningKeyRemoveButton.IsEnabled = !readOnly && _signingState is { HasKey: true };
        SigningKeyStatusText.Text = readOnly
            ? "A read-only connection cannot make or remove a key."
            : ViewerWebhookSigningKeys.StatusText(_signingState, DateTime.UtcNow);
        SigningKeyTestNoteText.Text = _signingState is { HasKey: true } ? ViewerWebhookSigningKeys.UnsignedTestText : "";
    }

    private async void SigningKeyButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || _stored is null)
        {
            return;
        }

        /* A key is tied to the saved URL, so a URL typed here and not yet saved would be the wrong one. */
        if (!string.Equals(GenericUrlBox.Text?.Trim() ?? "", ViewerWebhookSealing.ShownText(_stored.GenericUrl).Trim(), StringComparison.Ordinal))
        {
            MessageBox.Show(ViewerWebhookSigningKeys.SaveUrlFirstText, "Signing key", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        if (_signingState is { HasKey: true }
            && MessageBox.Show(
                $"Make a new key? The current key keeps signing alongside it for {WebhookSigning.RotationOverlapDays} days, so the receiver has time to switch.",
                "Rotate signing key", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes)
        {
            return;
        }

        SigningKeyButton.IsEnabled = false;
        try
        {
            var key = await ViewerPasswordKey.GetSealKeyAsync(_dataService, this);
            if (key.Notice is { } notice)
            {
                MessageBox.Show(notice, "Signing key", MessageBoxButton.OK, MessageBoxImage.Information);
            }

            if (key.Sealer is null)
            {
                MessageBox.Show(key.Refusal ?? ViewerPasswordKey.NoKeyText, "Signing key", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            /* Shown once, in a box to copy from. Not logged, not copied for the person, gone when the dialog closes. */
            SigningKeyShownBox.Text = await _dataService.GenerateRouteSigningKeyAsync(_stored.RouteId, key.Sealer);
            SigningKeyShownText.Text = ViewerWebhookSigningKeys.ShownOnceText;
            SigningKeyShownPanel.Visibility = Visibility.Visible;
            SigningKeyShownBox.SelectAll();
        }
        catch (ViewerPasswordRefusedException ex)
        {
            MessageBox.Show(ex.Message, "Signing key", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (ViewerReadOnlyException ex)
        {
            MessageBox.Show(ex.Message, "Read-only connection", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (ViewerSchemaSkewException ex)
        {
            MessageBox.Show(ex.Message, "Store out of date", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            MessageBox.Show("The signing key was not saved:\n\n" + ex.Message, "Signing key", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            await RefreshSigningKeyAsync();
        }
    }

    private async void SigningKeyRemoveButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || _stored is null
            || MessageBox.Show(
                "Remove the signing key and the previous key? Posts to this URL go out unsigned from the next alert, and a receiver that checks signatures will refuse them.",
                "Remove signing key", MessageBoxButton.YesNo, MessageBoxImage.Warning) != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            await _dataService.RemoveRouteSigningKeyAsync(_stored.RouteId);
            SigningKeyShownBox.Text = "";
            SigningKeyShownText.Text = "";
            SigningKeyShownPanel.Visibility = Visibility.Collapsed;
        }
        catch (ViewerReadOnlyException ex)
        {
            MessageBox.Show(ex.Message, "Read-only connection", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            MessageBox.Show("The signing key was not removed:\n\n" + ex.Message, "Signing key", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            await RefreshSigningKeyAsync();
        }
    }

    /// <summary>A box whose value is stored sealed shows nothing, and says so in its tooltip.</summary>
    private static void MarkSaved(System.Windows.Controls.TextBox box, string? stored)
    {
        if (ViewerWebhookSealing.IsSaved(stored))
        {
            box.ToolTip = ViewerWebhookSealing.KeepHint;
        }
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => DialogResult = false;

    /// <summary>Puts a route's agent channel on screen (#5632): the level's choice, and its four numbers.</summary>
    private void ShowAgent(NotificationRouteRow row)
    {
        AgentLevelCombo.SelectedItem = AgentLevelItemFor(row.AgentLevel);
        AgentMaxPerHourBox.Text = row.AgentMaxPerHour.ToString(CultureInfo.InvariantCulture);
        AgentMaxPerDayBox.Text = row.AgentMaxPerDay.ToString(CultureInfo.InvariantCulture);
        AgentLeaseMinutesBox.Text = row.AgentLeaseMinutes.ToString(CultureInfo.InvariantCulture);
        AgentExpiryMinutesBox.Text = row.AgentExpiryMinutes.ToString(CultureInfo.InvariantCulture);
        UpdateAgentLimitsEnabled();
    }

    /// <summary>The combo item for a stored level. A level the combo does not offer (a value outside Inherit, Off, Investigate and Act) gets
    /// a DISABLED item showing it, selected and so kept on save: the dialog neither throws on it nor silently saves it as Inherit, Off or
    /// Investigate. It cannot be picked again once the user moves off it.</summary>
    private ComboBoxItem AgentLevelItemFor(int? level)
    {
        var item = AgentLevelCombo.Items.Cast<ComboBoxItem>().FirstOrDefault(i => Equals(i.Tag, level));
        if (item is null)
        {
            item = new ComboBoxItem
            {
                Content = level == NotificationRoute.AgentLevelAct ? AgentActChoice : level == NotificationRoute.AgentLevelServerAct ? AgentServerActChoice : $"Level {level}",
                Tag = level,
                IsEnabled = false,
            };
            AgentLevelCombo.Items.Add(item);
        }

        return item;
    }

    /// <summary>The selected agent level: null for Inherit.</summary>
    private int? SelectedAgentLevel() => (AgentLevelCombo.SelectedItem as ComboBoxItem)?.Tag as int?;

    /// <summary>The limits apply only to a route that investigates, so they are editable only then.</summary>
    private void UpdateAgentLimitsEnabled()
    {
        var investigates = SelectedAgentLevel() is NotificationRoute.AgentLevelInvestigate or NotificationRoute.AgentLevelAct or NotificationRoute.AgentLevelServerAct;
        AgentMaxPerHourBox.IsEnabled = investigates;
        AgentMaxPerDayBox.IsEnabled = investigates;
        AgentLeaseMinutesBox.IsEnabled = investigates;
        AgentExpiryMinutesBox.IsEnabled = investigates;

        /* #6979: only a route with its own agent level owns requests, so a report notice can fire only there. The stored value is kept
           and still saved while the setting is disabled. */
        ReportNoticeCombo.IsEnabled = investigates;
        ReportNoticeHint.Visibility = investigates ? Visibility.Collapsed : Visibility.Visible;
    }

    private void AgentLevelCombo_SelectionChanged(object sender, SelectionChangedEventArgs e) => UpdateAgentLimitsEnabled();

    /// <summary>Reads the agent channel off the screen into <see cref="Route"/> (#5632), or says which number is not a
    /// whole number. The bounds are <see cref="ViewerDataService.ValidateNotificationRoute"/>'s.</summary>
    private string? ReadAgent()
    {
        Route.AgentLevel = SelectedAgentLevel();
        Route.SendResolutions = (SendResolutionsCombo.SelectedItem as ComboBoxItem)?.Tag as bool?;
        Route.FallbackRouteId = (FallbackRouteCombo.SelectedItem as ComboBoxItem)?.Tag as int?;
        if (!TryReadNumber(AgentMaxPerHourBox, "Requests per hour", out var perHour, out var error)
            || !TryReadNumber(AgentMaxPerDayBox, "Requests per day", out var perDay, out error)
            || !TryReadNumber(AgentLeaseMinutesBox, "Lease minutes", out var lease, out error)
            || !TryReadNumber(AgentExpiryMinutesBox, "Expiry minutes", out var expiry, out error))
        {
            return error;
        }

        Route.AgentMaxPerHour = perHour;
        Route.AgentMaxPerDay = perDay;
        Route.AgentLeaseMinutes = lease;
        Route.AgentExpiryMinutes = expiry;
        return null;
    }

    private static bool TryReadNumber(TextBox box, string name, out int value, out string? error)
    {
        if (int.TryParse(box.Text.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out value))
        {
            error = null;
            return true;
        }

        error = name + " must be a whole number.";
        return false;
    }
}
