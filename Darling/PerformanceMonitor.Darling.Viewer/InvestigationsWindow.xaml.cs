/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Investigations window (#5632 Phase A): the Level 1 requests an agent route opened, with each request's brief,
/// alerts, report and journal and a Close with a reason; the notes a brief may carry (approve or reject an agent's
/// proposal, or write one); and the environment profile per fleet, server or tag. Opened from the Settings window beside
/// Manage Notification Routes. Every write is awaited and the list reloaded from the store, as the routes window does.
///
/// <para>Each command checks its input first and says what is missing in the status line, so a command that cannot run
/// never reaches the store; the checks are the static <c>...Error</c> methods, which the STA tests call through the
/// window. A read-only seat (<c>postgres.connectAs = "viewer"</c>) sees everything and none of the write controls.</para>
/// </summary>
public partial class InvestigationsWindow : Window
{
    /// <summary>The status line a read-only seat shows in place of the write controls.</summary>
    internal const string ReadOnlyMessage =
        "Read-only connection (postgres.connectAs = \"viewer\"): investigations can be viewed but not closed, and notes, profiles and report feedback cannot be changed here. Set connectAs to \"admin\" and restart the viewer to change them.";

    private readonly ViewerDataService _dataService;
    private readonly ObservableCollection<InvestigationRequestViewRow> _requests = new();
    private readonly ObservableCollection<InvestigationNoteViewRow> _notes = new();
    private readonly ObservableCollection<FeedbackReportItem> _feedbackReports = new();
    private IReadOnlyList<InvestigationProfileViewRow> _profiles = Array.Empty<InvestigationProfileViewRow>();
    private readonly string _login;

    /// <summary>False on a read-only seat; set once at construction.</summary>
    public bool IsEditable { get; }

    public InvestigationsWindow(ViewerDataService dataService)
        : this(dataService, dataService?.IsReadOnly ?? false, Environment.UserName)
    {
        Loaded += async (_, _) => await LoadAllAsync();
    }

    /// <summary>The seam the STA tests use: the seat's role and the login are given, and nothing is read on load.</summary>
    internal InvestigationsWindow(ViewerDataService dataService, bool readOnly, string login)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        _login = login;
        IsEditable = !readOnly;
        RequestsGrid.ItemsSource = _requests;
        NotesGrid.ItemsSource = _notes;
        AgentChangesView.Initialize(_dataService, readOnly, login);
        AgentChangesView.Status += ShowStatus;
        ServerActionsView.Initialize(_dataService, readOnly, login);
        ServerActionsView.Status += ShowStatus;
        FeedbackReportsGrid.ItemsSource = _feedbackReports;
        PracticePanel.Initialize(_dataService, login, readOnly);
        SetScopes(null);

        if (readOnly)
        {
            GiveFeedbackPanel.Visibility = Visibility.Collapsed;
            CloseRequestPanel.Visibility = Visibility.Collapsed;
            DecideNotePanel.Visibility = Visibility.Collapsed;
            WriteNotePanel.Visibility = Visibility.Collapsed;
            SaveProfilePanel.Visibility = Visibility.Collapsed;
            ProfileTextBox.IsReadOnly = true;
            StatusText.Text = ReadOnlyMessage;
        }
    }

    /// <summary>One choice of a scope picker: the fleet (both ids null), one server or one tag.</summary>
    internal sealed record ScopeChoice(string Label, int? ServerId, int? TagId);

    /// <summary>The state filter's value: null for All.</summary>
    internal string? SelectedStateFilter =>
        RequestStateFilter.SelectedItem is ComboBoxItem { Content: string text } && text != "All" ? text : null;

    /// <summary>Why Close cannot run, or null when it can.</summary>
    internal static string? CloseError(InvestigationRequestViewRow? selected, string? reason)
    {
        if (selected is null)
        {
            return "Select a request to close.";
        }

        if (!selected.IsClosable)
        {
            return "This request is " + selected.State + " already; only an open, claimed or reported request can be closed.";
        }

        return ViewerDataService.ValidateInvestigationCloseReason(reason);
    }

    /// <summary>Why Approve or Reject cannot run, or null when it can.</summary>
    internal static string? DecideError(InvestigationNoteViewRow? selected) =>
        selected is null ? "Select a note to approve or reject."
        : !selected.IsPending ? "This note is " + selected.State + " already; only a pending note can be approved or rejected."
        : null;

    /// <summary>Why Write Note cannot run, or null when it can; <paramref name="days"/> is the parsed lifetime.</summary>
    internal static string? WriteNoteError(string? noteText, string? daysText, out int days)
    {
        if (!int.TryParse(daysText?.Trim(), NumberStyles.None, CultureInfo.CurrentCulture, out days))
        {
            days = 0;
            return string.Create(CultureInfo.InvariantCulture,
                $"Keep for days must be a whole number from 1 to {ViewerDataService.InvestigationNoteMaxDays}.");
        }

        return ViewerDataService.ValidateInvestigationNote(noteText, days);
    }

    /// <summary>One reported request in the feedback tab's list.</summary>
    internal sealed record FeedbackReportItem(InvestigationFeedbackListRow Row)
    {
        /// <summary>The instant the Reported column is formatted from, and sorts by (not its text).</summary>
        public DateTime ReportedAtUtc => Row.ReportedAtUtc;

        public string ReportedDisplay => Row.ReportedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture);

        public string FeedbackSummary => Row.Feedback.Count == 0 ? "none"
            : string.Create(CultureInfo.InvariantCulture, $"{Row.Feedback.Count}, newest: {Row.Feedback[0].Verdict}");
    }

    /// <summary>One feedback record in the feedback tab's "so far" list.</summary>
    internal sealed record FeedbackGivenItem(InvestigationFeedbackRow Row)
    {
        /// <summary>The instant the When column is formatted from, and sorts by (not its text).</summary>
        public DateTime CreatedAtUtc => Row.CreatedAtUtc;

        public string WhenDisplay => Row.CreatedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture);

        public string CorrectedDisplay => Row.CorrectedVerdict ?? "(report's)";
    }

    /// <summary>The corrected-verdict choice for the box's text: the first item means keep the report's verdict (null).</summary>
    internal static string? CorrectedVerdictOf(string? choice) =>
        choice is { } text && InvestigationFeedbackRules.CorrectedVerdicts.Contains(text, StringComparer.Ordinal) ? text : null;

    /// <summary>Why Save Feedback cannot run, or null when it can; the rules' sentences come from the store's rule set.</summary>
    internal static string? FeedbackError(FeedbackReportItem? selected, string? verdict, string? corrected, string? missedStep, string? comment) =>
        selected is null ? "Select a report to give feedback on."
        : InvestigationFeedbackRules.Validate(new InvestigationFeedbackSubmission(
            selected.Row.RequestId, verdict ?? "", CorrectedVerdictOf(corrected),
            string.IsNullOrWhiteSpace(missedStep) ? null : missedStep, string.IsNullOrWhiteSpace(comment) ? null : comment,
            "viewer")) is { Count: > 0 } problems ? InvestigationFeedbackRules.RefusalMessage(problems)
        : null;

    /// <summary>Why Save Profile cannot run, or null when it can.</summary>
    internal static string? SaveProfileError(ScopeChoice? scope, string? profileText) =>
        scope is null ? "Choose the fleet, a server or a tag."
        : ViewerDataService.ValidateInvestigationProfile(scope.ServerId, scope.TagId, profileText);

    private async Task LoadAllAsync()
    {
        try
        {
            SetScopes(await _dataService.GetNotificationRouteScopeInputsAsync());
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not read the servers and tags: " + ex.Message);
        }

        await LoadRequestsAsync();
        await LoadFeedbackAsync();
        await PracticePanel.LoadAsync();
        await LoadNotesAsync();
        await LoadProfilesAsync();
        await AgentChangesView.LoadAsync();
        await ServerActionsView.LoadAsync();
    }

    /// <summary>What the counts and the list are taken over, in words, and the settled-verdict rule the counts use (#5913).</summary>
    internal static string FeedbackWindowLabel(int days) =>
        string.Create(CultureInfo.InvariantCulture,
            $"Counts and list: reported requests opened in the last {days} days. The newest feedback that gives a corrected verdict settles a request; a later one that gives none does not displace it.");

    /// <summary>The window the feedback tab reads: the chosen number of days.</summary>
    internal int FeedbackDays =>
        FeedbackWindowBox.SelectedItem is ComboBoxItem { Tag: string tag } && int.TryParse(tag, NumberStyles.None, CultureInfo.InvariantCulture, out var days)
            ? days : 30;

    private async Task LoadFeedbackAsync()
    {
        try
        {
            var selected = (FeedbackReportsGrid.SelectedItem as FeedbackReportItem)?.Row.RequestId;
            var (rows, truncated) = await _dataService.GetInvestigationFeedbackListAsync(FeedbackDays);
            FamilyCountsGrid.ItemsSource = await _dataService.GetInvestigationFamilyCountsAsync(FeedbackDays);
            _feedbackReports.Clear();
            foreach (var row in rows)
            {
                _feedbackReports.Add(new FeedbackReportItem(row));
            }

            FeedbackWindowText.Text = FeedbackWindowLabel(FeedbackDays);
            FeedbackTruncatedText.Text = truncated
                ? string.Create(CultureInfo.InvariantCulture, $"Showing the newest {ViewerDataService.InvestigationFeedbackListLimit} reports; the counts above cover all of them.")
                : "";
            FeedbackReportsGrid.SelectedItem = _feedbackReports.FirstOrDefault(r => r.Row.RequestId == selected);
            ShowFeedbackOf(FeedbackReportsGrid.SelectedItem as FeedbackReportItem);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not read the report feedback: " + ex.Message);
        }
    }

    private void ShowFeedbackOf(FeedbackReportItem? item)
    {
        if (item is null)
        {
            FeedbackReportHeader.Text = "Select a report to see it and the feedback it has.";
            FeedbackReportBox.Text = "";
            FeedbackGivenGrid.ItemsSource = null;
            return;
        }

        var row = item.Row;
        FeedbackReportHeader.Text = row.ServerName + ", " + row.Family + " (" + row.FirstMetric + "): the agent said " + row.ReportVerdict;
        FeedbackReportBox.Text = "By " + row.AgentId + ".\r\nRoot cause: " + row.RootCause
            + (string.IsNullOrWhiteSpace(row.Summary) ? "" : "\r\nSummary: " + row.Summary);
        FeedbackGivenGrid.ItemsSource = row.Feedback.Select(f => new FeedbackGivenItem(f)).ToList();
    }

    private void SetScopes(ViewerRouteScopeInputs? inputs)
    {
        var fleet = new ScopeChoice("Fleet (every server)", null, null);
        var servers = inputs?.Servers.OrderBy(s => s.Name, StringComparer.OrdinalIgnoreCase)
            .Select(s => new ScopeChoice("Server: " + s.Name, s.Id, null)).ToList() ?? new List<ScopeChoice>();
        var tags = inputs?.Tags.OrderBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
            .Select(t => new ScopeChoice("Tag: " + t.Name, null, t.Id)).ToList() ?? new List<ScopeChoice>();
        NoteScopeBox.ItemsSource = new[] { fleet }.Concat(servers).ToList();
        NoteScopeBox.SelectedIndex = 0;
        ProfileScopeBox.ItemsSource = new[] { fleet }.Concat(servers).Concat(tags).ToList();
        ProfileScopeBox.SelectedIndex = 0;
    }

    private async Task LoadRequestsAsync()
    {
        try
        {
            var selected = (RequestsGrid.SelectedItem as InvestigationRequestViewRow)?.RequestId;
            var rows = await _dataService.GetInvestigationRequestsAsync(SelectedStateFilter);
            _requests.Clear();
            foreach (var row in rows)
            {
                _requests.Add(row);
            }

            RequestsGrid.SelectedItem = _requests.FirstOrDefault(r => r.RequestId == selected);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not read the investigation requests: " + ex.Message);
        }
    }

    private async Task LoadNotesAsync()
    {
        try
        {
            var rows = await _dataService.GetInvestigationNotesAsync(null);
            _notes.Clear();
            foreach (var row in rows)
            {
                _notes.Add(row);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not read the notes: " + ex.Message);
        }
    }

    private async Task LoadProfilesAsync()
    {
        try
        {
            _profiles = await _dataService.GetInvestigationProfilesAsync();
            ShowProfileOfScope();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not read the environment profiles: " + ex.Message);
        }
    }

    private void ShowProfileOfScope()
    {
        var scope = ProfileScopeBox.SelectedItem as ScopeChoice;
        var profile = scope is null ? null
            : _profiles.FirstOrDefault(p => p.ScopeServerId == scope.ServerId && p.ScopeTagId == scope.TagId);
        ProfileTextBox.Text = profile?.ProfileText ?? "";
        ProfileModifiedText.Text = profile is null ? "No profile yet."
            : "Saved " + profile.ModifiedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture)
              + (profile.ModifiedBy is { } by ? " by " + by : "");
    }

    private async Task ShowDetailAsync(InvestigationRequestViewRow? row)
    {
        if (row is null)
        {
            DetailHeader.Text = "Select a request to see its brief, alerts, report and journal.";
            BriefBox.Text = "";
            ReportBox.Text = "";
            ShowReportLinks(null);
            AlertsGrid.ItemsSource = null;
            JournalGrid.ItemsSource = null;
            return;
        }

        DetailHeader.Text = row.ServerName + ", " + row.Family + ": " + row.State
            + (row.ClosedReason is { } reason ? " (" + reason + ")" : "");
        try
        {
            var detail = await _dataService.GetInvestigationRequestDetailAsync(row.RequestId);
            BriefBox.Text = detail.BriefJson ?? "No brief yet: the brief is composed when an agent claims the request.";
            AlertsGrid.ItemsSource = detail.Alerts;
            JournalGrid.ItemsSource = detail.Journal;
            ReportBox.Text = detail.Reports.Count == 0 ? "No report yet." : FormatReport(detail.Reports[0]);
            ShowReportLinks(detail.Reports.Count == 0 ? null : detail.Reports[0]);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not read the request's detail: " + ex.Message);
        }
    }

    /// <summary>The report as the detail pane shows it: verdict, root cause, summary, then the evidence as returned.</summary>
    internal static string FormatReport(InvestigationReportViewRow report)
    {
        var text = new StringBuilder();
        text.Append("Verdict: ").Append(report.Verdict)
            .Append(report.RootCauseFound ? " (root cause found)" : " (root cause not found)")
            .Append(", by ").Append(report.AgentId).Append(" at ")
            .AppendLine(report.SubmittedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture));
        text.Append("Root cause: ").AppendLine(report.RootCause);
        if (!string.IsNullOrWhiteSpace(report.Summary))
        {
            text.Append("Summary: ").AppendLine(report.Summary);
        }

        text.Append("Evidence: ").Append(report.EvidenceJson);
        return text.ToString();
    }

    /// <summary>
    /// One report link as the detail pane shows it (#5914): its label (kind, key and URL), and the URI to open, or null
    /// with the reason when <see cref="InvestigationReportLinks.UrlProblem"/> refuses the stored URL. The check runs here
    /// again, on render, whatever the submit checked: a file or UNC link opened on Windows sends the user's credentials to
    /// its host, and a row can reach the store without the submit tool.
    /// </summary>
    internal static (string Label, Uri? Target, string? Refused) LinkItem(InvestigationReportLink link)
    {
        var label = link.Kind + (string.IsNullOrEmpty(link.Key) ? "" : " " + link.Key) + ": " + link.Url;
        if (InvestigationReportLinks.UrlProblem(link.Url) is { } problem)
            return (label, null, problem);
        return Uri.TryCreate(link.Url, UriKind.Absolute, out var uri) ? (label, uri, null) : (label, null, "not an absolute URL.");
    }

    /// <summary>The report's links under the report box: a safe one as a hyperlink, a refused one as plain text that says
    /// why. No report, or none with links: the line is hidden.</summary>
    private void ShowReportLinks(InvestigationReportViewRow? report)
    {
        ReportLinks.Inlines.Clear();
        if (report is null || report.Links.Count == 0)
        {
            ReportLinks.Visibility = Visibility.Collapsed;
            return;
        }

        ReportLinks.Inlines.Add(new System.Windows.Documents.Run("Links: "));
        for (var i = 0; i < report.Links.Count; i++)
        {
            if (i > 0)
                ReportLinks.Inlines.Add(new System.Windows.Documents.LineBreak());
            var (label, target, refused) = LinkItem(report.Links[i]);
            if (target is null)
            {
                ReportLinks.Inlines.Add(new System.Windows.Documents.Run(label + " (shown as text: " + refused + ")"));
                continue;
            }

            var hyperlink = new System.Windows.Documents.Hyperlink(new System.Windows.Documents.Run(label)) { NavigateUri = target };
            hyperlink.RequestNavigate += (_, e) =>
            {
                /* Checked once more at the click: only an https URL the rule passes is handed to the shell. */
                if (InvestigationReportLinks.IsSafeUrl(e.Uri.OriginalString))
                    System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo { FileName = e.Uri.AbsoluteUri, UseShellExecute = true });
                e.Handled = true;
            };
            ReportLinks.Inlines.Add(hyperlink);
        }

        ReportLinks.Visibility = Visibility.Visible;
    }

    /// <summary>Close: checks the selection and the reason, then closes the request and reloads.</summary>
    internal async Task CloseRequestCommandAsync()
    {
        var selected = RequestsGrid.SelectedItem as InvestigationRequestViewRow;
        if (CloseError(selected, CloseReasonBox.Text) is { } error)
        {
            ShowStatus(error);
            return;
        }

        try
        {
            var closed = await _dataService.CloseInvestigationRequestAsync(selected!.RequestId, _login, CloseReasonBox.Text);
            ShowStatus(closed ? "Request closed." : "The request was not closed: it is closed or expired already, or gone.");
            CloseReasonBox.Text = "";
            await LoadRequestsAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not close the request: " + ex.Message);
        }
    }

    /// <summary>
    /// Save Feedback (#5913): checks the selection and the values, then records the signed-in login's feedback on the report
    /// and reloads. A second save by the same login replaces the first, which the status line says. A read-only seat's refusal
    /// arrives as <see cref="ViewerReadOnlyException"/> and is shown in its own words.
    /// </summary>
    internal async Task SaveFeedbackCommandAsync()
    {
        var selected = FeedbackReportsGrid.SelectedItem as FeedbackReportItem;
        var verdict = (FeedbackVerdictBox.SelectedItem as ComboBoxItem)?.Content as string;
        var corrected = (FeedbackCorrectedBox.SelectedItem as ComboBoxItem)?.Content as string;
        if (FeedbackError(selected, verdict, corrected, FeedbackMissedBox.Text, FeedbackCommentBox.Text) is { } error)
        {
            ShowStatus(error);
            return;
        }

        try
        {
            var result = await _dataService.RecordInvestigationFeedbackAsync(
                selected!.Row.RequestId, verdict!, CorrectedVerdictOf(corrected), FeedbackMissedBox.Text, FeedbackCommentBox.Text, _login);
            ShowStatus(result.Outcome switch
            {
                InvestigationFeedbackOutcome.Recorded => "Feedback recorded.",
                InvestigationFeedbackOutcome.Replaced => "Feedback recorded; it replaced your earlier feedback on this report.",
                _ => result.Message ?? "The feedback was not recorded.",
            });
            if (result.Outcome is InvestigationFeedbackOutcome.Recorded or InvestigationFeedbackOutcome.Replaced)
            {
                FeedbackMissedBox.Text = "";
                FeedbackCommentBox.Text = "";
                await LoadFeedbackAsync();
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not record the feedback: " + ex.Message);
        }
    }

    /// <summary>Approve or Reject: checks the selection, then decides the note and reloads.</summary>
    internal async Task DecideNoteCommandAsync(bool approve)
    {
        var selected = NotesGrid.SelectedItem as InvestigationNoteViewRow;
        if (DecideError(selected) is { } error)
        {
            ShowStatus(error);
            return;
        }

        try
        {
            var decided = await _dataService.DecideInvestigationNoteAsync(selected!.NoteId, approve, _login);
            ShowStatus(decided ? (approve ? "Note approved." : "Note rejected.") : "The note was not changed: it was decided already, or is gone.");
            await LoadNotesAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not decide the note: " + ex.Message);
        }
    }

    /// <summary>Write Note: checks the text and the lifetime, then writes an approved note and reloads.</summary>
    internal async Task WriteNoteCommandAsync()
    {
        if (WriteNoteError(NoteTextBox.Text, NoteDaysBox.Text, out var days) is { } error)
        {
            ShowStatus(error);
            return;
        }

        var scope = NoteScopeBox.SelectedItem as ScopeChoice;
        try
        {
            await _dataService.WriteInvestigationNoteAsync(scope?.ServerId, NoteMetricBox.Text, NoteTextBox.Text, _login, days);
            ShowStatus("Note written.");
            NoteTextBox.Text = "";
            await LoadNotesAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not write the note: " + ex.Message);
        }
    }

    /// <summary>Save Profile: checks the scope and the text, then saves (or, blank, removes) the scope's profile.</summary>
    internal async Task SaveProfileCommandAsync()
    {
        var scope = ProfileScopeBox.SelectedItem as ScopeChoice;
        if (SaveProfileError(scope, ProfileTextBox.Text) is { } error)
        {
            ShowStatus(error);
            return;
        }

        try
        {
            /* The version this window showed (#6274): a profile saved by someone else since is refused, not overwritten. */
            var shown = _profiles.FirstOrDefault(p => p.ScopeServerId == scope!.ServerId && p.ScopeTagId == scope.TagId);
            await _dataService.SaveInvestigationProfileAsync(scope!.ServerId, scope.TagId, ProfileTextBox.Text, _login, shown?.ModifiedAtUtc);
            ShowStatus(string.IsNullOrWhiteSpace(ProfileTextBox.Text) ? "Profile removed." : "Profile saved.");
            await LoadProfilesAsync();
        }
        catch (PerformanceMonitor.Darling.Storage.InvestigationProfileConflictException ex)
        {
            /* Show what is saved now, so the next save carries its version. */
            await LoadProfilesAsync();
            ShowStatus(ex.Message);
        }
        catch (ArgumentException ex)
        {
            /* The store's refusal sentence (#6400: a tag that no longer exists), without the parameter name the exception adds. */
            ShowStatus(ex.Message.Split('\n')[0].Replace(" (Parameter 'profileText')", "", StringComparison.Ordinal));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ShowStatus("Could not save the profile: " + ex.Message);
        }
    }

    private void ShowStatus(string message) => StatusText.Text = message;

    private async void RequestStateFilter_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (IsLoaded)
        {
            await LoadRequestsAsync();
        }
    }

    private async void Refresh_Click(object sender, RoutedEventArgs e) => await LoadAllAsync();

    private async void RequestsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e) =>
        await ShowDetailAsync(RequestsGrid.SelectedItem as InvestigationRequestViewRow);

    private void FeedbackReportsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e) =>
        ShowFeedbackOf(FeedbackReportsGrid.SelectedItem as FeedbackReportItem);

    private async void FeedbackWindowBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (IsLoaded)
        {
            await LoadFeedbackAsync();
        }
    }

    private async void SaveFeedback_Click(object sender, RoutedEventArgs e) => await SaveFeedbackCommandAsync();

    private void ProfileScopeBox_SelectionChanged(object sender, SelectionChangedEventArgs e) => ShowProfileOfScope();

    private async void CloseRequest_Click(object sender, RoutedEventArgs e) => await CloseRequestCommandAsync();

    private async void ApproveNote_Click(object sender, RoutedEventArgs e) => await DecideNoteCommandAsync(approve: true);

    private async void RejectNote_Click(object sender, RoutedEventArgs e) => await DecideNoteCommandAsync(approve: false);

    private async void WriteNote_Click(object sender, RoutedEventArgs e) => await WriteNoteCommandAsync();

    private async void SaveProfile_Click(object sender, RoutedEventArgs e) => await SaveProfileCommandAsync();

    private void CloseWindow_Click(object sender, RoutedEventArgs e) => Close();
}
