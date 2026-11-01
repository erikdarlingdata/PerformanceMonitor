/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Practice tab of the Investigations window (#5918): the judged cases (reported requests somebody gave feedback on), a
/// button that starts a practice request for the selected one, and the advisory score per agent. Hosted by
/// <see cref="InvestigationsWindow"/>, which hands it the data service, the signed-in login and whether the seat is read-only.
/// </summary>
public partial class InvestigationPracticePanel : UserControl
{
    private readonly ObservableCollection<PracticeCaseItem> _cases = new();
    private ViewerDataService? _dataService;
    private string _login = "";

    public InvestigationPracticePanel()
    {
        InitializeComponent();
        PracticeCasesGrid.ItemsSource = _cases;
    }

    /// <summary>One judged case in the list: a reported request with at least one feedback record.</summary>
    internal sealed record PracticeCaseItem(InvestigationFeedbackListRow Row)
    {
        /// <summary>The instant the Reported column is formatted from, and sorts by (not its text).</summary>
        public DateTime ReportedAtUtc => Row.ReportedAtUtc;

        public string ReportedDisplay => Row.ReportedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture);

        public string FeedbackSummary => string.Create(CultureInfo.InvariantCulture, $"{Row.Feedback.Count}, newest: {Row.Feedback[0].Verdict}");
    }

    /// <summary>One agent's score row.</summary>
    internal sealed record PracticeScoreItem(InvestigationPracticeScore Score)
    {
        public string AgentId => Score.AgentId;

        public int Cases => Score.Cases;

        public int Scored => Score.Scored;

        public int Matched => Score.Matched;

        public int Unscored => Score.Unscored;

        public int MissedStepCases => Score.MissedStepCases;

        public string ScoreDisplay => ScoreText(Score);
    }

    /// <summary>The score in words: points of possible and the percentage, or "not scored yet" when no case had an answer.</summary>
    internal static string ScoreText(InvestigationPracticeScore score) =>
        score.Percent is { } percent
            ? string.Create(CultureInfo.InvariantCulture, $"{score.Points:0.#} of {score.Possible:0.#} ({percent:P0})")
            : "not scored yet";

    /// <summary>Why Start cannot run, or null when it can.</summary>
    internal static string? StartError(PracticeCaseItem? selected) =>
        selected is null ? "Select a judged case to replay." : null;

    /// <summary>What the status line says for how opening ended.</summary>
    internal static string OpenedText(InvestigationPracticeResult result) =>
        result.Outcome == InvestigationPracticeOutcome.Opened
            ? "Practice request opened (" + result.RequestId + "). An agent on the case's route can claim it; its report is scored when it arrives."
            : result.Message ?? "The practice request was not opened.";

    /// <summary>Gives the panel its data and the seat; a read-only seat cannot start a practice request.</summary>
    internal void Initialize(ViewerDataService dataService, string login, bool readOnly)
    {
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        _login = login;
        if (readOnly)
        {
            StartPracticePanel.Visibility = Visibility.Collapsed;
        }
    }

    private int Days =>
        PracticeWindowBox.SelectedItem is ComboBoxItem { Tag: string tag } && int.TryParse(tag, NumberStyles.None, CultureInfo.InvariantCulture, out var days)
            ? days : 90;

    /// <summary>Reads the judged cases and the scores.</summary>
    internal async Task LoadAsync()
    {
        if (_dataService is null)
        {
            return;
        }

        try
        {
            var (rows, _) = await _dataService.GetInvestigationFeedbackListAsync(Days);
            _cases.Clear();
            foreach (var row in rows.Where(r => r.Feedback.Count > 0))
            {
                _cases.Add(new PracticeCaseItem(row));
            }

            PracticeScoresGrid.ItemsSource = (await _dataService.GetInvestigationPracticeScoresAsync(Days))
                .Select(s => new PracticeScoreItem(s)).ToList();
            PracticeStatusText.Text = string.Create(CultureInfo.InvariantCulture,
                $"{_cases.Count} judged case(s) in the last {Days} days. A newest corrected verdict is the answer; else right or partly right makes the report's verdict the answer (partly counts half); wrong with no correction has no answer.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            PracticeStatusText.Text = "Could not read practice data: " + ex.Message;
        }
    }

    /// <summary>Start Practice Request: checks the selection, opens the practice request, and says how it ended.</summary>
    internal async Task StartCommandAsync()
    {
        var selected = PracticeCasesGrid.SelectedItem as PracticeCaseItem;
        if (StartError(selected) is { } error)
        {
            PracticeStatusText.Text = error;
            return;
        }

        try
        {
            var result = await _dataService!.OpenInvestigationPracticeAsync(selected!.Row.RequestId, _login);
            PracticeStatusText.Text = OpenedText(result);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            PracticeStatusText.Text = "Could not open the practice request: " + ViewerDataService.InputErrorText(ex);
        }
    }

    private async void PracticeRefresh_Click(object sender, RoutedEventArgs e) => await LoadAsync();

    private async void StartPractice_Click(object sender, RoutedEventArgs e) => await StartCommandAsync();

    private async void PracticeWindowBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (IsLoaded)
        {
            await LoadAsync();
        }
    }
}
