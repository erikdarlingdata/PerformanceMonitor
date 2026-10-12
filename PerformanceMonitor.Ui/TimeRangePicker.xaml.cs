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
using System.Windows.Automation;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Threading;

namespace PerformanceMonitor.Ui;

/// <summary>Raised when the user picks a range (<see cref="TimeRangePicker.RangeChanged"/>).</summary>
public sealed class TimeRangeChangedEventArgs : EventArgs
{
    /// <summary>Wraps the new range.</summary>
    public TimeRangeChangedEventArgs(TimeRangeSpec spec, ResolvedTimeRange range)
    {
        Spec = spec;
        Range = range;
    }

    /// <summary>The range as named; hold this to keep it live, or persist its <see cref="TimeRangeSpec.Id"/>.</summary>
    public TimeRangeSpec Spec { get; }

    /// <summary>The range at the moment it was picked.</summary>
    public ResolvedTimeRange Range { get; }
}

/// <summary>
/// The shared time range picker (#5562) for Lite and the Darling Viewer: a button naming the range, the resolved
/// start, end and zone beside it, and a popup with the short presets, the calendar periods (each with its current
/// length), a text box that shows what it parsed before Apply (or why it failed), and a calendar for picking days.
/// A tab sets <see cref="ZoneProvider"/> to its display zone and handles <see cref="RangeChanged"/>; on every
/// refresh it calls <see cref="Resolve"/> for the window to read, so a live range keeps sliding.
///
/// <para>Themed through the shared brush keys (BackgroundBrush, ForegroundBrush, BorderBrush, AccentBrush...) that both
/// apps' dictionaries define. Enter applies the typed range, Esc closes the popup. UI thread only.</para>
/// </summary>
public partial class TimeRangePicker : UserControl
{
    private static readonly TimeSpan ReopenGuard = TimeSpan.FromMilliseconds(250);

    private readonly List<(Button Button, TimeRangeSpec Spec)> _rollingButtons = new();
    private readonly List<(Button Button, TimeRangeSpec Spec, TextBlock Length)> _periodButtons = new();
    private TimeRangeSpec _value = TimeRangePresets.Rolling[4];
    private DateTime? _dataStartUtc;
    private TimeSpan? _sampleInterval;
    private bool _compact;
    private TimeSpan? _rollingUnit;
    private Button? _longestButton;
    private long _closedAtTick;
    private TimeRangeParseResult? _typed;
    private Func<TimeZoneInfo>? _zoneProvider;
    private Func<DateTime>? _nowProvider;

    /// <summary>Builds the picker with the default range (4h).</summary>
    public TimeRangePicker()
    {
        InitializeComponent();
        BuildPresetButtons();
        PickCalendar.GotMouseCapture += (_, e) =>
        {
            /* A Calendar in a popup keeps the mouse captured after a day click and swallows the next click. */
            if (e.OriginalSource is CalendarItem or CalendarDayButton or CalendarButton)
            {
                ((UIElement)e.OriginalSource).ReleaseMouseCapture();
            }
        };
        Refresh();
    }

    /// <summary>Raised when the user picks a range by preset, period, typed text or calendar. Not raised by <see cref="Value"/>.</summary>
    public event EventHandler<TimeRangeChangedEventArgs>? RangeChanged;

    /// <summary>The display zone for typed times, calendar periods and the shown text; the tab's own zone. Default is this machine's zone.</summary>
    public Func<TimeZoneInfo>? ZoneProvider
    {
        get => _zoneProvider;
        set
        {
            _zoneProvider = value;
            Refresh();
        }
    }

    /// <summary>The clock as naive UTC; default is the machine clock. Tests set it.</summary>
    public Func<DateTime>? NowProvider
    {
        get => _nowProvider;
        set
        {
            _nowProvider = value;
            Refresh();
        }
    }

    /// <summary>The zone in force now.</summary>
    public TimeZoneInfo Zone => ZoneProvider?.Invoke() ?? TimeZoneInfo.Local;

    /// <summary>The range held. Setting it re-renders the picker and does not raise <see cref="RangeChanged"/>.</summary>
    public TimeRangeSpec Value
    {
        get => _value;
        set
        {
            _value = value ?? TimeRangePresets.Rolling[4];
            Refresh();
        }
    }

    /// <summary>
    /// Where the tab's stored data starts, if known (naive UTC). When the range starts more than 90 minutes earlier
    /// the picker adds "Data starts Oct 3, 2:00 pm" beside the resolved range.
    /// </summary>
    public DateTime? DataStartUtc
    {
        get => _dataStartUtc;
        set
        {
            _dataStartUtc = value;
            Refresh();
        }
    }

    /// <summary>
    /// How often the tab's main collector samples (its actual interval). When the range holds fewer than 3 samples
    /// the picker adds "Data here is collected every N minutes." so a short or empty chart has a reason. Null for none.
    /// </summary>
    public TimeSpan? SampleInterval
    {
        get => _sampleInterval;
        set
        {
            _sampleInterval = value;
            Refresh();
        }
    }

    /// <summary>True for a one-button form (FinOps): the resolved range and notes move to the button's tooltip.</summary>
    public bool Compact
    {
        get => _compact;
        set
        {
            _compact = value;
            PickerButton.Padding = value ? new Thickness(8, 2, 8, 2) : new Thickness(10, 4, 10, 4);
            PickerButton.MinHeight = value ? 22 : 26;
            Refresh();
        }
    }

    /// <summary>
    /// Makes the picker rolling-only, in whole units of this length (#5562 R5): one hour for a FinOps list, one day for the
    /// object heatmap. These reads take "N units back from now", so the picker offers no calendar period, no custom end and no
    /// "since" range, no preset or typed length under one unit, and no length that is not a whole number of units
    /// (<see cref="RollingUnitRule"/>). Null (the default) offers everything.
    /// </summary>
    public TimeSpan? RollingUnit
    {
        get => _rollingUnit;
        set
        {
            if (value is { } unit && unit <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "A rolling unit is positive.");
            }

            _rollingUnit = value;
            var calendar = value is null ? Visibility.Visible : Visibility.Collapsed;
            CalendarHeader.Visibility = calendar;
            PeriodPanel.Visibility = calendar;
            PickCalendar.Visibility = calendar;
            UpdateRollingButtons();
            Refresh();
        }
    }

    /// <summary>
    /// Adds one more rolling choice after the presets, for a surface whose data has a natural longest span (#5562 R8): Alert
    /// History's "All" is the span its table keeps. Pass <c>null</c> to remove it. A rolling-only picker still refuses it when its
    /// unit does not fit (<see cref="RollingUnit"/>).
    /// </summary>
    /// <param name="spec">The span the choice selects.</param>
    /// <param name="label">The button text, such as "All".</param>
    public void SetLongestChoice(TimeRangeSpec? spec, string label = "All")
    {
        if (_longestButton is not null)
        {
            RollingPanel.Children.Remove(_longestButton);
            _rollingButtons.RemoveAll(entry => ReferenceEquals(entry.Button, _longestButton));
            _longestButton = null;
        }

        if (spec is not null)
        {
            var button = new Button
            {
                Content = label,
                MinWidth = 40,
                Padding = new Thickness(6, 3, 6, 3),
                Margin = new Thickness(0, 0, 4, 4),
                Tag = spec
            };
            AutomationProperties.SetName(button, label + " (" + spec.Name + ")");
            button.Click += Preset_Click;
            RollingPanel.Children.Add(button);
            _rollingButtons.Add((button, spec));
            _longestButton = button;
        }

        UpdateRollingButtons();
        Refresh();
    }

    /// <summary>Why this picker cannot take <paramref name="spec"/> (rolling-only form), or <c>null</c> when it can.</summary>
    public string? Refusal(TimeRangeSpec spec)
    {
        if (_rollingUnit is not { } unit)
        {
            return null;
        }

        return RollingUnitRule.Refusal(spec, unit);
    }

    /// <summary>True while the popup is open.</summary>
    public bool IsPopupOpen => PickerPopup.IsOpen;

    /// <summary>The held range at this moment, or <c>null</c> when it cannot be used right now (Today in its first minutes).</summary>
    public ResolvedTimeRange? Resolve()
        => _value.TryResolve(NowUtc, Zone, out var range, out _) ? range : null;

    /// <summary>Holds <paramref name="spec"/> and raises <see cref="RangeChanged"/>, as if the user picked it. <c>false</c> (nothing held) when it cannot be resolved now.</summary>
    public bool Select(TimeRangeSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        if (Refusal(spec) is not null || !spec.TryResolve(NowUtc, Zone, out var range, out _))
        {
            return false;
        }

        _value = spec;
        Refresh();
        RangeChanged?.Invoke(this, new TimeRangeChangedEventArgs(spec, range!));
        return true;
    }

    /// <summary>Redraws the button, the resolved text and the notes: call it when the zone, the clock tick of a live range, <see cref="DataStartUtc"/> or <see cref="SampleInterval"/> changes.</summary>
    public void Refresh()
    {
        DetailPanel.Visibility = _compact ? Visibility.Collapsed : Visibility.Visible;
        if (!_value.TryResolve(NowUtc, Zone, out var range, out var error))
        {
            ButtonText.Text = _value.Name;
            ResolvedText.Text = error!.Message;
            NoteText.Visibility = Visibility.Collapsed;
            PickerButton.ToolTip = error.Message;
            return;
        }

        ButtonText.Text = ButtonLabel(range!);
        ResolvedText.Text = range!.StartText + " - " + range.EndText + " (" + range.ZoneText + ")";
        var notes = NotesFor(range);
        NoteText.Text = notes;
        NoteText.Visibility = notes.Length == 0 || _compact ? Visibility.Collapsed : Visibility.Visible;
        PickerButton.ToolTip = notes.Length == 0 ? range.Label : range.Label + "\n" + notes;
        UpdateSelectionMarks();
    }

    private DateTime NowUtc => TimeRangeSpec.Naive(NowProvider?.Invoke() ?? DateTime.UtcNow);

    private static string ButtonLabel(ResolvedTimeRange range) => range.Spec.Kind switch
    {
        TimeRangeKind.Fixed => "Custom (" + range.Length + ")",
        TimeRangeKind.Since => "Since " + range.StartText,
        _ => range.Spec.Name
    };

    private string NotesFor(ResolvedTimeRange range)
    {
        var notes = new List<string>(2);
        var dataStart = TimeRangeNotes.DataStartNote(range, _dataStartUtc);
        if (dataStart is not null)
        {
            notes.Add(dataStart);
        }

        var sample = TimeRangeNotes.SampleIntervalNote(range.Span, _sampleInterval);
        if (sample is not null)
        {
            notes.Add(sample);
        }

        return string.Join("  ", notes);
    }

    private void BuildPresetButtons()
    {
        foreach (var spec in TimeRangePresets.Rolling)
        {
            var button = new Button
            {
                Content = TimeRangePresets.ShortLabel(spec),
                ToolTip = spec.Name,
                MinWidth = 40,
                Padding = new Thickness(6, 3, 6, 3),
                Margin = new Thickness(0, 0, 4, 4),
                Tag = spec
            };
            AutomationProperties.SetName(button, spec.Name);
            button.Click += Preset_Click;
            RollingPanel.Children.Add(button);
            _rollingButtons.Add((button, spec));
        }

        foreach (var spec in TimeRangePresets.CalendarPeriods)
        {
            var length = new TextBlock { HorizontalAlignment = HorizontalAlignment.Right, VerticalAlignment = VerticalAlignment.Center };
            length.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundDimBrush");
            var row = new Grid();
            row.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
            row.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
            row.Children.Add(new TextBlock { Text = spec.Name, VerticalAlignment = VerticalAlignment.Center });
            Grid.SetColumn(length, 1);
            row.Children.Add(length);
            var button = new Button
            {
                Content = row,
                HorizontalContentAlignment = HorizontalAlignment.Stretch,
                Padding = new Thickness(8, 3, 8, 3),
                Margin = new Thickness(0, 0, 0, 2),
                Tag = spec
            };
            AutomationProperties.SetName(button, spec.Name);
            button.Click += Preset_Click;
            PeriodPanel.Children.Add(button);
            _periodButtons.Add((button, spec, length));
        }
    }

    /// <summary>Greys the rolling presets a rolling-only picker cannot take (under one unit); a plain picker enables them all.</summary>
    private void UpdateRollingButtons()
    {
        foreach (var (button, spec) in _rollingButtons)
        {
            var refusal = Refusal(spec);
            button.IsEnabled = refusal is null;
            button.ToolTip = refusal ?? spec.Name;
        }
    }

    /// <summary>Fills each period's current length ("3d") from the clock and zone, and greys one that is under the minimum right now.</summary>
    private void RefreshPeriodLengths()
    {
        var now = NowUtc;
        var zone = Zone;
        foreach (var (button, spec, length) in _periodButtons)
        {
            var text = TimeRangePresets.CurrentLength(spec, now, zone);
            length.Text = text ?? string.Empty;
            button.IsEnabled = text is not null;
            button.ToolTip = text is null ? spec.Name + " has only just started. Pick another range." : spec.Name;
        }
    }

    private void UpdateSelectionMarks()
    {
        foreach (var (button, spec) in _rollingButtons)
        {
            Mark(button, spec);
        }

        foreach (var (button, spec, _) in _periodButtons)
        {
            Mark(button, spec);
        }
    }

    private void Mark(Button button, TimeRangeSpec spec)
    {
        var selected = spec.Equals(_value);
        button.FontWeight = selected ? FontWeights.Bold : FontWeights.Normal;
        if (selected)
        {
            button.SetResourceReference(Control.BorderBrushProperty, "AccentBrush");
        }
        else
        {
            button.ClearValue(Control.BorderBrushProperty);
        }
    }

    private void PickerButton_Click(object sender, RoutedEventArgs e)
    {
        if (PickerPopup.IsOpen)
        {
            PickerPopup.IsOpen = false;
            return;
        }

        /* The click that dismissed the popup (StaysOpen=False closes it on mouse down) arrives here as a Click too. */
        if (Environment.TickCount64 - _closedAtTick < ReopenGuard.TotalMilliseconds)
        {
            return;
        }

        PickerPopup.IsOpen = true;
    }

    private void PickerPopup_Opened(object? sender, EventArgs e)
    {
        RefreshPeriodLengths();
        UpdateSelectionMarks();
        var wallNow = DisplayZone.ToDisplay(NowUtc, Zone);
        PickCalendar.DisplayDateEnd = wallNow.Date;
        PickCalendar.SelectedDates.Clear();
        PickCalendar.DisplayDate = Resolve() is { } held ? DisplayZone.ToDisplay(held.StartUtc, Zone).Date : wallNow.Date;
        InputBox.Clear();
        UpdateEcho();
        InputBox.Focus();
        Dispatcher.BeginInvoke(DispatcherPriority.Loaded, new Action(ApplyCalendarTheme));
    }

    private void PickerPopup_Closed(object? sender, EventArgs e)
    {
        _closedAtTick = Environment.TickCount64;
    }

    private void Preset_Click(object sender, RoutedEventArgs e)
    {
        if (sender is Button { Tag: TimeRangeSpec spec })
        {
            Apply(spec);
        }
    }

    private void Apply(TimeRangeSpec spec)
    {
        if (Select(spec))
        {
            PickerPopup.IsOpen = false;
        }
    }

    private void InputBox_TextChanged(object sender, TextChangedEventArgs e) => UpdateEcho();

    private void InputBox_KeyDown(object sender, KeyEventArgs e)
    {
        if (e.Key == Key.Enter)
        {
            ApplyTyped();
            e.Handled = true;
        }
    }

    private void ApplyButton_Click(object sender, RoutedEventArgs e) => ApplyTyped();

    private void CancelButton_Click(object sender, RoutedEventArgs e) => PickerPopup.IsOpen = false;

    private void PopupRoot_PreviewKeyDown(object sender, KeyEventArgs e)
    {
        if (e.Key == Key.Escape)
        {
            PickerPopup.IsOpen = false;
            e.Handled = true;
        }
    }

    private void ApplyTyped()
    {
        if (_typed is { Ok: true, Spec: { } spec })
        {
            Apply(spec);
        }
    }

    /// <summary>Parses the text box and shows the resolved range (and any notes) before Apply, or the reason it failed.</summary>
    private void UpdateEcho()
    {
        var text = InputBox.Text;
        if (string.IsNullOrWhiteSpace(text))
        {
            _typed = null;
            ApplyButton.IsEnabled = false;
            EchoText.Text = _rollingUnit is null
                ? "Examples: 45m, 3 days, last month, Oct 1 - Oct 2, 1:00 am - 7:00 am, since 10/1"
                : "Examples: 4h, 3 days, 2 weeks";
            EchoText.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundDimBrush");
            return;
        }

        _typed = TimeRangeParser.Parse(text, NowUtc, Zone);
        if (_typed.Ok && _typed.Spec is { } typedSpec && Refusal(typedSpec) is { } refusal)
        {
            _typed = null;
            EchoText.Text = refusal;
            EchoText.SetResourceReference(TextBlock.ForegroundProperty, "ErrorBrush");
            ApplyButton.IsEnabled = false;
            return;
        }

        if (_typed.Ok)
        {
            var notes = NotesFor(_typed.Range!);
            EchoText.Text = notes.Length == 0 ? _typed.Echo : _typed.Echo + "\n" + notes;
            EchoText.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundBrush");
            ApplyButton.IsEnabled = true;
        }
        else
        {
            EchoText.Text = _typed.Error;
            EchoText.SetResourceReference(TextBlock.ForegroundProperty, "ErrorBrush");
            ApplyButton.IsEnabled = false;
        }
    }

    private void PickCalendar_SelectedDatesChanged(object? sender, SelectionChangedEventArgs e)
    {
        if (PickCalendar.SelectedDates.Count == 0)
        {
            return;
        }

        var first = PickCalendar.SelectedDates.Min();
        var last = PickCalendar.SelectedDates.Max();
        var thisYear = DisplayZone.ToDisplay(NowUtc, Zone).Year;
        var showYear = first.Year != thisYear || last.Year != thisYear;
        var format = showYear ? "MMM d, yyyy" : "MMM d";
        var text = first == last
            ? first.ToString(format, CultureInfo.InvariantCulture)
            : first.ToString(format, CultureInfo.InvariantCulture) + " - " + last.ToString(format, CultureInfo.InvariantCulture);
        InputBox.Text = text;
        InputBox.CaretIndex = text.Length;
    }

    // ---- calendar theming (replaces the per-app copies of ApplyThemeToCalendar) ----

    private Brush Res(string key, Brush fallback)
        => TryFindResource(key) as Brush ?? fallback;

    private static bool IsDark(Brush background)
        => background is SolidColorBrush solid
           && (0.299 * solid.Color.R + 0.587 * solid.Color.G + 0.114 * solid.Color.B) < 128;

    /// <summary>Paints the calendar from the theme's brushes: the stock Calendar template is light, so a dark theme needs its chrome recoloured.</summary>
    private void ApplyCalendarTheme()
    {
        var background = Res("BackgroundBrush", Brushes.White);
        var foreground = Res("ForegroundBrush", Brushes.Black);
        var border = Res("BorderBrush", Brushes.Gray);
        PickCalendar.Background = background;
        PickCalendar.Foreground = foreground;
        PickCalendar.BorderBrush = border;
        PaintCalendarTree(PickCalendar, background, foreground, IsDark(background));
    }

    private static void PaintCalendarTree(DependencyObject parent, Brush background, Brush foreground, bool dark)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);
            switch (child)
            {
                case CalendarItem item:
                    item.Background = background;
                    item.Foreground = foreground;
                    break;
                case CalendarDayButton day:
                    if (!day.IsSelected)
                    {
                        day.Background = Brushes.Transparent;
                    }

                    day.Foreground = foreground;
                    break;
                case CalendarButton month:
                    month.Background = Brushes.Transparent;
                    month.Foreground = foreground;
                    break;
                case Button button:
                    button.Background = Brushes.Transparent;
                    button.Foreground = foreground;
                    break;
                case TextBlock text:
                    text.Foreground = foreground;
                    break;
                case Border border when dark && border.Background is SolidColorBrush bg && bg.Color.R > 200 && bg.Color.G > 200 && bg.Color.B > 200:
                    border.Background = background;
                    break;
                case Grid grid when dark && grid.Background is SolidColorBrush gridBg && gridBg.Color.R > 200 && gridBg.Color.G > 200 && gridBg.Color.B > 200:
                    grid.Background = background;
                    break;
            }

            PaintCalendarTree(child, background, foreground, dark);
        }
    }
}
