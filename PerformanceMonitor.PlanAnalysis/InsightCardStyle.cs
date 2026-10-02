namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// The header foreground and accent-edge opacity for one Plan Insights card, given whether that
/// card currently has anything to report. Ported from PerformanceStudio dev (erikdarlingdata/PerformanceStudio@87bad14),
/// which unified five differently-tinted insight cards into one neutral surface with a per-card
/// accent edge and a quiet empty state: a card with nothing to say goes quiet instead of shouting
/// in its own tint ("No missing index suggestions" on a full colour block reads as an alert).
/// </summary>
/// <remarks>
/// A card carries two visual states: its accent identity (the header text colour and the accent
/// edge's own colour, both fixed per card) and quiet/non-quiet (whether that identity is shown at
/// full strength or dimmed toward the muted foreground). This type computes only the quiet/non-quiet
/// half, which is the part that changes at runtime as a panel fills or empties; the accent identity
/// is a compile-time theme resource lookup and does not need a helper.
/// </remarks>
public static class InsightCardStyle
{
    /// <summary>
    /// The accent edge's opacity for a card in the given state. PerformanceStudio's dimmed edge is
    /// 0.35; a populated card's edge is full strength.
    /// </summary>
    public const double QuietAccentOpacity = 0.35;
    public const double NormalAccentOpacity = 1.0;

    /// <summary>
    /// Whether a card's header should draw in its accent colour (false) or the muted foreground
    /// (true). Mirrors PS's mutually-exclusive ".header:not(.empty)" / ".header.empty" style
    /// selectors: exactly one is true for any given <paramref name="isEmpty"/>.
    /// </summary>
    public static bool HeaderUsesMutedForeground(bool isEmpty) => isEmpty;

    /// <summary>The accent edge's opacity for a card that is empty (<paramref name="isEmpty"/> true) or populated.</summary>
    public static double AccentOpacity(bool isEmpty) => isEmpty ? QuietAccentOpacity : NormalAccentOpacity;
}
