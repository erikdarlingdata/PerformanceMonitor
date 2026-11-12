using System;
using System.Windows;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// #8219: whether the window a control sits in is short, so the control can use a compact layout (the time slicer's 70 instead of
/// 130, Current Waits' smaller chart minimum). Lite's smallest window is 900x600, which leaves a tab only about 400 of height.
/// </summary>
internal static class ShortWindow
{
    /// <summary>A window under this many DIPs high is short. A 900x600 window is; 1280x800 is not.</summary>
    public const double Threshold = 700;

    public static bool IsShort(DependencyObject element) =>
        Window.GetWindow(element) is { ActualHeight: > 0 } window && window.ActualHeight < Threshold;

    /// <summary>
    /// Calls <paramref name="apply"/> when the element loads and each time its window changes size. Loaded fires again each time a tab is
    /// shown, so the window handler is moved with it and never doubles.
    /// </summary>
    public static void Track(FrameworkElement element, Action apply)
    {
        Window? hooked = null;
        void OnSize(object? sender, SizeChangedEventArgs e) => apply();
        void Unhook()
        {
            if (hooked is not null)
            {
                hooked.SizeChanged -= OnSize;
                hooked = null;
            }
        }

        element.Loaded += (_, _) =>
        {
            Unhook();
            hooked = Window.GetWindow(element);
            if (hooked is not null)
            {
                hooked.SizeChanged += OnSize;
            }

            apply();
        };
        element.Unloaded += (_, _) => Unhook();
    }
}
