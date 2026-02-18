using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite.Helpers;

namespace PerformanceMonitorLite.Controls;

public partial class LoadingOverlay : UserControl
{
    public static readonly DependencyProperty IsLoadingProperty =
        DependencyProperty.Register(
            nameof(IsLoading),
            typeof(bool),
            typeof(LoadingOverlay),
            new PropertyMetadata(false, OnIsLoadingChanged));

    public bool IsLoading
    {
        get => (bool)GetValue(IsLoadingProperty);
        set => SetValue(IsLoadingProperty, value);
    }

    public LoadingOverlay()
    {
        InitializeComponent();
    }

    private static void OnIsLoadingChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
    {
        if (d is LoadingOverlay overlay)
        {
            var isLoading = (bool)e.NewValue;
            overlay.Visibility = isLoading ? Visibility.Visible : Visibility.Collapsed;

            if (isLoading)
            {
                overlay.MessageText.Text = LoadingMessages.GetRandom();
            }
        }
    }
}
