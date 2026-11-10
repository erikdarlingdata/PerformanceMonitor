/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Shapes;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8540: an unchecked check box and an unchosen radio button are outlined like the text box beside them in each Viewer theme, so the
/// box shows against the page and the card. Each theme is loaded into the test window's own resources, and every colour is read from
/// the element its template rendered.
/// </summary>
public sealed class ViewerCheckBoxOutlineTests
{
    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");

    private readonly record struct Outlines(Color CheckOutline, Color CheckFill, Color RadioOutline, Color RadioFill, Color TextBoxOutline, Color Page, Color Card);

    private static Color ColorOf(Brush? brush) => brush is SolidColorBrush solid ? solid.Color : Colors.Magenta;

    private static Outlines Render(string theme) => StaTestThread.Run(() =>
    {
        _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var check = new CheckBox { Content = "All Databases", IsChecked = false };
        var radio = new RadioButton { Content = "Estimated plan", IsChecked = false };
        var text = new TextBox { Text = "filter", Width = 120 };
        var panel = new StackPanel();
        panel.Children.Add(check);
        panel.Children.Add(radio);
        panel.Children.Add(text);
        var window = new Window { Content = panel, Width = 400, Height = 300 };
        window.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri(theme) });
        StaTestThread.ShowOffScreen(window);
        try
        {
            ClippedTextProbe.Settle(window);
            var box = (Border)check.Template.FindName("Border", check);
            var bullet = (Ellipse)radio.Template.FindName("Border", radio);
            var field = (Border)text.Template.FindName("border", text);
            return new Outlines(
                ColorOf(box.BorderBrush), ColorOf(box.Background),
                ColorOf(bullet.Stroke), ColorOf(bullet.Fill),
                ColorOf(field.BorderBrush),
                ColorOf((Brush)window.FindResource("BackgroundBrush")),
                ColorOf((Brush)window.FindResource("BackgroundLightBrush")));
        }
        finally
        {
            window.Close();
        }
    });

    private static double Luminance(Color color)
    {
        static double Channel(byte value)
        {
            var c = value / 255.0;
            return c <= 0.03928 ? c / 12.92 : Math.Pow((c + 0.055) / 1.055, 2.4);
        }

        return (0.2126 * Channel(color.R)) + (0.7152 * Channel(color.G)) + (0.0722 * Channel(color.B));
    }

    private static double Contrast(Color a, Color b)
    {
        var la = Luminance(a);
        var lb = Luminance(b);
        return (Math.Max(la, lb) + 0.05) / (Math.Min(la, lb) + 0.05);
    }

    /// <summary>The floor is 3:1 on the two light themes. On Dark it is 1.4:1, which the theme's control outline (1.59:1 on the page, 1.41:1 on a card) meets and its grid-line colour (1.25:1 and 1.12:1) does not.</summary>
    [Theory]
    [InlineData("Light", 3.0)]
    [InlineData("CoolBreeze", 3.0)]
    [InlineData("Dark", 1.4)]
    public void AnUncheckedCheckBox_IsOutlinedLikeTheTextBoxBesideIt(string theme, double floor)
    {
        var o = Render(theme);

        Assert.NotEqual(o.CheckFill, o.CheckOutline);
        foreach (var (name, surface) in new[] { ("page", o.Page), ("card", o.Card) })
        {
            var box = Contrast(o.CheckOutline, surface);
            var field = Contrast(o.TextBoxOutline, surface);
            Assert.True(box >= floor, $"{theme}: the unchecked box's outline {o.CheckOutline} is {box:0.00}:1 against the {name} {surface}, under {floor:0.0}:1");
            Assert.True(box >= field - 0.01, $"{theme}: the unchecked box's outline {o.CheckOutline} is {box:0.00}:1 against the {name} {surface}; the text box's outline {o.TextBoxOutline} is {field:0.00}:1");
        }
    }

    [Theory]
    [InlineData("Light", 3.0)]
    [InlineData("CoolBreeze", 3.0)]
    [InlineData("Dark", 1.4)]
    public void AnUnchosenRadioButton_IsOutlinedLikeTheTextBoxBesideIt(string theme, double floor)
    {
        var o = Render(theme);

        Assert.NotEqual(o.RadioFill, o.RadioOutline);
        foreach (var (name, surface) in new[] { ("page", o.Page), ("card", o.Card) })
        {
            var bullet = Contrast(o.RadioOutline, surface);
            var field = Contrast(o.TextBoxOutline, surface);
            Assert.True(bullet >= floor, $"{theme}: the unchosen radio button's outline {o.RadioOutline} is {bullet:0.00}:1 against the {name} {surface}, under {floor:0.0}:1");
            Assert.True(bullet >= field - 0.01, $"{theme}: the unchosen radio button's outline {o.RadioOutline} is {bullet:0.00}:1 against the {name} {surface}; the text box's outline {o.TextBoxOutline} is {field:0.00}:1");
        }
    }
}
