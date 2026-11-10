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

    private readonly record struct States(
        Color CheckMark, Color CheckedFill, Color RadioMark, Color RadioCheckedFill,
        Color DisabledCheckOutline, Color DisabledCheckMark, Color DisabledRadioOutline, Color DisabledRadioMark,
        Color Muted, Color Page);

    /// <summary>A checked check box and radio button, and a disabled checked pair, read from the elements their templates rendered.</summary>
    private static States RenderStates(string theme) => StaTestThread.Run(() =>
    {
        _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var check = new CheckBox { Content = "All Databases", IsChecked = true };
        var radio = new RadioButton { Content = "Estimated plan", IsChecked = true, GroupName = "enabled" };
        var offCheck = new CheckBox { Content = "Auto-refresh", IsChecked = true, IsEnabled = false };
        var offRadio = new RadioButton { Content = "Actual plan", IsChecked = true, IsEnabled = false, GroupName = "disabled" };
        var panel = new StackPanel();
        panel.Children.Add(check);
        panel.Children.Add(radio);
        panel.Children.Add(offCheck);
        panel.Children.Add(offRadio);
        var window = new Window { Content = panel, Width = 400, Height = 300 };
        window.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri(theme) });
        StaTestThread.ShowOffScreen(window);
        try
        {
            ClippedTextProbe.Settle(window);
            var box = (Border)check.Template.FindName("Border", check);
            var tick = (Path)check.Template.FindName("CheckMark", check);
            var bullet = (Ellipse)radio.Template.FindName("Border", radio);
            var dot = (Ellipse)radio.Template.FindName("CheckMark", radio);
            var offBox = (Border)offCheck.Template.FindName("Border", offCheck);
            var offTick = (Path)offCheck.Template.FindName("CheckMark", offCheck);
            var offBullet = (Ellipse)offRadio.Template.FindName("Border", offRadio);
            var offDot = (Ellipse)offRadio.Template.FindName("CheckMark", offRadio);
            return new States(
                ColorOf(tick.Stroke), ColorOf(box.Background), ColorOf(dot.Fill), ColorOf(bullet.Fill),
                ColorOf(offBox.BorderBrush), ColorOf(offTick.Stroke), ColorOf(offBullet.Stroke), ColorOf(offDot.Fill),
                ColorOf((Brush)window.FindResource("ForegroundMutedBrush")),
                ColorOf((Brush)window.FindResource("BackgroundBrush")));
        }
        finally
        {
            window.Close();
        }
    });

    /// <summary>The mark is what says a box is checked, so it is held to 3:1 on the checked fill.</summary>
    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    [InlineData("Dark")]
    public void TheMarkOfACheckedBox_Is3To1OnItsFill(string theme)
    {
        var s = RenderStates(theme);

        var tick = Contrast(s.CheckMark, s.CheckedFill);
        var dot = Contrast(s.RadioMark, s.RadioCheckedFill);
        Assert.True(tick >= 3.0, $"{theme}: the check mark {s.CheckMark} is {tick:0.00}:1 on the checked fill {s.CheckedFill}, under 3.0:1");
        Assert.True(dot >= 3.0, $"{theme}: the radio button's dot {s.RadioMark} is {dot:0.00}:1 on its fill {s.RadioCheckedFill}, under 3.0:1");
    }

    /// <summary>A disabled box is outlined no more strongly than an enabled one, and its mark is the muted colour.</summary>
    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    [InlineData("Dark")]
    public void ADisabledBox_IsOutlinedNoMoreStronglyThanAnEnabledOne(string theme)
    {
        var s = RenderStates(theme);
        var o = Render(theme);

        var enabledCheck = Contrast(o.CheckOutline, s.Page);
        var disabledCheck = Contrast(s.DisabledCheckOutline, s.Page);
        var enabledRadio = Contrast(o.RadioOutline, s.Page);
        var disabledRadio = Contrast(s.DisabledRadioOutline, s.Page);
        Assert.True(disabledCheck <= enabledCheck + 0.01, $"{theme}: a disabled check box's outline {s.DisabledCheckOutline} is {disabledCheck:0.00}:1 against the page; an enabled one's {o.CheckOutline} is {enabledCheck:0.00}:1");
        Assert.True(disabledRadio <= enabledRadio + 0.01, $"{theme}: a disabled radio button's outline {s.DisabledRadioOutline} is {disabledRadio:0.00}:1 against the page; an enabled one's {o.RadioOutline} is {enabledRadio:0.00}:1");
        Assert.Equal(s.Muted, s.DisabledCheckMark);
        Assert.Equal(s.Muted, s.DisabledRadioMark);
    }
}
