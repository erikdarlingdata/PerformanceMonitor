/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Markup;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Xml.Linq;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4683: while the store is unavailable the sidebar's <c>ServerList</c> is disabled (#4648), and the stock
/// ListBox template's IsEnabled=False trigger sets its inner border to white by TargetName, which beats the
/// Background the list carries, so the list rendered as a solid white box in every theme. Each Viewer theme now
/// declares an implicit ListBox style whose template is a copy of the stock one with a different disabled
/// trigger: the list keeps its own Background and only dims. These render the result. Each check loads the
/// shipped theme dictionary from disk, hosts a ListBox in a container painted with the window background, and
/// reads pixels back from a <see cref="RenderTargetBitmap"/>. They need STA and WPF, which is why they are not
/// part of <c>ViewerStoreUnavailableShellTests</c> (source pins that run anywhere).
/// </summary>
public sealed class ViewerDisabledListBoxThemeTests
{
    private static readonly XNamespace Wpf = "http://schemas.microsoft.com/winfx/2006/xaml/presentation";
    private static readonly XNamespace X = "http://schemas.microsoft.com/winfx/2006/xaml";

    /// <summary>The theme's own disabled dimming: the implicit Button, AccentButton, SuccessButton and
    /// PasswordBox styles in the same dictionaries all use it.</summary>
    private const double DisabledOpacity = 0.5;

    private const int Width = 160;
    private const int Height = 120;

    [Theory]
    [InlineData("DarkTheme.xaml")]
    [InlineData("LightTheme.xaml")]
    [InlineData("CoolBreezeTheme.xaml")]
    public void DisabledListBox_RendersTheThemedBackgroundDimmed_NotStockWhite(string themeFile)
    {
        OnStaThread(() =>
        {
            var list = new ListBox { IsEnabled = false };
            list.SetResourceReference(Control.BackgroundProperty, "BackgroundDarkBrush");
            list.SetResourceReference(Control.BorderBrushProperty, "BorderBrush");

            AssertInteriorIsThemedAndDimmed(list, LoadTheme(themeFile), themeFile);
        });
    }

    [Theory]
    [InlineData("DarkTheme.xaml")]
    [InlineData("LightTheme.xaml")]
    [InlineData("CoolBreezeTheme.xaml")]
    public void DisabledServerList_AsShipped_RendersTheThemedBackgroundDimmed(string themeFile)
    {
        OnStaThread(() =>
        {
            var list = ParseServerList();
            list.IsEnabled = false;

            AssertInteriorIsThemedAndDimmed(list, LoadTheme(themeFile), themeFile + " ServerList");
        });
    }

    [Theory]
    [InlineData("DarkTheme.xaml")]
    [InlineData("LightTheme.xaml")]
    [InlineData("CoolBreezeTheme.xaml")]
    public void EnabledListBox_RendersIdenticallyToTheStockTemplate(string themeFile)
    {
        OnStaThread(() =>
        {
            var themeStyle = Assert.IsType<Style>(LoadTheme(themeFile)[typeof(ListBox)]);
            var withThemeStyle = new ResourceDictionary { [typeof(ListBox)] = themeStyle };

            var stock = RenderSample(null, enabled: true);
            var themed = RenderSample(withThemeStyle, enabled: true);
            int differing = stock.Where((b, i) => b != themed[i]).Count();
            Assert.True(differing == 0,
                $"{themeFile}: an ENABLED list must render exactly as the stock template does, but {differing} bytes differ");

            /* Guard against a vacuous pass: were the style not reaching the list, both renders above would be
               the stock one and match trivially. Disabled, the two templates must part ways (stock white). */
            Assert.False(RenderSample(null, enabled: false).SequenceEqual(RenderSample(withThemeStyle, enabled: false)),
                $"{themeFile}: the implicit ListBox style did not reach the sample list");
        });
    }

    private static void AssertInteriorIsThemedAndDimmed(ListBox list, ResourceDictionary theme, string label)
    {
        var surface = ((SolidColorBrush)theme["BackgroundLightBrush"]).Color;
        var listColor = ((SolidColorBrush)theme["BackgroundDarkBrush"]).Color;
        var pixels = Render(list, theme, surface);

        int at = ((Height / 2) * Width + Width / 2) * 4;
        byte blue = pixels[at], green = pixels[at + 1], red = pixels[at + 2];
        byte expectedRed = Blend(listColor.R, surface.R);
        byte expectedGreen = Blend(listColor.G, surface.G);
        byte expectedBlue = Blend(listColor.B, surface.B);

        Assert.True(
            Near(red, expectedRed) && Near(green, expectedGreen) && Near(blue, expectedBlue),
            $"{label}: the disabled list's interior renders as #{red:X2}{green:X2}{blue:X2}, but the theme's " +
            $"BackgroundDarkBrush {listColor} at {DisabledOpacity} opacity over the window background {surface} " +
            $"is #{expectedRed:X2}{expectedGreen:X2}{expectedBlue:X2} (stock white is #FFFFFF)");
    }

    private static byte Blend(byte list, byte surface) =>
        (byte)Math.Round(list * DisabledOpacity + surface * (1 - DisabledOpacity));

    private static bool Near(byte actual, byte expected) => Math.Abs(actual - expected) <= 2;

    /// <summary>Renders <paramref name="list"/> at a fixed size inside a border painted <paramref name="surface"/>
    /// (the window background), with <paramref name="resources"/> above it the way App.xaml's merged theme sits
    /// above the window. Returns the bitmap as Pbgra32 bytes.</summary>
    private static byte[] Render(ListBox list, ResourceDictionary? resources, Color surface)
    {
        list.Margin = new Thickness(20);
        var host = new Border { Background = new SolidColorBrush(surface), Child = list };
        if (resources is not null)
        {
            host.Resources = resources;
        }

        host.Measure(new Size(Width, Height));
        host.Arrange(new Rect(0, 0, Width, Height));
        host.UpdateLayout();

        var bitmap = new RenderTargetBitmap(Width, Height, 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(host);
        var pixels = new byte[Width * Height * 4];
        bitmap.CopyPixels(pixels, Width * 4, 0);
        return pixels;
    }

    /// <summary>A list with its own colors, padding and a selected row, so the border, the padding and the
    /// ScrollViewer / ItemsPresenter path all show up in the comparison.</summary>
    private static byte[] RenderSample(ResourceDictionary? resources, bool enabled)
    {
        var list = new ListBox
        {
            Background = new SolidColorBrush(Color.FromRgb(0x20, 0x30, 0x40)),
            BorderBrush = new SolidColorBrush(Color.FromRgb(0x80, 0x90, 0xA0)),
            BorderThickness = new Thickness(2),
            Padding = new Thickness(3),
            IsEnabled = enabled,
        };
        foreach (var item in new[] { "alpha", "beta", "gamma" })
        {
            list.Items.Add(item);
        }

        list.SelectedIndex = 1;
        return Render(list, resources, Colors.Gray);
    }

    private static ResourceDictionary LoadTheme(string themeFile)
    {
        using var stream = File.OpenRead(PathTo("Darling", "PerformanceMonitor.Darling.Viewer", "Themes", themeFile));
        return (ResourceDictionary)XamlReader.Load(stream);
    }

    /// <summary>The real <c>ServerList</c> element, minus what only the window can supply: the two handler
    /// attributes, and the row templates in <c>ListBox.Resources</c> / <c>ListBox.ItemTemplateSelector</c>
    /// (they name code-behind handlers and Window-level resources). Everything else, the Background /
    /// BorderBrush / BorderThickness attributes included, is parsed as shipped.</summary>
    private static ListBox ParseServerList()
    {
        var element = new XElement(XDocument
            .Load(PathTo("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"))
            .Descendants(Wpf + "ListBox")
            .Single(e => (string?)e.Attribute(X + "Name") == "ServerList"));
        element.Attribute("SelectionChanged")?.Remove();
        element.Attribute("MouseDoubleClick")?.Remove();
        element.Element(Wpf + "ListBox.Resources")?.Remove();
        element.Element(Wpf + "ListBox.ItemTemplateSelector")?.Remove();
        element.SetAttributeValue(XNamespace.Xmlns + "x", X.NamespaceName);
        return (ListBox)XamlReader.Parse(element.ToString());
    }

    /// <summary>WPF objects require STA; same shape as <c>RawWindowFloorViewerPortTests</c>.</summary>
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
