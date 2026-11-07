/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Markup;
using System.Windows.Media;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8169: Lite's three themes carry the Viewer's theme fixes: the editable ComboBox shows its text box (#6452), a disabled ListBox
/// keeps its own background (#4683), and the FreeTextCell style caps a free-text cell's height (#6225). Each control is rendered
/// with the real theme file.
/// </summary>
public sealed class LiteThemeViewerFixesTests
{
    private static ResourceDictionary Theme(string name) =>
        (ResourceDictionary)XamlReader.Parse(ParitySource.ReadFile($"Lite/Themes/{name}Theme.xaml"));

    private static T Render<T>(ResourceDictionary theme, T control) where T : FrameworkElement
    {
        var host = new Border { Child = control, Width = 300, Height = 200 };
        host.Resources.MergedDictionaries.Add(theme);
        host.Measure(new Size(300, 200));
        host.Arrange(new Rect(0, 0, 300, 200));
        host.UpdateLayout();
        return control;
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void AnEditableComboBox_ShowsItsTextBox_AndAFixedOneShowsItsSelection(string theme)
    {
        StaTestThread.Run(() =>
        {
            var dict = Theme(theme);
            var editable = Render(dict, new ComboBox { IsEditable = true, Text = "typed" });
            var box = (TextBox)editable.Template.FindName("PART_EditableTextBox", editable);
            var site = (FrameworkElement)editable.Template.FindName("ContentSite", editable);
            Assert.Equal(Visibility.Visible, box.Visibility);
            Assert.Equal(Visibility.Collapsed, site.Visibility);

            var fixedCombo = Render(dict, new ComboBox { IsEditable = false });
            var fixedBox = (TextBox)fixedCombo.Template.FindName("PART_EditableTextBox", fixedCombo);
            Assert.Equal(Visibility.Hidden, fixedBox.Visibility);
        });
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void ADisabledListBox_KeepsItsOwnBackground_AndOnlyDims(string theme)
    {
        StaTestThread.Run(() =>
        {
            var dict = Theme(theme);
            var own = new SolidColorBrush(Color.FromRgb(0x12, 0x34, 0x56));
            var list = Render(dict, new ListBox { Background = own, IsEnabled = false });
            var border = (Border)list.Template.FindName("Bd", list);
            Assert.Equal(own.Color, ((SolidColorBrush)border.Background).Color);
            Assert.True(list.Opacity < 1, "a disabled list dims");
        });
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheFreeTextCellStyle_CapsTheHeightAndTrimsTheText(string theme)
    {
        StaTestThread.Run(() =>
        {
            var dict = Theme(theme);
            var cell = Render(dict, new TextBlock
            {
                Style = (Style)dict["FreeTextCell"],
                Text = string.Join("\n", Enumerable.Range(0, 40).Select(i => "line " + i)),
                TextWrapping = TextWrapping.Wrap,
            });
            Assert.True(cell.ActualHeight <= 90 + 0.5, $"a 40-line text is {cell.ActualHeight:F0} tall");
            Assert.Equal(TextTrimming.CharacterEllipsis, cell.TextTrimming);
        });
    }
}
