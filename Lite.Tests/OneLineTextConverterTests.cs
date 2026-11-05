/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// A multi-line statement made its Long Queries row as tall as the statement, because a trimmed TextBlock still
/// breaks at every line break (#7382). The cell now shows <see cref="OneLineTextConverter"/>'s one-line form while
/// the bound property keeps the full text, so Copy Cell, Copy Row and the exports (which read that property off
/// the row) still give the original line breaks.
/// </summary>
public sealed class OneLineTextConverterTests
{
    private sealed class Row
    {
        public string StatementText { get; set; } = "";
    }

    private const string MultiLine = "\r\n\r\n   SELECT a,\r\n\t   b\r\n  FROM dbo.t\n WHERE x = 1  \r\n";

    [Fact]
    public void OneLine_CollapsesLineBreaksAndIndentation_AndDropsLeadingBlankLines()
    {
        Assert.Equal("SELECT a, b FROM dbo.t WHERE x = 1", OneLineTextConverter.OneLine(MultiLine));
    }

    [Theory]
    [InlineData(null, "")]
    [InlineData("", "")]
    [InlineData("   \r\n\t ", "")]
    [InlineData("SELECT 1", "SELECT 1")]
    public void OneLine_HandlesEmptyAndAlreadyFlatText(string? text, string expected)
    {
        Assert.Equal(expected, OneLineTextConverter.OneLine(text));
    }

    [Fact]
    public void OneLine_CapsAHugeBatch_AndStaysOnOneLine()
    {
        var huge = string.Concat(System.Linq.Enumerable.Repeat("SELECT 1\r\n", 100_000));

        var flat = OneLineTextConverter.OneLine(huge);

        Assert.InRange(flat.Length, 1, OneLineTextConverter.MaxLength + 1);
        Assert.DoesNotContain('\n', flat);
        Assert.DoesNotContain('\r', flat);
    }

    [Fact]
    public void ACellBoundThroughTheConverter_ShowsOneLine_AndCopyAndExportKeepTheFullText()
    {
        var (shown, templateCopy, boundCopy) = StaTestThread.Run(() =>
        {
            var row = new Row { StatementText = MultiLine };

            var cell = new TextBlock { DataContext = row };
            cell.SetBinding(TextBlock.TextProperty,
                new Binding(nameof(Row.StatementText)) { Converter = OneLineTextConverter.Instance });

            var factory = new FrameworkElementFactory(typeof(TextBlock));
            factory.SetBinding(TextBlock.TextProperty,
                new Binding(nameof(Row.StatementText)) { Converter = OneLineTextConverter.Instance });
            var template = new DataTemplate { VisualTree = factory };
            template.Seal();
            var templateColumn = new DataGridTemplateColumn { CellTemplate = template };
            var boundColumn = new DataGridTextColumn
            {
                Binding = new Binding(nameof(Row.StatementText)) { Converter = OneLineTextConverter.Instance },
            };

            return (cell.Text,
                    DataGridExport.GetRawCellValue(templateColumn, row),
                    DataGridExport.GetRawCellValue(boundColumn, row));
        });

        Assert.Equal("SELECT a, b FROM dbo.t WHERE x = 1", shown);
        Assert.Equal(MultiLine, templateCopy);
        Assert.Equal(MultiLine, boundCopy);
    }

    [Fact]
    public void ConvertBack_IsNotSupported()
    {
        Assert.Throws<NotSupportedException>(() =>
            OneLineTextConverter.Instance.ConvertBack("x", typeof(string), null!, System.Globalization.CultureInfo.InvariantCulture));
    }
}
