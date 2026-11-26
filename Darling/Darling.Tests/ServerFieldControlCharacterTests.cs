/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #9787: a display name, host, database or login holding a control character is refused by every writer, with one
/// sentence that names the field and never repeats the value, and a row saved before the rule prints escaped. The
/// service writers (MCP add_servers and edit_server, the web add and edit routes, the command-line add) are driven
/// through their own entry over a store that is never opened. The Viewer's save is driven through its upsert, which
/// refuses before it connects.
/// </summary>
public sealed class ServerFieldControlCharacterTests
{
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private static readonly Edit.ServerProbe ThrowingProbe =
        (_, _) => throw new InvalidOperationException("the probe must not run for a refused request");

    /// <summary>One of each class: line feed, carriage return, tab, DEL, a C1 control (NEL), the two Unicode separators,
    /// a right-to-left override and an invisible tag character from outside the Basic Multilingual Plane.</summary>
    public static TheoryData<string, string> FieldsAndCharacters()
    {
        var data = new TheoryData<string, string>();
        foreach (var field in new[] { "display_name", "host", "database", "username" })
        {
            foreach (var character in new[] { "\n", "\r", "\t", "\u007F", "\u0085", "\u2028", "\u2029", "\u202E", "\U000E0041" })
            {
                data.Add(field, character);
            }
        }

        return data;
    }

    private static string AddBody(string field, string value)
    {
        var entry = new System.Collections.Generic.Dictionary<string, object>
        {
            ["host"] = "sql01.example.test",
            ["auth"] = "SQL",
            ["username"] = "monitor",
            ["password"] = "pw-secret-1",
            [field] = "x" + value + "y",
        };
        return JsonSerializer.Serialize(new[] { entry });
    }

    private static string EditBody(string field, string value) =>
        JsonSerializer.Serialize(new System.Collections.Generic.Dictionary<string, object> { [field] = "x" + value + "y" });

    private static void AssertRefusesByField(string? message, string field, string value)
    {
        Assert.NotNull(message);
        Assert.Contains(field, message, StringComparison.Ordinal);
        Assert.DoesNotContain(value, message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(FieldsAndCharacters))]
    public void TheAddParse_RefusesAControlCharacterInTheField_AndNeverRepeatsTheValue(string field, string value)
    {
        var (entries, invalid, wholeError) = Edit.ParseRequest(AddBody(field, value), TestKeyRings.Healthy.Status);

        Assert.Null(wholeError);
        Assert.Empty(entries);
        var refused = Assert.Single(invalid);
        Assert.Equal("invalid", refused.Status);
        AssertRefusesByField(refused.Detail, field, value);
        Assert.DoesNotContain(value, refused.Server, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(FieldsAndCharacters))]
    public void TheCommandLineAddParse_RefusesTheSameFields(string field, string value)
    {
        var (entries, invalid, _) = Edit.ParseRequest(AddBody(field, value), TestKeyRings.Healthy.Status, allowSecretReferences: true);

        Assert.Empty(entries);
        AssertRefusesByField(Assert.Single(invalid).Detail, field, value);
    }

    [Theory]
    [MemberData(nameof(FieldsAndCharacters))]
    public async Task TheAddTool_AnswersInvalid_WithoutOpeningTheStoreOrProbing(string field, string value)
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);

        var answer = await Edit.AddServersAsync(dead, AddBody(field, value), ThrowingProbe, CancellationToken.None, ring: TestKeyRings.Healthy);

        using var doc = JsonDocument.Parse(answer);
        Assert.Equal(0, doc.RootElement.GetProperty("added").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("failed").GetInt32());
        AssertRefusesByField(answer, field, value);
    }

    [Theory]
    [MemberData(nameof(FieldsAndCharacters))]
    public void TheEditParse_RefusesAControlCharacterInTheField_AndNeverRepeatsTheValue(string field, string value)
    {
        var (changes, error) = Edit.ParseEditChanges(EditBody(field, value));

        Assert.Null(changes);
        AssertRefusesByField(error, field, value);
    }

    [Theory]
    [MemberData(nameof(FieldsAndCharacters))]
    public async Task TheEditTool_AndTheWebEditRoute_AnswerInvalid_WithoutOpeningTheStore(string field, string value)
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);

        var byName = await Edit.EditServerByNameAsync(
            dead, "sql01", EditBody(field, value), ThrowingProbe, TestKeyRings.Healthy, null, CancellationToken.None);
        var byId = await Edit.EditServerByIdAsync(dead, 41, EditBody(field, value), null, CancellationToken.None);

        foreach (var answer in new[] { byName, byId })
        {
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(answer));
            AssertRefusesByField(answer, field, value);
        }
    }

    [Fact]
    public void AnEditThatClearsTheDatabaseOrLogin_AndOrdinaryUnicodeText_AreNotRefused()
    {
        var (cleared, clearError) = Edit.ParseEditChanges("{\"database\":null,\"username\":null}");
        Assert.Null(clearError);
        Assert.NotNull(cleared);

        var (unicode, unicodeError) = Edit.ParseEditChanges(
            JsonSerializer.Serialize(new { display_name = "Z\u00FCrich \u2713 \U0001F680 prod", host = "db\u00E9.example.test" }));
        Assert.Null(unicodeError);
        Assert.Equal("Z\u00FCrich \u2713 \U0001F680 prod", unicode!.DisplayName);

        var (entries, invalid, _) = Edit.ParseRequest(
            JsonSerializer.Serialize(new[] { new { host = "sql01.example.test", display_name = "Z\u00FCrich \u2713", auth = "Windows" } }),
            TestKeyRings.Healthy.Status);
        Assert.Empty(invalid);
        Assert.Single(entries);
    }

    [Theory]
    [InlineData(nameof(MonitoredServerRow.Name))]
    [InlineData(nameof(MonitoredServerRow.Host))]
    [InlineData(nameof(MonitoredServerRow.Database))]
    [InlineData(nameof(MonitoredServerRow.Username))]
    public async Task TheViewerSave_RefusesAControlCharacterInTheField_BeforeItConnects(string property)
    {
        var row = new MonitoredServerRow { Name = "alpha", Host = "sql01.example.test" };
        typeof(MonitoredServerRow).GetProperty(property)!.SetValue(row, "x\ny");
        await using var viewer = new ViewerDataService(DeadStore);

        var refused = await Assert.ThrowsAsync<MonitoredServerFieldRefusedException>(() => viewer.UpsertMonitoredServerAsync(row));

        Assert.DoesNotContain("\n", refused.Message, StringComparison.Ordinal);
        Assert.Contains(property == nameof(MonitoredServerRow.Name) ? "display_name" : property.ToLowerInvariant(), refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ARowSavedBeforeTheRule_PrintsEscaped_InTheDriftLine_AndTheSkippedPasswordNote()
    {
        var drifted = new[]
        {
            new StoreConfigProvider.ServerSettingDrift(
                "evil\nname\u2028two", true,
                [new StoreConfigProvider.SettingDrift("host", "a\r\nb", "c\u0085d", true)],
                FileEntryCarriesPassword: true),
        };

        var line = StoreConfigProvider.FormatSettingDrift(drifted, 10);
        var note = StoreConfigProvider.DescribePasswordNotUsed(drifted, 10);

        foreach (var text in new[] { line, note })
        {
            Assert.DoesNotContain(text, ServerFieldText.IsControl);
            Assert.Contains("evil", text, StringComparison.Ordinal);
        }

        Assert.Contains("\\u000A", line, StringComparison.Ordinal);
        Assert.Contains("\\u2028", line, StringComparison.Ordinal);
        Assert.Contains("\\u0085", line, StringComparison.Ordinal);
    }

    [Fact]
    public void TheEscaper_WritesEachRefusedCharacterVisibly_AndLeavesEverythingElseAlone()
    {
        Assert.Equal("a\\u000Ab\\u0009c\\u007Fd\\u2029e\\u202Ef", ServerFieldText.Escape("a\nb\tc\u007Fd\u2029e\u202Ef"));
        Assert.Equal("Z\u00FCrich \u2713 \U0001F680", ServerFieldText.Escape("Z\u00FCrich \u2713 \U0001F680"));
        Assert.Equal(string.Empty, ServerFieldText.Escape(null));
        Assert.DoesNotContain(ServerFieldText.Escape("line\nbreak"), ServerFieldText.IsControl);
    }

    [Fact]
    public void TheRule_JudgesWholeCodePoints_SoATagCharacterIsRefused_AndASurrogateIsLeftAlone()
    {
        Assert.True(ServerFieldText.HasControlCharacter("ab\U000E0041\U000E007Fc"));
        Assert.NotNull(ServerFieldText.Refusal("display_name", "prod\U000E0001"));
        Assert.Equal("ab\\U000E0041\\U000E007Fc", ServerFieldText.Escape("ab\U000E0041\U000E007Fc"));

        Assert.False(ServerFieldText.HasControlCharacter("rocket \U0001F680"));
        Assert.False(ServerFieldText.HasControlCharacter("lone \uD800 half"));
        Assert.Equal("lone \uD800 half \\u000A", ServerFieldText.Escape("lone \uD800 half \n"));
    }
}
