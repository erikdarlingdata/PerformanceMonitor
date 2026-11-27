using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>One section of a built Teams card: the bold heading (when it has one), its other TextBlock texts, its facts in
/// order, and the texts of its TextRuns.</summary>
internal sealed record TeamsCardSection(
    string? Heading,
    IReadOnlyList<string> Texts,
    IReadOnlyList<(string Title, string Value)> Facts,
    IReadOnlyList<string> Runs);

/// <summary>
/// Reads a payload <c>WebhookAlertService.BuildTeamsPayload</c> built, the way a Teams webhook does: the message envelope,
/// its one Adaptive Card attachment, and the card's parts. Parsing asserts the envelope, so every test that reads a card
/// also checks the shape Teams takes.
/// </summary>
internal sealed class TeamsCard
{
    private TeamsCard(JsonElement root, JsonElement content)
    {
        Root = root;
        Content = content;
        var body = content.GetProperty("body").EnumerateArray().ToList();
        var header = body[0];
        Style = header.GetProperty("style").GetString()!;
        Bleed = header.GetProperty("bleed").GetBoolean();
        var headerItems = header.GetProperty("items").EnumerateArray().ToList();
        Title = headerItems[0].GetProperty("text").GetString()!;
        Subtitle = headerItems[1].GetProperty("text").GetString()!;
        Facts = FactsOf(body[1]);
        Sections = body.Skip(2).Select(SectionOf).ToList();
        Action = content.TryGetProperty("actions", out var actions)
            ? (actions[0].GetProperty("type").GetString()!, actions[0].GetProperty("title").GetString()!, actions[0].GetProperty("url").GetString()!)
            : null;
    }

    public JsonElement Root { get; }

    /// <summary>The Adaptive Card itself (the attachment's <c>content</c>).</summary>
    public JsonElement Content { get; }

    public string Style { get; }

    public bool Bleed { get; }

    public string Title { get; }

    public string Subtitle { get; }

    /// <summary>The lead facts under the header.</summary>
    public IReadOnlyList<(string Title, string Value)> Facts { get; }

    /// <summary>Every section after the lead facts, in order.</summary>
    public IReadOnlyList<TeamsCardSection> Sections { get; }

    public (string Type, string Title, string Url)? Action { get; }

    /// <summary>Every string a reader of the card sees: TextBlock texts, fact titles and values, TextRun texts and the action's title.</summary>
    public IReadOnlyList<string> AllText =>
        new[] { Title, Subtitle }
            .Concat(Facts.SelectMany(f => new[] { f.Title, f.Value }))
            .Concat(Sections.SelectMany(s => (s.Heading is null ? [] : new[] { s.Heading })
                .Concat(s.Texts).Concat(s.Facts.SelectMany(f => new[] { f.Title, f.Value })).Concat(s.Runs)))
            .Concat(Action is { } action ? [action.Title] : [])
            .ToList();

    public TeamsCardSection Section(string heading) => Assert.Single(Sections, s => s.Heading == heading);

    public static TeamsCard Parse(string payload)
    {
        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal(["type", "attachments"], root.EnumerateObject().Select(p => p.Name));
        Assert.Equal("message", root.GetProperty("type").GetString());
        var attachment = Assert.Single(root.GetProperty("attachments").EnumerateArray());
        Assert.Equal(["contentType", "contentUrl", "content"], attachment.EnumerateObject().Select(p => p.Name));
        Assert.Equal("application/vnd.microsoft.card.adaptive", attachment.GetProperty("contentType").GetString());
        Assert.Equal(JsonValueKind.Null, attachment.GetProperty("contentUrl").ValueKind);
        var content = attachment.GetProperty("content");
        Assert.Equal("AdaptiveCard", content.GetProperty("type").GetString());
        return new TeamsCard(root, content);
    }

    private static List<(string Title, string Value)> FactsOf(JsonElement factSet)
    {
        Assert.Equal("FactSet", factSet.GetProperty("type").GetString());
        return factSet.GetProperty("facts").EnumerateArray()
            .Select(f => (f.GetProperty("title").GetString()!, f.GetProperty("value").GetString()!)).ToList();
    }

    private static TeamsCardSection SectionOf(JsonElement container)
    {
        Assert.Equal("Container", container.GetProperty("type").GetString());
        Assert.True(container.GetProperty("separator").GetBoolean());
        string? heading = null;
        var texts = new List<string>();
        var facts = new List<(string, string)>();
        var runs = new List<string>();
        foreach (var item in container.GetProperty("items").EnumerateArray())
        {
            switch (item.GetProperty("type").GetString())
            {
                case "TextBlock":
                    Assert.True(item.GetProperty("wrap").GetBoolean());
                    if (heading is null && texts.Count == 0 && item.TryGetProperty("weight", out var weight) && weight.GetString() == "Bolder")
                    {
                        heading = item.GetProperty("text").GetString();
                    }
                    else
                    {
                        texts.Add(item.GetProperty("text").GetString()!);
                    }

                    break;
                case "FactSet":
                    facts.AddRange(FactsOf(item));
                    break;
                case "RichTextBlock":
                    foreach (var run in item.GetProperty("inlines").EnumerateArray())
                    {
                        Assert.Equal("TextRun", run.GetProperty("type").GetString());
                        Assert.Equal("Monospace", run.GetProperty("fontType").GetString());
                        runs.Add(run.GetProperty("text").GetString()!);
                    }

                    break;
                default:
                    Assert.Fail($"unexpected card element {item.GetProperty("type").GetString()}");
                    break;
            }
        }

        return new TeamsCardSection(heading, texts, facts, runs);
    }
}
