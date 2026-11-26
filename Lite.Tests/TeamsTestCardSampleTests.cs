using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9611: <c>tools/teams-test-card</c> holds two sample Teams payloads and a script that posts them to a Teams webhook, for a
/// check by eye in a real Teams client. The samples are the shared builder's own output for the made-up alerts below, so
/// these tests build each alert and compare it with the committed file: a sample cannot drift from what Darling and Lite send.
/// </summary>
public class TeamsTestCardSampleTests
{
    private static readonly DateTime Fired = new(2026, 11, 25, 12, 0, 0, DateTimeKind.Utc);
    private static readonly AlertBranding Darling = new("Performance Monitor Darling", null);

    /// <summary>A critical built-in alert on a made-up server, with every part a card can carry: prose, advice facts, an
    /// incident section, a record section with query texts, and the triage link on a placeholder host.</summary>
    internal static string BuiltInAlert()
    {
        var context = new AlertContext { SeverityOverride = AlertSeverityLevel.Critical };
        context.Details.Add(new AlertDetailItem
        {
            Heading = "Blocking Chain 1",
            Fields = { ("Database", "SampleDB"), ("Head blocker session", "71"), ("Blocked sessions", "8"), ("Longest wait", "42 s") }
        });
        var queries = new AlertDetailItem { Heading = "Top Blocked Queries" };
        queries.Records.Add(new AlertDetailRecord(1, "SampleDB · session 88 · waited 42 s",
            [("Query Text", "UPDATE dbo.Orders SET status_code = 2 WHERE order_id = @p1")]));
        queries.Records.Add(new AlertDetailRecord(2, "SampleDB · session 91 · waited 37 s",
            [("Blocked", "SELECT * FROM [dbo].[Order_Lines] WHERE order_id = @p1"), ("Blocking", "UPDATE dbo.Orders SET status_code = 2 WHERE order_id = @p1")]));
        context.Details.Add(queries);
        context.Details.Add(new AlertDetailItem
        {
            Heading = "Find the head blocker",
            Body = "Investigation: session 71 holds a lock on dbo.Orders and has an open transaction.\n\nRemediation: have the owner commit or roll back, or end the session."
        });

        return WebhookAlertService.BuildTeamsPayload(
            "Blocking Detected", "SQL-TEST-01", "8 sessions blocked", "1", Darling, context: context,
            triageUrl: TriageLink.Build("https://example.com", "SQL-TEST-01", "Blocking Detected", Fired, "sample-key"),
            detailText: "8 sessions are blocked on SQL-TEST-01, the longest for 42 seconds.\n\nThe head blocker is session 71 in SampleDB.",
            nowUtc: Fired);
    }

    /// <summary>An external alert whose detail has three lines: a plain one, an indented one, and one that starts with "- ".</summary>
    internal static string ExternalAlert() =>
        WebhookAlertService.BuildTeamsPayload(
            "External:sample-source:nightly-report", "SQL-TEST-01", "3 failed", "", Darling,
            context: new AlertContext { External = new AlertExternalOrigin("sample-source", "nightly-report", 1) },
            detailText: "Report for 2026-11-25\n  rows loaded: 120\n- 3 rows failed",
            displayName: "Nightly report", nowUtc: Fired);

    /* The card's "Time (Local)" fact is the fixed instant in the zone of the machine that builds it, so it is the one value
       left out of the comparison. */
    private static string WithoutLocalTime(string payload)
    {
        var pattern = new Regex("(\"title\":\"Time \\(Local\\)\",\"value\":\")\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\"", RegexOptions.CultureInvariant);
        Assert.Single(pattern.Matches(payload));
        return pattern.Replace(payload, "$1(local)\"");
    }

    private static string Committed(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "TeamsTestCard", name), Encoding.UTF8);

    [Theory]
    [InlineData("builtin-alert.teams.json")]
    [InlineData("external-alert.teams.json")]
    public void ACommittedSample_IsExactlyWhatTheBuilderProduces(string name)
    {
        var built = name.StartsWith("builtin", StringComparison.Ordinal) ? BuiltInAlert() : ExternalAlert();
        var committed = Committed(name);

        Assert.True(
            WithoutLocalTime(committed) == WithoutLocalTime(built),
            $"tools/teams-test-card/{name} is not what the builder produces. Replace the file's whole content with this line:\n{built}");
    }

    [Fact]
    public void TheSamples_HoldOnlyMadeUpNames_APlaceholderLink_AndAsciiBytes()
    {
        var builtIn = Committed("builtin-alert.teams.json");
        var external = Committed("external-alert.teams.json");

        var card = TeamsCard.Parse(builtIn);
        Assert.Equal("attention", card.Style);
        Assert.Contains(("Server", "SQL-TEST-01"), card.Facts);
        Assert.Equal("Action.OpenUrl", card.Action!.Value.Type);
        Assert.StartsWith("https://example.com/", card.Action!.Value.Url, StringComparison.Ordinal);

        var externalCard = TeamsCard.Parse(external);
        Assert.EndsWith(@"\[External: sample-source\] Nightly report", externalCard.Title, StringComparison.Ordinal);
        var lines = Assert.Single(externalCard.Section("Details").Texts).Split("\n\n");
        Assert.Equal(3, lines.Length);
        Assert.Equal("rows loaded: 120", lines[1]);
        Assert.EndsWith("- 3 rows failed", lines[2], StringComparison.Ordinal);
        Assert.Null(externalCard.Action);

        /* The script posts the file's bytes as they are: one line of ASCII, so no encoding choice can change them. */
        foreach (var sample in new[] { builtIn, external })
        {
            Assert.DoesNotContain('\n', sample);
            Assert.DoesNotContain('\r', sample);
            Assert.All(sample, c => Assert.InRange(c, ' ', '~'));
        }
    }
}
