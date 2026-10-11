/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5642: the newest-sample age line on the shared notification core. The engine computes the carrier ONCE
/// when the alert fires (<see cref="AlertContext.NewestSample"/>); every channel prints it once, from the
/// carrier, and none measures it again. CPU's hand-built detail text carries the line itself while its
/// context holds no Details, so the once-only rule is the one most worth pinning on every vehicle.
/// </summary>
public class AlertSampleAgeLineTests
{
    private const string Line = "Newest CPU sample: 15:43:29 UTC, 5m 20s old";
    private const string LinePrefix = "Newest CPU sample:";

    private static readonly AlertBranding Branding = new("Test Edition", null);

    private static AlertNewestSample Sample() =>
        new("CPU", new DateTime(2026, 10, 11, 15, 43, 29, DateTimeKind.Utc), TimeSpan.FromSeconds(320));

    /// <summary>A context with a structured Details item (every alert but CPU).</summary>
    private static AlertContext WithDetails(bool withSample = true)
    {
        var ctx = new AlertContext();
        var item = new AlertDetailItem { Heading = "Diagnosis" };
        item.Fields.Add(("Wait Type", "LCK_M_X"));
        ctx.Details.Add(item);
        if (withSample)
            ctx.NewestSample = Sample();
        return ctx;
    }

    /// <summary>CPU's shape: no Details, a typed carrier, and a hand-built text that carries the line.</summary>
    private static (AlertContext Context, string DetailText) CpuHandBuilt(bool withSample = true)
    {
        var ctx = new AlertContext { NewestSample = withSample ? Sample() : null };
        var text = withSample
            ? "CPU is at 97% over three readings.\n\n" + Line
            : "CPU is at 97% over three readings.";
        return (ctx, text);
    }

    private static int Count(string haystack, string needle)
    {
        var n = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            n++;
        return n;
    }

    /* ---------------- the line builder ---------------- */

    [Fact]
    public void SampleAgeLine_UsesTheHouseWording()
    {
        Assert.Equal(Line, AlertDetailText.SampleAgeLine(Sample()));
    }

    [Fact]
    public void SampleAgeLine_IsNullWithoutACarrierOrASource()
    {
        Assert.Null(AlertDetailText.SampleAgeLine(null));
        Assert.Null(AlertDetailText.SampleAgeLine(new AlertNewestSample(" ", DateTime.UtcNow, TimeSpan.Zero)));
    }

    /* ---------------- SampleAgeLineFor: skip prose that already has the line ---------------- */

    [Fact]
    public void SampleAgeLineFor_SkipsProseThatAlreadyCarriesTheLine()
    {
        var (ctx, text) = CpuHandBuilt();
        Assert.Null(AlertDetailText.SampleAgeLineFor(ctx, text));
    }

    [Fact]
    public void SampleAgeLineFor_ReturnsTheLineForOtherProseAndForNoProse()
    {
        var ctx = WithDetails();
        Assert.Equal(Line, AlertDetailText.SampleAgeLineFor(ctx, "Some independent prose."));
        Assert.Equal(Line, AlertDetailText.SampleAgeLineFor(ctx, null));
    }

    [Fact]
    public void SampleAgeLineFor_IsNullWhenTheCarrierIsNull()
    {
        Assert.Null(AlertDetailText.SampleAgeLineFor(WithDetails(withSample: false), "prose"));
        Assert.Null(AlertDetailText.SampleAgeLineFor(null, "prose"));
    }

    /* ---------------- Flatten: once, and not for a context without Details ---------------- */

    [Fact]
    public void Flatten_AppendsTheLineOnce_AtTheEnd()
    {
        var flat = AlertDetailText.Flatten(WithDetails())!;
        Assert.Equal(1, Count(flat, LinePrefix));
        Assert.EndsWith(Line, flat, StringComparison.Ordinal);
    }

    [Fact]
    public void Flatten_OmitsTheLine_WhenTheCarrierIsNull()
    {
        Assert.DoesNotContain(LinePrefix, AlertDetailText.Flatten(WithDetails(withSample: false)));
    }

    [Fact]
    public void Flatten_IsNullForEmptyDetails_EvenWithACarrier()
    {
        // CPU's hand-built text carries the line; flattening its Details-less context must not duplicate it.
        Assert.Null(AlertDetailText.Flatten(CpuHandBuilt().Context));
    }

    /* ---------------- ProseForDelivery: equality that ignores the line ---------------- */

    [Fact]
    public void ProseForDelivery_TreatsTextsThatDifferOnlyByTheLineAsTheSame()
    {
        var ctx = WithDetails();
        var flat = AlertDetailText.Flatten(ctx)!;
        var withoutLine = AlertDetailText.StripSampleAge(flat);

        Assert.Null(AlertDetailText.ProseForDelivery(flat, ctx));
        Assert.Null(AlertDetailText.ProseForDelivery(withoutLine, ctx)); // a text built before the age was stamped
    }

    [Fact]
    public void ProseForDelivery_KeepsGenuinelyDifferentProse()
    {
        var ctx = WithDetails();
        Assert.Equal("A real remedy.", AlertDetailText.ProseForDelivery("A real remedy.", ctx));
    }

    /* ---------------- WithSampleAge ---------------- */

    [Fact]
    public void WithSampleAge_AppendsOnce_AndIsIdempotent()
    {
        var ctx = WithDetails();
        var once = AlertDetailText.WithSampleAge("Some text.", ctx)!;
        var twice = AlertDetailText.WithSampleAge(once, ctx)!;

        Assert.Equal(1, Count(once, LinePrefix));
        Assert.Equal(once, twice);
    }

    [Fact]
    public void WithSampleAge_LeavesTextAlone_WithoutACarrier_AndGivesTheLineAloneForNoText()
    {
        Assert.Equal("Some text.", AlertDetailText.WithSampleAge("Some text.", WithDetails(withSample: false)));
        Assert.Null(AlertDetailText.WithSampleAge(null, null));
        Assert.Equal(Line, AlertDetailText.WithSampleAge("  ", WithDetails()));
    }

    /* ---------------- AlertContext round trip ---------------- */

    [Fact]
    public void AlertContext_RoundTripsTheCarrier_ThroughTheContextJson()
    {
        var json = AlertContextSerializer.Serialize(WithDetails());

        Assert.True(AlertContextSerializer.TryDeserialize(json, out var back));
        Assert.NotNull(back.NewestSample);
        Assert.Equal("CPU", back.NewestSample!.Source);
        Assert.Equal(new DateTime(2026, 10, 11, 15, 43, 29, DateTimeKind.Utc), back.NewestSample.InstantUtc);
        Assert.Equal(DateTimeKind.Utc, back.NewestSample.InstantUtc.Kind);
        Assert.Equal(TimeSpan.FromSeconds(320), back.NewestSample.Age);
    }

    [Fact]
    public void AlertContext_WithoutACarrier_RoundTripsToNull_AndALegacyRowReadsAsNone()
    {
        var json = AlertContextSerializer.Serialize(WithDetails(withSample: false));
        Assert.True(AlertContextSerializer.TryDeserialize(json, out var back));
        Assert.Null(back.NewestSample);
        Assert.Null(AlertContextSerializer.TryReadNewestSample(json));
        Assert.Null(AlertContextSerializer.TryReadNewestSample("{\"Details\":[]}"));
        Assert.Null(AlertContextSerializer.TryReadNewestSample("not json"));
        Assert.Null(AlertContextSerializer.TryReadNewestSample(null));
    }

    [Fact]
    public void TryReadNewestSample_ReadsTheFireTimeAge_NotTheAgeAtReadTime()
    {
        var read = AlertContextSerializer.TryReadNewestSample(AlertContextSerializer.Serialize(WithDetails()));
        Assert.Equal(TimeSpan.FromSeconds(320), read!.Age);
        Assert.Equal(Line, AlertDetailText.SampleAgeLine(read));
    }

    [Fact]
    public void TheWireField_CarriesSourceInstantAgeAndLine_AndIsNullWithoutACarrier()
    {
        var wire = AlertNewestSampleWire.FromContextJson(AlertContextSerializer.Serialize(WithDetails()))!;
        Assert.Equal("CPU", wire.Source);
        Assert.Equal("2026-10-11T15:43:29.0000000Z", wire.InstantUtc);
        Assert.Equal(320d, wire.AgeSeconds);
        Assert.Equal(Line, wire.Line);

        Assert.Null(AlertNewestSampleWire.FromContextJson(AlertContextSerializer.Serialize(WithDetails(withSample: false))));
        Assert.Null(AlertNewestSampleWire.FromContextJson(null));
    }

    [Fact]
    public void SampleAgeLineFromJson_ReadsThePersistedCarrier_AndSkipsProseThatHasTheLine()
    {
        var json = AlertContextSerializer.Serialize(WithDetails());
        Assert.Equal(Line, AlertDetailText.SampleAgeLineFromJson(json, null));
        Assert.Equal(Line, AlertDetailText.SampleAgeLineFromJson(json, "text without it"));
        Assert.Null(AlertDetailText.SampleAgeLineFromJson(json, "text\n\n" + Line));
        Assert.Null(AlertDetailText.SampleAgeLineFromJson(null, null));
    }

    /* ---------------- email: HTML and plain text, once each ---------------- */

    [Fact]
    public void Email_BothBodies_PrintTheLineOnce_ForAStructuredContext()
    {
        var ctx = WithDetails();
        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "Blocking Detected", "S1", "5", "1", 15, Branding, ctx, detailText: AlertDetailText.Flatten(ctx));

        Assert.Equal(1, Count(html, LinePrefix));
        Assert.Equal(1, Count(plain, LinePrefix));
        Assert.Contains(Line, html, StringComparison.Ordinal);
        Assert.Contains(Line, plain, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_BothBodies_PrintTheLineOnce_ForCpusHandBuiltText()
    {
        var (ctx, text) = CpuHandBuilt();
        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail("High CPU", "S1", "97%", "90%", 15, Branding, ctx, detailText: text);

        Assert.Equal(1, Count(html, LinePrefix));
        Assert.Equal(1, Count(plain, LinePrefix));
    }

    [Fact]
    public void Email_BothBodies_HaveNoLine_WhenTheCarrierIsNull()
    {
        var ctx = WithDetails(withSample: false);
        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "Blocking Detected", "S1", "5", "1", 15, Branding, ctx, detailText: AlertDetailText.Flatten(ctx));

        Assert.DoesNotContain("sample:", html);
        Assert.DoesNotContain("sample:", plain);
    }

    /* ---------------- the four webhook builders, once each ---------------- */

    public static IEnumerable<object[]> Shapes()
    {
        yield return new object[] { "structured" };
        yield return new object[] { "cpu" };
    }

    private static (AlertContext Context, string? DetailText) Shape(string shape)
    {
        if (shape == "cpu")
        {
            var (c, t) = CpuHandBuilt();
            return (c, t);
        }

        var ctx = WithDetails();
        return (ctx, AlertDetailText.Flatten(ctx));
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void Teams_PrintsTheLineOnce(string shape)
    {
        var (ctx, text) = Shape(shape);
        var payload = WebhookAlertService.BuildTeamsPayload("High CPU", "S1", "97%", "90%", Branding, context: ctx, detailText: text);
        Assert.Equal(1, Count(payload, LinePrefix));
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void Slack_PrintsTheLineOnce(string shape)
    {
        var (ctx, text) = Shape(shape);
        var payload = WebhookAlertService.BuildSlackPayload("High CPU", "S1", "97%", "90%", Branding, context: ctx, detailText: text);
        Assert.Equal(1, Count(payload, LinePrefix));
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void Generic_PrintsTheLineOnce_InTheContextText(string shape)
    {
        var (ctx, text) = Shape(shape);
        var payload = WebhookAlertService.BuildGenericPayload(
            "High CPU", "S1", "97%", "90%", Branding, context: ctx, bodyTemplate: "{\"context\": \"{{context}}\"}", detailText: text);
        Assert.Equal(1, Count(payload, LinePrefix));
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void PagerDuty_PrintsTheLineOnce(string shape)
    {
        var (ctx, text) = Shape(shape);
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "S1", "97%", "90%", Branding, "rk-test", context: ctx, detailText: text);
        Assert.Equal(1, Count(payload, LinePrefix));
    }

    [Fact]
    public void AllFourWebhooks_HaveNoLine_WhenTheCarrierIsNull()
    {
        var ctx = WithDetails(withSample: false);
        var text = AlertDetailText.Flatten(ctx);
        Assert.DoesNotContain("sample:", WebhookAlertService.BuildTeamsPayload("High CPU", "S1", "1", "1", Branding, context: ctx, detailText: text));
        Assert.DoesNotContain("sample:", WebhookAlertService.BuildSlackPayload("High CPU", "S1", "1", "1", Branding, context: ctx, detailText: text));
        Assert.DoesNotContain("sample:", WebhookAlertService.BuildGenericPayload("High CPU", "S1", "1", "1", Branding, context: ctx, bodyTemplate: "{\"c\": \"{{context}}\"}", detailText: text));
        Assert.DoesNotContain("sample:", WebhookAlertService.BuildPagerDutyPayload("High CPU", "S1", "1", "1", Branding, "rk", context: ctx, detailText: text));
    }

    /* ---------------- the carrier survives the copies a delivery makes ---------------- */

    private static AlertContext WithTwoIncidents()
    {
        var ctx = new AlertContext { NewestSample = Sample() };
        AlertIncidentRenderer.Apply(ctx, new[]
        {
            new AlertIncident("stale-fingerprint", new[] { "SalesDb.dbo.Orders" }),
            new AlertIncident("fresh-fingerprint", new[] { "SalesDb.dbo.Shipments" })
        });
        return ctx;
    }

    [Fact]
    public void ThePerEventSplit_KeepsTheCarrierOnEveryMessage()
    {
        var messages = PerEventNotification.Split(WithTwoIncidents(), 10);

        Assert.Equal(2, messages.Count);
        Assert.All(messages, m => Assert.Equal(Sample(), m.Context.NewestSample));
    }

    [Fact]
    public void TheIncidentFilter_KeepsTheCarrier_OnTheFilteredAndTheUnfilteredPaths()
    {
        var ctx = WithTwoIncidents();

        var filtered = IncidentDeliveryFilter.ForDelivery(ctx, null, new[] { "fresh-fingerprint" });
        var unfiltered = IncidentDeliveryFilter.ForDelivery(ctx, null, null);

        Assert.Equal(1, filtered.SuppressedIncidentCount);
        Assert.Equal(Sample(), filtered.Context!.NewestSample);
        Assert.Equal(Sample(), unfiltered.Context!.NewestSample);
    }

    [Fact]
    public void TheWebhookRedaction_KeepsTheCarrier_InTheContextJsonToken()
    {
        var ctx = WithDetails();
        var payload = WebhookAlertService.BuildGenericPayload(
            "High CPU", "S1", "97%", "90%", Branding, context: ctx,
            bodyTemplate: "{\"context\": {{context_json}}}", detailText: AlertDetailText.Flatten(ctx));

        using var doc = System.Text.Json.JsonDocument.Parse(payload);
        var sample = doc.RootElement.GetProperty("context").GetProperty("NewestSample");
        Assert.Equal("CPU", sample.GetProperty("Source").GetString());
        Assert.Equal(320d, sample.GetProperty("AgeSeconds").GetDouble());
    }
}
