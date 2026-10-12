/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#5366: sealed text is a secret in the diagnostics bundle, and the bundle names the password key and the
/// service's key state without carrying any key material. Values here are synthetic.</summary>
public sealed class DiagnosticsBundleSealedTextTests
{
    private const string SealedText = "sealed:v1:abcdef0123456789:QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo=";

    [Fact]
    public void SealedText_IsASecretValue()
    {
        Assert.True(SecretTextGuard.ValueLooksSecret(SealedText));
        Assert.True(SecretTextGuard.ValueLooksSecret("sealed:v9:anything:else"));
    }

    [Fact]
    public void SealedText_InsideALogLine_IsRedacted()
    {
        var a = new BundleAliaser();

        var text = a.Alias("route 3 value " + SealedText + " could not be read");

        Assert.DoesNotContain("QUJDREVGR0hJ", text, StringComparison.Ordinal);
        Assert.DoesNotContain("abcdef0123456789", text, StringComparison.Ordinal);
        Assert.Contains("[redacted]", text, StringComparison.Ordinal);
        Assert.Contains("could not be read", text, StringComparison.Ordinal);
    }

    [Fact]
    public void SealedText_InAWebhookColumnOrAConfigValue_IsNotExported()
    {
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = SealedText;
        config.Webhooks.GenericHeaders = SealedText;
        config.Webhooks.PagerDutyRoutingKey = SealedText;
        var a = new BundleAliaser();

        DiagnosticsBundle.SeedFromConfig(a, config, null);
        var text = a.Alias("failed " + SealedText + " end");

        Assert.DoesNotContain("QUJDREVGR0hJ", text, StringComparison.Ordinal);
        Assert.DoesNotContain("sealed:v1", text, StringComparison.Ordinal);
    }

    [Fact]
    public void PasswordKeyInfo_NamesTheKeyAndTheState_AndNoKeyMaterialOrHost()
    {
        var spki = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        var published = new PublishedPasswordKey("abcdef0123456789abcdef0123456789", spki, "RSA-OAEP-SHA256");
        var state = new PasswordKeyServiceState(
            "service-host-example", "abcdef0123456789abcdef0123456789", "ok", "a note that must not be exported",
            new DateTime(2026, 10, 6, 12, 30, 5, DateTimeKind.Utc));

        var text = DiagnosticsBundle.BuildPasswordKeyInfo(published, state).ToJsonString();

        Assert.Contains(PasswordSeal.DisplayKeyId(published.KeyId), text, StringComparison.Ordinal);
        Assert.Contains("\"service_state\":\"ok\"", text, StringComparison.Ordinal);
        Assert.Contains("2026-10-06T12:30:05Z", text, StringComparison.Ordinal);
        Assert.DoesNotContain("service-host-example", text, StringComparison.Ordinal);
        Assert.DoesNotContain("a note that must not", text, StringComparison.Ordinal);
        Assert.DoesNotContain(Convert.ToBase64String(spki), text, StringComparison.Ordinal);
        Assert.DoesNotContain("RSA", text, StringComparison.Ordinal);
    }

    [Fact]
    public void PasswordKeyInfo_WithNoKeyAndNoService_SaysSo()
    {
        var text = DiagnosticsBundle.BuildPasswordKeyInfo(null, null).ToJsonString();

        Assert.Contains("\"status\":\"not_published\"", text, StringComparison.Ordinal);
    }
}
