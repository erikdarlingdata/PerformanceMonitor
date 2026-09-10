using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1506: the generic webhook channel. The channel exists so an alert can drive automation we ship no
/// adapter for (PagerDuty, Opsgenie, n8n, a GitHub <c>repository_dispatch</c> that re-runs a workflow)
/// WITHOUT executing a script or an exe — so the two things that must hold are that the operator's JSON
/// body template cannot be broken out of by alert data, and that a malformed config degrades to a clear
/// error instead of an exception in the alert loop.
/// </summary>
public class GenericWebhookTests
{
    private static readonly AlertBranding Branding = new("Performance Monitor Lite", null);

    private static string Build(string metric, string server, string value = "0", string threshold = "0", string? template = null)
        => WebhookAlertService.BuildGenericPayload(metric, server, value, threshold, Branding, bodyTemplate: template);

    /* ---------------- JSON escaping (the injection guard) ---------------- */

    [Fact]
    public void ServerNameWithQuote_DoesNotBreakOutOfTheJsonStringLiteral()
    {
        /* A server name carrying a double quote is the classic break-out: naive substitution into
           "server": "{{server}}" would terminate the literal and corrupt — or inject into — the payload. */
        var payload = Build("High CPU", "SRV\"; DROP TABLE x; --");

        var root = JsonDocument.Parse(payload).RootElement; // must still be valid JSON
        Assert.Equal("SRV\"; DROP TABLE x; --", root.GetProperty("server").GetString());
    }

    [Fact]
    public void ServerNameWithBackslash_IsEscapedAndRoundTrips()
    {
        /* A named instance (HOST\INSTANCE) is the everyday case: an unescaped backslash makes "\I" an
           invalid JSON escape and the whole body unparseable. */
        var payload = Build("High CPU", @"HOST\SQLEXPRESS");

        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal(@"HOST\SQLEXPRESS", root.GetProperty("server").GetString());
    }

    [Fact]
    public void LoneSurrogate_DoesNotThrow_AndProducesValidJson()
    {
        /* SQL Server nvarchar/sysname can hold an unpaired surrogate (NCHAR(0xD800)). JsonEncodedText.Encode
           THROWS on that — which would let a low-privileged user drop this channel for the incident they cause
           while email/Teams/Slack still deliver. The escape must relax it (U+FFFD), never throw. */
        var payload = Build("High CPU", "SRV\uD800X"); // lone high surrogate

        var root = JsonDocument.Parse(payload).RootElement; // valid JSON, no throw
        Assert.StartsWith("SRV", root.GetProperty("server").GetString());
    }

    [Fact]
    public void ValueContainingAnotherPlaceholder_IsNotReExpanded()
    {
        /* Single-pass substitution: a server name that literally contains "{{timestamp}}" must render verbatim,
           not pull the timestamp field into itself (chained .Replace would). One field can't bleed into another. */
        var payload = Build("High CPU", "SRV {{timestamp}} {{value}}", value: "42");

        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("SRV {{timestamp}} {{value}}", root.GetProperty("server").GetString());
        Assert.Equal("42", root.GetProperty("value").GetString());
    }

    [Fact]
    public void InjectedJsonStructure_StaysDataNotStructure()
    {
        /* The value must land as a STRING, not as new JSON structure: an attacker-ish value that closes the
           literal and opens a new key must round-trip verbatim inside "value", adding no properties. */
        var payload = Build("High CPU", "SRV", value: "99\", \"injected\": \"pwned");

        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("99\", \"injected\": \"pwned", root.GetProperty("value").GetString());
        Assert.False(root.TryGetProperty("injected", out _));
    }

    [Fact]
    public void EveryPlaceholderIsSubstituted_AndNoneRemain()
    {
        var payload = Build("Server Unreachable", "SRV", value: "Login timeout", threshold: "Online");

        Assert.DoesNotContain("{{", payload, System.StringComparison.Ordinal);

        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("Server Unreachable", root.GetProperty("metric").GetString());
        Assert.Equal("SRV", root.GetProperty("server").GetString());
        Assert.Equal("Login timeout", root.GetProperty("value").GetString());
        Assert.Equal("Online", root.GetProperty("threshold").GetString());
        /* Severity comes from the shared AlertSeverity map, which already knows this metric. */
        Assert.Equal("CRITICAL", root.GetProperty("severity").GetString());
        Assert.False(string.IsNullOrWhiteSpace(root.GetProperty("timestamp").GetString()));
    }

    [Fact]
    public void OperatorTemplate_IsHonored_AndEscapedTheSameWay()
    {
        /* The motivating use case: a GitHub repository_dispatch body. Placeholders sit inside nested string
           literals, and a hostile server name must not escape them. */
        const string template =
            """{"event_type": "sql-monitor-alert", "client_payload": {"metric": "{{metric}}", "server": "{{server}}"}}""";

        var payload = Build("Server Unreachable", "SRV\"evil", template: template);

        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("sql-monitor-alert", root.GetProperty("event_type").GetString());
        Assert.Equal("SRV\"evil", root.GetProperty("client_payload").GetProperty("server").GetString());
    }

    [Fact]
    public void EmptyTemplate_FallsBackToTheDefault()
    {
        var payload = Build("High CPU", "SRV", template: "   ");

        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("SRV", root.GetProperty("server").GetString());
    }

    [Fact]
    public void DefaultTemplate_IsItselfValidJson()
    {
        /* Guards the shipped default: a typo there would break every un-customized deployment. */
        var payload = Build("High CPU", "SRV", template: WebhookAlertService.DefaultGenericBodyTemplate);
        JsonDocument.Parse(payload);
    }

    /* ---------------- headers parsing (never throw into the alert loop) ---------------- */

    [Fact]
    public void Headers_ParseIntoRequestHeaders()
    {
        var ok = WebhookAlertService.TryParseHeaders(
            """{"Authorization": "Bearer ghp_token", "Accept": "application/vnd.github+json"}""",
            out var headers, out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.Equal("Bearer ghp_token", headers["Authorization"]);
        Assert.Equal("application/vnd.github+json", headers["Accept"]);
    }

    [Fact]
    public void Headers_EmptyIsValid_MeaningNoCustomHeaders()
    {
        Assert.True(WebhookAlertService.TryParseHeaders("", out var headers, out var error));
        Assert.Empty(headers);
        Assert.Null(error);
    }

    [Fact]
    public void Headers_MalformedJson_FailsGracefullyWithAMessage()
    {
        /* A truncated paste is the realistic operator error. It must come back as an error, NOT an exception. */
        var ok = WebhookAlertService.TryParseHeaders("""{"Authorization": "Bearer x" """, out var headers, out var error);

        Assert.False(ok);
        Assert.Empty(headers);
        Assert.NotNull(error);
        Assert.Contains("not valid JSON", error, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Headers_NonObjectRoot_IsRejected()
    {
        Assert.False(WebhookAlertService.TryParseHeaders("""["Authorization", "Bearer x"]""", out _, out var error));
        Assert.NotNull(error);
        Assert.Contains("JSON object", error, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Headers_NonStringValue_IsRejectedByName()
    {
        Assert.False(WebhookAlertService.TryParseHeaders("""{"Retry-After": 30}""", out _, out var error));
        Assert.NotNull(error);
        Assert.Contains("Retry-After", error, System.StringComparison.Ordinal);
    }

    [Fact]
    public void Headers_ValueWithCrlf_IsRejected_NoHeaderSmuggling()
    {
        /* .NET's TryAddWithoutValidation lets a CR/LF in a header VALUE onto the wire, splitting it into a
           second header. Reject it at parse time so a token pasted with a stray newline can't smuggle one. */
        var ok = WebhookAlertService.TryParseHeaders(
            "{\"Authorization\": \"Bearer good\\r\\nX-Smuggled: injected\"}", out _, out var error);

        Assert.False(ok);
        Assert.NotNull(error);
        Assert.Contains("control character", error, System.StringComparison.OrdinalIgnoreCase);
    }

    /* ---------------- the settings-window validator ---------------- */

    [Fact]
    public void ValidateGenericConfig_AcceptsAGoodConfig()
    {
        Assert.Null(WebhookAlertService.ValidateGenericConfig(
            """{"Authorization": "Bearer x"}""",
            """{"metric": "{{metric}}"}"""));
    }

    [Fact]
    public void ValidateGenericConfig_RejectsMalformedHeaders()
    {
        Assert.NotNull(WebhookAlertService.ValidateGenericConfig("{not json", null));
    }

    [Fact]
    public void ValidateGenericConfig_RejectsATemplateThatCannotProduceValidJson()
    {
        /* An unquoted placeholder: the substituted text lands outside a string literal, so the body is not
           JSON. Caught at Save, rather than silently dropping every alert afterwards. */
        Assert.NotNull(WebhookAlertService.ValidateGenericConfig(null, """{"metric": {{metric}}}"""));
    }

    /* ---------------- ApplyHeaders (request/content header routing) ---------------- */

    private static (HttpRequestMessage Request, HttpContent Content) ApplyHeaders(Dictionary<string, string> headers)
    {
        var content = new StringContent("{}", System.Text.Encoding.UTF8, "application/json");
        var request = new HttpRequestMessage(HttpMethod.Post, "https://example.test/hook") { Content = content };
        WebhookAlertService.ApplyHeaders(request, content, headers);
        return (request, content);
    }

    [Fact]
    public void ApplyHeaders_DefaultsUserAgent_WhenOperatorSetsNone()
    {
        /* GitHub's REST API 403s a request with no User-Agent, and repository_dispatch is the motivating
           case — so one is defaulted rather than handing the operator a 403 to diagnose. */
        var (request, _) = ApplyHeaders(new Dictionary<string, string> { ["Accept"] = "application/vnd.github+json" });

        Assert.True(request.Headers.Contains("User-Agent"));
        Assert.Equal("PerformanceMonitor", string.Join("", request.Headers.GetValues("User-Agent")));
    }

    [Fact]
    public void ApplyHeaders_DoesNotOverrideAnOperatorUserAgent()
    {
        var (request, _) = ApplyHeaders(new Dictionary<string, string> { ["User-Agent"] = "my-agent/2.0" });

        Assert.Equal("my-agent/2.0", string.Join("", request.Headers.GetValues("User-Agent")));
    }

    [Fact]
    public void ApplyHeaders_RoutesContentTypeOntoTheContent_NotDroppedAsARequestHeader()
    {
        /* Content-Type is a CONTENT header — TryAddWithoutValidation on request.Headers rejects it, so a naive
           add would silently drop the override. ApplyHeaders must move it onto the content, replacing the
           default application/json StringContent set. (The framework won't even let you QUERY Content-Type on
           request.Headers — it throws "misused header name" — which is the same rule that makes routing it onto
           the content necessary; so the content-side assertion is the meaningful proof.) */
        var (_, content) = ApplyHeaders(new Dictionary<string, string> { ["Content-Type"] = "application/vnd.github+json" });

        Assert.Equal("application/vnd.github+json", content.Headers.ContentType?.ToString());
    }

    [Fact]
    public void ApplyHeaders_AddsAnOrdinaryOperatorHeader()
    {
        var (request, _) = ApplyHeaders(new Dictionary<string, string> { ["Authorization"] = "Bearer ghp_token" });

        Assert.Equal("Bearer ghp_token", string.Join("", request.Headers.GetValues("Authorization")));
    }

    /* ---------------- #2302: the automation tokens ---------------- */

    private static AlertContext TwoIncidentContext()
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem
        {
            Heading = "Deadlock 1",
            /* The motivating delimiter collision: {{context}}'s flattening joins on " | " and ": ",
               and this value contains BOTH — plus a quote — so only structure can carry it. */
            Fields = { ("Victim SQL", "SELECT a | b FROM t WHERE x = 'y: \"z\"'") }
        });
        context.Incidents = new List<AlertIncident>
        {
            new("aa11", new List<string> { "db1.dbo.t1" }, OccurrenceCount: 3, TotalOccurrences: 17,
                IncidentStartedUtc: new System.DateTime(2026, 8, 17, 10, 0, 0, System.DateTimeKind.Utc)),
            new("bb22", new List<string> { "db2.dbo.t2" }),
        };
        return context;
    }

    [Fact]
    public void ContextJsonToken_SubstitutesRawStructure_InThePersistedContextJsonShape()
    {
        const string template = """{"metric": "{{metric}}", "context": {{context_json}}}""";
        var payload = WebhookAlertService.BuildGenericPayload(
            "Deadlocks Detected", "SRV", "2", "0", Branding, context: TwoIncidentContext(), bodyTemplate: template);

        var root = JsonDocument.Parse(payload).RootElement;
        var contextElement = root.GetProperty("context");

        /* Structure, not a string — the whole point of the token. */
        Assert.Equal(JsonValueKind.Object, contextElement.ValueKind);
        var incidents = contextElement.GetProperty("Incidents");
        Assert.Equal(2, incidents.GetArrayLength());
        Assert.Equal("aa11", incidents[0].GetProperty("DedupKey").GetString());
        Assert.Equal(17, incidents[0].GetProperty("TotalOccurrences").GetInt64());

        /* The hostile Victim SQL arrives as a field VALUE, byte-exact — no delimiter parsing needed. */
        var field = contextElement.GetProperty("Details")[0].GetProperty("Fields")[0];
        Assert.Equal("SELECT a | b FROM t WHERE x = 'y: \"z\"'", field.GetProperty("Value").GetString());

        /* One shape for every consumer: the embedded JSON is EXACTLY what the alert-history
           ContextJson persists, so it must round-trip through the same serializer. */
        Assert.True(AlertContextSerializer.TryDeserialize(contextElement.GetRawText(), out var roundTripped));
        Assert.Equal(2, roundTripped.Incidents!.Count);
        Assert.Equal("bb22", roundTripped.Incidents[1].DedupKey);
    }

    [Fact]
    public void ContextJsonToken_IsAnEmptyObject_WhenTheAlertCarriesNoContext()
    {
        const string template = """{"context": {{context_json}}, "incidents": {{incidents_json}}}""";
        var payload = Build("High CPU", "SRV", template: template);

        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal(JsonValueKind.Object, root.GetProperty("context").ValueKind);
        Assert.Equal(0, root.GetProperty("incidents").GetArrayLength());
    }

    [Fact]
    public void IncidentsJsonToken_IsJustTheArray()
    {
        const string template = """{"incidents": {{incidents_json}}}""";
        var payload = WebhookAlertService.BuildGenericPayload(
            "Deadlocks Detected", "SRV", "2", "0", Branding, context: TwoIncidentContext(), bodyTemplate: template);

        var incidents = JsonDocument.Parse(payload).RootElement.GetProperty("incidents");
        Assert.Equal(2, incidents.GetArrayLength());
        Assert.Equal("db1.dbo.t1", incidents[0].GetProperty("InvolvedObjects")[0].GetString());
    }

    [Fact]
    public void DedupKeyToken_MatchesThePagerDutyDerivation_IncludingTheFallback()
    {
        const string template = """{"dedup": "{{dedup_key}}"}""";

        /* With an incident: the first incident's fingerprint, same anchor PagerDuty correlates on. */
        var withIncident = WebhookAlertService.BuildGenericPayload(
            "Deadlocks Detected", "SRV", "2", "0", Branding, context: TwoIncidentContext(),
            bodyTemplate: template, serverId: "37");
        Assert.Equal("aa11", JsonDocument.Parse(withIncident).RootElement.GetProperty("dedup").GetString());

        /* Without one: the stable metric+server fallback — the key level/threshold alerts (High CPU,
           Collection Stopped) never exposed before, keyed on serverId when the caller has one. */
        var fallback = WebhookAlertService.BuildGenericPayload(
            "High CPU", "SRV", "97", "90", Branding, bodyTemplate: template, serverId: "37");
        Assert.Equal("37:High CPU", JsonDocument.Parse(fallback).RootElement.GetProperty("dedup").GetString());

        /* And the serverName stands in when no id exists — the PagerDuty call site's own precedence. */
        var byName = Build("High CPU", "SRV", template: template);
        Assert.Equal("SRV:High CPU", JsonDocument.Parse(byName).RootElement.GetProperty("dedup").GetString());
    }

    /* ---------------- Datadog-parity tags (#2710) ---------------- */

    [Fact]
    public void ResourceNameAndDatabaseTokens_UseTheFirstIncident_SameAnchorAsDedupKey()
    {
        const string template = """{"resource": "{{resource_name}}", "db": "{{database}}"}""";

        /* TwoIncidentContext's first incident (aa11/db1.dbo.t1) carries no Database — resource_name
           still resolves, database stays empty (not the literal word "null"). */
        var payload = WebhookAlertService.BuildGenericPayload(
            "Deadlocks Detected", "SRV", "2", "0", Branding, context: TwoIncidentContext(), bodyTemplate: template);
        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("db1.dbo.t1", root.GetProperty("resource").GetString());
        Assert.Equal("", root.GetProperty("db").GetString());

        /* An incident that DOES carry a Database populates both. */
        var withDb = new AlertContext();
        AlertIncidentRenderer.Apply(withDb, new[] { new AlertIncident("k1", new[] { "SalesDB.dbo.Orders" }, Database: "SalesDB") });
        var dbPayload = WebhookAlertService.BuildGenericPayload(
            "Deadlocks Detected", "SRV", "2", "0", Branding, context: withDb, bodyTemplate: template);
        var dbRoot = JsonDocument.Parse(dbPayload).RootElement;
        Assert.Equal("SalesDB.dbo.Orders", dbRoot.GetProperty("resource").GetString());
        Assert.Equal("SalesDB", dbRoot.GetProperty("db").GetString());
    }

    [Fact]
    public void ResourceNameAndDatabaseTokens_AreEmptyStrings_WhenTheAlertCarriesNoIncident()
    {
        const string template = """{"resource": "{{resource_name}}", "db": "{{database}}"}""";
        var payload = Build("High CPU", "SRV", template: template);

        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("", root.GetProperty("resource").GetString());
        Assert.Equal("", root.GetProperty("db").GetString());
    }

    [Fact]
    public void ContextJsonToken_RedactsRemediationTsql_LikeEveryOtherChannel()
    {
        /* The review catch: every channel replaces copy-paste T-SQL with the "see email / in-app dialog"
           hint before anything leaves the process, and the raw token must not be the one exception. */
        var context = TwoIncidentContext();
        context.Details.Add(new AlertDetailItem
        {
            Heading = "Remediation T-SQL",
            Body = "ALTER DATABASE [x] SET READ_COMMITTED_SNAPSHOT ON;",
            IsCodeBlock = true,
            Remediation = new RemediationAction("RCSI_OFF", "DB_CONFIG", new List<ForcePlanTarget>())
        });

        const string template = """{"context": {{context_json}}}""";
        var payload = WebhookAlertService.BuildGenericPayload(
            "Deadlocks Detected", "SRV", "2", "0", Branding, context: context, bodyTemplate: template);

        /* The T-SQL never reaches the wire; the hint and the code-block FLAG do, so a consumer still
           learns a remediation exists and where to read it. */
        Assert.DoesNotContain("ALTER DATABASE", payload, System.StringComparison.Ordinal);
        Assert.DoesNotContain("RCSI_OFF", payload, System.StringComparison.Ordinal);
        var details = JsonDocument.Parse(payload).RootElement.GetProperty("context").GetProperty("Details");
        var codeBlock = details[1];
        Assert.True(codeBlock.GetProperty("IsCodeBlock").GetBoolean());
        Assert.Contains("in-app Alert Details", codeBlock.GetProperty("Body").GetString(), System.StringComparison.Ordinal);
        Assert.Equal(JsonValueKind.Null, codeBlock.GetProperty("Remediation").ValueKind);

        /* Redaction copies — the SAME context instance flows on to email/Teams afterwards, and those
           channels legitimately carry the T-SQL (email) or their own hint. Mutation here would be a
           cross-channel bug this pin exists to forbid. */
        Assert.Equal("ALTER DATABASE [x] SET READ_COMMITTED_SNAPSHOT ON;", context.Details[1].Body);
        Assert.NotNull(context.Details[1].Remediation);
    }

    [Fact]
    public void ValidateGenericConfig_RejectsAQuotedRawToken_AtSaveTime()
    {
        /* The trap this forbids (#2310 review catch): with a null context the raw tokens render to the
           quote-free {} / [], so `"context": "{{context_json}}"` — the exact mistake the doc comment
           warns about — used to validate clean and test-send clean, then break on the first deadlock
           alert with real structure. The stand-in context is quote-bearing by construction, so the
           mistake now fails where the operator can see it. */
        Assert.NotNull(WebhookAlertService.ValidateGenericConfig(null, """{"context": "{{context_json}}"}"""));
        Assert.NotNull(WebhookAlertService.ValidateGenericConfig(null, """{"incidents": "{{incidents_json}}"}"""));

        /* And the CORRECT unquoted usage still validates. */
        Assert.Null(WebhookAlertService.ValidateGenericConfig(null, """{"context": {{context_json}}, "incidents": {{incidents_json}}, "dedup": "{{dedup_key}}"}"""));
    }

    [Fact]
    public void DefaultTemplate_DoesNotCarryTheAutomationTokens()
    {
        /* #2302's compatibility promise: the shipped default stays byte-identical, so no existing
           consumer's payload changes shape. The automation tokens are opt-in via a custom template. */
        Assert.DoesNotContain("context_json", WebhookAlertService.DefaultGenericBodyTemplate, System.StringComparison.Ordinal);
        Assert.DoesNotContain("incidents_json", WebhookAlertService.DefaultGenericBodyTemplate, System.StringComparison.Ordinal);
        Assert.DoesNotContain("dedup_key", WebhookAlertService.DefaultGenericBodyTemplate, System.StringComparison.Ordinal);
    }
}
