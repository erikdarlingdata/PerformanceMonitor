/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Admin page's server edit (#5240) run under Node (<c>admin-server-edit-harness.mjs</c>). The facts ending in
/// <c>_Pure</c> call one export of the shipped <c>admin.js</c> at a time (the body, password and probe rules, the answer
/// table, the form values, the validation sentences, the conflict lines); the flow facts that drive the page through its
/// form are added beside them with the same names minus the suffix. The frame facts prove the harness itself: the
/// read-only Servers tab drawn through the real grid, and the fake DOM's focus model. Skipped when Node is not installed.
/// </summary>
public sealed class AdminServerEditBehaviourTests
{
    private const string Token = "2026-01-02T03:04:05.1234567Z";

    private const string PasswordNeeded =
        "Changing how this server is reached needs its password again: it is stored encrypted and this surface cannot read it back.";

    internal static JsonElement Run(string scenario, string? stdin = null)
    {
        var psi = new ProcessStartInfo("node")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = stdin is not null,
            StandardOutputEncoding = Encoding.UTF8,
            UseShellExecute = false,
        };
        if (stdin is not null)
        {
            psi.StandardInputEncoding = new UTF8Encoding(false);
        }

        // The grid's time cells follow the machine's zone; the harness pins UTC itself and so does the process it runs in.
        psi.Environment["TZ"] = "UTC";
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "admin-server-edit-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            if (stdin is not null)
            {
                proc.StandardInput.Write(stdin);
                proc.StandardInput.Close();
            }

            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the admin server edit harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the admin server edit harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    // One call of an admin.js export; the result is JSON null when the export answers null.
    private static JsonElement Pure(string fn, params object?[] args) =>
        Run("pure", JsonSerializer.Serialize(new { fn, args })).GetProperty("result");

    // A table of calls answered by one Node process.
    private static JsonElement[] PureAll(params (string Fn, object?[] Args)[] calls) =>
        Run("pure", JsonSerializer.Serialize(new { calls = calls.Select(c => new { fn = c.Fn, args = c.Args }) }))
            .GetProperty("results").EnumerateArray().ToArray();

    // The form's values for one kind of server, shaped as editFormValues returns them (the engine rides along).
    private static JsonObject Base(string kind)
    {
        var form = JsonNode.Parse("""{"engine":"sqlserver","host":"sql-a","display_name":"Orders","port":"","auth":"Windows","username":"","encrypt_mode":"Mandatory","trust_server_certificate":false,"database":"","read_only_intent":false,"multi_subnet_failover":false,"monthly_cost_usd":"100"}""")!.AsObject();
        switch (kind)
        {
            case "windows":
                break;
            case "sql":
                form["auth"] = "SQL";
                form["username"] = "sa";
                break;
            case "sp":
                form["auth"] = "ServicePrincipal";
                form["username"] = "11111111-1111-1111-1111-111111111111";
                break;
            case "managed":
                form["auth"] = "ManagedIdentity";
                break;
            case "postgres":
                form["engine"] = "postgres";
                form["host"] = "pg-a";
                form["port"] = "5433";
                form["auth"] = "SQL";
                form["username"] = "pgmon";
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }

        return form;
    }

    // What the form holds after the user's changes: the base with the named fields overwritten.
    private static JsonObject Edited(string kind, string overrides)
    {
        var form = Base(kind);
        foreach (var pair in JsonNode.Parse(overrides)!.AsObject().ToList())
        {
            form[pair.Key] = pair.Value?.DeepClone();
        }

        return form;
    }

    private static string Expand(string json) => json.Replace("TOKEN", Token, StringComparison.Ordinal);

    // ------------------------------------------------------------------ T2: the body

    [Theory]
    // A name-only edit sends exactly the name and the token.
    [InlineData("windows", """{"display_name":"Payments"}""", "", """{"display_name":"Payments","expected_modified_at":"TOKEN"}""")]
    // Nothing differs and no password is typed: no body, so no request.
    [InlineData("windows", """{}""", "", "null")]
    [InlineData("postgres", """{}""", "", "null")]
    // Text is trimmed before it is compared, so spaces alone change nothing.
    [InlineData("windows", """{"host":"  sql-a ","display_name":"Orders  "}""", "", "null")]
    // A typed cost goes as a number; the same number spelled differently is no change; one that is not a number is not sent.
    [InlineData("windows", """{"monthly_cost_usd":"250.50"}""", "", """{"monthly_cost_usd":250.5,"expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{"monthly_cost_usd":"100.00"}""", "", "null")]
    [InlineData("windows", """{"display_name":"Payments","monthly_cost_usd":"12abc"}""", "", """{"display_name":"Payments","expected_modified_at":"TOKEN"}""")]
    // SQL Server never sends port, even when the box holds a different one.
    [InlineData("windows", """{"port":"1433"}""", "", "null")]
    [InlineData("windows", """{"display_name":"Payments","port":"1433"}""", "", """{"display_name":"Payments","expected_modified_at":"TOKEN"}""")]
    // Encryption is compared ignoring case; a real change is sent.
    [InlineData("windows", """{"encrypt_mode":"mandatory"}""", "", "null")]
    [InlineData("windows", """{"encrypt_mode":"Strict"}""", "", """{"encrypt_mode":"Strict","expected_modified_at":"TOKEN"}""")]
    // Booleans are sent only when they differ.
    [InlineData("windows", """{"trust_server_certificate":true,"multi_subnet_failover":true,"read_only_intent":true}""", "", """{"trust_server_certificate":true,"read_only_intent":true,"multi_subnet_failover":true,"expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{"database":"Sales"}""", "", """{"database":"Sales","expected_modified_at":"TOKEN"}""")]
    // A SQL host change goes with the password; a password alone rotates the secret.
    [InlineData("sql", """{"host":"sql-b"}""", "typed", """{"host":"sql-b","password":"typed","expected_modified_at":"TOKEN"}""")]
    [InlineData("sql", """{}""", "typed", """{"password":"typed","expected_modified_at":"TOKEN"}""")]
    [InlineData("sp", """{}""", "secret", """{"password":"secret","expected_modified_at":"TOKEN"}""")]
    // Windows and managed identity store no secret, so a typed password is never sent, alone or with a change.
    [InlineData("windows", """{"host":"sql-b"}""", "typed", """{"host":"sql-b","expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{}""", "typed", "null")]
    [InlineData("managed", """{}""", "typed", "null")]
    // A switch of auth: into SQL or ServicePrincipal it carries the username and the password.
    [InlineData("windows", """{"auth":"SQL","username":"sa"}""", "typed", """{"auth":"SQL","username":"sa","password":"typed","expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{"auth":"ServicePrincipal","username":"app-id"}""", "secret", """{"auth":"ServicePrincipal","username":"app-id","password":"secret","expected_modified_at":"TOKEN"}""")]
    // The service drops the old username on a switch, so the username is sent even when its text did not change.
    [InlineData("sql", """{"auth":"ManagedIdentity"}""", "", """{"auth":"ManagedIdentity","username":"sa","expected_modified_at":"TOKEN"}""")]
    [InlineData("sql", """{"auth":"ManagedIdentity","username":""}""", "", """{"auth":"ManagedIdentity","username":null,"expected_modified_at":"TOKEN"}""")]
    // A switch to Windows sends no username (the service drops it) and no password, even if one was typed.
    [InlineData("sql", """{"auth":"Windows"}""", "typed", """{"auth":"Windows","expected_modified_at":"TOKEN"}""")]
    // PostgreSQL: no auth whatever the form says; a blank port is sent as 0 (the default); an invalid port is not sent.
    [InlineData("postgres", """{"display_name":"Payments"}""", "", """{"display_name":"Payments","expected_modified_at":"TOKEN"}""")]
    [InlineData("postgres", """{"auth":"Windows","display_name":"Payments"}""", "", """{"display_name":"Payments","expected_modified_at":"TOKEN"}""")]
    [InlineData("postgres", """{"port":""}""", "", """{"port":0,"expected_modified_at":"TOKEN"}""")]
    [InlineData("postgres", """{"port":"5434"}""", "typed", """{"port":5434,"password":"typed","expected_modified_at":"TOKEN"}""")]
    [InlineData("postgres", """{"port":"abc","display_name":"Payments"}""", "", """{"display_name":"Payments","expected_modified_at":"TOKEN"}""")]
    [InlineData("postgres", """{"host":"pg-b"}""", "typed", """{"host":"pg-b","password":"typed","expected_modified_at":"TOKEN"}""")]
    public void TheEditBody_CarriesOnlyChangedEditableFields_AndTheToken_Pure(string kind, string changes, string password, string expected)
    {
        var body = Pure("buildEditBody", Base(kind), Edited(kind, changes), Token, password);

        Assert.Equal(Expand(expected), body.GetRawText());
    }

    [Fact]
    public void TheEditBody_NeverCarriesAKeyOutsideTheEditableFields_AndTheTokenIsLast_Pure()
    {
        var values = Edited("windows", """{"display_name":"Payments","server_id":99,"engine":"postgres","is_enabled":false,"excluded_databases":"x","capture_plans":true,"alert_delivery_mode_override":"x","plan_force_bot_enabled":true,"remediation_mode":"x","modified_at":"y"}""");

        var body = Pure("buildEditBody", Base("windows"), values, Token, "");

        Assert.Equal(new[] { "display_name", "expected_modified_at" }, body.EnumerateObject().Select(p => p.Name).ToArray());
    }

    [Fact]
    public void TheEditBody_SendsABlankedDatabaseAsNull_AndEveryChangedFieldInFormOrder_Pure()
    {
        var original = Base("sql");
        original["database"] = "Sales";
        var values = Edited("sql", """{"database":" ","host":"sql-b","display_name":"Payments","monthly_cost_usd":"5","username":"app","trust_server_certificate":true}""");

        var body = Pure("buildEditBody", original, values, Token, "typed");

        Assert.Equal(
            Expand("""{"host":"sql-b","display_name":"Payments","username":"app","trust_server_certificate":true,"database":null,"monthly_cost_usd":5,"password":"typed","expected_modified_at":"TOKEN"}"""),
            body.GetRawText());
    }

    // ------------------------------------------------------------------ T3: the password

    [Theory]
    // Required: a SQL or ServicePrincipal server whose connection changes, however it changes, and a switch into either.
    [InlineData("sql", """{"host":"sql-b"}""", true)]
    [InlineData("sql", """{"database":"Sales"}""", true)]
    [InlineData("sql", """{"username":"sa2"}""", true)]
    [InlineData("sql", """{"encrypt_mode":"Strict"}""", true)]
    [InlineData("sql", """{"trust_server_certificate":true}""", true)]
    [InlineData("sql", """{"read_only_intent":true}""", true)]
    [InlineData("sql", """{"multi_subnet_failover":true}""", true)]
    [InlineData("windows", """{"auth":"SQL","username":"sa"}""", true)]
    [InlineData("windows", """{"auth":"ServicePrincipal","username":"app-id"}""", true)]
    [InlineData("managed", """{"auth":"SQL","username":"sa"}""", true)]
    [InlineData("sp", """{"username":"22222222-2222-2222-2222-222222222222"}""", true)]
    [InlineData("postgres", """{"host":"pg-b"}""", true)]
    [InlineData("postgres", """{"port":"5434"}""", true)]
    [InlineData("postgres", """{"port":""}""", true)]
    // Not required: a name or cost alone, a change of case or spacing, Windows and managed identity, a switch out of a secret auth.
    [InlineData("sql", """{"display_name":"Payments"}""", false)]
    [InlineData("sql", """{"monthly_cost_usd":"250"}""", false)]
    [InlineData("sql", """{"encrypt_mode":"mandatory"}""", false)]
    [InlineData("sql", """{"host":" sql-a "}""", false)]
    [InlineData("sp", """{"display_name":"Payments"}""", false)]
    [InlineData("postgres", """{"display_name":"Payments"}""", false)]
    [InlineData("postgres", """{"port":"5433"}""", false)]
    [InlineData("windows", """{"host":"sql-b","database":"Sales","trust_server_certificate":true}""", false)]
    [InlineData("managed", """{"host":"sql-b","database":"Sales"}""", false)]
    [InlineData("sql", """{"auth":"Windows"}""", false)]
    [InlineData("sql", """{"auth":"ManagedIdentity"}""", false)]
    public void ThePassword_IsRequiredExactlyWhenTheRulesSay_Pure(string kind, string changes, bool expected)
    {
        var required = Pure("passwordRequired", Base(kind), Edited(kind, changes));

        Assert.Equal(expected, required.GetBoolean());
    }

    [Theory]
    [InlineData("windows", false)]
    [InlineData("managed", false)]
    [InlineData("sql", true)]
    [InlineData("sp", true)]
    [InlineData("postgres", true)]
    public void ThePassword_IsSentOnlyWhenTheAuthStoresASecret_Pure(string kind, bool sent)
    {
        var body = Pure("buildEditBody", Base(kind), Edited(kind, """{"host":"somewhere-else"}"""), Token, "typed");

        Assert.Equal(sent, body.TryGetProperty("password", out _));
    }

    [Theory]
    // Required and blank: the service's own sentence, "Switching to ..." when the auth changes and the "again" sentence otherwise.
    [InlineData("sql", """{"host":"sql-b"}""", "", PasswordNeeded)]
    [InlineData("sp", """{"database":"Sales"}""", "", PasswordNeeded)]
    [InlineData("postgres", """{"host":"pg-b"}""", "", PasswordNeeded)]
    [InlineData("windows", """{"auth":"SQL","username":"sa"}""", "", "Switching to SQL authentication needs the password.")]
    [InlineData("managed", """{"auth":"SQL","username":"sa"}""", "", "Switching to SQL authentication needs the password.")]
    [InlineData("windows", """{"auth":"ServicePrincipal","username":"app-id"}""", "", "Switching to ServicePrincipal authentication needs the client secret as password.")]
    // Typed, or not required: nothing stops the save.
    [InlineData("sql", """{"host":"sql-b"}""", "typed", null)]
    [InlineData("windows", """{"auth":"SQL","username":"sa"}""", "typed", null)]
    [InlineData("windows", """{"host":"sql-b"}""", "", null)]
    [InlineData("sql", """{"display_name":"Payments"}""", "", null)]
    public void AMissingRequiredPassword_IsRefusedWithTheServicesSentence_Pure(string kind, string changes, string password, string? expected)
    {
        var sentence = Pure("validateEdit", Base(kind), Edited(kind, changes), password);

        Assert.Equal(expected, sentence.ValueKind == JsonValueKind.Null ? null : sentence.GetString());
    }

    [Theory]
    // The order the form's fields run in: the first failing rule wins.
    [InlineData("windows", """{"host":"  "}""", "", "Server name is required.")]
    [InlineData("postgres", """{"host":"","port":"abc","username":""}""", "", "Server name is required.")]
    [InlineData("postgres", """{"port":"abc","username":""}""", "typed", "Port must be between 1 and 65535, or blank for the default (5432).")]
    [InlineData("postgres", """{"port":"0"}""", "typed", "Port must be between 1 and 65535, or blank for the default (5432).")]
    [InlineData("postgres", """{"port":"65536"}""", "typed", "Port must be between 1 and 65535, or blank for the default (5432).")]
    [InlineData("postgres", """{"port":"","username":" "}""", "typed", "Username is required for SQL Server authentication.")]
    [InlineData("sql", """{"username":" ","monthly_cost_usd":"x"}""", "typed", "Username is required for SQL Server authentication.")]
    [InlineData("sp", """{"username":""}""", "typed", "The Application (client) ID is required for service-principal authentication.")]
    [InlineData("windows", """{"auth":"SQL","username":"","monthly_cost_usd":"x"}""", "", "Username is required for SQL Server authentication.")]
    [InlineData("sql", """{"host":"sql-b","monthly_cost_usd":"x"}""", "", PasswordNeeded)]
    [InlineData("windows", """{"monthly_cost_usd":"-5"}""", "", "Monthly cost must be a number, zero or more.")]
    [InlineData("sql", """{"monthly_cost_usd":"1,234"}""", "", "Monthly cost must be a number, zero or more.")]
    // A Windows form needs no username, and a good edit says nothing.
    [InlineData("windows", """{"username":"","display_name":"Payments"}""", "", null)]
    [InlineData("postgres", """{"display_name":"Payments"}""", "", null)]
    [InlineData("managed", """{"host":"sql-b"}""", "", null)]
    public void TheClientChecks_GiveTheFirstSentenceInTheDocumentedOrder_Pure(string kind, string changes, string password, string? expected)
    {
        var sentence = Pure("validateEdit", Base(kind), Edited(kind, changes), password);

        Assert.Equal(expected, sentence.ValueKind == JsonValueKind.Null ? null : sentence.GetString());
    }

    // ------------------------------------------------------------------ T13: the token, the probe and the answers

    [Fact]
    public void TheTokenRoundTripsVerbatim_AndInterpretEditBranchesOnStatusWords_Pure()
    {
        // The token goes out exactly as the read gave it: microseconds, trailing zeros, odd spacing and all, and always last.
        foreach (var token in new[] { Token, "2026-01-02T03:04:05.1230000Z", "2026-01-02T03:04:05.0000001+00:00", " as-is " })
        {
            var body = Pure("buildEditBody", Base("windows"), Edited("windows", """{"display_name":"Payments","monthly_cost_usd":"7"}"""), token, "");
            Assert.Equal(token, body.GetProperty("expected_modified_at").GetString());
            Assert.Equal("expected_modified_at", body.EnumerateObject().Last().Name);
        }

        // A missing token is sent as null, which the service refuses by name, never dropped (that would skip the stale check).
        var missing = Pure("buildEditBody", Base("windows"), Edited("windows", """{"display_name":"Payments"}"""), null, "");
        Assert.Equal(JsonValueKind.Null, missing.GetProperty("expected_modified_at").ValueKind);

        var results = PureAll(Answers.Select(a => ("interpretEdit", new object?[] { a.Status, a.Body is null ? null : JsonNode.Parse(a.Body), a.Message })).ToArray());
        for (var i = 0; i < Answers.Length; i++)
        {
            var actual = JsonNode.Parse(results[i].GetRawText());
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse(Answers[i].Expected), actual), Answers[i].Name + ": got " + results[i].GetRawText());
        }
    }

    private const string UnexpectedAnswer = "The service gave an answer this page does not understand, so it is not known whether anything was saved. Your entries are kept; check the server list before saving again.";

    [Fact]
    public void OnlyTheTransportsExpiredFlag_OrA401_ClosesTheFormAsAnExpiredSession()
    {
        var results = PureAll(
            ("interpretEdit", new object?[] { 200, null, null, true }),
            ("interpretEdit", new object?[] { 200, JsonNode.Parse("[]"), null, true }),
            ("interpretEdit", new object?[] { 200, JsonNode.Parse("[]"), null, false }));
        Assert.Equal("expired", results[0].GetProperty("kind").GetString());
        Assert.True(results[0].GetProperty("close").GetBoolean());
        Assert.Equal("expired", results[1].GetProperty("kind").GetString());
        Assert.Equal("unexpected", results[2].GetProperty("kind").GetString());
        Assert.False(results[2].GetProperty("close").GetBoolean());
    }

    // #5356: a 200 whose body is JSON but not an object (an array, a string, null) is not a sign-in page, so the shell is not told and
    // the form stays open with its typed entries and a sentence; the real expiry (a body that is no JSON at all) still closes it.
    [Theory]
    [InlineData("jsonArray")]
    [InlineData("jsonString")]
    [InlineData("jsonNull")]
    public void AJsonAnswerThatIsNotAnObject_KeepsTheFormOpenWithASentence(string kind)
    {
        var page = Run("save:" + kind);
        var after = page.GetProperty("after");

        Assert.Empty(page.GetProperty("expired").EnumerateArray());
        Assert.Equal(1, after.GetProperty("forms").GetInt32());
        Assert.Equal(new[] { UnexpectedAnswer }, Lines(after, "banner"));
        Assert.Equal("Alpha Two", page.GetProperty("form").GetProperty("values").GetProperty("display_name").GetString());
        Assert.False(after.GetProperty("saveDisabled").GetBoolean());
    }

    private static readonly (string Name, int Status, string? Body, string? Message, string Expected)[] Answers =
    [
        ("no answer at all shows the transport's message", 0, null, "Network error: connection refused",
            """{"kind":"network","close":false,"reread":false,"banner":"Network error: connection refused"}"""),
        ("a 401 is an expired session", 401, """{"error":"sign in"}""", null, """{"kind":"expired","close":true,"reread":false}"""),
        ("a 2xx that is not a JSON object keeps the form with a sentence; only the transport's expired flag closes it (#5356)", 200, null, null,
            """{"kind":"unexpected","close":false,"reread":false,"banner":"UNEXPECTED"}""".Replace("UNEXPECTED", UnexpectedAnswer, StringComparison.Ordinal)),
        ("a 2xx whose body is a JSON array keeps the form with the same sentence", 200, "[]", null,
            """{"kind":"unexpected","close":false,"reread":false,"banner":"UNEXPECTED"}""".Replace("UNEXPECTED", UnexpectedAnswer, StringComparison.Ordinal)),
        ("a 403 closes the form with the read-only notice, whatever the body says", 403, """{"error":"something else"}""", null,
            """{"kind":"readonly","close":true,"reread":false,"notice":"This account has read-only access. Nothing was saved."}"""),
        ("a 404 closes the form, re-reads the list and shows the server's sentence", 404, """{"error":"This server's definition no longer exists."}""", null,
            """{"kind":"notfound","close":true,"reread":true,"notice":"This server's definition no longer exists."}"""),
        ("a 404 with a message uses the message", 404, """{"status":"not_found","message":"Gone."}""", null,
            """{"kind":"notfound","close":true,"reread":true,"notice":"Gone."}"""),
        ("a 409 conflict carries the current values and keeps the form", 409,
            """{"status":"conflict","message":"This server was changed since you read it; nothing was saved.","current":{"host":"sql-z","modified_at":"2026-02-02T00:00:00.1234567Z"}}""", null,
            """{"kind":"conflict","close":false,"reread":false,"banner":"This server was changed since you opened it. Nothing was saved.","current":{"host":"sql-z","modified_at":"2026-02-02T00:00:00.1234567Z"}}"""),
        ("a 409 conflict with no current object is just a failure", 409, """{"status":"conflict","message":"No current."}""", null,
            """{"kind":"failed","close":false,"reread":false,"banner":"No current."}"""),
        ("a 409 collision is a failure that shows its message, even one that talks about a change", 409,
            """{"status":"collides","reason":"occupied","message":"This server was changed since you opened it: another monitored server already uses the address."}""", null,
            """{"kind":"failed","close":false,"reread":false,"banner":"This server was changed since you opened it: another monitored server already uses the address."}"""),
        ("a 400 keeps the form with the service's sentence", 400, """{"status":"invalid","message":"host must be non-blank text."}""", null,
            """{"kind":"failed","close":false,"reread":false,"banner":"host must be non-blank text."}"""),
        ("a 429 keeps the form", 429, """{"error":"Another server write is in progress."}""", null,
            """{"kind":"failed","close":false,"reread":false,"banner":"Another server write is in progress."}"""),
        ("a 500 keeps the form", 500, """{"error":"The edit failed."}""", null, """{"kind":"failed","close":false,"reread":false,"banner":"The edit failed."}"""),
        ("a 500 with no sentence says which status it was", 500, """{}""", null, """{"kind":"failed","close":false,"reread":false,"banner":"Request failed (HTTP 500)."}"""),
        ("a 502 with no body at all says which status it was", 502, null, null, """{"kind":"failed","close":false,"reread":false,"banner":"Request failed (HTTP 502)."}"""),
        ("a 200 connection_failed keeps the form with the message", 200, """{"status":"connection_failed","message":"Not saved: the new connection did not answer."}""", null,
            """{"kind":"failed","close":false,"reread":false,"banner":"Not saved: the new connection did not answer."}"""),
        ("a 503 timeout shows the banner and re-reads the list but keeps the form", 503, """{"error":"The edit timed out."}""", null,
            """{"kind":"timeout","close":false,"reread":true,"banner":"The edit timed out."}"""),
        ("an update closes the form, re-reads, and says the note and that the connection was tested", 200,
            """{"status":"updated","server":"sql-a","display_name":"Orders","server_id":7,"changed":["host"],"reconnects":1,"tested":true,"modified_at":"x","note":"The service applies this within one sweep."}""", null,
            """{"kind":"updated","close":true,"reread":true,"notice":"Saved \"Orders\". The service applies this within one sweep. The connection was tested before saving."}"""),
        ("an update that tested nothing leaves the sentence out, even when its message says otherwise", 200,
            """{"status":"updated","display_name":"Orders","tested":false,"note":"Done.","message":"connection_failed"}""", null,
            """{"kind":"updated","close":true,"reread":true,"notice":"Saved \"Orders\". Done."}"""),
        ("an unchanged edit closes the form and writes nothing", 200, """{"status":"unchanged","message":"Nothing to change."}""", null,
            """{"kind":"unchanged","close":true,"reread":false,"notice":"No change was needed; nothing was written."}"""),
    ];

    [Theory]
    // A connection change expects a probe whatever the auth (the service probes every one); a name or cost alone does not.
    [InlineData("windows", """{"host":"sql-b"}""", false, true)]
    [InlineData("managed", """{"database":"Sales"}""", false, true)]
    [InlineData("windows", """{"trust_server_certificate":true}""", false, true)]
    [InlineData("postgres", """{"port":"5434"}""", false, true)]
    [InlineData("windows", """{"auth":"SQL","username":"sa"}""", false, true)]
    [InlineData("sql", """{"auth":"Windows"}""", false, true)]
    [InlineData("windows", """{"display_name":"Payments"}""", false, false)]
    [InlineData("windows", """{"monthly_cost_usd":"5"}""", false, false)]
    [InlineData("windows", """{}""", false, false)]
    [InlineData("sql", """{"encrypt_mode":"mandatory","host":" sql-a "}""", false, false)]
    // A password sent expects one too, with nothing else changed.
    [InlineData("sql", """{}""", true, true)]
    [InlineData("sql", """{"display_name":"Payments"}""", true, true)]
    [InlineData("sp", """{"host":"sql-b"}""", true, true)]
    public void AProbeIsExpected_WhenTheConnectionChangesOrAPasswordIsSent_Pure(string kind, string changes, bool passwordSent, bool expected)
    {
        var probe = Pure("probeExpected", Base(kind), Edited(kind, changes), passwordSent);

        Assert.Equal(expected, probe.GetBoolean());
    }

    // ------------------------------------------------------------------ the rest of the exports

    [Fact]
    public void TheEditFields_AreTheElevenEditableKeysInFormOrder_AndNeverTheSecretOrTheToken_Pure()
    {
        var fields = Pure("EDIT_FIELDS").EnumerateArray().Select(f => (f.GetProperty("key").GetString(), f.GetProperty("label").GetString())).ToArray();

        Assert.Equal(
            new (string?, string?)[]
            {
                ("host", "Server Name / Address"), ("display_name", "Display Name"), ("port", "Port"), ("auth", "Authentication"),
                ("username", "Username"), ("encrypt_mode", "Encryption"), ("trust_server_certificate", "Trust server certificate"),
                ("database", "Database"), ("read_only_intent", "Read-only intent"), ("multi_subnet_failover", "Multi-subnet failover"),
                ("monthly_cost_usd", "Monthly Cost ($)"),
            },
            fields);
        Assert.Equal(new[] { "Windows", "SQL", "ServicePrincipal", "ManagedIdentity" }, Pure("EDIT_AUTHS").EnumerateArray().Select(a => a.GetString()).ToArray());
        Assert.Equal(new[] { "Optional", "Mandatory", "Strict" }, Pure("EDIT_ENCRYPT_MODES").EnumerateArray().Select(a => a.GetString()).ToArray());
    }

    [Theory]
    // The by-id read as the form holds it: port 0 is blank, an encryption mode matches an option ignoring case, a boolean is
    // true only when exactly true, and no secret, id or token comes along.
    [InlineData(
        """{"server_id":7,"display_name":"Orders","engine":"sqlserver","host":"sql-a","port":0,"database":null,"read_only_intent":true,"auth":"SQL","username":"sa","encrypt_mode":"mandatory","trust_server_certificate":true,"multi_subnet_failover":false,"monthly_cost_usd":1234.5,"modified_at":"2026-01-02T03:04:05.1234567Z"}""",
        """{"engine":"sqlserver","host":"sql-a","display_name":"Orders","port":"","auth":"SQL","username":"sa","encrypt_mode":"Mandatory","trust_server_certificate":true,"database":"","read_only_intent":true,"multi_subnet_failover":false,"monthly_cost_usd":"1234.5"}""")]
    // PostgreSQL always shows auth "SQL" and its port; an engine that is not sqlserver is PostgreSQL, whatever its case or spelling.
    [InlineData(
        """{"display_name":"Pg","engine":"postgres","host":"pg-a","port":5433,"database":"mon","read_only_intent":false,"auth":"Windows","username":"pgmon","encrypt_mode":"Optional","trust_server_certificate":"true","multi_subnet_failover":1,"monthly_cost_usd":0}""",
        """{"engine":"postgres","host":"pg-a","display_name":"Pg","port":"5433","auth":"SQL","username":"pgmon","encrypt_mode":"Optional","trust_server_certificate":false,"database":"mon","read_only_intent":false,"multi_subnet_failover":false,"monthly_cost_usd":"0"}""")]
    [InlineData(
        """{"display_name":"X","engine":"MySQL","host":"h","port":3306,"auth":"SQL","username":"u","encrypt_mode":"Optional"}""",
        """{"engine":"postgres","host":"h","display_name":"X","port":"3306","auth":"SQL","username":"u","encrypt_mode":"Optional","trust_server_certificate":false,"database":"","read_only_intent":false,"multi_subnet_failover":false,"monthly_cost_usd":"0"}""")]
    // An auth the list does not know shows as Windows, as the service's own word for it does; an unknown encryption mode stays as text.
    [InlineData(
        """{"display_name":"X","engine":"SQLServer","host":"h","auth":"Kerberos","username":null,"encrypt_mode":"Weird","monthly_cost_usd":12}""",
        """{"engine":"sqlserver","host":"h","display_name":"X","port":"","auth":"Windows","username":"","encrypt_mode":"Weird","trust_server_certificate":false,"database":"","read_only_intent":false,"multi_subnet_failover":false,"monthly_cost_usd":"12"}""")]
    public void TheFormValues_AreTheReadAsTheFormHoldsIt_Pure(string read, string expected)
    {
        var values = Pure("editFormValues", JsonNode.Parse(read));

        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(expected), JsonNode.Parse(values.GetRawText())), values.GetRawText());
    }

    [Theory]
    // PostgreSQL port: blank is 0 (the default), digits from 1 to 65535 are the number, everything else is not a number.
    [InlineData("postgres", "port", "", "0")]
    [InlineData("postgres", "port", " 5432 ", "5432")]
    [InlineData("postgres", "port", "65535", "65535")]
    [InlineData("postgres", "port", "0", """{"$number":"NaN"}""")]
    [InlineData("postgres", "port", "65536", """{"$number":"NaN"}""")]
    [InlineData("postgres", "port", "-1", """{"$number":"NaN"}""")]
    [InlineData("postgres", "port", "54.32", """{"$number":"NaN"}""")]
    [InlineData("postgres", "port", "abc", """{"$number":"NaN"}""")]
    [InlineData("windows", "port", "1433", "null")]
    // Cost: blank is 0, digits with an optional decimal point are the number, everything else is not a number.
    [InlineData("windows", "monthly_cost_usd", "", "0")]
    [InlineData("windows", "monthly_cost_usd", "12", "12")]
    [InlineData("windows", "monthly_cost_usd", "12.", "12")]
    [InlineData("windows", "monthly_cost_usd", ".5", "0.5")]
    [InlineData("windows", "monthly_cost_usd", " 12.50 ", "12.5")]
    [InlineData("windows", "monthly_cost_usd", "-1", """{"$number":"NaN"}""")]
    [InlineData("windows", "monthly_cost_usd", "1e3", """{"$number":"NaN"}""")]
    [InlineData("windows", "monthly_cost_usd", "1,234", """{"$number":"NaN"}""")]
    // Text is trimmed, a blank database or username is null, and Windows never has a username.
    [InlineData("windows", "database", "  ", "null")]
    [InlineData("windows", "database", " Sales ", "\"Sales\"")]
    [InlineData("windows", "username", "ignored", "null")]
    [InlineData("sql", "username", "  ", "null")]
    [InlineData("sql", "username", " sa ", "\"sa\"")]
    [InlineData("managed", "username", " client-id ", "\"client-id\"")]
    [InlineData("postgres", "auth", "Windows", "\"SQL\"")]
    [InlineData("windows", "encrypt_mode", "strict", "\"Strict\"")]
    public void NormalizingTheForm_FollowsTheServicesOwnParsing_Pure(string kind, string field, string typed, string expected)
    {
        var values = Edited(kind, "{}");
        values[field] = typed;
        var engine = values["engine"]!.GetValue<string>();

        var normalized = Pure("normalizeEdit", values, engine);

        Assert.Equal(expected, normalized.GetProperty(field).GetRawText());
    }

    [Fact]
    public void TheConflictLines_NameEachFieldTheOtherEditChanged_AndWhatTheUserEnteredForIt_Pure()
    {
        var original = Base("sql");
        var values = Edited("sql", """{"host":"sql-b","display_name":"Payments","monthly_cost_usd":"250"}""");
        var current = JsonNode.Parse("""{"server_id":7,"display_name":"Orders","engine":"sqlserver","host":"sql-z","port":0,"database":"Sales","read_only_intent":false,"auth":"SQL","username":"sa","encrypt_mode":"Mandatory","trust_server_certificate":true,"multi_subnet_failover":false,"monthly_cost_usd":100,"modified_at":"2026-02-02T00:00:00.1234567Z"}""");

        var lines = Pure("conflictChanges", original, current, values).EnumerateArray().Select(l => l.GetProperty("text").GetString()).ToArray();

        Assert.Equal(
            new[]
            {
                "Server Name / Address: was sql-a, now sql-z (you entered sql-b)",
                "Trust server certificate: was no, now yes",
                "Database: was (blank), now Sales",
            },
            lines);
        var host = Pure("conflictChanges", original, current, values).EnumerateArray().First();
        Assert.Equal("host", host.GetProperty("key").GetString());
        Assert.Equal("sql-b", host.GetProperty("yours").GetString());
        Assert.Equal(JsonValueKind.Null, Pure("conflictChanges", original, current, values).EnumerateArray().Last().GetProperty("yours").ValueKind);
    }

    [Fact]
    public void TheConflictLines_ShowAPostgresPortAsItsDefault_AndSkipThePortOfSqlServerAndTheAuthOfPostgres_Pure()
    {
        var postgres = Base("postgres");
        var postgresNow = JsonNode.Parse("""{"engine":"postgres","host":"pg-a","port":0,"auth":"Windows","username":"pgmon","encrypt_mode":"Mandatory","monthly_cost_usd":0,"display_name":"Orders"}""");
        var postgresLines = Pure("conflictChanges", postgres, postgresNow, Base("postgres")).EnumerateArray().Select(l => l.GetProperty("text").GetString()).ToArray();
        // Port 5433 became the default; the cost 100 became 0; the auth the read reports is not the form's business.
        Assert.Equal(new[] { "Port: was 5433, now (default)", "Monthly Cost ($): was 100, now 0" }, postgresLines);

        var sqlServerNow = JsonNode.Parse("""{"engine":"sqlserver","host":"sql-a","port":1433,"auth":"SQL","username":"sa","encrypt_mode":"Mandatory","monthly_cost_usd":100,"display_name":"Orders"}""");
        Assert.Empty(Pure("conflictChanges", Base("sql"), sqlServerNow, Base("sql")).EnumerateArray());
    }

    [Theory]
    [InlineData("a bad password: hunter2 was refused", "hunter2", "a bad password: [redacted] was refused")]
    [InlineData("hunter2hunter2", "hunter2", "[redacted][redacted]")]
    [InlineData("a.b*c( is not a pattern: a.b*c(", "a.b*c(", "[redacted] is not a pattern: [redacted]")]
    [InlineData("nothing to hide", "", "nothing to hide")]
    [InlineData("nothing to hide", "hunter2", "nothing to hide")]
    public void ThePasswordIsRedactedFromAServiceSentence_WithNoPatternMatching_Pure(string text, string password, string expected)
    {
        var redacted = Pure("redactPassword", text, password);

        Assert.Equal(expected, redacted.GetString());
    }

    // ------------------------------------------------------------------ the page: Edit column, notes, opening and closing the form

    private const string NoteStart = "Every configured server, enabled or disabled. ";

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static string StripText(JsonElement strip) => strip.GetProperty("text").GetString()!;

    [Fact]
    public void AReadOnlySeat_SeesNoEditAction_AndNeverReadsTheEditRow()
    {
        var page = Run("seat:readonly");

        Assert.Equal(0, page.GetProperty("editHeaders").GetInt32());
        Assert.Equal(0, page.GetProperty("editButtons").GetInt32());
        // The session probe and the list are the only requests: no by-id read and no PATCH.
        Assert.Equal(new[] { "GET /api/session", "GET /api/admin/servers" }, Strings(page.GetProperty("requests")));
        Assert.Equal("3 servers. " + NoteStart + "This sign-in is read-only. Add, edit and remove stay in the desktop Manage Servers window.", Assert.Single(Strings(page.GetProperty("notice"))));
        Assert.False(page.GetProperty("form").GetBoolean());
    }

    [Theory]
    [InlineData("probefailed")]
    [InlineData("probedropped")]
    public void ASeatWhoseSessionProbeFailed_SeesNoEditAction_AndANoteToReloadThePage(string kind)
    {
        var page = Run("seat:" + kind);

        Assert.Equal(0, page.GetProperty("editHeaders").GetInt32());
        Assert.Equal(0, page.GetProperty("editButtons").GetInt32());
        Assert.Equal(
            "3 servers. " + NoteStart + "Could not check whether this sign-in can make changes. Reload the page to try again.",
            Assert.Single(Strings(page.GetProperty("notice"))));
        Assert.DoesNotContain("read-only", Assert.Single(Strings(page.GetProperty("notice"))), StringComparison.Ordinal);
    }

    [Fact]
    public void AnEditingSeat_SeesAnEditButtonOnEachRowWithAnId_AndTheEditingNote()
    {
        var page = Run("seat:editor");

        Assert.Equal(1, page.GetProperty("editHeaders").GetInt32());
        Assert.Equal(new[] { "Edit Alpha", "Edit Bravo" }, Strings(page.GetProperty("labels")));
        Assert.Equal(new[] { "1", "2" }, Strings(page.GetProperty("ids")));
        // Echo is listed without a server_id, so it has no button.
        Assert.Equal(new[] { "Alpha", "Bravo", "Echo" }, Strings(page.GetProperty("rows")));
        Assert.Equal(
            "3 servers. " + NoteStart + "Edit changes a server in place. Add, remove, enable or disable, and excluded databases stay in the desktop Manage Servers window.",
            Assert.Single(Strings(page.GetProperty("notice"))));
    }

    [Fact]
    public void TheEditButton_ReadsTheServerById_FillsTheForm_AndFocusesItsHeading()
    {
        var page = Run("open:charlie");
        var form = page.GetProperty("form");

        Assert.Equal(new[] { "/api/admin/servers/3" }, Strings(page.GetProperty("byIdGets")));
        Assert.Equal("Edit SQL Server Connection", form.GetProperty("heading").GetString());
        Assert.True(form.GetProperty("headingFocused").GetBoolean());
        Assert.Equal("-1", form.GetProperty("headingTabindex").GetString());
        Assert.Equal("SQL Server", form.GetProperty("engine").GetString());
        Assert.Equal(
            new[] { "engine", "host", "display_name", "auth", "username", "password", "encrypt_mode", "trust_server_certificate", "database", "read_only_intent", "multi_subnet_failover", "monthly_cost_usd" },
            Strings(form.GetProperty("keys")));
        var values = form.GetProperty("values");
        Assert.Equal("charlie", values.GetProperty("host").GetString());
        Assert.Equal("Charlie", values.GetProperty("display_name").GetString());
        Assert.Equal("sa", values.GetProperty("username").GetString());
        Assert.Equal("Mandatory", values.GetProperty("encrypt_mode").GetString());
        Assert.Equal("Sales", values.GetProperty("database").GetString());
        Assert.Equal("1234", values.GetProperty("monthly_cost_usd").GetString());
        Assert.Equal("", values.GetProperty("password").GetString());
        Assert.Equal("password", form.GetProperty("passwordType").GetString());
        var checks = form.GetProperty("checks");
        Assert.True(checks.GetProperty("trust_server_certificate").GetBoolean());
        Assert.True(checks.GetProperty("read_only_intent").GetBoolean());
        Assert.False(checks.GetProperty("multi_subnet_failover").GetBoolean());
        Assert.Equal(new[] { "SQL" }, form.GetProperty("auth").EnumerateArray().Where(r => r.GetProperty("checked").GetBoolean()).Select(r => r.GetProperty("value").GetString()!).ToArray());
        Assert.Equal(new[] { "Save", "Cancel" }, Strings(form.GetProperty("buttons")));
        Assert.Equal(1, page.GetProperty("formBoxChildren").GetInt32());
    }

    [Fact]
    public void WhileTheByIdReadRuns_TheBoxSaysItIsLoading_ThenTheFormReplacesIt()
    {
        var page = Run("loading");

        Assert.Equal("Loading server settings", page.GetProperty("during").GetProperty("box").GetString());
        Assert.False(page.GetProperty("during").GetProperty("form").GetBoolean());
        Assert.Equal(new[] { "strip loading" }, Strings(page.GetProperty("during").GetProperty("strips")));
        Assert.Equal("Edit SQL Server Connection", page.GetProperty("form").GetProperty("heading").GetString());
    }

    [Theory]
    [InlineData("bravo", "5433")]
    [InlineData("bravo0", "")]
    public void APostgresForm_ShowsPortAndNoAuthChoice_AndNoSqlServerOnlyOptions(string which, string port)
    {
        var form = Run("open:" + which).GetProperty("form");

        Assert.Equal("Edit PostgreSQL Server Connection", form.GetProperty("heading").GetString());
        Assert.Equal("PostgreSQL", form.GetProperty("engine").GetString());
        // No auth radios, no read-only intent and no multi-subnet failover; the port is there, blank when stored as 0.
        Assert.Equal(
            new[] { "engine", "host", "display_name", "port", "username", "password", "encrypt_mode", "trust_server_certificate", "database", "monthly_cost_usd" },
            Strings(form.GetProperty("keys")));
        Assert.Empty(form.GetProperty("auth").EnumerateArray());
        Assert.Equal(port, form.GetProperty("values").GetProperty("port").GetString());
        Assert.Equal("pgmon", form.GetProperty("values").GetProperty("username").GetString());
        var notes = Strings(form.GetProperty("muted"));
        Assert.Contains("blank = 5432, the default", notes);
        Assert.Contains(notes, n => n.StartsWith("PostgreSQL targets connect with username and password authentication.", StringComparison.Ordinal));
        Assert.DoesNotContain(notes, n => n.Contains('—') || n.Contains('→'));
    }

    [Fact]
    public void AServerRemovedBeforeItsFormOpens_ShowsTheNotice_OpensNoForm_AndRereadsTheList()
    {
        var page = Run("byIdFails:404");
        var strips = page.GetProperty("notice").EnumerateArray().ToArray();

        Assert.Equal("This server's definition no longer exists.", StripText(strips[0]));
        Assert.Equal("strip notice", strips[0].GetProperty("cls").GetString());
        Assert.False(page.GetProperty("form").GetBoolean());
        Assert.Equal(0, page.GetProperty("formBoxChildren").GetInt32());
        // The list was read again, and the re-read no longer carries the removed server.
        Assert.Equal(1, page.GetProperty("listReads").GetInt32());
        Assert.Equal(new[] { "Bravo", "Charlie" }, Strings(page.GetProperty("rows")));
    }

    [Theory]
    [InlineData("403", "This account has read-only access.")]
    [InlineData("500", "admin server read failed (InvalidOperationException)")]
    [InlineData("network", "Network error: connection refused")]
    public void AFailedEditRead_ShowsTheSentence_OpensNoForm_AndKeepsTheList(string kind, string sentence)
    {
        var page = Run("byIdFails:" + kind);
        var strip = page.GetProperty("notice").EnumerateArray().First();

        Assert.Equal(sentence, StripText(strip));
        Assert.Equal("strip error", strip.GetProperty("cls").GetString());
        Assert.Equal("alert", strip.GetProperty("role").GetString());
        Assert.False(page.GetProperty("form").GetBoolean());
        Assert.Equal(0, page.GetProperty("formBoxChildren").GetInt32());
        Assert.Equal(0, page.GetProperty("listReads").GetInt32());
        Assert.Equal(new[] { "Alpha", "Bravo", "Charlie" }, Strings(page.GetProperty("rows")));
    }

    [Theory]
    [InlineData("ok")]
    [InlineData("listFails")]
    public void ThePollRepaint_KeepsAnOpenFormFocusedAndItsTypedValues(string kind)
    {
        var page = Run("poll:" + kind);

        // The repaint is under way (its list read held back): the form is untouched.
        Assert.True(page.GetProperty("during").GetProperty("active").GetBoolean());
        Assert.Equal("sql-prod-01", page.GetProperty("during").GetProperty("value").GetString());
        // And after the list read landed: the same host box, still focused, still holding what was typed.
        Assert.True(page.GetProperty("activeIsHost").GetBoolean());
        Assert.True(page.GetProperty("sameHost").GetBoolean());
        Assert.True(page.GetProperty("hostConnected").GetBoolean());
        Assert.Equal("sql-prod-01", page.GetProperty("hostValue").GetString());
        Assert.True(page.GetProperty("sameFormBox").GetBoolean());
        Assert.Equal(0, page.GetProperty("ancestorsRemoved").GetInt32());
        Assert.True(page.GetProperty("formOpen").GetBoolean());
        Assert.Equal(2, page.GetProperty("listReads").GetInt32());
        if (kind == "ok")
        {
            Assert.Equal(new[] { "Alpha", "Charlie" }, Strings(page.GetProperty("rows")));
        }
        else
        {
            // A failed list read shows in the table area, not over the form.
            Assert.Empty(Strings(page.GetProperty("rows")));
            Assert.Equal("the list could not be read", page.GetProperty("tableArea").GetString());
            Assert.Equal(new[] { "strip error" }, Strings(page.GetProperty("tableAreaStrips")));
        }
    }

    [Theory]
    [InlineData("cancel", 0)]
    [InlineData("another", 1)]
    [InlineData("tab", 0)]
    public void EveryWayOfClosingTheForm_ClearsThePasswordItHeld_BeforeItIsDetached(string path, int formsLeft)
    {
        var page = Run("closes:" + path);

        Assert.Equal("SECRET-PW", page.GetProperty("typed").GetString());
        // The very node typed into, read after the form is gone: nothing detached keeps the secret.
        Assert.Equal("", page.GetProperty("value").GetString());
        Assert.False(page.GetProperty("connected").GetBoolean());
        Assert.Equal(formsLeft, page.GetProperty("forms").GetInt32());
        Assert.Equal(0, page.GetProperty("secretNodes").GetInt32());
        if (path == "another")
        {
            Assert.Equal("Edit SQL Server Connection", page.GetProperty("heading").GetString());
        }
    }

    [Theory]
    [InlineData("another", 1, "Edit PostgreSQL Server Connection")]
    [InlineData("tab", 0, null)]
    public void AByIdReadThatLandsAfterItsFormWasDiscarded_OpensNothing(string path, int forms, string? heading)
    {
        var page = Run("staleOpen:" + path);

        Assert.Equal(forms, page.GetProperty("forms").GetInt32());
        Assert.Equal(heading, page.GetProperty("heading").GetString());
    }

    [Fact]
    public void TheAuthChoice_ShowsOnlyTheBoxesItNeeds_KeepsAUsernamePerMode_AndTellsWhenThePasswordIsNeeded()
    {
        var page = Run("authModes");
        JsonElement Step(string name) => page.GetProperty("steps").EnumerateArray().Single(s => s.GetProperty("step").GetString() == name);
        string Auths(JsonElement s) => string.Join(",", s.GetProperty("checked").EnumerateArray().Select(c => c.GetString()));
        bool Hidden(JsonElement row) => row.GetProperty("hidden").GetBoolean() && row.GetProperty("display").GetString() == "none";

        var opened = Step("opened");
        Assert.Equal("Windows", Auths(opened));
        Assert.True(Hidden(opened.GetProperty("username")));
        Assert.True(Hidden(opened.GetProperty("password")));

        var sql = Step("sql");
        Assert.Equal("SQL", Auths(sql));
        Assert.False(Hidden(sql.GetProperty("username")));
        Assert.Equal("Username", sql.GetProperty("username").GetProperty("label").GetString());
        Assert.Equal("sa-new", sql.GetProperty("usernameValue").GetString());
        Assert.False(Hidden(sql.GetProperty("password")));
        Assert.Equal("Password (required for this change)", sql.GetProperty("password").GetProperty("label").GetString());

        // The username typed under SQL does not become a client id: each mode has its own box contents.
        var principal = Step("serviceprincipal");
        Assert.Equal("Client (Application) ID", principal.GetProperty("username").GetProperty("label").GetString());
        Assert.Equal("", principal.GetProperty("usernameValue").GetString());
        Assert.Equal("Client Secret (required for this change)", principal.GetProperty("password").GetProperty("label").GetString());
        Assert.Equal("sa-new", Step("sql again").GetProperty("usernameValue").GetString());

        var managed = Step("managed identity");
        Assert.Equal("User-Assigned Identity Client ID (optional)", managed.GetProperty("username").GetProperty("label").GetString());
        Assert.False(Hidden(managed.GetProperty("username")));
        Assert.True(Hidden(managed.GetProperty("password")));
        Assert.False(Hidden(managed.GetProperty("note")));

        var windows = Step("windows");
        Assert.True(Hidden(windows.GetProperty("username")));
        Assert.True(Hidden(windows.GetProperty("password")));
        Assert.True(Hidden(windows.GetProperty("note")));

        // A SQL login's password is asked for again only once its host changes, and not once the host is put back.
        Assert.Equal(
            new[] { "Password (leave blank to keep the stored one)", "Password (required for this change)", "Password (leave blank to keep the stored one)" },
            Strings(page.GetProperty("suffixes")));
    }

    [Theory]
    [InlineData("mandatory", "Mandatory", "Optional,Mandatory,Strict")]
    [InlineData("STRICT", "Strict", "Optional,Mandatory,Strict")]
    [InlineData("Weird", "Weird", "Optional,Mandatory,Strict,Weird")]
    [InlineData("none", "", "Optional,Mandatory,Strict,")]
    public void TheStoredEncryptionWord_IsMatchedIgnoringCase_AndAnUnknownOneStaysSelectedAsAnExtraOption(string stored, string selected, string options)
    {
        var page = Run("encrypt:" + stored);

        Assert.Equal(selected, page.GetProperty("value").GetString());
        Assert.Equal(options, string.Join(",", Strings(page.GetProperty("options"))));
    }

    // ------------------------------------------------------------------ the harness frame

    [Fact]
    public void TheReadOnlyServersTab_DrawsItsRowsThroughTheRealGrid_AndOffersNoEdit()
    {
        var page = Run("frame");

        Assert.Equal(
            new[] { "Display Name", "Server", "Auth", "Engine", "Version", "Status", "Freshness", "Monthly Cost ($)", "Read only", "Added", "Last Collected" },
            page.GetProperty("headers").EnumerateArray().Select(h => h.GetString()).ToArray());
        var cells = page.GetProperty("cells").EnumerateArray().Select(r => r.EnumerateArray().Select(c => c.GetString()).ToArray()).ToArray();
        Assert.Equal(2, cells.Length);
        Assert.Equal(new[] { "Alpha", "alpha", "Windows", "sqlserver", "SQL Server 2022", "Enabled", "Online", "$1,234" }, cells[0].Take(8).ToArray());
        Assert.Equal(new[] { "Bravo", "bravo", "SQL Server", "postgres", "PostgreSQL 18", "Disabled", "AwaitingFirstCollection", "—" }, cells[1].Take(8).ToArray());
        // The grid's time cells follow the process zone, so the harness pins UTC; the cell text itself depends on the locale.
        Assert.Equal(0, page.GetProperty("utcOffsetMinutes").GetInt32());
        Assert.Equal(11, cells[0].Length);
        Assert.Equal(new[] { "", "band-Offline" }, page.GetProperty("rowClasses").EnumerateArray().Select(c => c.GetString()).ToArray());
        Assert.StartsWith("2 servers.", Assert.Single(page.GetProperty("notice").EnumerateArray()).GetString());
        Assert.Equal(0, page.GetProperty("editHeaders").GetInt32());
        Assert.Equal(0, page.GetProperty("editButtons").GetInt32());
        // The session probe (which decides whether the Edit column is drawn) comes first, then the list.
        Assert.Equal(new[] { "GET /api/session", "GET /api/admin/servers" }, page.GetProperty("requests").EnumerateArray().Select(r => r.GetString()).ToArray());
    }

    [Fact]
    public void TheFakeDomsFocus_SurvivesARepaintOnlyWhileTheSameNodeStaysAttached()
    {
        var focus = Run("focusModel");

        foreach (var check in focus.EnumerateObject())
        {
            Assert.True(check.Value.GetBoolean(), check.Name);
        }

        Assert.Equal(8, focus.EnumerateObject().Count());
    }

    [Fact]
    public void TheHarness_CopiesTheWholeJsFolder_NotAListOfModules()
    {
        var source = File.ReadAllText(PathTo("Darling", "Darling.Tests", "admin-server-edit-harness.mjs"));

        Assert.Contains("fs.cpSync(jsDir, scratch, { recursive: true })", source, StringComparison.Ordinal);
        Assert.DoesNotContain("copyTree", source, StringComparison.Ordinal);
        Assert.DoesNotContain("copyFileSync", source, StringComparison.Ordinal);
        Assert.DoesNotContain("\"util.js\", \"panels.js\"", source, StringComparison.Ordinal);
        Assert.Contains("process.env.TZ = \"UTC\"", source, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------ the save (T5, T7, T8, T9, T11, review findings 4 and 5, the hashchange discard)

    // The token the harness's by-id read hands out, which every PATCH must send back as it came.
    private const string ReadToken = "2026-10-05T12:34:56.1234567Z";

    private const string Testing = "Testing the connection, then saving. This can take up to a minute.";

    private static string[] Lines(JsonElement view, string name) => view.GetProperty(name).EnumerateArray().Select(StripText).ToArray();

    private static JsonElement[] Patches(JsonElement view) => view.GetProperty("patches").EnumerateArray().ToArray();

    private static bool[] Flags(JsonElement view, string name) => view.GetProperty(name).EnumerateArray().Select(e => e.GetBoolean()).ToArray();

    private static string Notice(JsonElement view) => StripText(view.GetProperty("notice").EnumerateArray().First());

    private static string Cls(JsonElement view, string name) => view.GetProperty(name).EnumerateArray().First().GetProperty("cls").GetString()!;

    private static void AssertPasswordGone(JsonElement page, bool connected)
    {
        Assert.Equal("", page.GetProperty("pw").GetProperty("value").GetString());
        Assert.Equal(connected, page.GetProperty("pw").GetProperty("connected").GetBoolean());
        Assert.Equal(0, page.GetProperty("secretNodes").GetInt32());
    }

    [Fact]
    public void AFailedProbe_ShowsTheServerSentence_KeepsTheForm_AndClearsThePassword()
    {
        var page = Run("save:connfail");
        var after = page.GetProperty("after");

        Assert.Equal("SECRET-PW", page.GetProperty("typed").GetString());
        Assert.Equal(new[] { "Could not connect to charlie-two: login failed for user 'sa'." }, Lines(after, "banner"));
        Assert.Equal("strip error", Cls(after, "banner"));
        Assert.Equal("alert", after.GetProperty("banner").EnumerateArray().First().GetProperty("role").GetString());
        Assert.Equal(1, after.GetProperty("forms").GetInt32());
        AssertPasswordGone(page, connected: true);
        // One PATCH carried the host, the password and the token; the list was not read again.
        var body = Assert.Single(Patches(after)).GetProperty("body");
        Assert.Equal("charlie-two", body.GetProperty("host").GetString());
        Assert.Equal("SECRET-PW", body.GetProperty("password").GetString());
        Assert.Equal(ReadToken, body.GetProperty("expected_modified_at").GetString());
        Assert.Equal(1, after.GetProperty("listReads").GetInt32());
        // The status line said the connection was being tested while it ran, and is gone; Save and Cancel are usable again.
        Assert.Equal(new[] { Testing }, Lines(page.GetProperty("during"), "status"));
        Assert.Empty(Lines(after, "status"));
        Assert.False(after.GetProperty("saveDisabled").GetBoolean());
        Assert.False(after.GetProperty("cancelDisabled").GetBoolean());
    }

    [Fact]
    public void AServer403_IsShown_TheUiDoesNotRelyOnHidingEdit()
    {
        var after = Run("save:forbidden").GetProperty("after");

        Assert.Equal("This account has read-only access. Nothing was saved.", Notice(after));
        Assert.Equal("strip error", Cls(after, "notice"));
        Assert.Equal(0, after.GetProperty("forms").GetInt32());
        Assert.Equal(0, after.GetProperty("formBoxChildren").GetInt32());
        // No retry, and no list read: nothing changed.
        Assert.Single(Patches(after));
        Assert.Equal(1, after.GetProperty("listReads").GetInt32());
    }

    [Fact]
    public void AServerRemovedWhileTheFormIsOpen_ClosesTheForm_ShowsTheSentence_AndRereadsTheList()
    {
        var page = Run("save:gone");
        var after = page.GetProperty("after");

        Assert.StartsWith("This server's definition no longer exists", Notice(after), StringComparison.Ordinal);
        Assert.Equal("strip notice", Cls(after, "notice"));
        Assert.Equal(0, after.GetProperty("forms").GetInt32());
        Assert.Equal(0, after.GetProperty("formBoxChildren").GetInt32());
        Assert.Single(Patches(after));
        Assert.Equal(2, after.GetProperty("listReads").GetInt32());
        Assert.Equal(new[] { "Bravo", "Charlie" }, Strings(after.GetProperty("rows")));
        AssertPasswordGone(page, connected: false);
    }

    [Theory]
    [InlineData("double")]
    [InlineData("429")]
    public void ADoubleSave_SendsOnePatch_AndA429KeepsTheForm(string how)
    {
        var page = Run("midsave:" + how);
        var during = page.GetProperty("during");
        var mid = page.GetProperty("mid");
        var late = page.GetProperty("late");

        // Save and Cancel are disabled while the request runs, and the status line says it is saving.
        Assert.True(during.GetProperty("saveDisabled").GetBoolean());
        Assert.True(during.GetProperty("cancelDisabled").GetBoolean());
        Assert.Equal(new[] { "Saving." }, Lines(during, "status"));
        // A second click on the disabled button, and a click forced past it, send nothing: one PATCH in all.
        Assert.Single(Patches(during));
        Assert.Single(Patches(mid));
        Assert.Single(Patches(late));
        if (how == "429")
        {
            Assert.Equal(new[] { "Another server change is in progress. Try again in a moment." }, Lines(late, "banner"));
            Assert.Equal(1, late.GetProperty("forms").GetInt32());
            Assert.False(late.GetProperty("saveDisabled").GetBoolean());
            Assert.False(late.GetProperty("cancelDisabled").GetBoolean());
            Assert.Empty(Lines(late, "status"));
            // And no retry on its own: still one PATCH after the page settled.
            Assert.Single(Patches(page.GetProperty("settled")));
        }
        else
        {
            Assert.Equal("Saved \"Alpha Two\". Takes effect at the next collection cycle.", Notice(late));
        }
    }

    [Fact]
    public void ASave_ClosesTheForm_ShowsTheNote_AndRereadsTheList()
    {
        var page = Run("save:updated");
        var after = page.GetProperty("after");

        Assert.Equal("Saved \"Alpha Prime\". Takes effect at the next collection cycle.", Notice(after));
        Assert.Equal("strip notice", Cls(after, "notice"));
        Assert.Equal(0, after.GetProperty("forms").GetInt32());
        Assert.Equal(0, after.GetProperty("formBoxChildren").GetInt32());
        Assert.Equal(2, after.GetProperty("listReads").GetInt32());
        Assert.Equal(new[] { "Alpha Prime", "Bravo", "Charlie" }, Strings(after.GetProperty("rows")));
        Assert.All(Flags(after, "editDisabled"), disabled => Assert.False(disabled));
        // The one PATCH went to the server's id with the JSON content type, carrying only the changed field and the token.
        var patch = Assert.Single(Patches(after));
        Assert.Equal("/api/servers/1", patch.GetProperty("url").GetString());
        Assert.Equal("application/json", patch.GetProperty("contentType").GetString());
        Assert.Equal(new[] { "display_name", "expected_modified_at" }, patch.GetProperty("body").EnumerateObject().Select(p => p.Name).ToArray());
        AssertPasswordGone(page, connected: false);
    }

    [Fact]
    public void ASaveThatTestedTheConnection_SaysSo()
    {
        var after = Run("save:tested").GetProperty("after");

        Assert.Equal("Saved \"Charlie\". Takes effect at the next collection cycle. The connection was tested before saving.", Notice(after));
        Assert.Equal(0, after.GetProperty("forms").GetInt32());
    }

    [Fact]
    public void AnUnchangedAnswer_ClosesTheForm_WithItsNotice_AndReadsNoList()
    {
        var after = Run("save:unchanged").GetProperty("after");

        Assert.Equal("No change was needed; nothing was written.", Notice(after));
        Assert.Equal(0, after.GetProperty("forms").GetInt32());
        Assert.Equal(1, after.GetProperty("listReads").GetInt32());
    }

    [Fact]
    public void AFormThatDiffersInNothing_ClosesWithNoChange_AndSendsNothing()
    {
        var after = Run("save:nochange").GetProperty("after");

        Assert.Equal("No change.", Notice(after));
        Assert.Equal(0, after.GetProperty("forms").GetInt32());
        Assert.Empty(Patches(after));
    }

    [Theory]
    [InlineData("clientcheck", "Monthly cost must be a number, zero or more.")]
    [InlineData("nopassword", PasswordNeeded)]
    public void AClientSideRefusal_SendsNothing_ShowsTheSentence_AndClearsThePassword(string kind, string sentence)
    {
        var page = Run("save:" + kind);
        var after = page.GetProperty("after");

        Assert.Equal(new[] { sentence }, Lines(after, "banner"));
        Assert.Equal(1, after.GetProperty("forms").GetInt32());
        Assert.Empty(Patches(after));
        Assert.Empty(Lines(after, "status"));
        AssertPasswordGone(page, connected: true);
    }

    [Theory]
    [InlineData("invalid", "The host 'charlie two' is not valid: [redacted] is not allowed here.")]
    [InlineData("limited", "Another server change is in progress. Try again in a moment.")]
    [InlineData("broken", "admin server edit failed (InvalidOperationException)")]
    [InlineData("collides", "Another server already uses the address alpha.")]
    [InlineData("network", "Network error: connection refused")]
    [InlineData("conflict", "This server was changed since you opened it. Nothing was saved.")]
    public void AnAnswerThatKeepsTheForm_ShowsItsSentenceInTheBanner_ClearsThePassword_AndEnablesSaveAgain(string kind, string sentence)
    {
        var page = Run("save:" + kind);
        var after = page.GetProperty("after");

        // The sentence has the typed password replaced by "[redacted]" when the service echoed it back.
        Assert.Equal(new[] { sentence }, Lines(after, "banner"));
        Assert.Equal(1, after.GetProperty("forms").GetInt32());
        Assert.Single(Patches(after));
        Assert.False(after.GetProperty("saveDisabled").GetBoolean());
        Assert.False(after.GetProperty("cancelDisabled").GetBoolean());
        AssertPasswordGone(page, connected: true);
    }

    [Fact]
    public void A503_KeepsTheForm_RereadsTheList_AndTheNextSaveSendsTheSameToken()
    {
        var page = Run("save:timedout");
        var after = page.GetProperty("after");

        Assert.Equal(new[] { "The edit did not finish in time. It may still complete; reload to see." }, Lines(after, "banner"));
        Assert.Equal(1, after.GetProperty("forms").GetInt32());
        Assert.Equal(2, after.GetProperty("listReads").GetInt32());
        var second = page.GetProperty("second");
        var patches = Patches(second);
        Assert.Equal(2, patches.Length);
        Assert.Equal(ReadToken, patches[0].GetProperty("body").GetProperty("expected_modified_at").GetString());
        Assert.Equal(ReadToken, patches[1].GetProperty("body").GetProperty("expected_modified_at").GetString());
        Assert.StartsWith("Saved \"Alpha\".", Notice(second), StringComparison.Ordinal);
    }

    [Fact]
    public void AnExpiredSession_HandsOverToTheShell_AndDiscardsTheForm()
    {
        var page = Run("save:expired");
        var after = page.GetProperty("after");

        var told = Assert.Single(page.GetProperty("expired").EnumerateArray());
        Assert.Equal("Your session has expired. Sign in again.", told.GetProperty("message").GetString());
        Assert.Equal(0, after.GetProperty("forms").GetInt32());
        Assert.Empty(Lines(after, "banner"));
        // No sentence of its own: the page notice is still just the count and note of the list.
        Assert.StartsWith("3 servers.", Notice(after), StringComparison.Ordinal);
        Assert.Single(after.GetProperty("notice").EnumerateArray());
        AssertPasswordGone(page, connected: false);
    }

    [Theory]
    [InlineData("probeWindowsHost", true)]
    [InlineData("probeWindowsTrust", true)]
    [InlineData("probeWindowsEncrypt", true)]
    [InlineData("probeSqlHost", true)]
    [InlineData("probeRotate", true)]
    [InlineData("probePostgresPort", true)]
    [InlineData("probeName", false)]
    [InlineData("probeCost", false)]
    public void TheStatusLine_SaysTheConnectionIsTested_ExactlyWhenTheServiceWillProbeIt(string kind, bool testing)
    {
        // Review finding 5: a Windows or managed-identity server's address, TLS or database change is probed with no password at all.
        var during = Run("save:" + kind).GetProperty("during");

        Assert.Equal(new[] { testing ? Testing : "Saving." }, Lines(during, "status"));
        Assert.Equal("strip loading", Cls(during, "status"));
        Assert.Single(Patches(during));
    }

    [Fact]
    public void WhileASaveRuns_NoOtherRowOpens_TheAnswerShowsItsNotice_AndTheNextFormSaves()
    {
        // Review finding 4: every Edit button is disabled mid-save; a click on one (or one forced past it) reads no row.
        var page = Run("midsave:edit");
        var during = page.GetProperty("during");
        var mid = page.GetProperty("mid");
        var late = page.GetProperty("late");
        var next = page.GetProperty("next");

        Assert.Equal(new[] { true, true, true }, Flags(during, "editDisabled"));
        Assert.Equal(new[] { "/api/admin/servers/1" }, Strings(mid.GetProperty("byIdGets")));
        Assert.Equal(1, mid.GetProperty("forms").GetInt32());
        Assert.Equal(new[] { true, true, true }, Flags(mid, "editDisabled"));
        Assert.Equal(new[] { false, false, false }, Flags(late, "editDisabled"));
        Assert.Equal("Saved \"Alpha Two\". Takes effect at the next collection cycle.", Notice(late));
        // The next row's form opens and its Save works: busy was cleared.
        Assert.Equal(2, Patches(next).Length);
        Assert.Equal("Saved \"Charlie Two\". Takes effect at the next collection cycle.", Notice(next));
    }

    [Theory]
    [InlineData("tab")]
    [InlineData("hash")]
    public void ALateAnswer_TouchesNoForm_ButStillSetsTheNotice_UnlocksTheGrid_AndRereadsTheList(string how)
    {
        // The form is discarded while the save runs (the Routes tab; another page). busy still clears, and the outcome is shown
        // when the Servers tab is on screen again, with the list read after the change.
        var page = Run("midsave:" + how);

        Assert.Equal(0, page.GetProperty("mid").GetProperty("forms").GetInt32());
        var shown = how == "tab" ? page.GetProperty("back") : page.GetProperty("late");
        Assert.Equal("Saved \"Alpha Two\". Takes effect at the next collection cycle.", Notice(shown));
        Assert.Equal(0, shown.GetProperty("forms").GetInt32());
        Assert.All(Flags(shown, "editDisabled"), disabled => Assert.False(disabled));
        Assert.Equal(3, Flags(shown, "editDisabled").Length);
        Assert.Equal(2, shown.GetProperty("listReads").GetInt32());
        Assert.Equal("Alpha Two", Strings(shown.GetProperty("rows"))[0]);
        Assert.Single(Patches(shown));
    }

    [Theory]
    [InlineData("#/fleet", false)]
    [InlineData("#/admin/routes", false)]
    [InlineData("#/admin/servers", true)]
    [InlineData("#/admin", true)]
    [InlineData("#/admin/bogus", true)]
    public void TheHashLeavingTheServersTab_DiscardsTheForm_ClearingThePasswordFirst(string hash, bool kept)
    {
        var page = Run("hash:" + hash);

        // Painted twice before the hash moved, yet one listener: it is registered once.
        Assert.Equal(1, page.GetProperty("listeners").GetInt32());
        Assert.Equal(kept, page.GetProperty("kept").GetBoolean());
        Assert.Equal(kept ? 1 : 0, page.GetProperty("forms").GetInt32());
        Assert.Equal(kept ? 1 : 0, page.GetProperty("formBoxChildren").GetInt32());
        if (kept)
        {
            Assert.Equal("charlie-two", page.GetProperty("host").GetString());
            Assert.Equal("SECRET-PW", page.GetProperty("value").GetString());
        }
        else
        {
            // The very box typed into, read after the form is gone, holds nothing, and nothing on the page does.
            Assert.Equal("", page.GetProperty("value").GetString());
            Assert.False(page.GetProperty("connected").GetBoolean());
            Assert.Equal(0, page.GetProperty("secretNodes").GetInt32());
        }
    }

    [Theory]
    [InlineData("#/fleet", 0)]
    [InlineData("#/admin/routes", 0)]
    [InlineData("#/admin/servers", 1)]
    public void AByIdReadStillRunningWhenTheHashLeaves_OpensNoForm(string hash, int forms)
    {
        var page = Run("hashOpen:" + hash);

        Assert.Equal(forms, page.GetProperty("forms").GetInt32());
        Assert.Equal(forms, page.GetProperty("formBoxChildren").GetInt32());
    }

    // ------------------------------------------------------------------ T4: the stale edit (a 409 conflict)

    private const string Token2 = "2026-10-05T13:00:00.7654321Z";

    private const string Stale = "This server was changed since you opened it. Nothing was saved.";

    private const string NoneChanged = "None of the fields on this form changed. Another setting, such as the enabled state, changed.";

    private static string Body(JsonElement patch) => patch.GetProperty("body").GetRawText();

    private static string[] FormValues(JsonElement step, params string[] keys) =>
        keys.Select(k => step.GetProperty("form").GetProperty("values").GetProperty(k).GetString()!).ToArray();

    [Theory]
    [InlineData("reapply")]
    [InlineData("reload")]
    [InlineData("reloadnone")]
    [InlineData("again")]
    [InlineData("none")]
    public void AStaleEdit_Shows409Changes_AndReapplyOrReloadNeverResubmits(string how)
    {
        var page = Run("conflict:" + how);
        var stale = page.GetProperty("stale");
        var staleView = stale.GetProperty("view");
        var none = how == "none";
        var userBody = none
            ? """{"display_name":"Charlie Two","password":"SECRET-PW","expected_modified_at":"READ"}"""
            : """{"host":"charlie-two","display_name":"Charlie Two","password":"SECRET-PW","expected_modified_at":"READ"}""";

        // The 409: the page's own sentence, what the other edit changed (and what the user entered for the same field), the two
        // buttons, exactly one PATCH, and the password gone from the box and from the page.
        Assert.Equal(new[] { Stale }, Lines(staleView, "banner"));
        var panel = staleView.GetProperty("conflict");
        Assert.Equal(new[] { "Reapply my changes", "Reload current values" }, Strings(panel.GetProperty("buttons")));
        if (none)
        {
            Assert.Empty(panel.GetProperty("lines").EnumerateArray());
            Assert.Equal(NoneChanged, panel.GetProperty("none").GetString());
        }
        else
        {
            Assert.Equal(
                new[]
                {
                    "Server Name / Address: was charlie, now charlie-new (you entered charlie-two)",
                    "Display Name: was Charlie, now Charlie Elsewhere (you entered Charlie Two)",
                    "Database: was Sales, now Orders",
                },
                Strings(panel.GetProperty("lines")));
            Assert.Equal(new[] { "host", "display_name", "database" }, Strings(panel.GetProperty("fields")));
            Assert.Equal(JsonValueKind.Null, panel.GetProperty("none").ValueKind);
        }

        Assert.Equal(userBody.Replace("READ", ReadToken, StringComparison.Ordinal), Body(Assert.Single(Patches(staleView))));
        Assert.Equal(1, staleView.GetProperty("forms").GetInt32());
        Assert.False(staleView.GetProperty("saveDisabled").GetBoolean());
        Assert.Equal("", stale.GetProperty("pw").GetProperty("value").GetString());
        Assert.Equal(0, stale.GetProperty("secretNodes").GetInt32());
        Assert.Equal(none ? "charlie" : "charlie-two", FormValues(stale, "host")[0]);

        if (how != "again")
        {
            // Pressing either button sends nothing at all: no PATCH, no read. The form is drawn again, focused, with a fresh empty password box.
            var pressed = page.GetProperty("pressed");
            var pressedView = pressed.GetProperty("view");
            Assert.Equal(0, pressed.GetProperty("sent").GetInt32());
            Assert.Single(Patches(pressedView));
            Assert.Equal(JsonValueKind.Null, pressedView.GetProperty("conflict").ValueKind);
            Assert.Empty(Lines(pressedView, "banner"));
            Assert.Equal(1, pressedView.GetProperty("forms").GetInt32());
            Assert.True(pressed.GetProperty("form").GetProperty("headingFocused").GetBoolean());
            Assert.Equal("", pressed.GetProperty("form").GetProperty("passwordValue").GetString());
            Assert.Equal("", pressed.GetProperty("oldPw").GetProperty("value").GetString());
            var values = FormValues(pressed, "host", "display_name", "database");
            Assert.Equal(
                how switch
                {
                    "reapply" => new[] { "charlie-two", "Charlie Two", "Orders" },
                    "none" => new[] { "charlie", "Charlie Two", "Sales" },
                    _ => new[] { "charlie-new", "Charlie Elsewhere", "Orders" },
                },
                values);
        }

        var saved = page.GetProperty("saved");
        var savedView = saved.GetProperty("view");
        Assert.Equal(0, saved.GetProperty("secretNodes").GetInt32());
        Assert.Equal(0, saved.GetProperty("otherNodes").GetInt32());
        switch (how)
        {
            case "reapply":
                // The next Save is the user's own: the password has to be typed again, then it sends the user's fields and the NEW token.
                Assert.Equal(new[] { PasswordNeeded }, Lines(page.GetProperty("refused").GetProperty("view"), "banner"));
                Assert.Single(Patches(page.GetProperty("refused").GetProperty("view")));
                Assert.Equal(2, Patches(savedView).Length);
                Assert.Equal(
                    """{"host":"charlie-two","display_name":"Charlie Two","password":"OTHER-PW","expected_modified_at":"TOKEN2"}""".Replace("TOKEN2", Token2, StringComparison.Ordinal),
                    Body(Patches(savedView)[1]));
                Assert.StartsWith("Saved \"Charlie Two\".", Notice(savedView), StringComparison.Ordinal);
                Assert.Equal(0, savedView.GetProperty("forms").GetInt32());
                break;
            case "reload":
                // The user's host and name are gone; a new edit goes out against the current values and the NEW token.
                Assert.Equal(2, Patches(savedView).Length);
                Assert.Equal("""{"monthly_cost_usd":5,"expected_modified_at":"TOKEN2"}""".Replace("TOKEN2", Token2, StringComparison.Ordinal), Body(Patches(savedView)[1]));
                break;
            case "reloadnone":
                Assert.Single(Patches(savedView));
                Assert.Equal("No change.", Notice(savedView));
                Assert.Equal(0, savedView.GetProperty("forms").GetInt32());
                break;
            case "again":
                // Save without choosing is the stale save again: the OLD token, so the service says conflict again, and one panel shows, not two.
                Assert.Equal(2, Patches(savedView).Length);
                Assert.Equal(userBody.Replace("READ", ReadToken, StringComparison.Ordinal).Replace("SECRET-PW", "OTHER-PW", StringComparison.Ordinal), Body(Patches(savedView)[1]));
                Assert.Equal(new[] { Stale }, Lines(savedView, "banner"));
                Assert.Equal(3, savedView.GetProperty("conflict").GetProperty("lines").GetArrayLength());
                break;
            default:
                Assert.Equal(2, Patches(savedView).Length);
                Assert.Equal("""{"display_name":"Charlie Two","expected_modified_at":"TOKEN2"}""".Replace("TOKEN2", Token2, StringComparison.Ordinal), Body(Patches(savedView)[1]));
                break;
        }
    }

    // ------------------------------------------------------------------ T6: nothing secret stays in the page

    [Theory]
    [InlineData("cancel", 0, false)]
    [InlineData("saved", 0, false)]
    [InlineData("hash", 0, false)]
    // Review finding 8: the next list read answers 401 and the shell takes the page over; the form is closed, the password cleared.
    [InlineData("listexpired", 0, false)]
    [InlineData("badrequest", 1, true)]
    [InlineData("echo", 1, true)]
    [InlineData("limited", 1, true)]
    [InlineData("conflict", 1, true)]
    // Reapply and Reload draw the form again, so the box typed into is gone and its replacement is empty.
    [InlineData("reapply", 1, false)]
    [InlineData("reload", 1, false)]
    // #5356: a password typed again after the 409 and before Reapply or Reload must not stay in the box the redraw detaches.
    [InlineData("reapplytyped", 1, false)]
    [InlineData("reloadtyped", 1, false)]
    public void NothingSecret_StaysInTheDom_AfterCloseSaveErrorOrLeaving(string path, int forms, bool connected)
    {
        var page = Run("secret:" + path);

        Assert.Equal("SECRET-PW", page.GetProperty("typed").GetString());
        // The very box typed into, read after whatever happened to it, holds nothing, and no text, attribute or value on the page does.
        Assert.Equal("", page.GetProperty("value").GetString());
        Assert.Equal(connected, page.GetProperty("connected").GetBoolean());
        Assert.Equal(0, page.GetProperty("secretNodes").GetInt32());
        Assert.Equal(forms, page.GetProperty("forms").GetInt32());
        if (forms == 1)
        {
            Assert.Equal("", page.GetProperty("nextValue").GetString());
        }

        var view = page.GetProperty("view");
        if (path == "echo")
        {
            Assert.Equal(new[] { "The host 'charlie two' is not valid: [redacted] is not allowed here." }, Lines(view, "banner"));
        }

        if (path == "conflict")
        {
            Assert.Contains("Display Name: was Charlie, now [redacted]", Strings(view.GetProperty("conflict").GetProperty("lines")));
        }
    }

    // ------------------------------------------------------------------ T2, T3 and T13: the flow halves, through the real form

    // The by-id read of one kind of server, as editFormValues turns it into Base(kind).
    private static JsonObject ReadFor(string kind, string token)
    {
        var read = JsonNode.Parse("""{"server_id":1,"display_name":"Orders","engine":"sqlserver","host":"sql-a","port":0,"database":null,"read_only_intent":false,"auth":"Windows","username":null,"encrypt_mode":"Mandatory","trust_server_certificate":false,"multi_subnet_failover":false,"monthly_cost_usd":100}""")!.AsObject();
        switch (kind)
        {
            case "windows":
                break;
            case "sql":
                read["auth"] = "SQL";
                read["username"] = "sa";
                break;
            case "sp":
                read["auth"] = "ServicePrincipal";
                read["username"] = "11111111-1111-1111-1111-111111111111";
                break;
            case "managed":
                read["auth"] = "ManagedIdentity";
                break;
            case "postgres":
                read["engine"] = "postgres";
                read["host"] = "pg-a";
                read["port"] = 5433;
                read["auth"] = "SQL";
                read["username"] = "pgmon";
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }

        read["modified_at"] = token;
        return read;
    }

    // One edit made through the form of that server (the edits typed, ticked or chosen in the order given, the authentication first).
    private static JsonElement Flow(string kind, string edits, string password, string token = Token) =>
        Run("flow", JsonSerializer.Serialize(new { read = ReadFor(kind, token), edits = JsonNode.Parse(edits), password }));

    private static readonly string[] EditableKeys =
        ["host", "display_name", "port", "auth", "username", "encrypt_mode", "trust_server_certificate", "database", "read_only_intent", "multi_subnet_failover", "monthly_cost_usd", "password", "expected_modified_at"];

    [Theory]
    // A name-only edit sends exactly the name and the token; nothing that did not change goes out.
    [InlineData("windows", """{"display_name":"Payments"}""", "", """{"display_name":"Payments","expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{}""", "", "null")]
    [InlineData("windows", """{"host":"  sql-a ","display_name":"Orders  "}""", "", "null")]
    [InlineData("windows", """{"monthly_cost_usd":"250.50"}""", "", """{"monthly_cost_usd":250.5,"expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{"monthly_cost_usd":"100.00"}""", "", "null")]
    [InlineData("windows", """{"encrypt_mode":"Strict"}""", "", """{"encrypt_mode":"Strict","expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{"trust_server_certificate":true,"multi_subnet_failover":true,"read_only_intent":true}""", "", """{"trust_server_certificate":true,"read_only_intent":true,"multi_subnet_failover":true,"expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{"database":"Sales"}""", "", """{"database":"Sales","expected_modified_at":"TOKEN"}""")]
    // SQL Server never sends a port; a password goes only with an auth that stores one.
    [InlineData("sql", """{"host":"sql-b"}""", "typed", """{"host":"sql-b","password":"typed","expected_modified_at":"TOKEN"}""")]
    [InlineData("sql", """{}""", "typed", """{"password":"typed","expected_modified_at":"TOKEN"}""")]
    [InlineData("sp", """{}""", "secret", """{"password":"secret","expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{"host":"sql-b"}""", "typed", """{"host":"sql-b","expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{}""", "typed", "null")]
    [InlineData("managed", """{}""", "typed", "null")]
    // An auth switch always sends the username box of the new mode, and Windows sends none.
    [InlineData("windows", """{"auth":"SQL","username":"sa"}""", "typed", """{"auth":"SQL","username":"sa","password":"typed","expected_modified_at":"TOKEN"}""")]
    [InlineData("windows", """{"auth":"ServicePrincipal","username":"app-id"}""", "secret", """{"auth":"ServicePrincipal","username":"app-id","password":"secret","expected_modified_at":"TOKEN"}""")]
    [InlineData("sql", """{"auth":"ManagedIdentity"}""", "", """{"auth":"ManagedIdentity","username":null,"expected_modified_at":"TOKEN"}""")]
    [InlineData("sql", """{"auth":"Windows"}""", "typed", """{"auth":"Windows","expected_modified_at":"TOKEN"}""")]
    // PostgreSQL never sends auth, and a blank port is sent as 0, the default.
    [InlineData("postgres", """{"display_name":"Payments"}""", "", """{"display_name":"Payments","expected_modified_at":"TOKEN"}""")]
    [InlineData("postgres", """{"port":""}""", "typed", """{"port":0,"password":"typed","expected_modified_at":"TOKEN"}""")]
    [InlineData("postgres", """{"port":"5434"}""", "typed", """{"port":5434,"password":"typed","expected_modified_at":"TOKEN"}""")]
    [InlineData("postgres", """{"host":"pg-b"}""", "typed", """{"host":"pg-b","password":"typed","expected_modified_at":"TOKEN"}""")]
    public void TheEditBody_CarriesOnlyChangedEditableFields_AndTheToken(string kind, string edits, string password, string expected)
    {
        var after = Flow(kind, edits, password).GetProperty("after");
        var patches = Patches(after);

        if (expected == "null")
        {
            Assert.Empty(patches);
            Assert.Equal("No change.", Notice(after));
            return;
        }

        var body = Assert.Single(patches).GetProperty("body");
        Assert.Equal(Expand(expected), body.GetRawText());
        // No PATCH body of any case has a key that is not editable (never server_id, engine, is_enabled, the secret store ...), the token is last,
        // a SQL Server body has no port and a PostgreSQL body no auth.
        var keys = body.EnumerateObject().Select(p => p.Name).ToArray();
        Assert.All(keys, key => Assert.Contains(key, EditableKeys));
        Assert.Equal("expected_modified_at", keys.Last());
        Assert.False(kind == "postgres" && keys.Contains("auth"));
        Assert.False(kind != "postgres" && keys.Contains("port"));
    }

    [Theory]
    // Required and blank: no request, and the service's own sentence in the banner.
    [InlineData("sql", """{"host":"sql-b"}""", "", true, PasswordNeeded)]
    [InlineData("sp", """{"username":"22222222-2222-2222-2222-222222222222"}""", "", true, PasswordNeeded)]
    [InlineData("postgres", """{"host":"pg-b"}""", "", true, PasswordNeeded)]
    [InlineData("windows", """{"auth":"SQL","username":"sa"}""", "", true, "Switching to SQL authentication needs the password.")]
    // Not required: name or cost only, and Windows or managed identity (a typed password is not even sent).
    [InlineData("windows", """{"display_name":"Payments"}""", "", false, null)]
    [InlineData("sql", """{"monthly_cost_usd":"250"}""", "", false, null)]
    [InlineData("windows", """{"host":"sql-b"}""", "typed", false, null)]
    [InlineData("managed", """{"host":"sql-b"}""", "typed", false, null)]
    public void ThePassword_IsRequiredExactlyWhenTheRulesSay(string kind, string edits, string password, bool required, string? sentence)
    {
        var page = Flow(kind, edits, password);
        var after = page.GetProperty("after");

        Assert.EndsWith(required ? "(required for this change)" : "(leave blank to keep the stored one)", page.GetProperty("label").GetString(), StringComparison.Ordinal);
        if (sentence is null)
        {
            var body = Assert.Single(Patches(after)).GetProperty("body");
            Assert.False(body.TryGetProperty("password", out _));
            Assert.Empty(Lines(after, "banner"));
        }
        else
        {
            Assert.Empty(Patches(after));
            Assert.Equal(new[] { sentence }, Lines(after, "banner"));
            Assert.Equal(1, after.GetProperty("forms").GetInt32());
        }
    }

    [Theory]
    // The token goes out exactly as the read gave it: microseconds, trailing zeros, a zone offset, no fraction, odd spacing.
    [InlineData(Token)]
    [InlineData("2026-01-02T03:04:05.1230000Z")]
    [InlineData("2026-01-02T03:04:05.0000001+00:00")]
    [InlineData("2026-01-02T03:04:05Z")]
    [InlineData(" as-is ")]
    public void TheTokenRoundTripsVerbatim_AndInterpretEditBranchesOnStatusWords(string token)
    {
        var body = Assert.Single(Patches(Flow("windows", """{"display_name":"Payments","monthly_cost_usd":"7"}""", "", token).GetProperty("after"))).GetProperty("body");

        Assert.Equal(token, body.GetProperty("expected_modified_at").GetString());
        Assert.Equal("expected_modified_at", body.EnumerateObject().Last().Name);
    }

    [Fact]
    public void TheAnswerIsReadByItsStatusWord_NotByItsMessage()
    {
        // A collision whose sentence talks about a change is no conflict: its own sentence in the banner, no panel.
        var collides = Run("save:collidesTalk").GetProperty("after");
        Assert.Equal(new[] { "This server was changed since you opened it: another monitored server already uses the address." }, Lines(collides, "banner"));
        Assert.Equal(JsonValueKind.Null, collides.GetProperty("conflict").ValueKind);

        // A conflict whose sentence talks about a collision still shows the page's own sentence and the panel.
        var conflict = Run("save:conflictTalk").GetProperty("after");
        Assert.Equal(new[] { Stale }, Lines(conflict, "banner"));
        Assert.Equal(new[] { "Server Name / Address: was charlie, now charlie-new (you entered alpha)" }, Strings(conflict.GetProperty("conflict").GetProperty("lines")));
    }

    // ------------------------------------------------------------------ the password-manager attributes

    [Fact]
    public void TheUsernameAndPasswordBoxes_AskPasswordManagersToLeaveThemAlone_AndSitInNoFormElement()
    {
        var page = Run("inputs");

        foreach (var engine in new[] { "sql", "postgres" })
        {
            var form = page.GetProperty(engine);
            Assert.Equal(0, form.GetProperty("formElements").GetInt32());
            foreach (var box in new[] { "username", "password" })
            {
                var input = form.GetProperty(box);
                var attrs = input.GetProperty("attrs");
                Assert.False(input.GetProperty("inForm").GetBoolean());
                // No name and no id: only data-field says what the box is, so a browser has no sign-in field to take it for.
                Assert.False(attrs.TryGetProperty("name", out _), engine + " " + box + " has a name");
                Assert.False(attrs.TryGetProperty("id", out _), engine + " " + box + " has an id");
                Assert.Equal(box, attrs.GetProperty("data-field").GetString());
                Assert.Equal("", attrs.GetProperty("data-1p-ignore").GetString());
                Assert.Equal("true", attrs.GetProperty("data-lpignore").GetString());
                Assert.Equal("", attrs.GetProperty("data-bwignore").GetString());
                Assert.Equal("other", attrs.GetProperty("data-form-type").GetString());
                Assert.Equal(box == "password" ? "new-password" : "off", attrs.GetProperty("autocomplete").GetString());
            }

            Assert.Equal("password", form.GetProperty("password").GetProperty("type").GetString());
        }
    }
}
