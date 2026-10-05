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

    private static JsonElement Run(string scenario, string? stdin = null)
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

    private static readonly (string Name, int Status, string? Body, string? Message, string Expected)[] Answers =
    [
        ("no answer at all shows the transport's message", 0, null, "Network error: connection refused",
            """{"kind":"network","close":false,"reread":false,"banner":"Network error: connection refused"}"""),
        ("a 401 is an expired session", 401, """{"error":"sign in"}""", null, """{"kind":"expired","close":true,"reread":false}"""),
        ("a 2xx that is not a JSON object is the sign-in page of an expired session", 200, null, null, """{"kind":"expired","close":true,"reread":false}"""),
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
        Assert.Equal(new[] { "GET /api/admin/servers" }, page.GetProperty("requests").EnumerateArray().Select(r => r.GetString()).ToArray());
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

        Assert.Contains("copyTree(jsDir, scratch)", source, StringComparison.Ordinal);
        Assert.Contains("withFileTypes: true", source, StringComparison.Ordinal);
        Assert.DoesNotContain("\"util.js\", \"panels.js\"", source, StringComparison.Ordinal);
        Assert.Contains("process.env.TZ = \"UTC\"", source, StringComparison.Ordinal);
    }
}
