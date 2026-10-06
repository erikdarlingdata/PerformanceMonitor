/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Keeps the fields a saved password is bound to in step with everything else that names the same set (#5366): the
/// <see cref="ServerConnectionIdentity"/> properties, the bound field list, the settings the connection builder reads,
/// and the columns the store's edit function compares. Also pins the identity's comparison rule to the one the edit
/// and connection-test paths use today, and the byte layout of the associated data.
/// </summary>
public sealed class PasswordBindingCensusTests
{
    private static readonly ServerConnectionIdentity Base =
        new("example-sql-01", 1433, "sqlserver", "example_db", false, "sql", "example_login", "Mandatory", false, false);

    private static string[] IdentityProperties() =>
        typeof(ServerConnectionIdentity).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();

    private static string Snake(string pascal) =>
        Regex.Replace(pascal, "(?<!^)([A-Z])", "_$1").ToLowerInvariant();

    private static string Source(string relativeToDarling, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", relativeToDarling));

    private static Dictionary<string, ServerConnectionIdentity> OneFieldChanged() => new()
    {
        [nameof(ServerConnectionIdentity.Host)] = Base with { Host = "example-sql-02" },
        [nameof(ServerConnectionIdentity.Port)] = Base with { Port = 1434 },
        [nameof(ServerConnectionIdentity.Engine)] = Base with { Engine = "postgres" },
        [nameof(ServerConnectionIdentity.Database)] = Base with { Database = "other_db" },
        [nameof(ServerConnectionIdentity.ReadOnlyIntent)] = Base with { ReadOnlyIntent = true },
        [nameof(ServerConnectionIdentity.Auth)] = Base with { Auth = "serviceprincipal" },
        [nameof(ServerConnectionIdentity.Username)] = Base with { Username = "other_login" },
        [nameof(ServerConnectionIdentity.EncryptMode)] = Base with { EncryptMode = "Optional" },
        [nameof(ServerConnectionIdentity.TrustServerCertificate)] = Base with { TrustServerCertificate = true },
        [nameof(ServerConnectionIdentity.MultiSubnetFailover)] = Base with { MultiSubnetFailover = true },
    };

    [Fact]
    public void The_server_binding_names_exactly_the_identity_properties()
    {
        var names = PasswordBinding.ServerFieldNames.OrderBy(n => n, StringComparer.Ordinal).ToArray();

        Assert.Equal(IdentityProperties(), names);
        Assert.Equal(names.Length, PasswordBinding.ForServer(Base).Fields.Count);
        Assert.Equal(IdentityProperties(), OneFieldChanged().Keys.OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void Changing_any_identity_property_changes_exactly_the_field_the_binding_names_for_it()
    {
        var baseline = PasswordBinding.ForServer(Base).Fields;

        foreach (var (property, changed) in OneFieldChanged())
        {
            var fields = PasswordBinding.ForServer(changed).Fields;
            var differing = Enumerable.Range(0, fields.Count).Where(i => fields[i] != baseline[i]).ToArray();

            Assert.True(differing.Length == 1, $"{property} changed {differing.Length} bound fields");
            Assert.Equal(property, PasswordBinding.ServerFieldNames[differing[0]]);
        }
    }

    [Fact]
    public void The_stored_column_list_and_the_row_mapping_cover_the_identity_properties()
    {
        var columns = ServerConnectionIdentity.StoredColumns.Split(',', StringSplitOptions.TrimEntries)
            .OrderBy(c => c, StringComparer.Ordinal).ToArray();
        var expected = IdentityProperties().Select(Snake).OrderBy(c => c, StringComparer.Ordinal).ToArray();

        Assert.Equal(expected, columns);
        Assert.Equal(
            IdentityProperties().Length,
            typeof(ServerConnectionIdentity).GetMethod(nameof(ServerConnectionIdentity.FromStoredColumns))!.GetParameters().Length);
    }

    [Fact]
    public void A_raw_row_maps_with_nulls_as_empty_text_and_zero_and_no_trimming_or_case_change()
    {
        var empty = ServerConnectionIdentity.FromStoredColumns(null, null, null, null, false, null, null, null, false, false);
        var row = ServerConnectionIdentity.FromStoredColumns(
            " Example-SQL-01 ", 1433, "SqlServer", "Example_Db", true, "SQL", "Example_Login", "Mandatory", true, true);

        Assert.Equal(new ServerConnectionIdentity("", 0, "", "", false, "", "", "", false, false), empty);
        Assert.Equal(
            new ServerConnectionIdentity(
                " Example-SQL-01 ", 1433, "SqlServer", "Example_Db", true, "SQL", "Example_Login", "Mandatory", true, true),
            row);
    }

    [Fact]
    public void Every_server_setting_the_connection_builder_reads_is_bound_or_on_the_allowlist()
    {
        var allowed = new HashSet<string>(StringComparer.Ordinal)
        {
            "DisplayName", "UsesSqlAuth", "UsesServicePrincipal", "UsesManagedIdentity", "IsPostgres",
            "HasRemediationCredential", "RemediationUsername",
        };
        var bound = IdentityProperties().ToHashSet(StringComparer.Ordinal);
        var read = Regex.Matches(Source("PerformanceMonitor.Darling.Service/MonitoredServerConnection.cs"), @"\bserver\.([A-Za-z_]\w*)")
            .Select(m => m.Groups[1].Value).Distinct(StringComparer.Ordinal).ToArray();

        Assert.NotEmpty(read);
        var unbound = read.Where(r => !bound.Contains(r) && !allowed.Contains(r)).ToArray();
        Assert.True(
            unbound.Length == 0,
            "MonitoredServerConnection reads a server setting that no password is bound to: " + string.Join(", ", unbound)
            + ". Bind it in PasswordBinding (and ServerConnectionIdentity), or add it to the allowlist here with the reason.");
    }

    private static string[] UnboundReads(string source, ISet<string> bound, ISet<string> allowed) =>
        Regex.Matches(source, @"\bserver\.([A-Za-z_]\w*)")
            .Select(m => m.Groups[1].Value).Distinct(StringComparer.Ordinal)
            .Where(r => !bound.Contains(r) && !allowed.Contains(r)).ToArray();

    [Fact]
    public void Every_server_setting_the_remediation_builder_reads_is_bound_for_remediation_or_on_the_allowlist()
    {
        var allowed = new HashSet<string>(StringComparer.Ordinal) { "DisplayName", "IsPostgres", "HasRemediationCredential" };
        var bound = PasswordBinding.RemediationFieldNames.ToHashSet(StringComparer.Ordinal);
        var source = Source("PerformanceMonitor.Darling.Service/MonitoredServerConnection.cs");
        var start = source.IndexOf("public static string BuildRemediationConnectionString(", StringComparison.Ordinal);
        var end = source.IndexOf("public const string RemediationApplicationName", StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "the remediation connection builder moved");
        var body = source[start..end];

        Assert.Equal(PasswordBinding.ForRemediation(Base, "example_fix").Fields.Count, bound.Count);
        Assert.Contains("server.Host", body, StringComparison.Ordinal);
        var unbound = UnboundReads(body, bound, allowed);
        Assert.True(
            unbound.Length == 0,
            "The remediation connection builder reads a server setting that the remediation password is not bound to: "
            + string.Join(", ", unbound) + ". Bind it in PasswordBinding.ForRemediation, or add it to the allowlist here with the reason.");
    }

    [Fact]
    public void The_remediation_check_flags_a_read_of_a_setting_that_is_bound_for_servers_but_not_for_remediation()
    {
        var allowed = new HashSet<string>(StringComparer.Ordinal) { "DisplayName", "IsPostgres", "HasRemediationCredential" };
        var bound = PasswordBinding.RemediationFieldNames.ToHashSet(StringComparer.Ordinal);

        Assert.Equal(new[] { "Port" }, UnboundReads("var x = server.Host + server.Port + server.DisplayName;", bound, allowed));
        Assert.Equal(new[] { "ReadOnlyIntent" }, UnboundReads("var x = server.ReadOnlyIntent;", bound, allowed));
        Assert.Empty(UnboundReads("var x = server.Host + server.RemediationUsername + server.Engine;", bound, allowed));
    }

    [Fact]
    public void The_columns_the_edit_function_compares_plus_engine_are_the_identity_properties()
    {
        var sql = Source("tools/provision-roles.sql");
        var marker = sql.IndexOf("'password_needed'", StringComparison.Ordinal);
        Assert.True(marker > 0, "the edit function no longer answers password_needed");
        var start = sql.LastIndexOf("v_connection_changed :=", marker, StringComparison.Ordinal);
        Assert.True(start > 0);
        var block = sql[start..marker];

        var columns = Regex.Matches(block, @"v_old_([a-z_]+)").Select(m => m.Groups[1].Value)
            .Append("engine").Distinct(StringComparer.Ordinal).OrderBy(c => c, StringComparer.Ordinal).ToArray();
        var expected = IdentityProperties().Select(Snake).OrderBy(c => c, StringComparer.Ordinal).ToArray();

        Assert.Equal(expected, columns);
    }

    [Fact]
    public void Identity_Differ_compares_each_field_by_its_rule()
    {
        Assert.False(ServerConnectionIdentity.Differ(Base, Base));
        foreach (var (field, changed) in OneFieldChanged())
        {
            Assert.True(ServerConnectionIdentity.Differ(Base, changed), field + " does not make two connections differ");
            Assert.True(ServerConnectionIdentity.Differ(changed, Base), field + " is not symmetric");
        }

        /* Host, username and database are ordinal. */
        Assert.True(ServerConnectionIdentity.Differ(Base, Base with { Host = "EXAMPLE-SQL-01" }));
        Assert.True(ServerConnectionIdentity.Differ(Base, Base with { Host = "example-sql-01 " }));
        Assert.True(ServerConnectionIdentity.Differ(Base, Base with { Username = "EXAMPLE_LOGIN" }));
        Assert.True(ServerConnectionIdentity.Differ(Base, Base with { Database = "EXAMPLE_DB" }));
        Assert.True(ServerConnectionIdentity.Differ(Base, Base with { Database = null }));

        /* Authentication, encrypt mode and engine ignore case. */
        Assert.False(ServerConnectionIdentity.Differ(Base, Base with { Auth = "SQL", EncryptMode = "MANDATORY", Engine = "SQLSERVER" }));
    }

    /// <summary>
    /// "Two connections differ" is <see cref="ServerConnectionIdentity.Differ"/> and nothing else (#5366): the edit core,
    /// the connection test, the file matching and the Viewer all use it. This reads every source file under Darling
    /// (tests and build output left out) and finds no other comparison of the trust or multi-subnet flags, and none of the
    /// deleted types or helper names. The worker's own reconnect check compares the whole definition, secrets and display
    /// name included, so it is the one allowed exception.
    /// </summary>
    [Fact]
    public void No_other_connection_predicate_remains_under_Darling()
    {
        var darling = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(ThisFile())!, ".."));
        var allowed = new HashSet<string>(StringComparer.Ordinal) { "ServerConnectionIdentity.cs", "DarlingWorker.cs" };
        var flagCompare = new Regex(@"\b(TrustServerCertificate|MultiSubnetFailover)\s*(!=|==)", RegexOptions.CultureInvariant);
        var offenders = new List<string>();
        foreach (var file in Directory.EnumerateFiles(darling, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(darling, file).Replace('\\', '/');
            if (relative.StartsWith("Darling.Tests/", StringComparison.Ordinal)
                || relative.Contains("/obj/", StringComparison.Ordinal) || relative.Contains("/bin/", StringComparison.Ordinal))
            {
                continue;
            }

            var text = File.ReadAllText(file);
            if (text.Contains("ConnectionSettingsDiffer", StringComparison.Ordinal)
                || Regex.IsMatch(text, @"\bServerConnectionSettings\b")
                || (!allowed.Contains(Path.GetFileName(file)) && flagCompare.IsMatch(text)))
            {
                offenders.Add(relative);
            }
        }

        Assert.True(
            offenders.Count == 0,
            "Another connection comparison exists besides ServerConnectionIdentity.Differ: " + string.Join(", ", offenders));
    }

    private static string ThisFile([CallerFilePath] string thisFile = "") => thisFile;

    [Fact]
    public void The_associated_data_is_the_label_key_id_purpose_and_fields_each_with_a_four_byte_length()
    {
        var binding = PasswordBinding.ForSmtp("omega-01", 587, true, null);
        var expected = new List<byte>();
        foreach (var part in new[] { "PerformanceMonitor.Darling.sealed.v1", "0123456789abcdef", "smtp", "omega-01", "587", "1", "" })
        {
            var bytes = Encoding.UTF8.GetBytes(part);
            expected.AddRange(new byte[] { 0, 0, 0, (byte)bytes.Length });
            expected.AddRange(bytes);
        }

        Assert.Equal(expected.ToArray(), binding.EncodeAad("0123456789abcdef"));
        Assert.Equal(
            PasswordBinding.ForSmtp("omega-01", 587, true, "").EncodeAad("0123456789abcdef"),
            binding.EncodeAad("0123456789abcdef"));
        Assert.Equal(SHA256.HashData(binding.EncodeAad("")), binding.LegacyPinHash());
        Assert.NotEqual(binding.EncodeAad("0123456789abcdef"), binding.EncodeAad("fedcba9876543210"));
    }

    [Fact]
    public void Each_purpose_binds_its_fixed_number_of_fields_in_the_documented_text_form()
    {
        var server = PasswordBinding.ForServer(Base with { Auth = "SQL", EncryptMode = "Mandatory", Engine = "SqlServer", ReadOnlyIntent = true });
        var remediation = PasswordBinding.ForRemediation(Base with { Engine = "SqlServer" }, "example_fix");
        var smtp = PasswordBinding.ForSmtp("omega-01", 587, false, "example_mail_user");

        Assert.Equal("server", server.Purpose);
        Assert.Equal(
            new string?[] { "example-sql-01", "1433", "example_db", "1", "sql", "example_login", "mandatory", "0", "0", "sqlserver" },
            server.Fields.ToArray());
        Assert.Equal("remediation", remediation.Purpose);
        Assert.Equal(
            new string?[] { "example-sql-01", "example_db", "mandatory", "0", "0", "example_fix", "sqlserver" },
            remediation.Fields.ToArray());
        Assert.Equal("smtp", smtp.Purpose);
        Assert.Equal(new string?[] { "omega-01", "587", "0", "example_mail_user" }, smtp.Fields.ToArray());
    }
}
