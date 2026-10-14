/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.Frozen;
using System.Text;

namespace PerformanceMonitor.Common;

/// <summary>What a field holds, as far as <c>mcp.dataDetail</c> is concerned (#5632). Each class is one row of the
/// field table in <see cref="McpDataDetailKeys"/>.</summary>
public enum McpDataDetailClass
{
    /// <summary>Object and database names, counts, durations, waits, times, program names, and every field the table
    /// does not list: kept at every level.</summary>
    Kept = 0,

    /// <summary>Statement text, mute-rule patterns, error text, and the statement text inside deadlock and
    /// blocked-process input buffers: literals replaced under <c>maskLiterals</c>, withheld under
    /// <c>metadataOnly</c>.</summary>
    Statement = 1,

    /// <summary>Parameter values: replaced under <c>maskLiterals</c>, withheld under <c>metadataOnly</c>.</summary>
    ParameterValue = 2,

    /// <summary>Logins: kept under <c>maskLiterals</c>, masked under <c>metadataOnly</c>.</summary>
    Login = 3,

    /// <summary>Client host names and addresses: kept under <c>maskLiterals</c>, masked under
    /// <c>metadataOnly</c>.</summary>
    Host = 4,
}

/// <summary>
/// The field table of <c>mcp.dataDetail</c> (#5632): the JSON keys, alert context labels (<c>Query</c>,
/// <c>Victim SQL</c>, from <c>AlertContextBuilders</c>), XML attribute names and XML element names whose values are
/// not kept at every level, each with its class.
/// <list type="table">
/// <listheader><term>Class</term><description>maskLiterals / metadataOnly</description></listheader>
/// <item><term>statement text, mute-rule patterns, error text</term><description>literals replaced / withheld</description></item>
/// <item><term>plan XML (<c>ConstValue</c>, <c>ParameterCompiledValue</c>, <c>ParameterRuntimeValue</c>,
/// <c>ScalarString</c>)</term><description>values replaced / withheld (by shape: any value that is a showplan
/// document; the attribute list is <c>SensitiveStatements.IsPlanValueAttribute</c>)</description></item>
/// <item><term>deadlock and blocked-process <c>inputbuf</c></term><description>literals replaced / withheld (the
/// <c>inputbuf</c> and <c>frame</c> elements below)</description></item>
/// <item><term>parameter values</term><description>replaced / withheld</description></item>
/// <item><term>logins, host names</term><description>kept / masked</description></item>
/// <item><term>object and database names, counts, durations, waits, times, program names</term><description>kept /
/// kept (not listed here)</description></item>
/// </list>
/// A name is matched after <see cref="Normalize"/>: lower case, with <c>_</c>, <c>-</c>, <c>.</c> and blanks removed,
/// so <c>query_text</c>, <c>QueryText</c> and the label <c>Query Text</c> are one entry. The keyed census in the six
/// statement-filter live classes fails when a key that carries a planted literal, login or host is missing here.
/// </summary>
public static class McpDataDetailKeys
{
    private static readonly (McpDataDetailClass Class, string[] Names)[] s_table =
    {
        (McpDataDetailClass.Statement, new[]
        {
            // Statement text, under the names the readers and their row types use.
            "query_text", "sql_text", "statement_text", "statement", "query", "text", "batch_text", "command_text",
            "full_query_text", "sample_query_text", "sample_text", "sql_normalized", "normalized_text",
            "parameterized_text", "query_sql_text", "stmt_text", "statement_sql_text", "module_definition",
            "victim_statement", "victim_sql_text", "victim_query",
            "blocked_sql_text", "blocking_sql_text", "blocked_query", "blocking_query", "bad_actor_query",
            "input_buffer", "inputbuf", "frame", "most_recent_sql_text", "last_sql_text", "current_query",
            // The alert context labels that hold a statement (AlertContextBuilders) and the flattened detail text
            // that repeats them.
            "Victim SQL", "Blocking Query", "Blocked Query", "detail_text", "body",
            // Mute-rule patterns.
            "query_text_pattern", "query_pattern", "text_pattern", "pattern", "sql_text_pattern",
            // Error text.
            "error_message", "message_text", "last_error", "last_error_truncated", "last_message", "error",
            "errors",
        }),
        (McpDataDetailClass.ParameterValue, new[]
        {
            "parameter_values", "parameter_value", "param_value", "param_values", "parameters", "compiled_value",
            "runtime_value", "parameter_compiled_value", "parameter_runtime_value", "sniffed_value",
            "ParameterCompiledValue", "ParameterRuntimeValue", "ConstValue", "ScalarString",
        }),
        (McpDataDetailClass.Login, new[]
        {
            "login_name", "login", "loginname", "original_login_name", "nt_user_name", "nt_username", "user_name",
            "username", "usename", "blocked_login_name", "blocking_login_name", "victim_login_name", "ownerloginname",
            "owner_login_name", "session_login_name", "remediation_username",
        }),
        (McpDataDetailClass.Host, new[]
        {
            "host_name", "hostname", "client_hostname", "client_host_name", "client_host", "client_addr",
            "client_address", "client_net_address", "client_ip", "ip_address", "workstation", "workstation_id",
            "blocked_host_name", "blocking_host_name", "victim_host_name",
        }),
    };

    private static readonly FrozenDictionary<string, McpDataDetailClass> s_byName = Build();

    /// <summary>Every listed name, normalized, with its class: the table a census reads.</summary>
    public static IReadOnlyDictionary<string, McpDataDetailClass> Table => s_byName;

    /// <summary>The class of the field named <paramref name="name"/> (a JSON key, a context label, an XML attribute
    /// or element name); <see cref="McpDataDetailClass.Kept"/> for a name the table does not list.</summary>
    public static McpDataDetailClass Classify(string? name) =>
        name is { Length: > 0 } && s_byName.TryGetValue(Normalize(name), out var cls) ? cls : McpDataDetailClass.Kept;

    /// <summary>Lower case with <c>_</c>, <c>-</c>, <c>.</c> and white space removed.</summary>
    public static string Normalize(string name)
    {
        var sb = new StringBuilder(name.Length);
        foreach (char c in name)
        {
            if (c == '_' || c == '-' || c == '.' || char.IsWhiteSpace(c)) continue;
            sb.Append(char.ToLowerInvariant(c));
        }

        return sb.ToString();
    }

    private static FrozenDictionary<string, McpDataDetailClass> Build()
    {
        var map = new Dictionary<string, McpDataDetailClass>(StringComparer.Ordinal);
        foreach (var (cls, names) in s_table)
        {
            foreach (string name in names)
            {
                string key = Normalize(name);
                if (map.TryGetValue(key, out var existing) && existing != cls)
                {
                    throw new InvalidOperationException($"mcp.dataDetail field table lists '{name}' under two classes.");
                }

                map[key] = cls;
            }
        }

        return map.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
