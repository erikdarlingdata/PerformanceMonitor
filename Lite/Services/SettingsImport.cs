/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Linq;
using System.Text.Json;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What Import Settings copies from a previous install's folder into this one, as named lists so a test can read
/// them. The list is short on purpose and <see cref="InstallIdStore.FileName"/> is never on it (#4961): the
/// previous install may still run, and two installs that shared an id would share Extended Events session names
/// and drop each other's sessions. Servers and credential profiles are not in these lists; they are imported
/// through <see cref="ServerManager"/> and <see cref="ProfileManager"/>.
///
/// <para>The MCP settings that loosen who can use the MCP server are never brought in from another folder: the imported
/// <c>settings.json</c> loses them, so this user's value (off unless this user set it) stays. A folder the import is pointed
/// at can be written by people other than this user, and a copied <c>settings.json</c> should not turn the MCP token check off or
/// the write tools on.</para>
/// </summary>
internal static class SettingsImport
{
    /// <summary>Files copied from the previous install's <c>config</c> folder into this install's.</summary>
    internal static readonly string[] ConfigFiles = ["settings.json", "collection_schedule.json", "ignored_wait_types.json"];

    /// <summary>Files copied from the previous install's data root into this install's.</summary>
    internal static readonly string[] RootFiles = ["alert_state.json"];

    /// <summary>The <c>settings.json</c> keys that loosen the MCP server's checks. They are dropped from an imported file.</summary>
    internal static readonly string[] McpGateKeys = ["mcp_allow_tokenless_loopback", "mcp_allow_writes", "mcp_allow_investigations"];

    private const string SettingsFileName = "settings.json";

    /// <summary>
    /// Copies each listed file the previous install has and this one does not, and returns how many it copied. A
    /// file this install already has is kept as it is.
    /// </summary>
    internal static int CopyMissing(string oldConfigDir, string oldRootDir, string newConfigDir, string newRootDir) =>
        CopyEach(ConfigFiles, oldConfigDir, newConfigDir) + CopyEach(RootFiles, oldRootDir, newRootDir);

    private static int CopyEach(string[] fileNames, string fromDir, string toDir)
    {
        var copied = 0;
        foreach (var fileName in fileNames)
        {
            var source = Path.Combine(fromDir, fileName);
            var target = Path.Combine(toDir, fileName);

            if (File.Exists(source) && !File.Exists(target))
            {
                if (fileName == SettingsFileName)
                {
                    File.WriteAllBytes(target, WithoutMcpGateKeys(File.ReadAllBytes(source)));
                }
                else
                {
                    File.Copy(source, target);
                }

                copied++;
            }
        }

        return copied;
    }

    /// <summary>
    /// The settings file's bytes without the keys in <see cref="McpGateKeys"/>. A file that is not a JSON object is returned as it
    /// is: the MCP settings reader refuses it the same way, so it turns nothing on.
    /// </summary>
    internal static byte[] WithoutMcpGateKeys(byte[] settingsJson)
    {
        try
        {
            using var document = JsonDocument.Parse(settingsJson);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object || !root.EnumerateObject().Any(p => McpGateKeys.Contains(p.Name)))
            {
                return settingsJson;
            }

            using var stream = new MemoryStream();
            using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
            {
                writer.WriteStartObject();
                foreach (var property in root.EnumerateObject().Where(p => !McpGateKeys.Contains(p.Name)))
                {
                    property.WriteTo(writer);
                }

                writer.WriteEndObject();
            }

            AppLogger.Info("Import", "Import Settings left out the MCP settings that loosen who can use the MCP server ("
                + string.Join(", ", McpGateKeys) + "); they keep this user's values.");
            return stream.ToArray();
        }
        catch (JsonException)
        {
            return settingsJson;
        }
    }
}
