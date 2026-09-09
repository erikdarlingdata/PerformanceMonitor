/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net.Http;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;

namespace PerformanceMonitor.Common;

public record UpdateInfo(
    bool IsUpdateAvailable,
    string CurrentVersion,
    string LatestVersion,
    string ReleaseUrl,
    string ReleaseNotes);

public static class UpdateCheckService
{
    private const string ReleasesApiUrl =
        "https://api.github.com/repos/erikdarlingdata/PerformanceMonitor/releases/latest";

    private static readonly HttpClient HttpClient = CreateHttpClient();
    private static UpdateInfo? _cachedResult;
    private static DateTime _cacheExpiry = DateTime.MinValue;

    private static HttpClient CreateHttpClient()
    {
        var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        client.DefaultRequestHeaders.Add("User-Agent", "PerformanceMonitor");
        client.DefaultRequestHeaders.Add("Accept", "application/vnd.github.v3+json");
        return client;
    }

    public static async Task<UpdateInfo?> CheckForUpdateAsync(bool bypassCache = false)
    {
        try
        {
            if (!bypassCache && _cachedResult != null && DateTime.UtcNow < _cacheExpiry)
                return _cachedResult;

            var response = await HttpClient.GetAsync(ReleasesApiUrl).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
                return null;

            var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var tagName = root.GetProperty("tag_name").GetString() ?? "";
            var releaseUrl = root.GetProperty("html_url").GetString() ?? "";
            var releaseNotes = root.TryGetProperty("body", out var bodyProp)
                ? bodyProp.GetString() ?? ""
                : "";

            var currentVersion = GetCurrentVersion();
            var latestVersion = ParseVersion(tagName);
            var isUpdateAvailable = latestVersion != null
                && currentVersion != null
                && latestVersion > currentVersion;

            var result = new UpdateInfo(
                isUpdateAvailable,
                FormatVersion(currentVersion),
                tagName,
                releaseUrl,
                releaseNotes);

            _cachedResult = result;
            _cacheExpiry = DateTime.UtcNow.AddHours(24);

            return result;
        }
        catch
        {
            return null;
        }
    }

    private static Version? GetCurrentVersion()
    {
        // Use the entry assembly (the app .exe) rather than the executing assembly.
        // The comparison is against the latest release tag, so the version that answers
        // it is the one the user is RUNNING - the host app's. This service lives in
        // PerformanceMonitor.Common.dll, which inherits the same <Version> from
        // Directory.Build.props (#3222) and so agrees today; it agrees by inheritance
        // rather than by definition, and a library whose version diverged would make
        // every real release look like an available update forever.
        return Assembly.GetEntryAssembly()?.GetName().Version;
    }

    private static Version? ParseVersion(string tagName)
    {
        var versionString = tagName.TrimStart('v', 'V');
        return Version.TryParse(versionString, out var version) ? version : null;
    }

    private static string FormatVersion(Version? version)
    {
        if (version == null) return "unknown";
        return $"{version.Major}.{version.Minor}.{version.Build}";
    }
}
