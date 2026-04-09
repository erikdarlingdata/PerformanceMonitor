/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Diagnostics;
using Installer.Core.Models;
using Microsoft.Data.SqlClient;

namespace Installer.Core;

/// <summary>
/// Installs community dependencies (sp_WhoIsActive, DarlingData, First Responder Kit)
/// from a local community/ directory or GitHub. Local files are checked first — if
/// present, the network is not used. This supports air-gapped installations.
/// </summary>
public sealed class DependencyInstaller : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string? _communityDirectory;
    private bool _disposed;

    /// <param name="communityDirectory">
    /// Optional path to a community/ directory containing pre-downloaded SQL files.
    /// When provided and files exist, they are used instead of downloading from GitHub.
    /// </param>
    public DependencyInstaller(string? communityDirectory = null)
    {
        _httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(30)
        };
        _communityDirectory = communityDirectory;
    }

    /// <summary>
    /// Install community dependencies into the PerformanceMonitor database.
    /// Checks the community/ directory first, falls back to GitHub download.
    /// Returns the number of successfully installed dependencies.
    /// </summary>
    public async Task<int> InstallDependenciesAsync(
        string connectionString,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var dependencies = new List<(string Name, string Url, string LocalFile, string Description)>
        {
            (
                "sp_WhoIsActive",
                "https://raw.githubusercontent.com/amachanic/sp_whoisactive/refs/heads/master/sp_WhoIsActive.sql",
                "sp_WhoIsActive.sql",
                "Query activity monitoring by Adam Machanic (GPLv3)"
            ),
            (
                "DarlingData",
                "https://raw.githubusercontent.com/erikdarlingdata/DarlingData/main/Install-All/DarlingData.sql",
                "DarlingData.sql",
                "sp_HealthParser, sp_HumanEventsBlockViewer by Erik Darling (MIT)"
            ),
            (
                "First Responder Kit",
                "https://raw.githubusercontent.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/refs/heads/main/Install-All-Scripts.sql",
                "Install-All-Scripts.sql",
                "sp_BlitzLock and diagnostic tools by Brent Ozar Unlimited (MIT)"
            )
        };

        progress?.Report(new InstallationProgress
        {
            Message = "Installing community dependencies...",
            Status = "Info"
        });

        int successCount = 0;

        foreach (var (name, url, localFile, description) in dependencies)
        {
            cancellationToken.ThrowIfCancellationRequested();

            progress?.Report(new InstallationProgress
            {
                Message = $"Installing {name}...",
                Status = "Info"
            });

            try
            {
                var depSw = Stopwatch.StartNew();
                string sql;

                /* Check community/ directory first */
                string? localPath = ResolveLocalFile(localFile);
                if (localPath != null)
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"[DEBUG] {name}: loading from {localPath}",
                        Status = "Debug"
                    });
                    sql = await File.ReadAllTextAsync(localPath, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"[DEBUG] Downloading {name} from {url}",
                        Status = "Debug"
                    });
                    sql = await DownloadWithRetryAsync(url, progress, cancellationToken: cancellationToken).ConfigureAwait(false);
                }

                progress?.Report(new InstallationProgress
                {
                    Message = $"[DEBUG] {name}: {(localPath != null ? "loaded" : "downloaded")} {sql.Length} chars in {depSw.ElapsedMilliseconds}ms",
                    Status = "Debug"
                });

                if (string.IsNullOrWhiteSpace(sql))
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"{name} - FAILED (empty {(localPath != null ? "file" : "response")})",
                        Status = "Error"
                    });
                    continue;
                }

                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var useDbCommand = new SqlCommand("USE PerformanceMonitor;", connection))
                {
                    await useDbCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                string[] batches = Patterns.GoBatchSplitter.Split(sql);
                int nonEmpty = batches.Count(b => !string.IsNullOrWhiteSpace(b));
                progress?.Report(new InstallationProgress { Message = $"[DEBUG] {name}: executing {nonEmpty} batches", Status = "Debug" });

                foreach (string batch in batches)
                {
                    string trimmedBatch = batch.Trim();
                    if (string.IsNullOrWhiteSpace(trimmedBatch))
                        continue;

                    using var command = new SqlCommand(trimmedBatch, connection);
                    command.CommandTimeout = InstallationService.DependencyTimeoutSeconds;
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                string source = localPath != null ? "local" : "GitHub";
                progress?.Report(new InstallationProgress
                {
                    Message = $"{name} - Success ({description}) [{source}]",
                    Status = "Success"
                });

                successCount++;
            }
            catch (HttpRequestException ex)
            {
                progress?.Report(new InstallationProgress
                {
                    Message = $"{name} - Download failed: {ex.Message}",
                    Status = "Error"
                });
            }
            catch (SqlException ex)
            {
                progress?.Report(new InstallationProgress
                {
                    Message = $"{name} - SQL execution failed: {ex.Message}",
                    Status = "Error"
                });
            }
            catch (Exception ex)
            {
                progress?.Report(new InstallationProgress
                {
                    Message = $"{name} - Failed: {ex.Message}",
                    Status = "Error"
                });
            }
        }

        progress?.Report(new InstallationProgress
        {
            Message = $"Dependencies installed: {successCount}/{dependencies.Count}",
            Status = successCount == dependencies.Count ? "Success" : "Warning"
        });

        return successCount;
    }

    /// <summary>
    /// Checks the community directory for a local copy of the dependency file.
    /// Returns the full path if found, null otherwise.
    /// </summary>
    private string? ResolveLocalFile(string fileName)
    {
        if (string.IsNullOrEmpty(_communityDirectory) || !Directory.Exists(_communityDirectory))
            return null;

        string path = Path.Combine(_communityDirectory, fileName);
        return File.Exists(path) ? path : null;
    }

    private async Task<string> DownloadWithRetryAsync(
        string url,
        IProgress<InstallationProgress>? progress = null,
        int maxRetries = 3,
        CancellationToken cancellationToken = default)
    {
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                return await _httpClient.GetStringAsync(url, cancellationToken).ConfigureAwait(false);
            }
            catch (HttpRequestException) when (attempt < maxRetries)
            {
                int delaySeconds = (int)Math.Pow(2, attempt);
                progress?.Report(new InstallationProgress
                {
                    Message = $"Network error, retrying in {delaySeconds}s ({attempt}/{maxRetries})...",
                    Status = "Warning"
                });
                await Task.Delay(delaySeconds * 1000, cancellationToken).ConfigureAwait(false);
            }
        }
        return await _httpClient.GetStringAsync(url, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _httpClient?.Dispose();
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}
