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
/// from GitHub. Requires an HttpClient — create one instance and dispose when done.
/// </summary>
public sealed class DependencyInstaller : IDisposable
{
    private readonly HttpClient _httpClient;
    private bool _disposed;

    public DependencyInstaller()
    {
        _httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(30)
        };
    }

    /// <summary>
    /// Install community dependencies from GitHub into the PerformanceMonitor database.
    /// Returns the number of successfully installed dependencies.
    /// </summary>
    public async Task<int> InstallDependenciesAsync(
        string connectionString,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var dependencies = new List<(string Name, string Url, string Description)>
        {
            (
                "sp_WhoIsActive",
                "https://raw.githubusercontent.com/amachanic/sp_whoisactive/refs/heads/master/sp_WhoIsActive.sql",
                "Query activity monitoring by Adam Machanic (GPLv3)"
            ),
            (
                "DarlingData",
                "https://raw.githubusercontent.com/erikdarlingdata/DarlingData/main/Install-All/DarlingData.sql",
                "sp_HealthParser, sp_HumanEventsBlockViewer by Erik Darling (MIT)"
            ),
            (
                "First Responder Kit",
                "https://raw.githubusercontent.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/refs/heads/main/Install-All-Scripts.sql",
                "sp_BlitzLock and diagnostic tools by Brent Ozar Unlimited (MIT)"
            )
        };

        progress?.Report(new InstallationProgress
        {
            Message = "Installing community dependencies...",
            Status = "Info"
        });

        int successCount = 0;

        foreach (var (name, url, description) in dependencies)
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
                progress?.Report(new InstallationProgress { Message = $"[DEBUG] Downloading {name} from {url}", Status = "Debug" });
                string sql = await DownloadWithRetryAsync(url, progress, cancellationToken: cancellationToken).ConfigureAwait(false);
                progress?.Report(new InstallationProgress { Message = $"[DEBUG] {name}: downloaded {sql.Length} chars in {depSw.ElapsedMilliseconds}ms", Status = "Debug" });

                if (string.IsNullOrWhiteSpace(sql))
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"{name} - FAILED (empty response)",
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

                progress?.Report(new InstallationProgress
                {
                    Message = $"{name} - Success ({description})",
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
