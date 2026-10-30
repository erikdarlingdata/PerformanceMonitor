/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Windows;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Text for the clipboard that Windows must keep out of Clipboard History (Win+V) and out of the cloud clipboard that syncs
/// to the user's other devices: the MCP token and the client setup command that carries it. The three registered formats and their
/// values are the ones Microsoft documents under "Cloud Clipboard and Clipboard History Formats": the first only has to be present;
/// the other two are a DWORD of zero.
/// </summary>
internal static class SecretClipboard
{
    internal const string ExcludeFromMonitorFormat = "ExcludeClipboardContentFromMonitorProcessing";
    internal const string HistoryFormat = "CanIncludeInClipboardHistory";
    internal const string CloudFormat = "CanUploadToCloudClipboard";

    /// <summary>A data object holding <paramref name="text"/> plus the three opt-out formats.</summary>
    internal static DataObject Build(string text)
    {
        var data = new DataObject();
        data.SetText(text);
        data.SetData(ExcludeFromMonitorFormat, new MemoryStream(BitConverter.GetBytes(1)));
        data.SetData(HistoryFormat, new MemoryStream(BitConverter.GetBytes(0)));
        data.SetData(CloudFormat, new MemoryStream(BitConverter.GetBytes(0)));
        return data;
    }
}
