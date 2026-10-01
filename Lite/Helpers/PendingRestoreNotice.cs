using System.Windows;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// The window-level face of the pending-restore refusal. The mute-rule service swallows store failures and keeps
/// its in-memory list in step with the request, so a window that edits mute rules asks here first and shows the
/// refusal itself.
/// </summary>
internal static class PendingRestoreNotice
{
    /// <summary>True, after telling the user why, when a restore of <paramref name="table"/> is pending.</summary>
    internal static bool Refuse(string table, Window? owner = null)
    {
        try
        {
            PreservedTableRestore.ThrowIfRestorePending(App.ArchiveDirectory, table);
            return false;
        }
        catch (PendingRestoreException ex)
        {
            if (owner != null) MessageBox.Show(owner, ex.Message, "Saved settings restore pending", MessageBoxButton.OK, MessageBoxImage.Warning);
            else MessageBox.Show(ex.Message, "Saved settings restore pending", MessageBoxButton.OK, MessageBoxImage.Warning);
            return true;
        }
    }
}
