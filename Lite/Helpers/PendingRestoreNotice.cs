using System.Windows;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// The window-level face of the pending-restore refusal. The mute-rule service reports whether each write was
/// saved and changes its in-memory list only after a save. A window that edits mute rules checks for a pending
/// restore first and shows the specific message (<see cref="Refuse"/>), and shows <see cref="SaveFailed"/> when
/// a write was not saved.
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

    /// <summary>Tells the user a mute-rule write the store refused was not applied (the service returned false).</summary>
    internal static void SaveFailed(Window? owner = null)
    {
        const string message = "The mute rule change couldn't be saved, so it wasn't applied. The log has the reason.";
        if (owner != null) MessageBox.Show(owner, message, "Mute rule not saved", MessageBoxButton.OK, MessageBoxImage.Warning);
        else MessageBox.Show(message, "Mute rule not saved", MessageBoxButton.OK, MessageBoxImage.Warning);
    }
}
