#!/bin/bash
# Upgrades an installed PerformanceMonitor Darling under systemd from a newer linux-x64 tarball (#9688).
#
#   sudo ./upgrade-darling.sh [--source <tarball or folder>] [--sha256 <hash>] [--skip-hash-check]
#                             [--replace-unit] [--health-seconds <n>] [--check]
#   sudo ./upgrade-darling.sh --rollback [--health-seconds <n>]
#
# Run the copy of this script that came out of the NEW tarball, as root, from a folder that is not /opt/darling.
# That folder, every folder above it, this script and darling-script-lib.sh must be root's own and closed to group
# and other writes, because root runs what they hold.
# What it does, in order:
#   1. Pre-flight, with the service still running: root, systemd, the .NET runtime host, an installed unit and build,
#      the source and its SHA256, the new build's --version as the service user, and a refusal when any process
#      other than the service runs from /opt/darling. --check stops here, and nothing is changed.
#   2. Stops the service, and checks that no process is left that runs from /opt/darling.
#   3. Copies /opt/darling onto new files that root owns, as /opt/darling.previous, the rollback copy, and puts the
#      new build in /opt/darling with a rename, so /opt/darling never holds a mix of two builds. /etc/darling (the
#      config, the secret files and the keys) is not touched, and the script checks that afterwards.
#   4. Starts the service and runs the health check.
#   5. When the start or the health check fails: stops the service, moves the new build to /opt/darling.failed, moves
#      /opt/darling.previous back, starts the old build and checks it too. The exit code is 1 either way.
#
# The store is not rolled back: migrations that the new build applied stay applied. Read "Rolling back to an older
# build" in docs/darling/admin.md before you upgrade.
#
# --rollback does step 5 by hand, for a new build that started and that you do not want to keep.
#
# Exit codes: 0 the new build runs and is healthy (or --check passed, or --rollback finished healthy). 1 a refusal,
# a failure, or an upgrade that was rolled back.
set -euo pipefail
umask 022
# The tools come from the system folders, whatever PATH the caller has.
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
# What the caller's environment could change in the shell and in the tools: bash's POSIX mode, the folders "cd"
# searches, the names a glob skips, and extra arguments for every tar (#9688).
set +o posix
unset TAR_OPTIONS POSIXLY_CORRECT CDPATH GLOBIGNORE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# Root sources darling-script-lib.sh from this folder. An account that could write the folder, a folder above it or
# either file would run its own commands as root, so a root run stops before it reads the file (#9688).
[ -f "${SCRIPT_DIR}/darling-script-lib.sh" ] || { echo "ERROR: darling-script-lib.sh is not beside this script. Run the script from the folder the new tarball extracted into." >&2; exit 1; }
if [ "$(id -u)" -eq 0 ]; then
    trusted="${SCRIPT_DIR}"
    trusted_paths=("${BASH_SOURCE[0]}" "${SCRIPT_DIR}/darling-script-lib.sh" "${trusted}")
    while [ "${trusted}" != / ]; do
        trusted="$(dirname "${trusted}")"
        trusted_paths+=("${trusted}")
    done
    for trusted in "${trusted_paths[@]}"; do
        case "$(stat -L -c '%u %A' -- "${trusted}" 2>/dev/null || true)" in
            "0 "?????-??-?) ;;
            *) echo "ERROR: ${trusted} is not owned by root, or its group or other users can write it. Extract the new tarball as root into a new folder under /opt (sudo mkdir <folder>, then sudo tar -xzf <tarball> -C <folder> --no-same-owner --no-same-permissions) and run the script from there." >&2; exit 1 ;;
        esac
    done
fi
# shellcheck source=darling-script-lib.sh
. "${SCRIPT_DIR}/darling-script-lib.sh"

source_arg=""
sha256_arg=""
skip_hash=0
replace_unit=0
health_seconds=30
check_only=0
rollback_only=0
while [ $# -gt 0 ]; do
    case "$1" in
        --source) [ $# -ge 2 ] || die "--source needs a value."; source_arg="$2"; shift 2 ;;
        --sha256) [ $# -ge 2 ] || die "--sha256 needs a value."; sha256_arg="$2"; shift 2 ;;
        --skip-hash-check) skip_hash=1; shift ;;
        --replace-unit) replace_unit=1; shift ;;
        --health-seconds) [ $# -ge 2 ] || die "--health-seconds needs a value."; health_seconds="$2"; shift 2 ;;
        --check) check_only=1; shift ;;
        --rollback) rollback_only=1; shift ;;
        -h|--help) sed -n '2,29p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "Unknown argument '$1'. --help lists the arguments." ;;
    esac
done
case "${health_seconds}" in ''|*[!0-9]*) die "--health-seconds takes a whole number of seconds." ;; esac
[ "${#health_seconds}" -le 5 ] || die "--health-seconds takes 5 digits at most (99999 seconds)."
# Read as base 10: with a leading zero the shell would take 08 for a bad octal number, and the health check would not run (#9688).
health_seconds=$((10#${health_seconds}))
[ "${rollback_only}" = 0 ] || [ "${check_only}" = 0 ] || die "--rollback and --check cannot be combined."

# One "hash path" line for the config, each secret file beside it and each key file. The upgrade writes none of them. The
# service may add a key file at a start, so the check afterwards is that every line from before is still there. The
# service user can swap a key file for a link or a FIFO, so the hashes are taken as the service user, which reads every
# file the service reads, and for 60 seconds at most. Returns 1 when the 60 seconds ran out or no line came back: the
# config is always there, so a listing with no line in it did not run, and the check has nothing to compare (#9688).
config_listing() {
    local listing status=0
    listing="$(as_service_user timeout 60 find "${CONFIG_DIR}" "${KEY_DIR}" -maxdepth 1 -type f -exec sha256sum {} + 2>/dev/null)" || status=$?
    [ "${status}" != 124 ] && [ -n "${listing}" ] || return 1
    printf '%s\n' "${listing}" | sort
}

# Stops the unit and waits until no process runs from the install folder. Returns 1 with the list in LEFT_RUNNING when one stays.
stop_and_confirm() {
    systemctl stop "${UNIT_NAME}" || return 1
    local waited
    for ((waited = 0; waited < 30; waited++)); do
        LEFT_RUNNING="$(processes_from "${INSTALL_ROOT}" "")"
        [ -z "${LEFT_RUNNING}" ] && return 0
        sleep 1
    done
    return 1
}

# Puts the build in /opt/darling.previous back, with the unit file when this run replaced it, and starts it. The build it
# replaces stays in /opt/darling.failed for a look. Returns the old build's own start and health check result. The
# callers run it inside "if", where a failed command does not stop the script, so each step that moves a build is
# checked here: when one fails, RESTORED stays 0, HEALTH_REASON says which, and nothing is started (#9688).
RESTORED=0
restore_previous() {
    RESTORED=0
    systemctl stop "${UNIT_NAME}" 2>/dev/null || true
    # A rollback copy that this script did not make can hold files of another account, so what goes back is a copy
    # of it on new files.
    if ! fresh_copy "${PREVIOUS_ROOT}" "${STAGED_RESTORE}"; then
        HEALTH_REASON="could not copy ${PREVIOUS_ROOT} onto new files, so no build was moved"
        return 1
    fi
    if [ -d "${INSTALL_ROOT}" ]; then
        rm -rf "${FAILED_ROOT}"
        if ! mv -T "${INSTALL_ROOT}" "${FAILED_ROOT}"; then
            HEALTH_REASON="could not move ${INSTALL_ROOT} to ${FAILED_ROOT}, so no build was moved"
            return 1
        fi
    fi
    if ! mv -T "${STAGED_RESTORE}" "${INSTALL_ROOT}"; then
        HEALTH_REASON="could not move the copy of ${PREVIOUS_ROOT} into ${INSTALL_ROOT}, so ${INSTALL_ROOT} holds no build"
        return 1
    fi
    rm -rf "${PREVIOUS_ROOT}"
    RESTORED=1
    if [ "${unit_replaced}" = 1 ]; then
        cp -p "${UNIT_PATH}.previous" "${UNIT_PATH}"
        systemctl daemon-reload
    fi
    start_and_check "${health_seconds}"
}

# ---- 1. pre-flight ----
require_host
case "${SCRIPT_DIR}/" in "${INSTALL_ROOT}"/*) die "This script is running from ${INSTALL_ROOT}, the folder the upgrade replaces. Extract the new tarball to another folder and run its copy of this script." ;; esac
[ "$(systemctl show -p LoadState --value "${UNIT_NAME}")" = loaded ] || die "The ${UNIT_NAME} unit is not installed, so there is nothing to upgrade. install-darling.sh installs Darling."
id -u "${SERVICE_USER}" >/dev/null 2>&1 || die "The '${SERVICE_USER}' user does not exist. install-darling.sh installs Darling."
unit_replaced=0

if [ "${rollback_only}" = 1 ]; then
    [ -f "${PREVIOUS_ROOT}/${SERVICE_DLL}" ] || die "${PREVIOUS_ROOT} holds no build to go back to."
    others="$(processes_from "${INSTALL_ROOT}" "$(unit_pids)")"
    [ -z "${others}" ] || die "A process other than the service still runs from ${INSTALL_ROOT}. Nothing was changed. Let it finish or stop it, then run this script again:
${others}"
    note "Going back from $(build_version "${INSTALL_ROOT}") to $(build_version "${PREVIOUS_ROOT}")..."
    if ! stop_and_confirm; then
        die "A process still runs from ${INSTALL_ROOT} after the stop, so nothing was moved. The service is STOPPED: start it with 'systemctl start darling'.
${LEFT_RUNNING:-}"
    fi
    if restore_previous; then
        note "The build from ${PREVIOUS_ROOT} is back in ${INSTALL_ROOT} and is healthy. The build it replaced is in ${FAILED_ROOT}. The store's migrations stay applied."
        exit 0
    fi
    [ "${RESTORED}" = 1 ] || die "The rollback did NOT finish: ${HEALTH_REASON}. The service is STOPPED. Fix what stopped the move, then run this script with --rollback again."
    die "The build from ${PREVIOUS_ROOT} is back in ${INSTALL_ROOT}, and its health check failed: ${HEALTH_REASON}. The build it replaced is in ${FAILED_ROOT}."
fi

if [ ! -f "${INSTALL_ROOT}/${SERVICE_DLL}" ]; then
    # An upgrade that stopped between its two renames leaves the old build in the rollback copy and nothing in the install folder.
    [ -f "${PREVIOUS_ROOT}/${SERVICE_DLL}" ] && [ ! -e "${INSTALL_ROOT}" ] || die "${INSTALL_ROOT} holds no Darling build, so there is nothing to upgrade. install-darling.sh installs Darling."
    [ "${check_only}" = 0 ] || die "${INSTALL_ROOT} is missing and ${PREVIOUS_ROOT} holds a build: an earlier upgrade stopped half way. Run this script without --check, and it puts that build back first."
    mv -T "${PREVIOUS_ROOT}" "${INSTALL_ROOT}" || die "Could not move ${PREVIOUS_ROOT} back to ${INSTALL_ROOT}. Nothing else was changed."
    note "An earlier upgrade stopped half way: moved ${PREVIOUS_ROOT} back to ${INSTALL_ROOT} before this upgrade."
fi
[ -e "${CONFIG_FILE}" ] || die "${CONFIG_FILE} does not exist. This script upgrades the install that docs/darling/linux.md and install-darling.sh make."

resolve_source "${source_arg}" "${SCRIPT_DIR}"

others="$(processes_from "${INSTALL_ROOT}" "$(unit_pids)")"
if [ -n "${others}" ]; then
    die "A process other than the service still runs from ${INSTALL_ROOT}. Nothing was changed, and the service was not stopped. Let it finish or stop it, then run this script again:
${others}"
fi

# After the refusal above, because a tarball source is copied before it is hashed.
verify_source "${sha256_arg}" "${skip_hash}"
old_version="$(build_version "${INSTALL_ROOT}")"
stage_source "${SCRIPT_DIR}"
note "Upgrading ${old_version} to ${STAGED_VERSION}."
[ "${old_version}" != "${STAGED_VERSION}" ] || note "The source is ${STAGED_VERSION}, the version that is already installed. The upgrade goes on and replaces the build with this copy."

if [ "${check_only}" = 1 ]; then
    note "The pre-flight passed. Nothing was changed, and the service was not stopped."
    exit 0
fi

# ---- 2. stop ----
config_checked=1
config_before="$(config_listing)" || config_checked=0
note "Stopping ${UNIT_NAME}..."
if ! stop_and_confirm; then
    die "A process still runs from ${INSTALL_ROOT} after the stop, so nothing was replaced. The service is STOPPED and the install is intact: start it with 'systemctl start darling'.
${LEFT_RUNNING:-}"
fi

# ---- 3. the rollback copy and the new build ----
# -T on each move of a whole folder: when the target is still there, the move fails, where a plain mv would put the
# folder inside it.
rm -rf "${PREVIOUS_ROOT}" || die "Could not remove the old rollback copy ${PREVIOUS_ROOT}, so nothing was replaced. The service is STOPPED and the install is intact: start it with 'systemctl start darling'."
# The rollback copy is a copy on new files that root owns, whatever owners the old build's files record and whoever
# holds one of them open. The old files go into the staging folder, which goes when the script ends.
fresh_copy "${INSTALL_ROOT}" "${STAGED_PREVIOUS}" || die "Could not copy ${INSTALL_ROOT} onto new files for the rollback copy, so nothing was replaced. The service is STOPPED and the install is intact: start it with 'systemctl start darling'."
mv -T "${STAGED_PREVIOUS}" "${PREVIOUS_ROOT}" || die "Could not move the rollback copy to ${PREVIOUS_ROOT}, so nothing was replaced. The service is STOPPED and the install is intact: start it with 'systemctl start darling'."
mv -T "${INSTALL_ROOT}" "${STAGED_REPLACED}" || die "Could not move ${INSTALL_ROOT} out of the way, so nothing was replaced. The service is STOPPED and the install is intact: start it with 'systemctl start darling'."
mv -T "${STAGED_BUILD}" "${INSTALL_ROOT}" || die "Could not move the new build into ${INSTALL_ROOT}. The service is STOPPED, ${old_version} is in ${PREVIOUS_ROOT} and ${INSTALL_ROOT} holds no build. Fix the cause, then run this script again: it puts that build back first."
note "The rollback copy of ${old_version} is ${PREVIOUS_ROOT}."

if [ -n "${SHIPPED_UNIT}" ] && ! cmp -s "${SHIPPED_UNIT}" "${UNIT_PATH}"; then
    if [ "${replace_unit}" = 1 ]; then
        cp -p "${UNIT_PATH}" "${UNIT_PATH}.previous"
        install -o root -g root -m 644 "${SHIPPED_UNIT}" "${UNIT_PATH}"
        systemctl daemon-reload
        unit_replaced=1
        note "Replaced ${UNIT_PATH}. The one it replaced is ${UNIT_PATH}.previous."
    else
        warn "${UNIT_PATH} is not the unit in the tarball, and it was left as it is. --replace-unit installs the tarball's unit, with its sandbox and its stopped marker."
    fi
fi

# ---- 4. start and check ----
note "Starting ${UNIT_NAME} and watching it for ${health_seconds} seconds..."
if start_and_check "${health_seconds}"; then
    if [ "${config_checked}" = 1 ] && config_after="$(config_listing)"; then
        changed="$(comm -23 <(printf '%s\n' "${config_before}") <(printf '%s\n' "${config_after}") | cut -d' ' -f3- | printable)"
        if [ -n "${changed}" ]; then
            warn "These files in ${CONFIG_DIR} changed or went away during the upgrade. The script writes nothing there, so compare them with your backup: ${changed}"
        else
            note "The config, the secret files and the keys in ${CONFIG_DIR} are as they were."
        fi
    else
        warn "The check that the config, the secret files and the keys in ${CONFIG_DIR} are unchanged could NOT run: the listing that the '${SERVICE_USER}' user takes of them came back empty or did not finish in 60 seconds. The script writes nothing there. Compare them with your backup yourself."
    fi
    note "Darling ${STAGED_VERSION} is in ${INSTALL_ROOT}, and ${UNIT_NAME} is active."
    note "To go back: run this script with --rollback. Remove ${PREVIOUS_ROOT} when you no longer need it."
    [ ! -e "${FAILED_ROOT}" ] || note "${FAILED_ROOT} holds a build that an earlier upgrade or rollback set aside. Nothing uses it: remove it when you have looked at it."
    print_next_checks
    exit 0
fi

# ---- 5. roll back ----
failure="${HEALTH_REASON}"
warn "The new build failed its start or its health check: ${failure}"
note "Rolling back to ${old_version}..."
if restore_previous; then
    die "The upgrade to ${STAGED_VERSION} was ROLLED BACK: ${failure}. ${old_version} is back in ${INSTALL_ROOT} and is healthy. The new build is in ${FAILED_ROOT} for a look. Migrations the new build applied stay applied in the store."
fi
[ "${RESTORED}" = 1 ] || die "The upgrade to ${STAGED_VERSION} failed (${failure}), and the rollback did NOT finish: ${HEALTH_REASON}. The service is STOPPED and ${old_version} is still in ${PREVIOUS_ROOT}. Fix what stopped the move, then run this script with --rollback. Migrations the new build applied stay applied in the store."
die "The upgrade to ${STAGED_VERSION} was ROLLED BACK: ${failure}. ${old_version} is back in ${INSTALL_ROOT}, and its own health check failed too: ${HEALTH_REASON}. So the cause is probably not the new build: read 'journalctl -u darling' and the file log in ${LOG_DIR}. The new build is in ${FAILED_ROOT}."
