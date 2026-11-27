#!/bin/bash
# Installs PerformanceMonitor Darling as a systemd service from the linux-x64 tarball (#9688).
#
#   sudo ./install-darling.sh [--source <tarball or folder>] [--sha256 <hash>] [--skip-hash-check]
#                             [--skip-test-connection] [--replace-unit] [--health-seconds <n>] [--check]
#
# It does steps 5 to 8 of docs/darling/linux.md, with the same paths:
#   1. Pre-flight: root, systemd, the .NET runtime host, the source, and its SHA256 when the source is a tarball.
#   2. The 'darling' user, and the folders with their owners and modes: /etc/darling (root:darling, 750),
#      /etc/darling/darling-keys (darling, 700, key files 600) and the log folder (darling, 700, in a parent that root keeps).
#   3. The build in /opt/darling, owned by root, after its --version ran as the service user.
#   4. /etc/darling/darling.json from darling.linux.sample.json when there is none, with empty secret files beside it.
#      A config this run created has placeholders in it, so the run stops here with exit code 2: edit it and run again.
#   5. The pre-flight of the config (--test-connection) as the service user.
#   6. The unit, enabled and started, and the health check.
#
# Every step keeps what is already right, so the script is safe to run again. It refuses while the service runs:
# upgrade-darling.sh replaces the build of a running install.
#
# Run it as root from the folder the tarball extracted into. That folder, every folder above it, this script and
# darling-script-lib.sh must be root's own and closed to group and other writes, because root runs what they hold.
#
# Exit codes: 0 the service is installed, started and healthy (or --check passed). 1 a refusal or a failure.
# 2 everything is in place except a config that you still have to edit.
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
[ -f "${SCRIPT_DIR}/darling-script-lib.sh" ] || { echo "ERROR: darling-script-lib.sh is not beside this script. Run the script from the folder the tarball extracted into." >&2; exit 1; }
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
            *) echo "ERROR: ${trusted} is not owned by root, or its group or other users can write it. Extract the tarball as root into a new folder under /opt (sudo mkdir <folder>, then sudo tar -xzf <tarball> -C <folder> --no-same-owner --no-same-permissions) and run the script from there." >&2; exit 1 ;;
        esac
    done
fi
# shellcheck source=darling-script-lib.sh
. "${SCRIPT_DIR}/darling-script-lib.sh"

source_arg=""
sha256_arg=""
skip_hash=0
skip_test=0
replace_unit=0
health_seconds=30
check_only=0
while [ $# -gt 0 ]; do
    case "$1" in
        --source) [ $# -ge 2 ] || die "--source needs a value."; source_arg="$2"; shift 2 ;;
        --sha256) [ $# -ge 2 ] || die "--sha256 needs a value."; sha256_arg="$2"; shift 2 ;;
        --skip-hash-check) skip_hash=1; shift ;;
        --skip-test-connection) skip_test=1; shift ;;
        --replace-unit) replace_unit=1; shift ;;
        --health-seconds) [ $# -ge 2 ] || die "--health-seconds needs a value."; health_seconds="$2"; shift 2 ;;
        --check) check_only=1; shift ;;
        -h|--help) sed -n '2,24p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "Unknown argument '$1'. --help lists the arguments." ;;
    esac
done
case "${health_seconds}" in ''|*[!0-9]*) die "--health-seconds takes a whole number of seconds." ;; esac
[ "${#health_seconds}" -le 5 ] || die "--health-seconds takes 5 digits at most (99999 seconds)."
# Read as base 10: with a leading zero the shell would take 08 for a bad octal number, and the health check would not run (#9688).
health_seconds=$((10#${health_seconds}))

# ---- 1. pre-flight ----
require_host
resolve_source "${source_arg}" "${SCRIPT_DIR}"

# activating covers a unit that systemd is restarting after a failed start: no process runs between two tries.
case "$(systemctl is-active "${UNIT_NAME}" 2>/dev/null || true)" in
    active|activating|reloading|deactivating) die "The ${UNIT_NAME} unit is running, so Darling is already installed here. To put a new build on a running install, use upgrade-darling.sh. To install again, stop the unit first." ;;
esac
running="$(processes_from "${INSTALL_ROOT}" "")"
if [ -n "${running}" ]; then
    die "A process still runs from ${INSTALL_ROOT}. Nothing was changed. Let it finish or stop it, then run this script again:
${running}"
fi

# The unit and the config sample come from a folder source, or from this script's own folder when the source is a
# tarball or an older build without them. stage_source takes root's own copy of each.
[ -f "${SCRIPT_DIR}/darling.service" ] || die "Found no darling.service beside this script."
[ -f "${SCRIPT_DIR}/darling.linux.sample.json" ] || die "Found no darling.linux.sample.json beside this script."

# After the refusals above, because a tarball source is copied before it is hashed.
verify_source "${sha256_arg}" "${skip_hash}"

if [ "${check_only}" = 1 ]; then
    note "The pre-flight passed. Nothing was changed."
    exit 0
fi

# ---- 2. the user and the folders ----
if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
    note "Created the system user '${SERVICE_USER}'."
fi
# Root keeps each parent folder, and the service user gets only the folder it writes. "install -d" follows a symbolic
# link and changes the owner of what the link names, so a folder the service user gets is checked first, once its
# parent is root's and the service user can no longer swap it. An install by hand from an older docs/darling/linux.md
# gave the service user the parent of the log folder, and this takes it back.
install -d -o root -g "${SERVICE_USER}" -m 750 "${CONFIG_DIR}"
require_real_folder "${KEY_DIR}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_USER}" -m 700 "${KEY_DIR}"
install -d -o root -g root -m 755 "${LOG_ROOT}"
require_real_folder "${LOG_DIR}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_USER}" -m 700 "${LOG_DIR}"
# Key files from an earlier install stay as they are, apart from a mode wider than the owner's. The service user owns
# the folder and can swap a file in it for a link at any moment, so the chmod runs as the service user, not as root.
as_service_user find "${KEY_DIR}" -maxdepth 1 -type f -perm /077 -exec chmod 600 {} + >/dev/null 2>&1 || warn "The step that narrows the mode of the key files in ${KEY_DIR} to 600 could not run as '${SERVICE_USER}', or could not change one of them. Check them yourself: every file there should be owned by '${SERVICE_USER}' with mode 600."

# ---- 3. the build ----
stage_source "${SCRIPT_DIR}"
[ -n "${SHIPPED_UNIT}" ] && [ -n "${SHIPPED_SAMPLE}" ] || die "darling.service or darling.linux.sample.json beside this script is not a regular file. Extract the tarball again and run the script from there."
rm -rf "${INSTALL_ROOT}"
# -T: when the folder is still there, the move fails, where a plain mv would put the build inside it.
mv -T "${STAGED_BUILD}" "${INSTALL_ROOT}" || die "Could not move the new build into ${INSTALL_ROOT}. Look at what is in ${INSTALL_ROOT}, remove it, then run this script again."
note "Darling ${STAGED_VERSION} is in ${INSTALL_ROOT}, owned by root."

# ---- 4. the config ----
config_created=0
if [ ! -e "${CONFIG_FILE}" ]; then
    install -o "${SERVICE_USER}" -g "${SERVICE_USER}" -m 600 "${SHIPPED_SAMPLE}" "${CONFIG_FILE}"
    for secret in store_connection sql_password; do
        [ -e "${CONFIG_DIR}/${secret}" ] || install -o "${SERVICE_USER}" -g "${SERVICE_USER}" -m 600 /dev/null "${CONFIG_DIR}/${secret}"
    done
    config_created=1
fi

# ---- 6a. the unit file (before the stop for a config, so the next run has less to do) ----
if [ ! -e "${UNIT_PATH}" ]; then
    install -o root -g root -m 644 "${SHIPPED_UNIT}" "${UNIT_PATH}"
    note "Installed ${UNIT_PATH}."
elif ! cmp -s "${SHIPPED_UNIT}" "${UNIT_PATH}"; then
    if [ "${replace_unit}" = 1 ]; then
        cp -p "${UNIT_PATH}" "${UNIT_PATH}.previous"
        install -o root -g root -m 644 "${SHIPPED_UNIT}" "${UNIT_PATH}"
        note "Replaced ${UNIT_PATH}. The one it replaced is ${UNIT_PATH}.previous."
    else
        warn "${UNIT_PATH} is not the unit in the tarball, and it was left as it is. --replace-unit installs the tarball's unit."
    fi
fi
systemctl daemon-reload

if [ "${config_created}" = 1 ]; then
    note ""
    note "Created ${CONFIG_FILE} from the Linux sample, with empty ${CONFIG_DIR}/store_connection and ${CONFIG_DIR}/sql_password (mode 600, owned by ${SERVICE_USER})."
    note "The service is NOT started. Edit the three files as root (docs/darling/linux.md, step 6), then run this script again."
    exit 2
fi

# ---- 5. the config pre-flight ----
if [ "${skip_test}" = 1 ]; then
    warn "Skipped the config pre-flight (--skip-test-connection)."
else
    note "Running the config pre-flight (--test-connection) as '${SERVICE_USER}'..."
    # Its output comes through a pipe, so the process holds no handle on root's terminal.
    if ! as_service_user env "DARLING_CONFIG=${CONFIG_FILE}" "${DOTNET}" "${INSTALL_ROOT}/${SERVICE_DLL}" --test-connection 2>&1 | printable_lines; then
        die "The config pre-flight failed, so the service was not started. Fix ${CONFIG_FILE} or the server it names, then run this script again. --skip-test-connection starts the service without this check."
    fi
fi

# ---- 6b. enable, start and check ----
systemctl enable "${UNIT_NAME}" >/dev/null 2>&1 || die "systemctl enable ${UNIT_NAME} failed."
note "Starting ${UNIT_NAME} and watching it for ${health_seconds} seconds..."
if ! start_and_check "${health_seconds}"; then
    die "The service is installed, and its health check failed: ${HEALTH_REASON}. Read 'sudo journalctl -u darling' and the file log in ${LOG_DIR}, fix the cause, then run 'systemctl restart darling'."
fi
note "Darling ${STAGED_VERSION} is installed, and ${UNIT_NAME} is active."
print_next_checks
