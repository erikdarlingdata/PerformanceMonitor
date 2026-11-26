# shellcheck shell=bash disable=SC2034
# The parts install-darling.sh and upgrade-darling.sh share (#9688). Both source this file from their own folder.
# The paths are fixed, because darling.service and docs/darling/linux.md name the same ones.

INSTALL_ROOT=/opt/darling
PREVIOUS_ROOT=/opt/darling.previous
FAILED_ROOT=/opt/darling.failed
STAGING_ROOT=/opt/darling.staging
CONFIG_DIR=/etc/darling
CONFIG_FILE=/etc/darling/darling.json
KEY_DIR=/etc/darling/darling-keys
LOG_ROOT=/usr/share/PerformanceMonitorDarling
LOG_DIR=/usr/share/PerformanceMonitorDarling/logs
UNIT_NAME=darling.service
UNIT_PATH=/etc/systemd/system/darling.service
SERVICE_USER=darling
DOTNET=/usr/bin/dotnet
SERVICE_DLL=PerformanceMonitor.Darling.Service.dll
SUMS_NAME=SHA256SUMS-linux.txt

note() { printf '%s\n' "$*"; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# What every run needs before it changes anything: root, a host that systemd runs, the tools the steps call and the runtime host.
require_host() {
    [ "$(id -u)" -eq 0 ] || die "Run this script as root (sudo)."
    [ -d /run/systemd/system ] || die "This host does not run systemd. These scripts install Darling as a systemd service."
    local tool
    for tool in systemctl tar sha256sum runuser useradd install readlink; do
        command -v "${tool}" >/dev/null 2>&1 || die "The '${tool}' command is not on this host, and this script needs it."
    done
    [ -x "${DOTNET}" ] || die "${DOTNET} is missing. Install the .NET 10 runtime and the ASP.NET Core 10 runtime (docs/darling/linux.md, step 2). The unit starts the service with ${DOTNET}, so a dotnet in another folder needs a link there."
}

# Sets SOURCE_KIND (tarball or folder) and SOURCE_PATH from the --source value, or from the folder the script is in.
resolve_source() {
    local source="$1" script_dir="$2"
    [ -n "${source}" ] || source="${script_dir}"
    [ -e "${source}" ] || die "The source '${source}' does not exist."
    SOURCE_PATH="$(readlink -f "${source}")"
    if [ -d "${SOURCE_PATH}" ]; then
        SOURCE_KIND=folder
        [ -f "${SOURCE_PATH}/${SERVICE_DLL}" ] || die "The folder '${SOURCE_PATH}' holds no ${SERVICE_DLL}, so it is not an extracted Darling tarball."
        [ "${SOURCE_PATH}" != "${INSTALL_ROOT}" ] || die "The source is the install folder ${INSTALL_ROOT} itself. Extract the tarball to another folder and run the script from there, or pass --source with the tarball."
    else
        SOURCE_KIND=tarball
    fi
}

# A tarball source is checked against --sha256, or against the SHA256SUMS-linux.txt beside it. A folder source has no checksum to check.
verify_source() {
    local expected="$1" skip="$2" actual sums name line
    [ "${SOURCE_KIND}" = tarball ] || return 0
    actual="$(sha256sum "${SOURCE_PATH}" | cut -d' ' -f1)"
    if [ -n "${expected}" ]; then
        [ "${actual}" = "$(printf '%s' "${expected}" | tr 'A-F' 'a-f')" ] || die "The SHA256 of ${SOURCE_PATH} is ${actual}, not the --sha256 value. Download the tarball again."
        note "SHA256 matches --sha256."
        return 0
    fi
    sums="$(dirname "${SOURCE_PATH}")/${SUMS_NAME}"
    name="$(basename "${SOURCE_PATH}")"
    if [ -f "${sums}" ]; then
        line="$(awk -v n="${name}" '{ f = $2; sub(/^\*/, "", f); if (f == n) print $1 }' "${sums}" | head -n 1)"
        [ -n "${line}" ] || die "${sums} has no line for ${name}. Download the checksum file of the same release."
        [ "${actual}" = "${line}" ] || die "The SHA256 of ${SOURCE_PATH} is ${actual}, and ${sums} says ${line}. Download the tarball again."
        note "SHA256 matches ${sums}."
        return 0
    fi
    [ "${skip}" = 1 ] || die "Found no ${SUMS_NAME} beside ${SOURCE_PATH}, so the tarball is not verified. Download it from the same release, pass --sha256 <hash>, or pass --skip-hash-check to go on without the check."
    warn "The tarball is NOT verified (--skip-hash-check)."
}

# Lays the source out in the staging folder, owned by root whatever the archive or the source folder records, and
# writable by root only. Then runs the staged build's --version as the service user: that proves the runtimes the
# build needs are on the host and that the service user can read the files. Sets STAGED_VERSION.
stage_source() {
    rm -rf "${STAGING_ROOT}"
    mkdir -m 755 "${STAGING_ROOT}"
    if [ "${SOURCE_KIND}" = tarball ]; then
        tar -xzf "${SOURCE_PATH}" -C "${STAGING_ROOT}" --no-same-owner --no-same-permissions || { rm -rf "${STAGING_ROOT}"; die "Could not extract ${SOURCE_PATH}."; }
    else
        cp -R "${SOURCE_PATH}/." "${STAGING_ROOT}/" || { rm -rf "${STAGING_ROOT}"; die "Could not copy ${SOURCE_PATH}."; }
    fi
    chown -R root:root "${STAGING_ROOT}"
    chmod -R u+rwX,go+rX,go-w "${STAGING_ROOT}"
    [ -f "${STAGING_ROOT}/${SERVICE_DLL}" ] || { rm -rf "${STAGING_ROOT}"; die "The source holds no ${SERVICE_DLL}, so it is not a Darling linux-x64 tarball."; }
    local output
    if ! output="$(runuser -u "${SERVICE_USER}" -- "${DOTNET}" "${STAGING_ROOT}/${SERVICE_DLL}" --version 2>&1)"; then
        rm -rf "${STAGING_ROOT}"
        die "The new build does not run on this host as the '${SERVICE_USER}' user. Its --version said: ${output}"
    fi
    STAGED_VERSION="$(printf '%s\n' "${output}" | tail -n 1)"
}

# The version of the build in a folder, or "unknown" when it does not run.
build_version() {
    runuser -u "${SERVICE_USER}" -- "${DOTNET}" "$1/${SERVICE_DLL}" --version 2>/dev/null | tail -n 1 || printf 'unknown\n'
}

# The PIDs of the unit's own processes, one a line: the service the script is about to stop.
unit_pids() {
    local group
    group="$(systemctl show -p ControlGroup --value "${UNIT_NAME}" 2>/dev/null || true)"
    [ -n "${group}" ] && [ -r "/sys/fs/cgroup${group}/cgroup.procs" ] && cat "/sys/fs/cgroup${group}/cgroup.procs"
    return 0
}

# Prints "pid command" for every process that runs from the folder, other than the PIDs listed in $2: its executable is
# in the folder, an argument names a file in it (dotnet <folder>/<name>.dll), or it has a file of the folder mapped.
processes_from() {
    local folder="$1" skip entry pid exe hit arg
    # shellcheck disable=SC2086
    skip=" $(printf '%s ' $2) "
    for entry in /proc/[0-9]*; do
        pid="${entry#/proc/}"
        case "${skip}" in *" ${pid} "*) continue ;; esac
        hit=0
        exe="$(readlink "${entry}/exe" 2>/dev/null || true)"
        case "${exe}" in "${folder}"/*) hit=1 ;; esac
        if [ "${hit}" = 0 ] && [ -r "${entry}/cmdline" ]; then
            while IFS= read -r -d '' arg; do
                case "${arg}" in "${folder}"/*) hit=1; break ;; esac
            done <"${entry}/cmdline" 2>/dev/null
        fi
        if [ "${hit}" = 0 ] && grep -q -F " ${folder}/" "${entry}/maps" 2>/dev/null; then
            hit=1
        fi
        [ "${hit}" = 1 ] && printf '%s %s\n' "${pid}" "$(tr '\0' ' ' <"${entry}/cmdline" 2>/dev/null | cut -c1-160)"
    done
    return 0
}

# The path the unit gives the service in DARLING_STOPPED_MARKER, or nothing when the unit sets none.
unit_stopped_marker() {
    systemctl show -p Environment --value "${UNIT_NAME}" 2>/dev/null | tr ' ' '\n' | sed -n 's/^DARLING_STOPPED_MARKER=//p' | head -n 1
}

# The health check after a start. The web dashboard is off by default, so there is no port to ask. What the service
# gives instead: systemd's own state (Type=notify: active means the host started), the main PID (a new one means the
# process exited and Restart= started another), and the stopped marker (the process stays up after a startup failure
# that stops collection for good, and keeps the marker file present). Healthy means active, the same PID and no
# marker for the whole wait. Returns 1 with HEALTH_REASON set otherwise.
health_check() {
    local seconds="$1" marker first_pid pid state elapsed
    marker="$(unit_stopped_marker)"
    [ -n "${marker}" ] || warn "The unit sets no DARLING_STOPPED_MARKER, so the health check sees only whether the process stays up. The darling.service in the tarball sets it (--replace-unit installs that unit)."
    first_pid="$(systemctl show -p MainPID --value "${UNIT_NAME}")"
    for ((elapsed = 0; elapsed <= seconds; elapsed++)); do
        state="$(systemctl is-active "${UNIT_NAME}" 2>/dev/null || true)"
        if [ "${state}" != active ]; then
            HEALTH_REASON="the unit is '${state}', not active"
            return 1
        fi
        pid="$(systemctl show -p MainPID --value "${UNIT_NAME}")"
        if [ "${pid}" != "${first_pid}" ]; then
            HEALTH_REASON="the service process exited and systemd started another (PID ${first_pid}, then ${pid})"
            return 1
        fi
        if [ -n "${marker}" ] && [ -e "${marker}" ]; then
            # The marker text ends with a full stop and a line end. Both come off, because the messages that quote the reason add their own (#9688).
            HEALTH_REASON="the service says collection has stopped: $(head -c 600 "${marker}" 2>/dev/null | tr '\n' ' ' | sed -e 's/[[:space:]]*$//' -e 's/\.$//')"
            return 1
        fi
        [ "${elapsed}" -lt "${seconds}" ] && sleep 1
    done
    return 0
}

# Starts the unit and runs the health check. Returns 1 with HEALTH_REASON set when either fails.
start_and_check() {
    local seconds="$1"
    if ! systemctl start "${UNIT_NAME}"; then
        HEALTH_REASON="systemctl start ${UNIT_NAME} failed (the process exited before it reported ready)"
        return 1
    fi
    health_check "${seconds}"
}

# What "healthy" does and does not prove, and the store check that proves collection.
print_next_checks() {
    note ""
    note "The start is healthy: the process is up and has reported no startup failure. That is not proof that it collects."
    note "In about 10 to 15 minutes, check the store for SUCCESS rows from every server (docs/darling/linux.md, step 9):"
    note "  SELECT server_id, status, count(*) FROM collect.collection_log WHERE collection_time > (now() AT TIME ZONE 'UTC') - interval '15 minutes' GROUP BY server_id, status ORDER BY server_id, status"
    note "The file log is in ${LOG_DIR}, and 'journalctl -u darling' has what systemd reports."
}
