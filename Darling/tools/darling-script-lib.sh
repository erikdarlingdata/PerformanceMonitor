# shellcheck shell=bash disable=SC2034
# The parts install-darling.sh and upgrade-darling.sh share (#9688). Both source this file from their own folder.
# The paths are fixed, because darling.service and docs/darling/linux.md name the same ones.

INSTALL_ROOT=/opt/darling
PREVIOUS_ROOT=/opt/darling.previous
FAILED_ROOT=/opt/darling.failed
# The staging folder is root's alone (700) while a run works in it: the copy of a tarball source, the build that is being
# laid out, and the copies of the unit and the config sample that the run installs.
STAGING_ROOT=/opt/darling.staging
STAGED_BUILD=/opt/darling.staging/build
STAGED_TARBALL=/opt/darling.staging/source.tar.gz
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

# Text that the service user or a tarball controls (a command line, a file name, a build's output, the stopped marker)
# goes through this before root's terminal shows it: every byte that is not printable ASCII becomes "?", so no escape
# sequence reaches the terminal.
printable() { LC_ALL=C tr -c '\040-\176\n' '?'; }
# The same for the lines a build prints for the operator to read, a line at a time. The pre-flight prints UTF-8 text
# (an em dash in every result line). Where root's locale is UTF-8, a whole UTF-8 character stays and the rest goes:
# the C0 control characters apart from tab, DEL, every byte that is not part of a whole UTF-8 character, and then the
# C1 control characters in their UTF-8 form, in a loop until none is left, because taking one out can join the bytes
# around it into another. In any other locale the terminal may read one byte at a time, where the second byte of a
# UTF-8 character can be a C1 control character itself, so only printable ASCII and tab stay there (#9688).
UTF8_CHARACTER='[\xc2-\xdf][\x80-\xbf]\|\xe0[\xa0-\xbf][\x80-\xbf]\|[\xe1-\xef][\x80-\xbf][\x80-\xbf]\|\xf0[\x90-\xbf][\x80-\xbf][\x80-\xbf]\|[\xf1-\xf4][\x80-\xbf][\x80-\xbf][\x80-\xbf]'
printable_lines() {
    case "$(locale charmap 2>/dev/null || true)" in
        UTF-8) LC_ALL=C sed -u -e 's/[\x01-\x08\x0b-\x1f\x7f]//g' -e "s/\\(${UTF8_CHARACTER}\\)\\|[\\x80-\\xff]/\\1/g" -e ':a' -e 's/\xc2[\x80-\x9f]//' -e 'ta' ;;
        *) LC_ALL=C sed -u -e "s/${UTF8_CHARACTER}/?/g" -e 's/[^\x20-\x7e\t]/?/g' ;;
    esac
}

# Runs a command as the service user in a session of its own, with no terminal: a new session has no controlling
# terminal, stdin is /dev/null, and every caller pipes or drops the output. A process of the service user can then
# reach nothing of the root session that started it, even on a host that lets one process of a user attach to another.
# The command starts in /, because root's own folder can be closed to the service user (/root is), and find goes back
# to its start folder before each -exec. It gets PATH and nothing else of root's environment, which every process of
# the service user could read from /proc (#9688).
as_service_user() {
    (cd / && setsid -w runuser -u "${SERVICE_USER}" -- env -i "PATH=${PATH}" "$@" </dev/null)
}

# True when root owns the path and neither its group nor other users can write it.
root_only_writable() {
    case "$(stat -L -c '%u %A' -- "$1" 2>/dev/null)" in "0 "?????-??-?) return 0 ;; esac
    return 1
}

# True when that holds for the folder and for every folder above it, up to /. An account that could write a folder
# above it could put another folder in its place between this check and the read. $1 is an absolute path.
root_only_path() {
    local folder="$1"
    while :; do
        root_only_writable "${folder}" || return 1
        [ "${folder}" != / ] || return 0
        folder="$(dirname "${folder}")"
    done
}

# Makes a build folder root's own and closed to group and other writes, whatever owners its files record. A build that
# was extracted by hand from an older tarball can belong to the uid of the machine that built it, and an account of
# this host with that uid could then change what a rollback starts (#9688).
make_root_owned() {
    chown -R root:root "$1" && chmod -R go-w "$1"
}

# Stops when the path is a symbolic link, or exists and is not a folder. The install script creates its folders with
# "install -d", which follows a link and changes the owner of whatever the link names.
require_real_folder() {
    if [ -L "$1" ] || { [ -e "$1" ] && [ ! -d "$1" ]; }; then
        die "$1 is a symbolic link or is not a folder, so its owner and mode were left alone. Look at what put it there, remove it, then run this script again."
    fi
}

# Makes the staging folder once in a run, and removes it when the run ends, however it ends.
staging_made=0
ensure_staging() {
    [ "${staging_made}" = 0 ] || return 0
    rm -rf "${STAGING_ROOT}"
    mkdir -m 700 "${STAGING_ROOT}"
    staging_made=1
    trap 'rm -rf "${STAGING_ROOT}"' EXIT
}

# What every run needs before it changes anything: root, a host that systemd runs, the tools the steps call and the runtime host.
require_host() {
    [ "$(id -u)" -eq 0 ] || die "Run this script as root (sudo)."
    [ -d /run/systemd/system ] || die "This host does not run systemd. These scripts install Darling as a systemd service."
    local tool
    for tool in systemctl tar sha256sum runuser setsid timeout stat useradd install readlink; do
        command -v "${tool}" >/dev/null 2>&1 || die "The '${tool}' command is not on this host, and this script needs it."
    done
    [ -x "${DOTNET}" ] || die "${DOTNET} is missing. Install the .NET 10 runtime and the ASP.NET Core 10 runtime (docs/darling/linux.md, step 2). The unit starts the service with ${DOTNET}, so a dotnet in another folder needs a link there."
}

# Sets SOURCE_KIND (tarball or folder) and SOURCE_PATH from the --source value, or from the folder the script is in.
resolve_source() {
    local source="$1" script_dir="$2" open_entry
    [ -n "${source}" ] || source="${script_dir}"
    [ -e "${source}" ] || die "The source '${source}' does not exist."
    SOURCE_PATH="$(readlink -f "${source}")"
    if [ -d "${SOURCE_PATH}" ]; then
        SOURCE_KIND=folder
        [ -f "${SOURCE_PATH}/${SERVICE_DLL}" ] || die "The folder '${SOURCE_PATH}' holds no ${SERVICE_DLL}, so it is not an extracted Darling tarball."
        [ "${SOURCE_PATH}" != "${INSTALL_ROOT}" ] || die "The source is the install folder ${INSTALL_ROOT} itself. Extract the tarball to another folder and run the script from there, or pass --source with the tarball."
        root_only_path "${SOURCE_PATH}" || die "The source folder '${SOURCE_PATH}' or a folder above it is not owned by root, or its group or other users can write it, so another account could change the build while root installs it. Extract the tarball as root into a new folder under /opt (sudo mkdir, then sudo tar -xzf <tarball> -C <folder> --no-same-owner), or pass --source with the tarball."
        # The same for what the folder holds. A link has no mode of its own, and the folder that holds it is checked.
        open_entry="$(find "${SOURCE_PATH}" \( ! -user 0 -o -perm /022 \) ! -type l -print -quit)"
        [ -z "${open_entry}" ] || die "'$(printf '%s' "${open_entry}" | printable)' in the source folder is not owned by root, or its group or other users can write it, so another account could change the build while root installs it. Extract the tarball as root into a new folder under /opt (sudo mkdir, then sudo tar -xzf <tarball> -C <folder> --no-same-owner), or pass --source with the tarball."
    else
        SOURCE_KIND=tarball
    fi
}

# A tarball source is copied into the staging folder first, and that copy is the one that is hashed and later extracted,
# so nobody can swap the file between the check and the extract. The hash is checked against --sha256, or against the
# SHA256SUMS-linux.txt beside the tarball. That file proves the download is whole, not where it came from: anyone who
# can replace the tarball can replace the file beside it. A folder source has no checksum to check, so the two hash
# arguments are refused with one, not ignored.
verify_source() {
    local expected="$1" skip="$2" actual sums name line entry
    if [ "${SOURCE_KIND}" != tarball ]; then
        [ -z "${expected}" ] && [ "${skip}" = 0 ] || die "--sha256 and --skip-hash-check are for a tarball source. The source '${SOURCE_PATH}' is a folder, which has no checksum to check. Pass --source with the tarball, or leave the argument out."
        return 0
    fi
    ensure_staging
    cp -- "${SOURCE_PATH}" "${STAGED_TARBALL}" || die "Could not copy ${SOURCE_PATH} into ${STAGING_ROOT}."
    actual="$(sha256sum "${STAGED_TARBALL}" | cut -d' ' -f1)"
    if [ -n "${expected}" ]; then
        [ "${actual}" = "$(printf '%s' "${expected}" | tr 'A-F' 'a-f')" ] || die "The SHA256 of ${SOURCE_PATH} is ${actual}, not the --sha256 value. Download the tarball again."
        note "SHA256 matches --sha256."
        return 0
    fi
    sums="$(dirname "${SOURCE_PATH}")/${SUMS_NAME}"
    name="$(basename "${SOURCE_PATH}")"
    if [ -f "${sums}" ]; then
        # A line counts when it is 64 hex characters, a space, a second space or the "*" of binary mode, and this
        # tarball's name. Only the hash of such a line is printed, so nothing else the file holds reaches the terminal.
        line=""
        while IFS= read -r entry || [ -n "${entry}" ]; do
            if [[ "${entry}" =~ ^([0-9A-Fa-f]{64})\ [\ *]?(.*)$ ]] && [ "${BASH_REMATCH[2]}" = "${name}" ]; then
                line="$(printf '%s' "${BASH_REMATCH[1]}" | tr 'A-F' 'a-f')"
                break
            fi
        done <"${sums}"
        [ -n "${line}" ] || die "${sums} has no SHA256 line for ${name} (64 hex characters, then the file name). Download the checksum file of the same release."
        [ "${actual}" = "${line}" ] || die "The SHA256 of ${SOURCE_PATH} is ${actual}, and ${sums} says ${line}. Download the tarball again."
        note "SHA256 matches ${sums}. That shows the file is whole. --sha256 with a hash from the release page shows where it came from."
        return 0
    fi
    [ "${skip}" = 1 ] || die "Found no ${SUMS_NAME} beside ${SOURCE_PATH}, so the tarball is not verified. Download it from the same release, pass --sha256 <hash>, or pass --skip-hash-check to go on without the check."
    warn "The tarball is NOT verified (--skip-hash-check)."
}

# Lays the source out in the staging folder, owned by root whatever the archive or the source folder records, and
# writable by root only. The staging folder stays closed to every other account until the build has passed its checks:
# root's tar and cp create a device node, a FIFO or a socket that a source holds, and a Darling build has none, so a
# source with one is refused before any mode is opened. Then runs the staged build's --version as the service user:
# that proves the runtimes the build needs are on the host and that the service user can read the files. Sets
# STAGED_VERSION, and SHIPPED_UNIT and SHIPPED_SAMPLE (see below). $1 is the folder the running script is in.
stage_source() {
    local script_dir="$1" odd output name from
    ensure_staging
    mkdir -m 755 "${STAGED_BUILD}"
    if [ "${SOURCE_KIND}" = tarball ]; then
        tar -xzf "${STAGED_TARBALL}" -C "${STAGED_BUILD}" --no-same-owner --no-same-permissions || die "Could not extract ${SOURCE_PATH}."
    else
        cp -R "${SOURCE_PATH}/." "${STAGED_BUILD}/" || die "Could not copy ${SOURCE_PATH}."
    fi
    odd="$(find "${STAGED_BUILD}" ! -type f ! -type d ! -type l -print -quit)"
    [ -z "${odd}" ] || die "The source holds '$(printf '%s' "${odd#"${STAGED_BUILD}"/}" | printable)', which is not a file, a folder or a link (a device node, a FIFO or a socket). A Darling tarball holds none, so the source was refused and nothing was installed."
    chown -R root:root "${STAGED_BUILD}"
    chmod -R u+rwX,go+rX,go-w "${STAGED_BUILD}"
    [ -f "${STAGED_BUILD}/${SERVICE_DLL}" ] || die "The source holds no ${SERVICE_DLL}, so it is not a Darling linux-x64 tarball."
    # The unit and the config sample that this run installs: the file a folder source holds, or the one beside the
    # script when the source is a tarball or an older build without them. Each is read once, into the staging folder,
    # so the file the script compares is the file it installs.
    SHIPPED_UNIT=""
    SHIPPED_SAMPLE=""
    for name in darling.service darling.linux.sample.json; do
        from="${script_dir}/${name}"
        [ "${SOURCE_KIND}" = folder ] && [ -f "${STAGED_BUILD}/${name}" ] && [ ! -L "${STAGED_BUILD}/${name}" ] && from="${STAGED_BUILD}/${name}"
        [ -f "${from}" ] && [ ! -L "${from}" ] || continue
        # Root installs the unit as a root unit, so the file beside the script passes the check its folder passed.
        root_only_writable "${from}" || die "${from} is not owned by root, or its group or other users can write it, so it was not installed. Extract the tarball as root into a new folder under /opt (sudo mkdir, then sudo tar -xzf <tarball> -C <folder> --no-same-owner) and run the script from there."
        install -m 644 "${from}" "${STAGING_ROOT}/${name}"
        case "${name}" in darling.service) SHIPPED_UNIT="${STAGING_ROOT}/${name}" ;; *) SHIPPED_SAMPLE="${STAGING_ROOT}/${name}" ;; esac
    done
    # Only now can the service user reach the build, which its --version needs.
    chmod 711 "${STAGING_ROOT}"
    if ! output="$(as_service_user "${DOTNET}" "${STAGED_BUILD}/${SERVICE_DLL}" --version 2>&1 | printable)"; then
        die "The new build does not run on this host as the '${SERVICE_USER}' user. Its --version said: ${output}"
    fi
    STAGED_VERSION="$(printf '%s\n' "${output}" | tail -n 1)"
}

# The version of the build in a folder, or "unknown" when it does not run.
build_version() {
    as_service_user "${DOTNET}" "$1/${SERVICE_DLL}" --version 2>/dev/null | tail -n 1 | printable || printf 'unknown\n'
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
        [ "${hit}" = 1 ] && printf '%s %s\n' "${pid}" "$(tr '\0\n' '  ' <"${entry}/cmdline" 2>/dev/null | cut -c1-160 | printable)"
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
        if [ -n "${marker}" ] && { [ -e "${marker}" ] || [ -L "${marker}" ]; }; then
            # The service user owns the marker's folder, so the marker can be a link to a file that only root reads, or
            # a FIFO that never answers. Only a regular file is read, as the service user and for 5 seconds at most.
            # The marker text ends with a full stop and a line end. Both come off, because the messages that quote the reason add their own (#9688).
            if [ -f "${marker}" ] && [ ! -L "${marker}" ]; then
                HEALTH_REASON="the service says collection has stopped: $(as_service_user timeout 5 head -c 600 "${marker}" 2>/dev/null | tr '\n' ' ' | printable | sed -e 's/[[:space:]]*$//' -e 's/\.$//' || true)"
            else
                HEALTH_REASON="the stopped marker ${marker} is there and is not a regular file, so it was not read"
            fi
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
