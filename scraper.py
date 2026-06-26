#!/usr/bin/env python3
"""
fleet_monitor.py
Logs into fleet.shiftiq.us (password: workstream) and polls
/api/fleet/status every 5 s. 
- Tracks ongoing recording sessions even through disconnects.
- Logs daily totals (running sum).
- Builds operator session logs strictly by scraping the Completed Tasks tab.
- Alerts if CPU >= 75C or storage reaches 80%/90%+.
- Warns if the entire board drops offline.
"""

import json
import sys
import time
import datetime
import requests
import threading
import os
import re
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL      = "https://fleet.shiftiq.us"
PASSWORD      = "workstream"
POLL_INTERVAL = 5          # seconds between polls

# ANSI Color Codes
ANSI_BRIGHT_RED   = "\033[91m"
ANSI_YELLOW       = "\033[93m"
ANSI_GREEN        = "\033[92m"
ANSI_BLUE         = "\033[94m"
ANSI_LIGHT_BLUE   = "\033[96m"
ANSI_LIGHT_PURPLE = "\033[95m"
ANSI_RESET        = "\033[0m"

# Formatting
ANSI_BOLD         = "\033[1m"
ANSI_UNDERLINE    = "\033[4m"
ANSI_BLINK        = "\033[5m"
ANSI_BG_RED       = "\033[41m"
ANSI_BG_YELLOW    = "\033[43m"

# Compound styles
ANSI_REC          = f"\033[91m\033[1m\033[4m"
ANSI_WARN_FMT     = f"\033[43m\033[30m\033[1m\033[4m\033[5m"
ANSI_URGENT_FMT   = f"\033[41m\033[97m\033[1m\033[4m\033[5m"
# ─────────────────────────────────────────────────────────────────────────────

# Global state for logging
daily_totals: dict[str, dict] = {}
recording_cache: dict[str, dict] = {}
device_cache: dict[str, dict] = {}
scraped_sessions = set()  # Tracks unique sessions synced from the Completed Tasks tab
log_lock = threading.Lock()
global_session = None

def ts():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_date_str():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def format_time(seconds: float) -> str:
    """Formats a duration strictly into hh:mm:ss format, safely handling negatives."""
    sign = "-" if seconds < 0 else ""
    sec_val = abs(float(seconds))
    h = int(sec_val // 3600)
    m = int((sec_val % 3600) // 60)
    s = int(sec_val % 60)
    return f"{sign}{h:02d}:{m:02d}:{s:02d}"

def clean_str(val, default="Unknown"):
    """Ensures empty strings from the API are properly labeled as Unknown."""
    if not val or str(val).strip() in ("", "None", "null", "—"):
        return default
    return str(val).strip()

def parse_duration_from_log(duration_str: str) -> float:
    """Safely extracts raw seconds from ANY historical log format using Regex."""
    duration_str = duration_str.strip()
    match_paren = re.search(r"\((\-?[\d\.]+)s\)", duration_str)
    if match_paren: return float(match_paren.group(1))
    match_multi = re.search(r"^(\-?[\d\.]+)s\s*\|", duration_str)
    if match_multi: return float(match_multi.group(1))
    if duration_str.endswith("s") and ":" not in duration_str:
        try: return float(duration_str.replace("s", ""))
        except ValueError: pass
    match_hms = re.search(r"(\-?\d{1,2}):(\d{2}):([\d\.]+)", duration_str)
    if match_hms:
        h, m, s = match_hms.groups()
        sign = -1 if h.startswith("-") else 1
        return sign * (abs(float(h)) * 3600 + float(m) * 60 + float(s))
    return 0.0

def load_daily_totals():
    """Scans today's daily log file to verify and restore the running history math."""
    with log_lock:
        today_str = get_date_str()
        log_filename = f"daily_recording_log_{today_str}.txt"
        
        if today_str not in daily_totals:
            daily_totals[today_str] = {"total": 0, "by_pi": {}, "by_operator": {}}
            
        day_stats = daily_totals[today_str]
        if not os.path.exists(log_filename): return
            
        print(f"[{ts()}] DEBUG  Scanning {log_filename} to restore math history...")
        pattern = re.compile(r"Session Ended \| Pi:\s*(.*?)\s*\| Operator:\s*(.*?)\s*\|.*?Session Duration:\s*(.*)")
        try:
            with open(log_filename, "r") as f:
                for line in f:
                    match = pattern.search(line)
                    if match:
                        label, op, dur_str = match.group(1).strip(), match.group(2).strip(), match.group(3).strip()
                        dur = parse_duration_from_log(dur_str)
                        day_stats["total"] += dur
                        day_stats["by_pi"][label] = day_stats["by_pi"].get(label, 0) + dur
                        day_stats["by_operator"][op] = day_stats["by_operator"][op].get(op, 0) + dur
            print(f"[{ts()}] DEBUG  Successfully recovered history: {ANSI_LIGHT_PURPLE}{format_time(day_stats['total'])}{ANSI_RESET} fleet overall.")
        except Exception as e:
            print(f"[{ts()}] ERROR  Could not scan log file for history: {e}")

def fetch_and_log_tasks(http_session, hostname, label, fallback_op=None, fallback_task=None, fallback_dur=None):
    """
    Scrapes the Pi's Completed Tasks tab via the statusboard API and writes
    today's sessions to operator_sessions_<date>.txt.
    """
    today_str = get_date_str()
    op_filename = f"operator_sessions_{today_str}.txt"
    today_prefix = today_str.replace("-", "")
    success = False

    recordings_url = f"{BASE_URL}/proxy/{hostname}/recordings?embed=1"
    try:
        http_session.get(recordings_url, timeout=10)
    except Exception:
        pass

    api_url = f"{BASE_URL}/proxy/{hostname}/statusboard-api/mcap-sync/sessions?light=1&limit=100"
    try:
        r = http_session.get(api_url, timeout=10)
        if r.ok:
            data = r.json()
            if isinstance(data, list):
                groups = data
            else:
                groups = data.get("session_groups") or data.get("sessions") or []

            for rec in groups:
                name = str(rec.get("name", ""))
                start_unix = rec.get("start_time_unix") or rec.get("mtime") or 0
                try:
                    from_unix_today = (
                        start_unix > 0 and
                        datetime.datetime.fromtimestamp(float(start_unix)).strftime("%Y-%m-%d") == today_str
                    )
                except Exception:
                    from_unix_today = False

                if not (name.startswith(today_prefix) or from_unix_today):
                    continue

                op   = clean_str(rec.get("operator"))
                task = clean_str(rec.get("task"))
                loc  = clean_str(rec.get("location") or rec.get("environment"), default="")
                dur  = float(rec.get("duration_s") or 0)

                sig = f"{label}|{name}|{op}|{task}|{dur:.0f}"
                if sig not in scraped_sessions:
                    scraped_sessions.add(sig)
                    loc_str = f" | Location: {loc:<20}" if loc and loc != "Unknown" else ""
                    with log_lock:
                        with open(op_filename, "a") as f:
                            f.write(
                                f"[{ts()}] Operator: {op:<20} | Pi: {label:<18}"
                                f" | Task: {task:<25}{loc_str}"
                                f" | Session Duration: {format_time(dur)} ({dur:.2f}s)\n"
                            )
            success = True
    except Exception as exc:
        print(f"[{ts()}] DEBUG  Statusboard API failed for {label}: {exc}")

    if not success and fallback_dur is not None:
        op   = clean_str(fallback_op)
        task = clean_str(fallback_task)
        sig  = f"{label}|Fallback|{op}|{task}|{fallback_dur:.0f}"
        if sig not in scraped_sessions:
            scraped_sessions.add(sig)
            with log_lock:
                with open(op_filename, "a") as f:
                    f.write(
                        f"[{ts()}] Operator: {op:<20} | Pi: {label:<18}"
                        f" | Task: {task:<25}"
                        f" | Session Duration: {format_time(fallback_dur)} ({fallback_dur:.2f}s)"
                        f" [fallback]\n"
                    )

def scrape_all_device_tasks(http_session):
    with log_lock:
        snapshot = dict(device_cache)

    if not snapshot:
        print(f"[{ts()}] INFO   No devices in cache yet — skipping task scrape.")
        return

    print(f"[{ts()}] INFO   Scraping Completed Tasks for {len(snapshot)} device(s)...")

    threads = []
    for hostname, d in snapshot.items():
        label = device_label(d)
        t = threading.Thread(
            target=fetch_and_log_tasks,
            args=(http_session, hostname, label),
            daemon=True,
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=300)

    today_str  = get_date_str()
    op_file    = f"operator_sessions_{today_str}.txt"
    print(f"[{ts()}] INFO   Task scrape complete → {op_file}")

def sync_all_tasks(http_session, devices):
    print(f"[{ts()}] INFO   Syncing Operator Sessions from Completed Tasks tabs...")
    threads = []
    for d in devices:
        hostname = d.get("hostname")
        label    = device_label(d)
        t = threading.Thread(
            target=fetch_and_log_tasks,
            args=(http_session, hostname, label),
            daemon=True,
        )
        threads.append(t)
        t.start()
    for t in threads:
        t.join(timeout=15)
    print(f"[{ts()}] INFO   Completed Tasks sync finished.")

def log_session_end(label: str, op: str, task: str, dur: float):
    op, task = clean_str(op), clean_str(task)
    with log_lock:
        today_str = get_date_str()
        if today_str not in daily_totals:
            daily_totals[today_str] = {"total": 0, "by_pi": {}, "by_operator": {}}
        
        day_stats = daily_totals[today_str]
        day_stats["total"] += dur
        day_stats["by_pi"][label] = day_stats["by_pi"].get(label, 0) + dur
        day_stats["by_operator"][op] = day_stats["by_operator"].get(op, 0) + dur
        
        log_filename = f"daily_recording_log_{today_str}.txt"
        try:
            with open(log_filename, "a") as f:
                f.write(f"[{ts()}] Session Ended | Pi: {label:<15} | Operator: {op:<15} | Task: {task:<15} | Session Duration: {format_time(dur)} ({dur:.2f}s)\n")
                f.write(f"[{ts()}] --- Daily Running Totals ---\n")
                f.write(f"[{ts()}] Overall Fleet: {format_time(day_stats['total'])}\n")
                f.write(f"[{ts()}] Pi ({label}): {format_time(day_stats['by_pi'][label])}\n")
                f.write(f"[{ts()}] Operator ({op}): {format_time(day_stats['by_operator'][op])}\n\n")
        except Exception as e:
            print(f"[{ts()}] ERROR  Could not write to {log_filename}: {e}")

def log_current_totals(reason: str):
    if global_session:
        threading.Thread(
            target=scrape_all_device_tasks,
            args=(global_session,),
            daemon=True,
        ).start()

    with log_lock:
        today_str = get_date_str()
        snap_total = daily_totals.get(today_str, {}).get("total", 0)
        snap_by_pi = dict(daily_totals.get(today_str, {}).get("by_pi", {}))
        snap_by_op = dict(daily_totals.get(today_str, {}).get("by_operator", {}))
        
        for host, info in recording_cache.items():
            dur = info.get("duration", 0)
            op = info.get("operator", "Unknown")
            label = info.get("label", host)
            snap_total += dur
            snap_by_pi[label] = snap_by_pi.get(label, 0) + dur
            snap_by_op[op] = snap_by_op.get(op, 0) + dur
            
        log_filename = f"daily_recording_log_{today_str}.txt"
        try:
            with open(log_filename, "a") as f:
                f.write(f"[{ts()}] === Snapshot Triggered By: {reason} ===\n")
                f.write(f"[{ts()}] Overall Fleet: {format_time(snap_total)}\n")
                if snap_by_pi:
                    f.write(f"[{ts()}] --- By Pi ---\n")
                    for p, d in sorted(snap_by_pi.items()):
                        f.write(f"[{ts()}]   {p:<15}: {format_time(d)}\n")
                if snap_by_op:
                    f.write(f"[{ts()}] --- By Operator ---\n")
                    for o, d in sorted(snap_by_op.items()):
                        f.write(f"[{ts()}]   {o:<15}: {format_time(d)}\n")
                f.write("\n")
            print(f"\n[{ts()}] INFO   Wrote snapshot to {log_filename} ({reason})")
        except Exception as e:
            print(f"\n[{ts()}] ERROR  Could not write to {log_filename}: {e}")

def get_status(device: dict) -> str:
    if not device.get("online"): return "offline"
    cs = device.get("capture_state", "unknown")
    if cs == "recording": return "recording"
    if (device.get("upload_queue") or 0) > 0: return "uploading"
    return cs or "idle"

def device_label(device: dict) -> str:
    return device.get("display_name") or device.get("hostname", "?")

def verify_on_close(session: requests.Session):
    """
    Overwrites the unreliable local sums by scraping and aggregating individual
    session timings parsed directly from the proxy database for the specific calendar day.
    Corrects both Pi metrics and Operator math balances simultaneously.
    """
    print(f"\n[{ts()}] INFO   Starting granular verification of individual rows vs local history...")
    today_str = get_date_str()
    today_prefix = today_str.replace("-", "")
    log_filename = f"daily_recording_log_{today_str}.txt"
    
    local_sessions = {}
    if os.path.exists(log_filename):
        pattern = re.compile(r"Session Ended \| Pi:\s*(.*?)\s*\| Operator:\s*(.*?)\s*\|.*?Session Duration:\s*(.*)")
        with open(log_filename, "r") as f:
            for line in f:
                match = pattern.search(line)
                if match:
                    label = match.group(1).strip()
                    dur = parse_duration_from_log(match.group(3).strip())
                    if label not in local_sessions: local_sessions[label] = []
                    local_sessions[label].append(dur)

    correction_lines = []
    
    for hostname, d in list(device_cache.items()):
        label = device_label(d)
        local_sum = sum(local_sessions.get(label, []))
        
        scraped_day_sum = 0.0
        operator_contributions = {}
        
        api_url = f"{BASE_URL}/proxy/{hostname}/statusboard-api/mcap-sync/sessions?light=1&limit=100"
        try:
            r = session.get(api_url, timeout=10)
            if r.ok:
                data = r.json()
                groups = data if isinstance(data, list) else (data.get("session_groups") or data.get("sessions") or [])
                for rec in groups:
                    name = str(rec.get("name", ""))
                    start_unix = rec.get("start_time_unix") or rec.get("mtime") or 0
                    try:
                        from_unix_today = (
                            start_unix > 0 and
                            datetime.datetime.fromtimestamp(float(start_unix)).strftime("%Y-%m-%d") == today_str
                        )
                    except Exception:
                        from_unix_today = False

                    if name.startswith(today_prefix) or from_unix_today:
                        duration = float(rec.get("duration_s") or 0)
                        scraped_day_sum += duration
                        op_name = clean_str(rec.get("operator"))
                        operator_contributions[op_name] = operator_contributions.get(op_name, 0.0) + duration
        except Exception as e:
            print(f"[{ts()}] WARN   Could not fetch database rows to overwrite {label}: {e}")
            continue

        diff = scraped_day_sum - local_sum
        if abs(diff) >= 5 and scraped_day_sum > 0:
            with log_lock:
                if today_str not in daily_totals:
                    daily_totals[today_str] = {"total": 0, "by_pi": {}, "by_operator": {}}
                day_stats = daily_totals[today_str]
                
                # Correct running sum math profiles inside the system cache
                old_pi_total = day_stats["by_pi"].get(label, 0)
                day_stats["by_pi"][label] = scraped_day_sum
                day_stats["total"] = day_stats["total"] - old_pi_total + scraped_day_sum
                
                # Clear and reconstruct operator shares discovered on this Pi
                for op_k, op_v in operator_contributions.items():
                    day_stats["by_operator"][op_k] = day_stats["by_operator"].get(op_k, 0) + op_v
                
            correction_lines.append(
                f"[{ts()}] CORRECTION | Pi: {label:<15} | Local Running Sum: {ANSI_LIGHT_PURPLE}{format_time(local_sum)}{ANSI_RESET} → Scraped True Total: {ANSI_LIGHT_PURPLE}{format_time(scraped_day_sum)}{ANSI_RESET} (diff={ANSI_LIGHT_PURPLE}{format_time(diff)}{ANSI_RESET})\n"
            )
            print(f"[{ts()}] OVERWRITE {ANSI_LIGHT_BLUE}{label:<15}{ANSI_RESET} | True Scraped Total: {ANSI_LIGHT_PURPLE}{format_time(scraped_day_sum)}{ANSI_RESET} | Local: {ANSI_LIGHT_PURPLE}{format_time(local_sum)}{ANSI_RESET} | Corrected")
        else:
            print(f"[{ts()}] VERIFY    {ANSI_GREEN}{label:<15}{ANSI_RESET} | True Scraped Total: {ANSI_LIGHT_PURPLE}{format_time(scraped_day_sum)}{ANSI_RESET} | Local: {ANSI_LIGHT_PURPLE}{format_time(local_sum)}{ANSI_RESET} | Matches")

    if correction_lines:
        try:
            with open(log_filename, "a") as f:
                f.write(f"[{ts()}] === Consolidated Dashboard Validation Summary ===\n")
                f.writelines(correction_lines)
                f.write(f"[{ts()}] ===================================================\n\n")
        except Exception as e:
            print(f"[{ts()}] ERROR  Could not write consolidated verification block: {e}")

def start_key_listener():
    def _listener():
        if sys.platform == 'win32':
            import msvcrt
            while True:
                try:
                    key = msvcrt.getwch()
                    if key in ('~', '`'): log_current_totals("Tilde Key Pressed")
                    elif key == '\x03': import _thread; _thread.interrupt_main(); break
                except Exception: pass
        else:
            import tty, termios, _thread
            try:
                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
            except Exception: return
            try:
                tty.setcbreak(fd)
                while True:
                    ch = sys.stdin.read(1)
                    if ch in ('~', '`'): log_current_totals("Tilde Key Pressed")
                    elif ch == '\x03': _thread_interrupt_main(); break
            except Exception: pass
            finally:
                try: termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                except Exception: pass

    t = threading.Thread(target=_listener, daemon=True)
    t.start()

def login(session: requests.Session) -> bool:
    try:
        resp = session.get(BASE_URL + "/", timeout=10, allow_redirects=True)
    except requests.RequestException as exc:
        print(f"[{ts()}] ERROR  Could not reach {BASE_URL}: {exc}")
        return False

    if resp.status_code == 200 and "device-grid" in resp.text: return True

    soup = BeautifulSoup(resp.text, "html.parser")
    form = soup.find("form")
    if form is None:
        session.auth = ("", PASSWORD)
        r = session.get(BASE_URL + "/", timeout=10)
        return r.status_code == 200

    action  = form.get("action") or "/"
    method  = (form.get("method") or "POST").upper()
    post_url = action if action.startswith("http") else BASE_URL + action

    payload: dict = {}
    for inp in form.find_all("input"):
        name, itype, val = inp.get("name"), (inp.get("type") or "text").lower(), inp.get("value", "")
        if not name: continue
        if itype == "password": payload[name] = PASSWORD
        elif itype not in ("submit", "button", "image", "reset"): payload[name] = val

    if not any(v == PASSWORD for v in payload.values()): payload["password"] = PASSWORD

    try:
        r = session.request(method, post_url, data=payload, timeout=10, allow_redirects=True)
    except requests.RequestException: return False

    if r.status_code in range(200, 400):
        if "device-grid" in r.text or r.status_code in (301, 302, 303): return True
        dashboard = session.get(BASE_URL + "/", timeout=10)
        if dashboard.status_code == 200 and "device-grid" in dashboard.text: return True

    return False

def poll(session: requests.Session) -> list[dict] | None:
    try:
        r = session.get(BASE_URL + "/api/fleet/status", timeout=10)
    except requests.RequestException: return None
    if r.status_code in (401, 403) or not r.ok: return None
    try: return r.json().get("devices", [])
    except ValueError: return None

def run():
    global global_session
    global_session = requests.Session()
    global_session.headers.update({"User-Agent": "fleet-monitor/1.0"})
    session = global_session

    if not login(session):
        print(f"[{ts()}] FATAL  Cannot authenticate. Exiting.")
        sys.exit(1)

    load_daily_totals()
    print(f"[{ts()}] DEBUG  Fetching initial device states …")
    
    prev_status: dict[str, str] = {}
    hot_pis: set[str] = set()       
    full_pis_80: set[str] = set()   
    full_pis_90: set[str] = set()
    board_down_alerted = False
    
    devices = poll(session)
    if devices:
        sync_all_tasks(session, devices)
        
        for d in devices:
            hostname = d.get("hostname", "")
            device_cache[hostname] = d
            s = get_status(d)
            prev_status[hostname] = s
            label = device_label(d)
            print(f"[{ts()}] INIT   {ANSI_LIGHT_BLUE}{label:<30}{ANSI_RESET}  {s}")
            
            if s == "recording":
                recording_cache[hostname] = {
                    "duration": d.get("recording_duration_s", 0),
                    "operator": clean_str(d.get("operator")),
                    "task": clean_str(d.get("task")),
                    "label": label
                }
            
            temp, disk = d.get("cpu_temp"), d.get("disk_percent")
            if temp is not None:
                try:
                    if float(temp) >= 75.0:
                        print(f"{ANSI_URGENT_FMT}[{ts()}] URGENT   {label:<30}  CPU Temp is {float(temp):.1f}°C{ANSI_RESET}")
                        hot_pis.add(hostname)
                except ValueError: pass
            if disk is not None:
                try:
                    if float(disk) >= 90.0:
                        print(f"{ANSI_URGENT_FMT}[{ts()}] URGENT   {label:<30}  Storage at {float(disk):.1f}%{ANSI_RESET}")
                        full_pis_90.add(hostname)
                    elif float(disk) >= 80.0:
                        print(f"{ANSI_WARN_FMT}[{ts()}] WARNING  {label:<30}  Storage at {float(disk):.1f}%{ANSI_RESET}")
                        full_pis_80.add(hostname)
                except ValueError: pass

    print(f"\n[{ts()}] INFO   Monitoring devices. Press '~' to log snapshot. Press Ctrl+C to exit.\n")

    while True:
        time.sleep(POLL_INTERVAL)
        devices = poll(session)

        if devices is None:
            if not login(session): time.sleep(30)
            continue

        for d in devices:
            hostname = d.get("hostname", "")
            device_cache[hostname] = d
            new_s    = get_status(d)
            old_s    = prev_status.get(hostname)
            label    = device_label(d)
            was_recording = hostname in recording_cache

            temp, disk = d.get("cpu_temp"), d.get("disk_percent")
            if temp is not None:
                try:
                    temp_val = float(temp)
                    if temp_val >= 75.0 and hostname not in hot_pis:
                        print(f"{ANSI_URGENT_FMT}[{ts()}] URGENT   {label:<30}  CPU Temp reached {temp_val:.1f}°C{ANSI_RESET}")
                        hot_pis.add(hostname)
                    elif temp_val < 75.0 and hostname in hot_pis:
                        print(f"[{ts()}] RECOVER  {ANSI_LIGHT_BLUE}{label:<30}{ANSI_RESET}  CPU Temp dropped to {temp_val:.1f}°C")
                        hot_pis.remove(hostname)
                except ValueError: pass
            if disk is not None:
                try:
                    disk_val = float(disk)
                    if disk_val >= 90.0 and hostname not in full_pis_90:
                        print(f"{ANSI_URGENT_FMT}[{ts()}] URGENT   {label:<30}  Storage reached {disk_val:.1f}%{ANSI_RESET}")
                        full_pis_90.add(hostname); full_pis_80.discard(hostname)
                    elif 80.0 <= disk_val < 90.0 and hostname not in full_pis_80:
                        print(f"{ANSI_WARN_FMT}[{ts()}] WARNING  {label:<30}  Storage reached {disk_val:.1f}%{ANSI_RESET}")
                        full_pis_80.add(hostname); full_pis_90.discard(hostname)
                    elif disk_val < 80.0:
                        if hostname in full_pis_80:
                            print(f"[{ts()}] RECOVER  {ANSI_LIGHT_BLUE}{label:<30}{ANSI_RESET}  Storage dropped to {disk_val:.1f}%")
                            full_pis_80.remove(hostname)
                        if hostname in full_pis_90:
                            print(f"[{ts()}] RECOVER  {ANSI_LIGHT_BLUE}{label:<30}{ANSI_RESET}  Storage dropped to {disk_val:.1f}%")
                            full_pis_90.remove(hostname)
                except ValueError: pass

            if new_s == "recording":
                recording_cache[hostname] = {
                    "duration": d.get("recording_duration_s", 0),
                    "operator": clean_str(d.get("operator")),
                    "task": clean_str(d.get("task")),
                    "label": label
                }

            if old_s is None:
                if was_recording:
                    if new_s not in ("recording", "offline"):
                        info = recording_cache.pop(hostname)
                        dur, op, task = info.get("duration", 0), info.get("operator", "Unknown"), info.get("task", "Unknown")
                        print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  Reappeared as {new_s} (Stopped) | Duration: {format_time(dur)} | Op: {op}{ANSI_RESET}")
                        log_session_end(label, op, task, dur)
                        threading.Thread(target=fetch_and_log_tasks, args=(session, hostname, label, op, task, dur)).start()
                    elif new_s == "recording":
                        print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  Reappeared and still recording!{ANSI_RESET}")
                else:
                    print(f"[{ts()}] NEW    {ANSI_LIGHT_BLUE}{label:<30}{ANSI_RESET}  → {new_s}")
                    if new_s == "recording":
                        op, task = clean_str(d.get("operator")), clean_str(d.get("task"))
                        print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  Started | Op: {op} | Task: {task}{ANSI_RESET}")
            
            elif new_s != old_s:
                if was_recording and new_s not in ("recording", "offline"):
                    info = recording_cache.pop(hostname)
                    dur, op, task = info.get("duration", 0), info.get("operator", "Unknown"), info.get("task", "Unknown")
                    print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  {old_s} → STOPPED | Duration: {format_time(dur)} | Op: {op} | Task: {task}{ANSI_RESET}")
                    log_session_end(label, op, task, dur)
                    threading.Thread(target=fetch_and_log_tasks, args=(session, hostname, label, op, task, dur)).start()
                    
                elif old_s == "recording" and new_s == "offline":
                    print(f"{ANSI_WARN_FMT}[{ts()}] WARN   {label:<30}  Went offline! Recording session kept alive in cache.{ANSI_RESET}")
                    
                elif old_s == "offline" and new_s == "recording" and was_recording:
                    print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  Reconnected and still recording!{ANSI_RESET}")
                    
                elif new_s == "recording" and not was_recording:
                    op, task = clean_str(d.get("operator")), clean_str(d.get("task"))
                    print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  {old_s} → RECORDING | Op: {op} | Task: {task}{ANSI_RESET}")
                    
                else:
                    print(f"[{ts()}] CHANGE {ANSI_LIGHT_BLUE}{label:<30}{ANSI_RESET}  {old_s} → {new_s}")
            
            prev_status[hostname] = new_s

        current_hostnames = {d.get("hostname") for d in devices}
        for hostname in list(prev_status.keys()):
            if hostname not in current_hostnames:
                print(f"[{ts()}] GONE   {ANSI_LIGHT_BLUE}{hostname:<30}{ANSI_RESET}")
                old_s = prev_status[hostname]
                if hostname in recording_cache and old_s == "recording":
                    print(f"{ANSI_WARN_FMT}[{ts()}] WARN   {hostname:<30} disappeared from API! Recording session kept alive in cache.{ANSI_RESET}")
                del prev_status[hostname]
                hot_pis.discard(hostname)
                full_pis_80.discard(hostname)
                full_pis_90.discard(hostname)

        if devices and len(devices) > 0:
            offline_count = sum(1 for d in devices if get_status(d) == "offline")
            if offline_count == len(devices):
                if not board_down_alerted:
                    print(f"{ANSI_URGENT_FMT}[{ts()}] URGENT   ALL DEVICES ARE OFFLINE. The board or network might be down!{ANSI_RESET}")
                    board_down_alerted = True
            else:
                if board_down_alerted:
                    print(f"{ANSI_GREEN}[{ts()}] RECOVER  Devices are coming back online. Board connection restored.{ANSI_RESET}")
                    board_down_alerted = False

if __name__ == "__main__":
    try:
        start_key_listener()
        run()
    except KeyboardInterrupt:
        print(f"\n[{ts()}] INFO   Stopped by user.")
    finally:
        if global_session:
            print(f"[{ts()}] INFO   Final Completed Tasks scrape on exit...")
            scrape_all_device_tasks(global_session)
            verify_on_close(global_session)
        log_current_totals("Program Exited")