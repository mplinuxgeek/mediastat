#!/usr/bin/env python3
"""
MediaStat hardware acceleration diagnostic + test script.

Tests every encoder/decoder combination across software, Intel QSV,
Intel VAAPI, AMD VAAPI and NVIDIA NVENC/NVDEC; produces a human-readable
report and a JSON file with full results.

Usage:
    python hw_test.py [--out report.json] [--no-color]
"""

import argparse
import glob as _glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── ANSI colours ──────────────────────────────────────────────────────────────
def _no_color():
    return not sys.stdout.isatty() or os.environ.get("NO_COLOR")

G  = lambda s: s if _no_color() else f"\033[92m{s}\033[0m"   # green
R  = lambda s: s if _no_color() else f"\033[91m{s}\033[0m"   # red
Y  = lambda s: s if _no_color() else f"\033[93m{s}\033[0m"   # yellow
C  = lambda s: s if _no_color() else f"\033[96m{s}\033[0m"   # cyan
B  = lambda s: s if _no_color() else f"\033[1m{s}\033[0m"    # bold
DM = lambda s: s if _no_color() else f"\033[2m{s}\033[0m"    # dim

PASS = G("✓ PASS")
FAIL = R("✗ FAIL")
SKIP = Y("- SKIP")

# ── Helpers ───────────────────────────────────────────────────────────────────
def run(cmd: list, timeout: int = 30, env_extra: dict | None = None,
        input_data: bytes | None = None) -> tuple[int, str, str, float]:
    env = {**os.environ, **(env_extra or {})}
    t0 = time.monotonic()
    try:
        r = subprocess.run(
            cmd, capture_output=True, timeout=timeout, env=env,
            input=input_data,
        )
        elapsed = time.monotonic() - t0
        return r.returncode, r.stdout.decode(errors="replace"), r.stderr.decode(errors="replace"), elapsed
    except subprocess.TimeoutExpired:
        return -1, "", f"TIMEOUT after {timeout}s", time.monotonic() - t0
    except FileNotFoundError as e:
        return -2, "", f"NOT FOUND: {e}", 0.0
    except Exception as e:
        return -3, "", str(e), 0.0

def read_file(path: str) -> str:
    try:
        return Path(path).read_text(errors="replace")
    except Exception:
        return ""

def section(title: str) -> None:
    width = 72
    print(f"\n{B('═' * width)}")
    print(f"{B(f'  {title}')}")
    print(f"{B('═' * width)}")

def subsection(title: str) -> None:
    print(f"\n{C(f'── {title} ──')}")

def kv(key: str, val: str, ok: bool | None = None) -> None:
    label = f"  {key:<30}"
    if ok is True:
        val_str = G(val)
    elif ok is False:
        val_str = R(val)
    else:
        val_str = val
    print(f"{label} {val_str}")

# ── System info ───────────────────────────────────────────────────────────────
def collect_system() -> dict:
    info: dict = {}

    # Kernel
    rc, out, _, _ = run(["uname", "-r"])
    info["kernel"] = out.strip() if rc == 0 else "unknown"

    rc, out, _, _ = run(["uname", "-m"])
    info["arch"] = out.strip() if rc == 0 else "unknown"

    # OS release
    for f in ("/etc/os-release", "/etc/debian_version", "/etc/alpine-release"):
        txt = read_file(f)
        if txt:
            info["os_release"] = txt.strip()
            break

    # CPU
    cpuinfo = read_file("/proc/cpuinfo")
    for line in cpuinfo.splitlines():
        if line.startswith("model name"):
            info["cpu_model"] = line.split(":", 1)[1].strip()
            break
    info["cpu_cores"] = os.cpu_count()

    # RAM
    meminfo = read_file("/proc/meminfo")
    for line in meminfo.splitlines():
        if line.startswith("MemTotal:"):
            kb = int(line.split()[1])
            info["ram_gb"] = round(kb / 1024 / 1024, 1)
            break

    return info

# ── Kernel modules ────────────────────────────────────────────────────────────
GPU_MODULES = [
    "i915", "xe", "amdgpu", "radeon", "nouveau",
    "nvidia", "nvidia_drm", "nvidia_uvm", "nvidia_modeset",
]

def collect_modules() -> dict[str, bool]:
    text = read_file("/proc/modules")
    loaded = {line.split()[0] for line in text.splitlines() if line}
    return {m: m in loaded for m in GPU_MODULES}

# ── DRI devices ───────────────────────────────────────────────────────────────
def collect_dri() -> list[dict]:
    devices = []
    for dev in sorted(_glob.glob("/dev/dri/renderD*")):
        entry: dict = {"path": dev}
        try:
            st = os.stat(dev)
            entry["mode"] = oct(st.st_mode)
            entry["gid"]  = st.st_gid
            import stat as _stat
            entry["world_readable"] = bool(st.st_mode & _stat.S_IROTH)
        except Exception as e:
            entry["stat_error"] = str(e)

        name = Path(dev).name
        for attr in ("vendor", "device"):
            p = f"/sys/class/drm/{name}/device/{attr}"
            try:
                entry[attr] = Path(p).read_text().strip()
            except Exception:
                pass

        uevent = read_file(f"/sys/class/drm/{name}/device/uevent")
        for line in uevent.splitlines():
            if line.startswith("DRIVER="):
                entry["kernel_driver"] = line.split("=", 1)[1]
            if line.startswith("PCI_ID="):
                entry["pci_id"] = line.split("=", 1)[1]

        devices.append(entry)
    return devices

# ── Packages / libraries ──────────────────────────────────────────────────────
def collect_packages() -> dict:
    pkgs: dict = {}

    # ffmpeg version + full build config
    rc, out, err, _ = run(["ffmpeg", "-version"])
    if rc == 0:
        lines = out.splitlines()
        pkgs["ffmpeg_version"] = lines[0] if lines else "?"
        for line in lines:
            if line.startswith("  configuration:"):
                pkgs["ffmpeg_config"] = line.replace("  configuration:", "").strip()
    else:
        pkgs["ffmpeg_version"] = None

    # ffprobe
    rc, out, _, _ = run(["ffprobe", "-version"])
    if rc == 0:
        pkgs["ffprobe_version"] = out.splitlines()[0] if out else "?"
    else:
        pkgs["ffprobe_version"] = None

    # vainfo
    rc, out, err, _ = run(["vainfo", "--version"])
    if rc != 0:
        rc, out, err, _ = run(["vainfo"])  # older vainfo has no --version
    pkgs["vainfo_available"] = rc in (0, 1) and ("VA-API" in out + err or "libva" in out + err)
    # grab version from output
    for line in (out + err).splitlines():
        if "VA-API version" in line or "vainfo version" in line:
            pkgs["vainfo_version"] = line.strip()
            break

    # dpkg / rpm packages
    pkg_list: dict = {}
    for name in ("intel-media-va-driver", "intel-media-va-driver-non-free",
                 "i965-va-driver", "mesa-va-drivers", "libva2", "libva-drm2",
                 "libmfx1", "libvpl2", "libvpl-dev",
                 "linux-firmware", "firmware-linux", "firmware-misc-nonfree"):
        rc2, out2, _, _ = run(["dpkg-query", "-W", "-f=${Version}", name])
        if rc2 == 0 and out2.strip():
            pkg_list[name] = out2.strip()
        else:
            rc3, out3, _, _ = run(["rpm", "-q", "--queryformat", "%{VERSION}", name])
            if rc3 == 0 and out3.strip() and "not installed" not in out3:
                pkg_list[name] = out3.strip()
    pkgs["installed_packages"] = pkg_list

    # Shared libraries present
    libs: dict = {}
    search_paths = ["/usr/lib", "/usr/lib/x86_64-linux-gnu",
                    "/usr/lib64", "/usr/local/lib"]
    for lib in ("libva.so", "libva-drm.so", "iHD_drv_video.so",
                "i965_drv_video.so", "libvpl.so", "libMFX.so",
                "libmfx.so", "libOpenCL.so"):
        found = None
        for sp in search_paths:
            hits = _glob.glob(f"{sp}/**/{lib}*", recursive=True)
            if hits:
                found = hits[0]
                break
        libs[lib] = found
    pkgs["libraries"] = libs

    return pkgs

# ── lspci ─────────────────────────────────────────────────────────────────────
def collect_lspci() -> list[str]:
    if not shutil.which("lspci"):
        return []
    lines = []
    for cls in ("0300", "0302", "0380"):
        rc, out, _, _ = run(["lspci", "-vmm", "-d", f"::{cls}"])
        if rc == 0 and out.strip():
            lines.append(out.strip())
    return lines

# ── VA-API profiles ───────────────────────────────────────────────────────────
def collect_vainfo(dev: str, driver: str | None = None) -> dict:
    env: dict = {}
    if driver:
        env["LIBVA_DRIVER_NAME"] = driver
    rc, out, err, _ = run(
        ["vainfo", "--display", "drm", "--device", dev],
        timeout=10, env_extra=env,
    )
    result: dict = {
        "returncode": rc,
        "stdout": out.strip(),
        "stderr": err.strip(),
        "profiles": [],
        "has_av1_encode": False,
        "has_hevc_encode": False,
        "has_h264_encode": False,
    }
    if rc == 0:
        for line in out.splitlines():
            line = line.strip()
            if "VAEntrypoint" in line:
                result["profiles"].append(line)
            if "AV1" in line and "EncSlice" in line:
                result["has_av1_encode"] = True
            if "HEVC" in line and "EncSlice" in line:
                result["has_hevc_encode"] = True
            if "H264" in line and "EncSlice" in line:
                result["has_h264_encode"] = True
    return result

# ── ffmpeg encoder list ───────────────────────────────────────────────────────
def collect_ff_encoders() -> set[str]:
    rc, out, _, _ = run(["ffmpeg", "-hide_banner", "-encoders"])
    encoders: set[str] = set()
    for line in out.splitlines():
        m = re.match(r"^\s+[A-Z.]+\s+(\S+)", line)
        if m:
            encoders.add(m.group(1))
    return encoders

def collect_ff_decoders() -> set[str]:
    rc, out, _, _ = run(["ffmpeg", "-hide_banner", "-decoders"])
    decoders: set[str] = set()
    for line in out.splitlines():
        m = re.match(r"^\s+[A-Z.]+\s+(\S+)", line)
        if m:
            decoders.add(m.group(1))
    return decoders

# ── Encode test builders ───────────────────────────────────────────────────────
IHD_ENV = {"LIBVA_DRIVER_NAME": "iHD"}
NULLSRC = "nullsrc=size=1920x1080:rate=24"
NULLSRC_23976 = "nullsrc=size=1920x1080:rate=24000/1001"
FRAMES = "30"

def _sw_encode(codec: str, extra: list | None = None) -> list:
    return (
        ["ffmpeg", "-hide_banner", "-v", "error",
         "-f", "lavfi", "-i", NULLSRC,
         "-vframes", FRAMES, "-c:v", codec]
        + (extra or [])
        + ["-f", "null", "-"]
    )

def _qsv_encode(codec: str, dev: str, extra: list | None = None,
                fps_filter: bool = False) -> list:
    vf = []
    if fps_filter:
        vf.append("fps=24")
    vf += ["format=nv12", "hwupload=extra_hw_frames=64"]
    return (
        ["ffmpeg", "-hide_banner", "-v", "error",
         "-init_hw_device", f"vaapi=va:{dev}",
         "-init_hw_device", "qsv=hw@va",
         "-filter_hw_device", "hw",
         "-f", "lavfi", "-i", NULLSRC_23976,
         "-vframes", FRAMES,
         "-vf", ",".join(vf),
         "-c:v", codec]
        + (extra or [])
        + ["-f", "null", "-"]
    )

def _vaapi_encode(codec: str, dev: str, extra: list | None = None) -> list:
    return (
        ["ffmpeg", "-hide_banner", "-v", "error",
         "-vaapi_device", dev,
         "-f", "lavfi", "-i", NULLSRC,
         "-vframes", FRAMES,
         "-vf", "format=nv12,hwupload",
         "-c:v", codec]
        + (extra or [])
        + ["-f", "null", "-"]
    )

def _nvenc_encode(codec: str, extra: list | None = None) -> list:
    return (
        ["ffmpeg", "-hide_banner", "-v", "error",
         "-f", "lavfi", "-i", NULLSRC,
         "-vframes", FRAMES, "-c:v", codec]
        + (extra or [])
        + ["-f", "null", "-"]
    )

# ── Decode test ───────────────────────────────────────────────────────────────
def _make_test_stream(codec: str = "libx264") -> bytes | None:
    """Encode 30 frames to a buffer with software for decode testing."""
    cmd = [
        "ffmpeg", "-hide_banner", "-v", "error",
        "-f", "lavfi", "-i", NULLSRC,
        "-vframes", "30", "-c:v", codec,
        "-f", "matroska", "-"
    ]
    rc, _, err, _ = run(["ffmpeg", "-hide_banner", "-encoders"])
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=30)
        if r.returncode == 0 and r.stdout:
            return r.stdout
    except Exception:
        pass
    return None

def _hw_decode(codec: str, hwaccel: str, dev: str,
               stream: bytes) -> tuple[int, str, str, float]:
    cmd = [
        "ffmpeg", "-hide_banner", "-v", "error",
        "-hwaccel", hwaccel,
        "-hwaccel_device", dev,
        "-c:v", codec,
        "-i", "pipe:0",
        "-f", "null", "-"
    ]
    return run(cmd, timeout=30, input_data=stream)

def _qsv_decode(codec: str, dev: str, stream: bytes) -> tuple[int, str, str, float]:
    cmd = [
        "ffmpeg", "-hide_banner", "-v", "error",
        "-init_hw_device", f"vaapi=va:{dev}",
        "-init_hw_device", "qsv=hw@va",
        "-c:v", codec,
        "-i", "pipe:0",
        "-f", "null", "-"
    ]
    return run(cmd, timeout=30, env_extra=IHD_ENV, input_data=stream)

# ── Single test runner ────────────────────────────────────────────────────────
class TestResult:
    def __init__(self, name: str, category: str, codec: str, hw: str,
                 rc: int, stderr: str, elapsed: float, skipped: bool = False,
                 skip_reason: str = ""):
        self.name       = name
        self.category   = category
        self.codec      = codec
        self.hw         = hw
        self.rc         = rc
        self.stderr     = stderr
        self.elapsed    = elapsed
        self.skipped    = skipped
        self.skip_reason= skip_reason

    @property
    def passed(self) -> bool:
        return not self.skipped and self.rc == 0

    def to_dict(self) -> dict:
        return {
            "name": self.name, "category": self.category,
            "codec": self.codec, "hw": self.hw,
            "passed": self.passed, "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "returncode": self.rc,
            "elapsed_s": round(self.elapsed, 2),
            "stderr_tail": self.stderr[-800:] if self.stderr else "",
        }

def run_test(name: str, category: str, codec: str, hw: str,
             cmd: list, env_extra: dict | None = None,
             skip_if: bool = False, skip_reason: str = "") -> TestResult:
    if skip_if:
        return TestResult(name, category, codec, hw, 0, "", 0,
                          skipped=True, skip_reason=skip_reason)
    rc, _, stderr, elapsed = run(cmd, timeout=60, env_extra=env_extra)
    return TestResult(name, category, codec, hw, rc, stderr, elapsed)

def run_decode_test(name: str, codec: str, hw: str,
                    cmd_fn, stream: bytes | None,
                    skip_if: bool = False, skip_reason: str = "") -> TestResult:
    if skip_if or stream is None:
        reason = skip_reason or "no test stream available"
        return TestResult(name, "decode", codec, hw, 0, "", 0,
                          skipped=True, skip_reason=reason)
    rc, _, stderr, elapsed = cmd_fn(stream)
    return TestResult(name, "decode", codec, hw, rc, stderr, elapsed)

# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="MediaStat HW acceleration test")
    parser.add_argument("--out", default="hw_test_report.json", help="JSON output path")
    parser.add_argument("--no-color", action="store_true")
    args = parser.parse_args()

    if args.no_color:
        global G, R, Y, C, B, DM
        G = R = Y = C = B = DM = lambda s: s  # noqa

    started_at = datetime.now().isoformat()
    all_results: list[TestResult] = []

    # ── Collect system info ────────────────────────────────────────────────
    section("SYSTEM INFORMATION")
    sys_info   = collect_system()
    modules    = collect_modules()
    dri_devs   = collect_dri()
    lspci_info = collect_lspci()
    packages   = collect_packages()
    ff_encoders = collect_ff_encoders()
    ff_decoders = collect_ff_decoders()

    subsection("OS / Hardware")
    kv("Kernel",   sys_info.get("kernel", "?"))
    kv("Arch",     sys_info.get("arch", "?"))
    kv("CPU",      sys_info.get("cpu_model", "?") + f" ({sys_info.get('cpu_cores')} cores)")
    kv("RAM",      f"{sys_info.get('ram_gb', '?')} GB")
    os_lines = sys_info.get("os_release", "").splitlines()
    for line in os_lines[:3]:
        kv("OS",   line.strip())

    subsection("GPU (lspci)")
    if lspci_info:
        for block in lspci_info:
            for line in block.splitlines():
                if line.strip():
                    print(f"  {DM(line)}")
    else:
        print(f"  {Y('lspci not available')}")

    subsection("DRI Devices")
    if dri_devs:
        for d in dri_devs:
            vid = d.get("vendor", "?")
            drv = d.get("kernel_driver", "?")
            mode = d.get("mode", "?")
            gid  = d.get("gid", "?")
            accessible = d.get("world_readable", False)
            kv(d["path"],
               f"vendor={vid}  driver={drv}  mode={mode}  gid={gid}",
               ok=accessible)
    else:
        kv("/dev/dri/renderD*", "NONE FOUND", ok=False)

    subsection("Kernel Modules")
    for mod, loaded in modules.items():
        kv(mod, "loaded" if loaded else "not loaded", ok=loaded if loaded else None)

    subsection("Component Versions")
    kv("ffmpeg",  packages.get("ffmpeg_version") or "NOT FOUND",
       ok=bool(packages.get("ffmpeg_version")))
    kv("ffprobe", packages.get("ffprobe_version") or "NOT FOUND",
       ok=bool(packages.get("ffprobe_version")))
    kv("vainfo",  packages.get("vainfo_version", "available" if packages.get("vainfo_available") else "NOT FOUND"),
       ok=packages.get("vainfo_available"))

    subsection("Installed Packages")
    for pkg, ver in packages.get("installed_packages", {}).items():
        kv(pkg, ver, ok=True)
    if not packages.get("installed_packages"):
        print(f"  {DM('(none of the tracked packages found via dpkg/rpm)')}")

    subsection("Key Libraries")
    for lib, path in packages.get("libraries", {}).items():
        kv(lib, path or "NOT FOUND", ok=bool(path))

    subsection("ffmpeg Build Config")
    config_line = packages.get("ffmpeg_config", "")
    if config_line:
        # Print key enable flags
        flags = re.findall(r"--enable-\S+", config_line)
        hw_flags = [f for f in flags if any(k in f for k in
            ("vaapi", "qsv", "vpl", "mfx", "drm", "opencl", "opengl",
             "nvenc", "cuvid", "v4l2"))]
        if hw_flags:
            print(f"  {DM('Hardware-related flags:')}")
            for f in hw_flags:
                print(f"  {DM(f)}")
        else:
            print(f"  {Y('No hardware flags detected in ffmpeg build')}")

    # ── VA-API profiles ────────────────────────────────────────────────────
    section("VA-API PROFILES")
    vainfo_results: dict = {}
    for dev in [d["path"] for d in dri_devs]:
        for driver, label in [("iHD", "iHD (Intel Gen9+)"),
                               (None,  "default (no LIBVA_DRIVER_NAME)")]:
            key = f"{dev}:{driver or 'default'}"
            subsection(f"{dev} — driver={label}")
            vi = collect_vainfo(dev, driver)
            vainfo_results[key] = vi
            if vi["returncode"] == 0:
                print(f"  {G('VA-API initialised successfully')}")
                kv("  AV1 encode",  "supported" if vi["has_av1_encode"]  else "not supported",
                   ok=vi["has_av1_encode"])
                kv("  HEVC encode", "supported" if vi["has_hevc_encode"] else "not supported",
                   ok=vi["has_hevc_encode"])
                kv("  H264 encode", "supported" if vi["has_h264_encode"] else "not supported",
                   ok=vi["has_h264_encode"])
                if vi["profiles"]:
                    print(f"  {DM('Profiles:')}")
                    for p in vi["profiles"]:
                        print(f"    {DM(p)}")
            else:
                print(f"  {R('VA-API failed to initialise')}")
                for line in (vi["stdout"] + "\n" + vi["stderr"]).splitlines():
                    if line.strip():
                        print(f"  {R(line.strip())}")

    # ── Encode tests ───────────────────────────────────────────────────────
    section("ENCODER TESTS")

    has_ffmpeg  = bool(packages.get("ffmpeg_version"))
    has_vaapi   = any(v["returncode"] == 0 for v in vainfo_results.values())
    has_nvidia  = bool(shutil.which("nvidia-smi"))
    render_devs = [d["path"] for d in dri_devs]
    primary_dev = render_devs[0] if render_devs else ""

    # Use iHD vainfo result to check codec support
    iHD_key = f"{primary_dev}:iHD" if primary_dev else ""
    iHD_va  = vainfo_results.get(iHD_key, {})

    def test_encode(name, category, codec, hw, cmd,
                    env_extra=None, skip_if=False, skip_reason=""):
        if not args.no_color:
            print(f"  Testing {B(name):<50}", end="", flush=True)
        t = run_test(name, category, codec, hw, cmd, env_extra,
                     skip_if=skip_if or not has_ffmpeg,
                     skip_reason=skip_reason or ("ffmpeg not found" if not has_ffmpeg else ""))
        all_results.append(t)
        status = SKIP if t.skipped else (PASS if t.passed else FAIL)
        suffix = f"  {t.elapsed:.1f}s" if not t.skipped else f"  {DM(t.skip_reason)}"
        if not args.no_color:
            print(f"{status}{suffix}")
        else:
            print(f"  {'SKIP' if t.skipped else ('PASS' if t.passed else 'FAIL'):<6} {name}{suffix}")
        if not t.passed and not t.skipped and t.stderr:
            # Print meaningful lines from stderr
            for line in t.stderr.splitlines()[-5:]:
                if line.strip() and not line.startswith("libva"):
                    print(f"         {R(line.strip())}")
        return t

    # Software encoders
    subsection("Software")
    for codec, extra in [
        ("libx264",   ["-preset", "fast"]),
        ("libx265",   ["-preset", "fast"]),
        ("libsvtav1", ["-preset", "6"]),
    ]:
        test_encode(
            f"SW {codec}", "encode", codec, "software",
            _sw_encode(codec, extra),
            skip_if=codec not in ff_encoders,
            skip_reason=f"{codec} not compiled into ffmpeg",
        )

    # Intel QSV
    subsection("Intel QSV (via VAAPI bridge)")
    for codec, fps_filter in [
        ("h264_qsv",  False),
        ("hevc_qsv",  False),
        ("av1_qsv",   True),   # needs fps=24 due to fractional fps rejection
    ]:
        has_qsv_hw = iHD_va.get("has_hevc_encode", False)  # proxy: if HEVC works, QSV init works
        skip = not primary_dev or codec not in ff_encoders
        reason = ("no DRI device" if not primary_dev
                  else f"{codec} not compiled into ffmpeg" if codec not in ff_encoders else "")
        test_encode(
            f"QSV {codec}", "encode", codec, "intel_qsv",
            _qsv_encode(codec, primary_dev, fps_filter=fps_filter),
            env_extra=IHD_ENV,
            skip_if=skip,
            skip_reason=reason,
        )

    # Intel VAAPI (direct — no QSV bridge)
    subsection("Intel VAAPI (direct)")
    for codec in ("h264_vaapi", "hevc_vaapi", "av1_vaapi"):
        skip = not primary_dev or codec not in ff_encoders
        reason = ("no DRI device" if not primary_dev
                  else f"{codec} not compiled into ffmpeg" if codec not in ff_encoders else "")
        test_encode(
            f"VAAPI {codec}", "encode", codec, "intel_vaapi",
            _vaapi_encode(codec, primary_dev),
            env_extra=IHD_ENV,
            skip_if=skip,
            skip_reason=reason,
        )

    # NVIDIA NVENC
    subsection("NVIDIA NVENC")
    for codec in ("h264_nvenc", "hevc_nvenc", "av1_nvenc"):
        skip = not has_nvidia or codec not in ff_encoders
        reason = ("nvidia-smi not found" if not has_nvidia
                  else f"{codec} not compiled into ffmpeg" if codec not in ff_encoders else "")
        test_encode(
            f"NVENC {codec}", "encode", codec, "nvidia_nvenc",
            _nvenc_encode(codec),
            skip_if=skip,
            skip_reason=reason,
        )

    # ── Decode tests ───────────────────────────────────────────────────────
    section("DECODER TESTS")

    # Build test streams
    print(f"  {DM('Building test streams...')}")
    h264_stream = _make_test_stream("libx264") if "libx264" in ff_encoders else None
    hevc_stream = _make_test_stream("libx265") if "libx265" in ff_encoders else None
    print(f"  H.264 test stream: {G('ok') if h264_stream else R('failed')}")
    print(f"  H.265 test stream: {G('ok') if hevc_stream else R('failed')}")

    def test_decode(name, codec, hw, cmd_fn, stream,
                    skip_if=False, skip_reason=""):
        if not args.no_color:
            print(f"  Testing {B(name):<50}", end="", flush=True)
        t = run_decode_test(name, codec, hw, cmd_fn, stream,
                            skip_if=skip_if, skip_reason=skip_reason)
        all_results.append(t)
        status = SKIP if t.skipped else (PASS if t.passed else FAIL)
        suffix = f"  {t.elapsed:.1f}s" if not t.skipped else f"  {DM(t.skip_reason)}"
        if not args.no_color:
            print(f"{status}{suffix}")
        else:
            print(f"  {'SKIP' if t.skipped else ('PASS' if t.passed else 'FAIL'):<6} {name}{suffix}")
        if not t.passed and not t.skipped and t.stderr:
            for line in t.stderr.splitlines()[-5:]:
                if line.strip() and not line.startswith("libva"):
                    print(f"         {R(line.strip())}")
        return t

    # VAAPI decode (hwaccel=vaapi)
    subsection("Intel VAAPI decode")
    for codec_name, codec_sw, stream in [
        ("h264", "h264", h264_stream),
        ("hevc", "hevc", hevc_stream),
    ]:
        skip = not primary_dev or not stream
        reason = ("no DRI device" if not primary_dev
                  else f"no {codec_name} test stream" if not stream else "")
        dev = primary_dev
        test_decode(
            f"VAAPI decode {codec_name}",
            codec_sw, "intel_vaapi",
            lambda s, d=dev, c=codec_sw: _hw_decode(c, "vaapi", d, s),
            stream, skip_if=skip, skip_reason=reason,
        )

    # QSV decode
    subsection("Intel QSV decode")
    for codec_name, qsv_codec, stream in [
        ("h264", "h264_qsv", h264_stream),
        ("hevc", "hevc_qsv", hevc_stream),
    ]:
        skip = not primary_dev or qsv_codec not in ff_decoders or not stream
        reason = ("no DRI device" if not primary_dev
                  else f"{qsv_codec} not in ffmpeg decoders" if qsv_codec not in ff_decoders
                  else f"no {codec_name} test stream" if not stream else "")
        dev = primary_dev
        test_decode(
            f"QSV decode {codec_name}",
            qsv_codec, "intel_qsv",
            lambda s, d=dev, c=qsv_codec: _qsv_decode(c, d, s),
            stream, skip_if=skip, skip_reason=reason,
        )

    # NVIDIA NVDEC / cuvid
    subsection("NVIDIA NVDEC (cuvid)")
    for codec_name, cuvid_codec, stream in [
        ("h264", "h264_cuvid", h264_stream),
        ("hevc", "hevc_cuvid", hevc_stream),
    ]:
        skip = not has_nvidia or cuvid_codec not in ff_decoders or not stream
        reason = ("nvidia-smi not found" if not has_nvidia
                  else f"{cuvid_codec} not in ffmpeg decoders" if cuvid_codec not in ff_decoders
                  else f"no {codec_name} test stream" if not stream else "")
        dev_idx = "0"
        test_decode(
            f"CUVID decode {codec_name}",
            cuvid_codec, "nvidia_nvdec",
            lambda s, d=dev_idx, c=cuvid_codec: _hw_decode(c, "cuda", d, s),
            stream, skip_if=skip, skip_reason=reason,
        )

    # ── Summary ────────────────────────────────────────────────────────────
    section("SUMMARY")
    passed  = [r for r in all_results if r.passed]
    failed  = [r for r in all_results if not r.passed and not r.skipped]
    skipped = [r for r in all_results if r.skipped]

    print(f"\n  {G(f'{len(passed)} passed')}  "
          f"{R(f'{len(failed)} failed')}  "
          f"{Y(f'{len(skipped)} skipped')}")

    if failed:
        print(f"\n  {B('Failed tests:')}")
        for r in failed:
            print(f"    {R('✗')} {r.name}")
            tail = [l for l in r.stderr.splitlines()
                    if l.strip() and not l.startswith("libva")][-2:]
            for line in tail:
                print(f"       {DM(line.strip())}")

    if skipped:
        print(f"\n  {B('Skipped tests:')}")
        for r in skipped:
            print(f"    {Y('–')} {r.name:<45} {DM(r.skip_reason)}")

    # ── Write JSON ─────────────────────────────────────────────────────────
    report = {
        "generated_at":    started_at,
        "summary": {
            "passed":  len(passed),
            "failed":  len(failed),
            "skipped": len(skipped),
        },
        "system":    sys_info,
        "modules":   modules,
        "dri_devices": dri_devs,
        "lspci":     lspci_info,
        "packages":  packages,
        "vainfo":    vainfo_results,
        "results":   [r.to_dict() for r in all_results],
    }
    out_path = args.out
    Path(out_path).write_text(json.dumps(report, indent=2))
    print(f"\n  {DM(f'Full report written to {out_path}')}\n")


if __name__ == "__main__":
    main()
