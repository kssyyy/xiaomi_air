import json
import math
import queue
import subprocess
import threading
import time
import tkinter as tk
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk

from miio import AirConditioningCompanion, DeviceException

try:
    from miio.integrations.lumi.acpartner import FanSpeed, Led, OperationMode, Power, SwingMode
except Exception:
    FanSpeed = Led = OperationMode = Power = SwingMode = None


CONFIG_PATH = Path("air_devices.json")
TIME_CONFIG_PATH = Path("time.json")
DEFAULT_CHECK_INTERVAL_SECONDS = 120
HIDE_CONSOLE_FLAG = getattr(subprocess, "CREATE_NO_WINDOW", 0)
MAX_DEVICES_PER_BATCH = 2
PING_TIMEOUT_MS = 500
UI_QUEUE_POLL_MS = 100
MIN_COMMAND_INTERVAL_SECONDS = 15
DEFAULT_FORCE_RESEND_SECONDS = 1800
CHECK_INTERVAL_SECONDS = DEFAULT_CHECK_INTERVAL_SECONDS
FORCE_RESEND_SECONDS = DEFAULT_FORCE_RESEND_SECONDS
LOG_MAX_LINES = 1000
DOUBLE_SEND_GAP_SECONDS = 2.0

COMMAND_MODEL_MAP = {
    "airconditioningcompanionmcn02": "lumi.acpartner.mcn02",
    "airconditioningcompanionv3": "lumi.acpartner.v3",
    "airconditioningcompanion": "lumi.acpartner.v1",
}

MODE_OPTIONS = [
    ("制冷", "cool"),
    ("制热", "heat"),
    ("自动", "auto"),
    ("除湿", "dry"),
    ("送风", "wind"),
]
MODE_LABEL_TO_VALUE = {label: value for label, value in MODE_OPTIONS}
MODE_VALUE_TO_LABEL = {value: label for label, value in MODE_OPTIONS}
MODE_DISPLAY_MAP = {
    "manual": "手动",
    "auto": "自动",
}
STATE_LABEL_MAP = {"on": "开", "off": "关"}


def load_time_settings(path: Path) -> tuple[int, int]:
    interval = DEFAULT_CHECK_INTERVAL_SECONDS
    force = DEFAULT_FORCE_RESEND_SECONDS
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            interval = int(data.get("check_interval_seconds", interval))
            force = int(data.get("force_resend_seconds", force))
    except Exception:
        pass
    if interval < 1:
        interval = DEFAULT_CHECK_INTERVAL_SECONDS
    if force < 1:
        force = DEFAULT_FORCE_RESEND_SECONDS
    return interval, force


def save_time_settings(path: Path, interval: int, force: int):
    payload = {
        "check_interval_seconds": int(interval),
        "force_resend_seconds": int(force),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


@dataclass
class DeviceConfig:
    name: str
    monitor_ips: list[str]
    ac_ip: str
    token: str
    command: str = "airconditioningcompanionmcn02"
    ping_mode: str = "any"
    model: str = ""
    ac_model: str = ""


class DeviceRow:
    def __init__(self, parent: tk.Widget, config: DeviceConfig, request_cb):
        self.config = config
        self.request_cb = request_cb

        self.mode = tk.StringVar(value=MODE_DISPLAY_MAP["auto"])
        self.state_text = tk.StringVar(value="未知")
        self.ping_text = tk.StringVar(value="-")

        self.mode_state = "auto"
        self.last_sent_action = None
        self.last_send_ts = 0.0
        self.last_decision = None
        self.last_change_ts = 0.0
        self.client = None

        self.ac_mode_var = tk.StringVar(value="制冷")
        self.ac_temp_var = tk.IntVar(value=26)

        row = tk.Frame(parent, bd=1, relief=tk.GROOVE, padx=8, pady=6)
        row.pack(fill=tk.X, padx=6, pady=4)

        monitors = ", ".join(config.monitor_ips)
        title = tk.Label(
            row,
            text=f"{config.name}  |  monitor: [{monitors}] ({config.ping_mode})  |  ac: {config.ac_ip}",
            anchor="w",
        )
        title.pack(fill=tk.X)

        info = tk.Frame(row)
        info.pack(fill=tk.X, pady=(4, 0))
        tk.Label(info, text="模式:").pack(side=tk.LEFT)
        tk.Label(info, textvariable=self.mode, width=8).pack(side=tk.LEFT)
        tk.Label(info, text="Ping:").pack(side=tk.LEFT, padx=(12, 0))
        tk.Label(info, textvariable=self.ping_text, width=12).pack(side=tk.LEFT)
        tk.Label(info, text="空调:").pack(side=tk.LEFT, padx=(12, 0))
        tk.Label(info, textvariable=self.state_text, width=16).pack(side=tk.LEFT)

        actions = tk.Frame(row)
        actions.pack(fill=tk.X, pady=(6, 0))
        tk.Button(actions, text="开", width=8, command=self.manual_on).pack(side=tk.LEFT)
        tk.Button(actions, text="关", width=8, command=self.manual_off).pack(side=tk.LEFT, padx=6)
        tk.Button(actions, text="自动", width=8, command=self.set_auto).pack(side=tk.LEFT)
        tk.Button(actions, text="手动", width=8, command=self.set_manual_mode).pack(side=tk.LEFT, padx=6)

        hvac = tk.Frame(row)
        hvac.pack(fill=tk.X, pady=(6, 0))
        tk.Label(hvac, text="空调模式:").pack(side=tk.LEFT)
        ttk.Combobox(
            hvac,
            textvariable=self.ac_mode_var,
            values=[label for label, _ in MODE_OPTIONS],
            width=8,
            state="readonly",
        ).pack(side=tk.LEFT, padx=(4, 8))
        tk.Label(hvac, text="温度:").pack(side=tk.LEFT)
        tk.Spinbox(hvac, from_=16, to=30, textvariable=self.ac_temp_var, width=5).pack(
            side=tk.LEFT, padx=(4, 8)
        )
        tk.Button(hvac, text="应用模式+温度", command=self.apply_mode_temp).pack(side=tk.LEFT)

    def set_mode_ui(self, mode: str):
        self.mode_state = mode
        self.mode.set(MODE_DISPLAY_MAP.get(mode, mode))

    def set_auto(self):
        self.set_mode_ui("auto")
        self.request_cb("log", self, f"{self.config.name}: 模式 -> 自动")

    def set_manual_mode(self):
        self.set_mode_ui("manual")
        self.request_cb("ui", self, {"ping": "手动空闲"})
        self.request_cb("log", self, f"{self.config.name}: 模式 -> 手动")

    def manual_on(self):
        self.request_cb("action_now", self, {"action": "on"})

    def manual_off(self):
        self.request_cb("action_now", self, {"action": "off"})

    def apply_mode_temp(self):
        mode_label = self.ac_mode_var.get().strip()
        mode = MODE_LABEL_TO_VALUE.get(mode_label)
        if not mode:
            self.request_cb("log", self, f"{self.config.name}: 无效模式 {mode_label}")
            return
        temp = int(self.ac_temp_var.get())
        self.request_cb("mode_temp_now", self, {"mode": mode, "temp": temp})

    def get_client(self) -> AirConditioningCompanion:
        if self.client is None:
            self.client = AirConditioningCompanion(
                ip=self.config.ac_ip,
                token=self.config.token,
                model=self.get_model(),
            )
        return self.client

    def get_model(self) -> str:
        if self.config.model:
            return self.config.model
        return COMMAND_MODEL_MAP.get(self.config.command, "lumi.acpartner.mcn02")

    def resolve_ac_model(self) -> str:
        if self.config.ac_model:
            return self.config.ac_model
        try:
            st = self.get_client().status()
            candidate = getattr(st, "air_condition_model", None)
            if candidate:
                return str(candidate)
        except Exception:
            pass
        return self.get_model()

    def ping_ok(self, host: str) -> bool:
        cmd = ["ping", "-n", "1", "-w", str(PING_TIMEOUT_MS), host]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            creationflags=HIDE_CONSOLE_FLAG,
        )
        return result.returncode == 0


class ACControllerApp:
    def __init__(self, root: tk.Tk, configs: list[DeviceConfig]):
        self.root = root
        self.root.title("空调自动控制 (python-miio v8)")
        interval, force = load_time_settings(TIME_CONFIG_PATH)
        self.running = True
        self.interval_seconds = interval
        self.interval_var = tk.StringVar(value=str(interval))
        self.force_resend_seconds = force
        self.force_resend_var = tk.StringVar(value=str(force))
        self.next_row_index = 0

        self.work_queue: queue.Queue = queue.Queue()
        self.ui_queue: queue.Queue = queue.Queue()

        top = tk.Frame(root)
        top.pack(fill=tk.X, padx=8, pady=8)

        tk.Button(top, text="全部自动", command=self.all_auto).pack(side=tk.LEFT)
        tk.Button(top, text="全部手动", command=self.all_manual).pack(side=tk.LEFT, padx=6)
        tk.Button(top, text="全部开", command=self.all_on).pack(side=tk.LEFT, padx=6)
        tk.Button(top, text="全部关", command=self.all_off).pack(side=tk.LEFT)
        tk.Label(top, text="轮询间隔(秒):").pack(side=tk.LEFT, padx=(12, 0))
        tk.Entry(top, textvariable=self.interval_var, width=6).pack(side=tk.LEFT, padx=(4, 0))
        tk.Label(top, text="强制重发(秒):").pack(side=tk.LEFT, padx=(12, 0))
        tk.Entry(top, textvariable=self.force_resend_var, width=6).pack(side=tk.LEFT, padx=(4, 0))
        tk.Button(top, text="应用", command=self.apply_interval).pack(side=tk.LEFT, padx=6)

        self.status = tk.StringVar(value="运行中")
        tk.Label(top, textvariable=self.status, fg="blue").pack(side=tk.RIGHT)

        list_wrapper = tk.Frame(root)
        list_wrapper.pack(fill=tk.BOTH, expand=True, padx=8)

        self.canvas = tk.Canvas(list_wrapper, highlightthickness=0)
        self.scrollbar = tk.Scrollbar(list_wrapper, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.container = tk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0), window=self.container, anchor="nw")

        self.container.bind("<Configure>", self._on_frame_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

        self.rows = [DeviceRow(self.container, cfg, self.enqueue_request) for cfg in configs]

        self.log_box = tk.Text(root, height=10)
        self.log_box.pack(fill=tk.BOTH, expand=False, padx=8, pady=8)

        self.worker = threading.Thread(target=self.worker_loop, daemon=True)
        self.worker.start()

        self.root.after(UI_QUEUE_POLL_MS, self.process_ui_queue)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def enqueue_request(self, kind: str, row: DeviceRow, payload):
        if kind == "log":
            self.ui_queue.put(("log", payload))
            return
        if kind == "action_now":
            threading.Thread(
                target=self.send_ac,
                args=(row, payload["action"]),
                daemon=True,
            ).start()
            return
        if kind == "mode_temp_now":
            threading.Thread(
                target=self.apply_mode_temp_worker,
                args=(row, payload["mode"], payload["temp"]),
                daemon=True,
            ).start()
            return
        self.work_queue.put((kind, row, payload))

    def _on_frame_configure(self, _event):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        self.canvas.itemconfigure(self.canvas_window, width=event.width)

    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def process_ui_queue(self):
        while True:
            try:
                item = self.ui_queue.get_nowait()
            except queue.Empty:
                break

            kind = item[0]
            if kind == "log":
                self._append_log(item[1])
            elif kind == "update":
                row, data = item[1], item[2]
                if "state" in data:
                    row.state_text.set(data["state"])
                if "ping" in data:
                    row.ping_text.set(data["ping"])

        if self.running:
            self.root.after(UI_QUEUE_POLL_MS, self.process_ui_queue)

    def log(self, msg: str):
        self.ui_queue.put(("log", msg))

    def _append_log(self, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}\n"
        self.log_box.insert(tk.END, line)
        # Keep only recent logs to prevent long-run memory/performance growth.
        line_count = int(float(self.log_box.index("end-1c").split(".")[0]))
        if line_count > LOG_MAX_LINES:
            remove_lines = line_count - LOG_MAX_LINES
            self.log_box.delete("1.0", f"{remove_lines + 1}.0")
        self.log_box.see(tk.END)

    def all_auto(self):
        for row in self.rows:
            row.set_auto()

    def all_manual(self):
        for row in self.rows:
            row.set_manual_mode()

    def all_on(self):
        for row in self.rows:
            self.work_queue.put(("action", row, {"action": "on"}))

    def all_off(self):
        for row in self.rows:
            self.work_queue.put(("action", row, {"action": "off"}))

    def apply_interval(self):
        raw_interval = self.interval_var.get().strip()
        raw_force = self.force_resend_var.get().strip()
        try:
            interval_value = int(raw_interval)
            force_value = int(raw_force)
            if interval_value < 1 or force_value < 1:
                raise ValueError
        except Exception:
            messagebox.showerror("错误", "轮询间隔和强制重发必须是大于等于 1 的整数")
            self.interval_var.set(str(self.interval_seconds))
            self.force_resend_var.set(str(self.force_resend_seconds))
            return
        self.interval_seconds = interval_value
        self.force_resend_seconds = force_value
        try:
            save_time_settings(TIME_CONFIG_PATH, self.interval_seconds, self.force_resend_seconds)
        except Exception as exc:
            self.log(f"保存time.json失败: {exc}")
        self.log(
            f"参数已更新: 轮询间隔={self.interval_seconds}秒, "
            f"强制重发={self.force_resend_seconds}秒"
        )

    def send_ac(self, row: DeviceRow, action: str):
        succeeded = False
        for attempt in (1, 2):
            try:
                client = row.get_client()
                if action == "on":
                    client.on()
                else:
                    client.off()
                succeeded = True
                self.log(
                    f"{row.config.name}: 第{attempt}次发送成功 -> "
                    f"{STATE_LABEL_MAP.get(action, action)}"
                )
            except DeviceException as exc:
                row.client = None
                self.log(
                    f"{row.config.name}: 第{attempt}次发送失败 -> "
                    f"{STATE_LABEL_MAP.get(action, action)}: {exc}"
                )
            except Exception as exc:
                self.log(
                    f"{row.config.name}: 第{attempt}次发送失败 -> "
                    f"{STATE_LABEL_MAP.get(action, action)}: {exc}"
                )
            if attempt == 1:
                time.sleep(DOUBLE_SEND_GAP_SECONDS)

        if succeeded:
            row.last_sent_action = action
            row.last_send_ts = time.time()
            self.ui_queue.put(("update", row, {"state": STATE_LABEL_MAP.get(action, action)}))
            self.log(f"{row.config.name}: 空调 -> {STATE_LABEL_MAP.get(action, action)}")
        else:
            self.log(f"{row.config.name}: 空调{STATE_LABEL_MAP.get(action, action)}失败")

    def apply_mode_temp_worker(self, row: DeviceRow, mode: str, temp: int):
        if mode not in MODE_VALUES:
            self.log(f"{row.config.name}: 无效模式 {mode}")
            return
        if temp < 16 or temp > 30:
            self.log(f"{row.config.name}: 无效温度 {temp}")
            return

        try:
            ok = False
            client = row.get_client()
            if hasattr(client, "send_configuration") and all([OperationMode, Power, FanSpeed, SwingMode]):
                mode_map = {
                    "cool": OperationMode.Cool,
                    "heat": OperationMode.Heat,
                    "auto": OperationMode.Auto,
                    "dry": OperationMode.Dehumidify,
                    "wind": OperationMode.Ventilate,
                }
                kwargs = {
                    "model": row.resolve_ac_model(),
                    "power": Power.On,
                    "operation_mode": mode_map[mode],
                    "target_temperature": temp,
                    "fan_speed": FanSpeed.Auto,
                    "swing_mode": SwingMode.Off,
                }
                if Led is not None:
                    kwargs["led"] = Led.On
                try:
                    client.send_configuration(**kwargs)
                    ok = True
                except Exception:
                    ok = False

            if not ok:
                client.send("set_mode", [mode])
                client.send("set_tar_temp", [temp])
                client.send("set_power", ["on"])
                ok = True

            if ok:
                row.last_sent_action = "on"
                mode_label = MODE_VALUE_TO_LABEL.get(mode, mode)
                self.ui_queue.put(("update", row, {"state": f"{mode_label}/{temp}C"}))
                self.log(f"{row.config.name}: 设置模式={mode_label}, 温度={temp}")
        except DeviceException as exc:
            row.client = None
            self.log(f"{row.config.name}: 设置模式/温度失败: {exc}")
        except Exception as exc:
            self.log(f"{row.config.name}: 设置模式/温度失败: {exc}")

    def tick_row(self, row: DeviceRow):
        mode = row.mode_state
        if mode == "manual":
            self.ui_queue.put(("update", row, {"ping": "手动空闲"}))
            return

        if mode == "auto":
            results = [row.ping_ok(host) for host in row.config.monitor_ips]
            up_count = sum(1 for x in results if x)
            self.ui_queue.put(("update", row, {"ping": f"{up_count}/{len(results)} 在线"}))

            reachable = all(results) if row.config.ping_mode == "all" else any(results)
            # Auto-close only: when reachable, do nothing; when unreachable, force off.
            if reachable:
                self.ui_queue.put(("update", row, {"state": "未知(未校验)"}))
                # Clear cached command state so next required off command will be sent.
                row.last_sent_action = None
                row.last_decision = None
                return
            action = "off"

            now_ts = time.time()
            force_due = now_ts - row.last_send_ts >= self.force_resend_seconds
            action_changed = action != row.last_decision
            in_debounce_window = now_ts - row.last_change_ts < MIN_COMMAND_INTERVAL_SECONDS
            should_send = (action != row.last_sent_action) or force_due

            if not action_changed and in_debounce_window and not force_due:
                return

            row.last_decision = action
            row.last_change_ts = now_ts

            if should_send:
                self.send_ac(row, action)

    def worker_loop(self):
        while self.running:
            # High-priority user actions first
            while True:
                try:
                    kind, row, payload = self.work_queue.get_nowait()
                except queue.Empty:
                    break

                if kind == "action":
                    self.send_ac(row, payload["action"])
                elif kind == "mode_temp":
                    self.apply_mode_temp_worker(row, payload["mode"], payload["temp"])
                elif kind == "ui":
                    self.ui_queue.put(("update", row, payload))

            if not self.rows:
                time.sleep(1)
                continue

            total = len(self.rows)
            batch_size = min(MAX_DEVICES_PER_BATCH, total)
            batch = []
            for _ in range(batch_size):
                batch.append(self.rows[self.next_row_index])
                self.next_row_index = (self.next_row_index + 1) % total

            for row in batch:
                try:
                    self.tick_row(row)
                except Exception as exc:
                    self.log(f"{row.config.name}: 轮询异常: {exc}")

            batches_per_round = max(1, math.ceil(total / batch_size))
            sleep_seconds = max(0.5, self.interval_seconds / batches_per_round)
            time.sleep(sleep_seconds)

    def on_close(self):
        self.running = False
        self.status.set("已停止")
        try:
            self.canvas.bind_all("<MouseWheel>", "")
        except Exception:
            pass
        self.root.destroy()


def load_config(path: Path) -> list[DeviceConfig]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        raise ValueError("Config must be a non-empty JSON array")

    result = []
    for item in data:
        monitor_ips = item.get("monitor_ips")
        if not monitor_ips:
            monitor_ip = item.get("monitor_ip")
            if monitor_ip:
                monitor_ips = [monitor_ip]
            else:
                raise ValueError(f"{item.get('name', 'unknown')}: missing monitor_ips")

        ping_mode = item.get("ping_mode", "any")
        if ping_mode not in {"any", "all"}:
            raise ValueError(f"{item.get('name', 'unknown')}: ping_mode must be 'any' or 'all'")

        result.append(
            DeviceConfig(
                name=item["name"],
                monitor_ips=monitor_ips,
                ac_ip=item["ac_ip"],
                token=item["token"],
                command=item.get("command", "airconditioningcompanionmcn02"),
                ping_mode=ping_mode,
                model=item.get("model", ""),
                ac_model=item.get("ac_model", ""),
            )
        )
    return result


def main():
    try:
        configs = load_config(CONFIG_PATH)
    except Exception as exc:
        messagebox.showerror("配置错误", str(exc))
        return

    root = tk.Tk()
    root.geometry("980x760")
    app = ACControllerApp(root, configs)
    app.log(
        f"已加载 {len(configs)} 台空调。轮询间隔={app.interval_seconds}秒, 批量={MAX_DEVICES_PER_BATCH}"
    )
    root.mainloop()


if __name__ == "__main__":
    main()
