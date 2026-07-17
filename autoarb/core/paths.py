from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # core/ -> autoarb package root

# Папки
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"

# Убедимся, что папки существуют при импорте
DATA_DIR.mkdir(exist_ok=True)
CONFIG_DIR.mkdir(exist_ok=True)

# Конфиг: config/config.json5 → config/config.json → (старый) корень
_cfg_json5 = CONFIG_DIR / "config.json5"
_cfg_json = CONFIG_DIR / "config.json"
_cfg_json5_root = BASE_DIR / "config.json5"
_cfg_json_root = BASE_DIR / "config.json"
if _cfg_json5.is_file():
    CONFIG_PATH = _cfg_json5
elif _cfg_json.is_file():
    CONFIG_PATH = _cfg_json
elif _cfg_json5_root.is_file():
    CONFIG_PATH = _cfg_json5_root
elif _cfg_json_root.is_file():
    CONFIG_PATH = _cfg_json_root
else:
    CONFIG_PATH = _cfg_json5

# Файлы данных / состояния
DATES_OF_CHECK = DATA_DIR / "dates_of_check.txt"
CHECKED_ITEMS = DATA_DIR / "checked_items.txt"
GUARANTEE_TXT = DATA_DIR / "Guarantee.txt"
BLACKLIST_TXT = DATA_DIR / "Blacklist.txt"
VALID_HISTORY = DATA_DIR / "valid_check_history.txt"
PROLIV_QUEUE = DATA_DIR / "proliv_queue.json"
PROLIV_HISTORY = DATA_DIR / "proliv_history.txt"
PIPELINE_LOG = DATA_DIR / "pipeline_log.txt"
CLAIM_HISTORY = DATA_DIR / "claim_history.txt"
CLAIM_RATE_FILE = DATA_DIR / "claim_last_sent_unix.txt"
TELEGRAM_ERR_LOG = DATA_DIR / "telegram_notify_errors.txt"
RESOLD_FILE = DATA_DIR / "resold_items.json"
VALIDATION_ERRORS = DATA_DIR / "validation_errors.json"
TRANSFER_LOG = DATA_DIR / "transfer_log.txt"
TRANSFER_SETTINGS = DATA_DIR / "transfer_settings.json"
TRANSFERRED_ITEMS = DATA_DIR / "transferred_items.json"
TRANSFER_SECRET = DATA_DIR / "transfer_secret.dpapi"
CONFIG_SECRETS = DATA_DIR / "config_secrets.dpapi"
