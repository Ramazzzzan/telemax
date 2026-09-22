Here is a complete step-by-step Deployment Guide (DEPLOYMENT.md) for setting up Telemax on a new Linux server.
Telemax Deployment Guide
This guide covers installing, configuring, and deploying the Telemax MAX-to-Telegram Bridge on a clean Linux system.
1. Prerequisites
Before starting, ensure your system meets the following requirements:
OS: Linux (Ubuntu 20.04+, Debian 11+, or similar)
Python: Python 3.10 or newer
System Utilities: curl, sqlite3, systemd


Proxy Server: A running SOCKS5 proxy on 127.0.0.1:10808 (required for Telegram Bot API traffic).
Traffic Routing Rule:
Telegram API: Routed through SOCKS5 proxy (socks5h://127.0.0.1:10808).
MAX Messenger API: Connected directly (no proxy).
2. Directory Structure & constants.json
Create the project directory structure under /home/htpc/telemax:



Bash
mkdir -p /home/htpc/telemax/media_queue
mkdir -p /home/htpc/telemax/dumps
cd /home/htpc/telemax


Directory Layout



Plaintext
/home/htpc/telemax/
├── constants.json          # Credentials and tokens
├── telemax.py              # Main application script
├── telegram_queue.db       # Auto-created SQLite database
├── telemax.log             # Application execution log
├── media_queue/            # Temporary media download folder
├── dumps/                  # Raw JSON event dumps
└── venv/                   # Isolated Python environment


Structure of constants.json
Create /home/htpc/telemax/constants.json with your credentials:



JSON
{
  "MAX_PHONE": "+79001234567",
  "TG_BOT_TOKEN": "1234567890:ABCdefGHIjklMNOpqrsTUVwxy-z",
  "TG_CHAT_ID": "-1001234567890",
  "NTFY_URL": "https://ntfy.sh/your_topic_name",
  "MY_MAX_ID": 119079316
}


Field Descriptions:
MAX_PHONE: The phone number associated with your MAX Messenger account (formatted with country code).
TG_BOT_TOKEN: Telegram Bot Token created via @BotFather.
TG_CHAT_ID: Telegram Forum Supergroup Chat ID where topics will be generated.
NTFY_URL (Optional): NTFY push notification endpoint URL for critical alerts. Set to "" or null if unused.
MY_MAX_ID (Optional): Your personal MAX User ID to prevent the bot from forwarding your own outgoing messages.
3. Python Virtual Environment Setup
Set up an isolated virtual environment and install dependencies using the SOCKS5 proxy:



Bash
cd /home/htpc/telemax

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip tools
pip install --upgrade pip setuptools

# 1. Download and install PySocks wheel (enables SOCKS support in pip)
curl -x socks5h://127.0.0.1:10808 -A "Mozilla/5.0" -L -o PySocks-1.7.1-py3-none-any.whl "https://files.pythonhosted.org/packages/6c/a0/da868b832d93d20eb99e3503a65ed8a631156a938f3ef58c6a3abcf03542/PySocks-1.7.1-py3-none-any.whl"
pip install PySocks-1.7.1-py3-none-any.whl
rm PySocks-1.7.1-py3-none-any.whl

# 2. Install required packages via SOCKS5 proxy
pip install --proxy socks5://127.0.0.1:10808 maxapi-python requests


4. First-Time Authentication
Run the script manually once to complete SMS and 2FA authentication for MAX Messenger:



Bash
/home/htpc/telemax/venv/bin/python /home/htpc/telemax/telemax.py


Enter the SMS code sent to your phone when prompted.
Enter your 2FA password (if enabled on your account).
Once you see log lines indicating session saved and Запуск MAX-TG Bridge..., press Ctrl + C to stop the script.
5. Systemd Service Setup
Create a systemd unit file to manage Telemax as a background service with auto-restart and watchdog monitoring[cite: 2].
Create /etc/systemd/system/telemax.service:



Bash
sudo nano /etc/systemd/system/telemax.service


Paste the following configuration:



Ini, TOML
[Unit]
Description=Telemax MAX-to-Telegram Bridge
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=notify
User=htpc
Group=htpc
WorkingDirectory=/home/htpc/telemax
ExecStart=/home/htpc/telemax/venv/bin/python /home/htpc/telemax/telemax.py
Restart=always
RestartSec=10
WatchdogSec=30

[Install]
WantedBy=multi-user.target


Enable and Start Service



Bash
# Reload systemd configuration
sudo systemctl daemon-reload

# Enable service auto-start on boot
sudo systemctl enable telemax.service

# Start the service
sudo systemctl start telemax.service


6. Service Management & Troubleshooting
View Service Status



Bash
sudo systemctl status telemax.service


View Live Logs



Bash
# Systemd journal output
journalctl -u telemax.service -f -n 50

# Application log file
tail -f /home/htpc/telemax/telemax.log


Telegram Bot Commands
Once running, send these commands inside your Telegram Forum Group[cite: 2]:
/status — Displays live bridge, MAX API, and queue statistics[cite: 2].
/dlq — Shows up to 20 failed/stuck messages in the Dead Letter Queue[cite: 2].
/retry_dlq — Re-queues all failed messages from DLQ back into processing queue.
/clear_dlq — Clears DLQ records and deletes stranded media files[cite: 2].
/alias <Name> — Renames contact and Telegram Forum Topic (must be sent inside a topic)[cite: 2].
