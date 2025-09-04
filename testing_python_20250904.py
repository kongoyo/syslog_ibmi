import logging
import os
import sys
import time
import json
from typing import Optional

from logging.handlers import SysLogHandler
import pyodbc
from dotenv import load_dotenv


def setup_syslog_logging(server='127.0.0.1', port=514):
    """設定 Syslog 處理器"""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = SysLogHandler(address=(server, port))
    logger.addHandler(handler)
    return logger


class IbmiJournalMonitor:
    """管理 IBM i 稽核日誌的連線、查詢和日誌發送。"""

    # 將 severity_map 移至類別層級，因為它對所有實例都是一樣的
    SEVERITY_MAP = {
        0: logging.CRITICAL, # Emergency
        1: logging.CRITICAL, # Alert
        2: logging.CRITICAL, # Critical
        3: logging.ERROR,    # Error
        4: logging.WARNING,  # Warning
        5: logging.INFO,     # Notice
        6: logging.INFO,     # Informational
        7: logging.DEBUG,    # Debug
    }

    def __init__(self, host, user, password, driver, logger, journal_lib, journal_name, journal_types, interval):
        # 在連線字串中加入 DefaultLibraries=QGPL 來穩定連線環境
        # 加入 NAMING=1 (System Naming) 和 DefaultLibraries=QGPL 來穩定連線環境
        self.conn_str = f"DRIVER={{{driver}}};SYSTEM={host};UID={user};PWD={password};DBQ=,QGPL;"
        self.logger = logger
        self.interval = interval
        self.journal_lib = journal_lib
        self.journal_name = journal_name
        # 將逗號分隔的字串轉換為列表，並過濾掉空字串
        self.journal_types = [t.strip() for t in journal_types.split(',') if t.strip()]

        # 狀態持久化設定
        self.state_file = 'journal_monitor.state'
        # 狀態管理：追蹤最後處理的日誌條目
        self.last_receiver_name: Optional[str] = None
        self.last_sequence_number: Optional[int] = None
        self._load_state()

    def _load_state(self):
        """從檔案載入上次處理的日誌位置。"""
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                self.last_receiver_name = state.get('last_receiver_name')
                self.last_sequence_number = state.get('last_sequence_number')
                if self.last_receiver_name and self.last_sequence_number:
                    self.logger.info(f"Loaded state, starting from: {self.last_receiver_name}/{self.last_sequence_number + 1}")
        except FileNotFoundError:
            self.logger.info("State file not found, starting from the beginning.")
        except (json.JSONDecodeError, TypeError):
            self.logger.exception("Error loading state file. Starting from the beginning.")

    def _save_state(self):
        """將當前處理的日誌位置儲存到檔案。"""
        if not self.last_receiver_name or not self.last_sequence_number:
            return # 沒有有效狀態可儲存
        state = {
            'last_receiver_name': self.last_receiver_name,
            'last_sequence_number': self.last_sequence_number,
        }
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
        except IOError:
            self.logger.exception("Error saving state file:")

    def _process_one_batch(self):
        """連接、獲取一批新的日誌條目、處理它們並更新狀態。"""
        conn = None
        cursor = None

        # --- 準備查詢 ---
        journal_call_args = ["?, ?"]
        params = [self.journal_lib, self.journal_name]

        # 如果有已儲存的狀態，則加入起始點參數，從下一筆開始讀取
        if self.last_receiver_name and self.last_sequence_number:
            journal_call_args.extend(["STARTING_RECEIVER_NAME => ?", "STARTING_SEQUENCE => ?"])
            params.extend([self.last_receiver_name, self.last_sequence_number + 1])

        journal_call_args.append("GENERATE_SYSLOG => 'RFC5424'")
        journal_call_str = ', '.join(journal_call_args)

        sql = f"""
            SELECT syslog_facility, syslog_severity, syslog_event, journal_entry_type,
                   receiver_name, sequence_number
            FROM TABLE (QSYS2.DISPLAY_JOURNAL({journal_call_str})) AS X
        """

        # 動態建立 WHERE 條件
        where_clauses = ["syslog_event IS NOT NULL"]
        if self.journal_types:
            placeholders = ', '.join('?' for _ in self.journal_types)
            where_clauses.append(f"journal_entry_type IN ({placeholders})")
            params.extend(self.journal_types)

        sql += " WHERE " + " AND ".join(where_clauses)

        try:
            conn = pyodbc.connect(self.conn_str, autocommit=True)
            cursor = conn.cursor()

            print("Checking for new journal entries...")
            rows = list(cursor.execute(sql, params))
            count = len(rows)

            if count == 0:
                print("No new journal entries found in this cycle.")
                return

            print(f"Found {count} new entries. Processing and sending to syslog...")

            for row in rows:
                syslog_event = row.SYSLOG_EVENT
                syslog_severity = row.SYSLOG_SEVERITY
                log_level = self.SEVERITY_MAP.get(syslog_severity, logging.INFO)
                self.logger.log(log_level, syslog_event)
            
            # 處理完畢，從最後一筆記錄更新書籤狀態
            last_row = rows[-1]
            self.last_receiver_name = last_row.RECEIVER_NAME
            # 將 Decimal 物件轉換為 int，以利 JSON 序列化
            self.last_sequence_number = int(last_row.SEQUENCE_NUMBER)

            print(f"Finished sending {count} entries. New bookmark: {self.last_receiver_name}/{self.last_sequence_number}")
            self.logger.info(f"Processed {count} entries. New state: {self.last_receiver_name}/{self.last_sequence_number}")
            
            # 儲存新的書籤狀態到檔案
            self._save_state()

        except pyodbc.Error as e:
            # 使用 logger.exception 可以自動記錄堆疊追蹤，更利於除錯
            self.logger.exception("Database cycle failed:")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def start(self):
        """啟動持續監控的迴圈。"""
        print("Starting IBM i Journal Monitor. Press Ctrl+C to stop.")
        self.logger.info("Daemon started.")
        while True:
            try:
                self._process_one_batch()
                print(f"Waiting for {self.interval} seconds before next check...")
                time.sleep(self.interval)
            except KeyboardInterrupt:
                print("\nShutdown signal received. Exiting.")
                self.logger.info("Daemon stopped by user.")
                break

if __name__ == "__main__":
    # 載入 .env 檔案中的環境變數
    load_dotenv()

    # 從 .env 讀取 Syslog 伺服器 IP，並提供預設值
    syslog_server = os.getenv('SYSLOG_SERVER_IP', '127.0.0.1')

    logger = setup_syslog_logging(server=syslog_server)
    logger.info(f'Syslog handler configured for server: {syslog_server}')

    # --- IBM i 連線範例 ---
    # 程式會從 .env 檔案或系統環境變數中尋找 IBMI_USER 和 IBMI_PASSWORD
    ibmi_host = os.getenv('IBMI_HOST')
    ibmi_user = os.getenv('IBMI_USER')
    ibmi_password = os.getenv('IBMI_PASSWORD')
    ibmi_driver = os.getenv('IBMI_ODBC_DRIVER')
    
    # 讀取 Journal 相關設定，並提供預設值
    journal_lib = os.getenv('IBMI_JOURNAL_LIBRARY', 'QSYS')
    journal_name = os.getenv('IBMI_JOURNAL_NAME', 'QAUDJRN')
    journal_types = os.getenv('IBMI_JOURNAL_TYPES', '') # 預設為空字串
    interval = int(os.getenv('POLLING_INTERVAL_SECONDS', 60))

    # 檢查是否成功讀取到憑證
    if not all([ibmi_host, ibmi_user, ibmi_password, ibmi_driver]):
        error_msg = "錯誤：必須在 .env 檔案或環境變數中設定 IBMI_HOST, IBMI_USER, IBMI_PASSWORD 和 IBMI_ODBC_DRIVER。"
        print(error_msg, file=sys.stderr)
        logger.critical(error_msg)
        sys.exit(1) # 結束程式並回傳錯誤碼

    # 執行查詢並將日誌發送到 Syslog
    monitor = IbmiJournalMonitor(ibmi_host, ibmi_user, ibmi_password, ibmi_driver, logger,
                                 journal_lib, journal_name, journal_types, interval)
    monitor.start()