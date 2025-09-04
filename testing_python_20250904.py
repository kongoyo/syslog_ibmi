import logging
import os
import sys
import time
import json
import threading
from typing import Optional
from typing import Dict, Any

from logging.handlers import SysLogHandler
import pyodbc
from dotenv import load_dotenv


def setup_syslog_logging(server:str ='127.0.0.1', port:int=514):
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

    def __init__(self, host:str, user:str, password:str, driver:str, logger:logging.Logger, journal_lib:str, journal_name:str, journal_types:str, interval:int):
        # Final Solution: Reverting to the simplest connection string from the successful testing.py.
        # This avoids the SQL0443 error that occurs when NAMING=1 is combined with STARTING_* parameters in the UDF call.
        self.host = host
        self.conn_str = f"DRIVER={{{driver}}};SYSTEM={host};UID={user};PWD={password};"
        self.logger = logger
        self.interval = interval
        self.journal_lib = journal_lib
        self.journal_name = journal_name
        # 將逗號分隔的字串轉換為列表，並過濾掉空字串
        self.journal_types = [t.strip() for t in journal_types.split(',') if t.strip()]
        
        # State is now managed in memory and reset on each program start.
        # This ensures the first query of every run is a full sync.
        self.last_receiver_name: Optional[str] = None
        self.last_sequence_number: Optional[int] = None

    def _process_one_batch(self):
        """連接、獲取一批新的日誌條目、處理它們並更新狀態。"""
        # 1. Attempt to connect to the database
        try:
            # Reverting to autocommit=False to match the behavior of the successful testing.py script.
            conn = pyodbc.connect(self.conn_str)
        except pyodbc.Error:
            # Use logger.exception to automatically log stack traces for better debugging
            self.logger.exception("Database connection failed. Check credentials, host, and driver.")
            # If connection fails, we cannot proceed. Return and wait for the next cycle.
            return

        # 2. If connection is successful, prepare and execute the query
        try:
            # --- Final and Most Robust Solution: Filter in the WHERE clause ---
            # All attempts to use STARTING_* parameters in the UDF call have proven unstable.
            # The most reliable method is to fetch all entries from the UDF and apply filtering
            # in a standard SQL WHERE clause, which is stable and well-supported.
            journal_call_str = f"'{self.journal_lib}', '{self.journal_name}', GENERATE_SYSLOG => 'RFC5424'"

            sql = f"""
                SELECT syslog_facility, syslog_severity, syslog_event, journal_entry_type,
                       receiver_name, sequence_number
                FROM TABLE (QSYS2.DISPLAY_JOURNAL({journal_call_str})) AS X
            """
            
            params: list[Any] = []
            where_clauses = ["syslog_event IS NOT NULL"]
            
            # Add filtering for the starting point directly in the WHERE clause
            if self.last_receiver_name and self.last_sequence_number:
                where_clauses.append("(receiver_name > ? OR (receiver_name = ? AND sequence_number > ?))")
                params.extend([self.last_receiver_name, self.last_receiver_name, self.last_sequence_number])

            if self.journal_types:
                placeholders = ', '.join('?' for _ in self.journal_types)
                where_clauses.append(f"journal_entry_type IN ({placeholders})")
                params.extend(self.journal_types)

            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

            sql += " ORDER BY receiver_name, sequence_number"

            with conn.cursor() as cursor:
                print(f"[{self.host}] Checking for new journal entries...")
                # 顯示將要執行的 SQL 語句和參數，以利除錯
                print(f"[{self.host}] Executing SQL: {sql}")
                print(f"[{self.host}] With parameters: {params}")
                cursor.execute(sql, params)
                
                count = 0
                last_row = None
                # Iterate directly on the cursor to save memory, especially for large batches.
                for row in cursor:
                    if count == 0:
                        print(f"[{self.host}] Found new entries. Processing and sending to syslog...")
                    
                    syslog_event = row.SYSLOG_EVENT
                    syslog_severity = row.SYSLOG_SEVERITY
                    log_level = self.SEVERITY_MAP.get(syslog_severity, logging.INFO)
                    self.logger.log(log_level, syslog_event)
                    
                    last_row = row # Keep track of the last processed row
                    count += 1

                if count == 0:
                    print(f"[{self.host}] No new journal entries found in this cycle.")
                    return
                
                if last_row:
                    self.last_receiver_name = last_row.RECEIVER_NAME
                    self.last_sequence_number = int(last_row.SEQUENCE_NUMBER)
                    print(f"[{self.host}] Finished sending {count} entries. New bookmark: {self.last_receiver_name}/{self.last_sequence_number}")
                    self.logger.info(f"[{self.host}] Processed {count} entries. New state: {self.last_receiver_name}/{self.last_sequence_number}")

        except pyodbc.Error:
            self.logger.exception("Database query or processing failed. Check SQL syntax and permissions.")
        finally:
            # The connection will be closed here regardless of whether the query succeeded or failed.
            if conn:
                conn.close()

    def start(self, shutdown_event: threading.Event):
        """啟動持續監控的迴圈。"""
        self.logger.info(f"[{self.host}] Monitor thread started.")
        while not shutdown_event.is_set():
            self._process_one_batch()
            print(f"[{self.host}] Waiting for {self.interval} seconds before next check...")
            # Use event.wait for a stoppable sleep
            shutdown_event.wait(self.interval)

if __name__ == "__main__":
    # 載入 .env 檔案中的環境變數
    load_dotenv()

    # --- 全域設定 ---
    syslog_server = os.getenv('SYSLOG_SERVER_IP', '127.0.0.1')
    logger = setup_syslog_logging(server=syslog_server)
    logger.info(f'Syslog handler configured for server: {syslog_server}')
    interval = int(os.getenv('POLLING_INTERVAL_SECONDS', 60))

    # --- 讀取多主機設定並啟動監控執行緒 ---
    monitors = []
    threads = []
    index = 1
    while True:
        host = os.getenv(f'IBMI_HOST_{index}')
        if not host:
            break # 找不到下一個主機設定，結束迴圈

        user = os.getenv(f'IBMI_USER_{index}')
        password = os.getenv(f'IBMI_PASSWORD_{index}')
        driver = os.getenv(f'IBMI_DRIVER_{index}')
        
        if not all([user, password, driver]):
            logger.error(f"Configuration for host {host} (IBMI_HOST_{index}) is incomplete. Skipping.")
            index += 1
            continue

        journal_lib = os.getenv(f'IBMI_JOURNAL_LIBRARY_{index}', 'QSYS')
        journal_name = os.getenv(f'IBMI_JOURNAL_NAME_{index}', 'QAUDJRN')
        journal_types = os.getenv(f'IBMI_JOURNAL_TYPES_{index}', '')

        print(f"Found configuration for host: {host}")
        monitor = IbmiJournalMonitor(host, user, password, driver, logger,
                                     journal_lib, journal_name, journal_types, interval)
        monitors.append(monitor)
        index += 1

    if not monitors:
        print("No host configurations found. Please check your .env file.", file=sys.stderr)
        sys.exit(1)

    shutdown_event = threading.Event()
    print(f"Starting {len(monitors)} monitor(s). Press Ctrl+C to stop.")

    for monitor in monitors:
        thread = threading.Thread(target=monitor.start, args=(shutdown_event,))
        thread.start()
        threads.append(thread)

    try:
        # 主執行緒等待，直到所有監控執行緒結束
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nShutdown signal received. Stopping all monitors...")
        shutdown_event.set()
        # 再次 join 以確保所有執行緒都已乾淨地退出
        for thread in threads:
            thread.join()

    print("All monitors have been shut down. Exiting.")
    logger.info("Daemon stopped.")