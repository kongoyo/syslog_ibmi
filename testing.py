import logging
import os
import sys

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


def fetch_and_log_journal_entries(host, user, password, driver, logger):
    """連線到 IBM i，查詢稽核日誌，並將結果發送到 Syslog。"""
    # 將 Syslog 嚴重性等級對應到 Python 的 logging 等級
    # RFC5424: 0-emerg, 1-alert, 2-crit, 3-err, 4-warn, 5-notice, 6-info, 7-debug
    severity_map = {
        0: logging.CRITICAL, # Emergency
        1: logging.CRITICAL, # Alert
        2: logging.CRITICAL, # Critical
        3: logging.ERROR,    # Error
        4: logging.WARNING,  # Warning
        5: logging.INFO,     # Notice
        6: logging.INFO,     # Informational
        7: logging.DEBUG,    # Debug
    }

    # pyodbc 的連線字串格式
    # DRIVER 名稱從環境變數讀取
    conn_str = f"DRIVER={{{driver}}};SYSTEM={host};UID={user};PWD={password};"
    conn = None
    cursor = None
    try:
        # 1. 使用 pyodbc 建立資料庫連線
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("Successfully connected to IBM i database.")
        logger.info("Successfully connected to IBM i database.")

        # 2. 準備要執行的 SQL 查詢
        sql = """
            SELECT syslog_facility, syslog_severity, syslog_event
            FROM TABLE (QSYS2.DISPLAY_JOURNAL('QSYS', 'QAUDJRN', GENERATE_SYSLOG => 'RFC5424')) AS X
            WHERE syslog_event IS NOT NULL
        """

        # 3. 執行 SQL，並直接在 cursor 上迭代結果
        print("Executing SQL to fetch journal entries...")
        count = 0
        for row in cursor.execute(sql):
            # row 物件可以直接透過欄位名稱存取
            syslog_event = row.SYSLOG_EVENT
            syslog_severity = row.SYSLOG_SEVERITY
            
            # 取得對應的 logging 等級，如果找不到則預設為 INFO
            log_level = severity_map.get(syslog_severity, logging.INFO)
            
            # 使用 logger 發送日誌
            logger.log(log_level, syslog_event)
            count += 1

        print(f"Finished processing. Sent {count} log entries to syslog server.")
        logger.info(f"Finished processing. Sent {count} log entries to syslog server.")
        return True
    except pyodbc.Error as e:
        # 捕捉特定的 pyodbc 錯誤
        error_msg = f"Database error occurred: {e}"
        print(error_msg, file=sys.stderr)
        logger.critical(error_msg)
        return False
    except Exception as e:
        error_msg = f"An error occurred: {e}"
        print(error_msg, file=sys.stderr)
        logger.critical(error_msg)
        return False
    finally:
        if conn:
            # 關閉 cursor 和 connection
            if cursor:
                cursor.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    # 載入 .env 檔案中的環境變數
    load_dotenv()

    logger = setup_syslog_logging()
    logger.warning('This is a warning message from python script')

    # --- IBM i 連線範例 ---
    # 程式會從 .env 檔案或系統環境變數中尋找 IBMI_USER 和 IBMI_PASSWORD
    ibmi_host = os.getenv('IBMI_HOST')
    ibmi_user = os.getenv('IBMI_USER')
    ibmi_password = os.getenv('IBMI_PASSWORD')
    ibmi_driver = os.getenv('IBMI_ODBC_DRIVER')

    # 檢查是否成功讀取到憑證
    if not all([ibmi_host, ibmi_user, ibmi_password, ibmi_driver]):
        error_msg = "錯誤：必須在 .env 檔案或環境變數中設定 IBMI_HOST, IBMI_USER, IBMI_PASSWORD 和 IBMI_ODBC_DRIVER。"
        print(error_msg, file=sys.stderr)
        logger.critical(error_msg)
        sys.exit(1) # 結束程式並回傳錯誤碼

    # 執行查詢並將日誌發送到 Syslog
    fetch_and_log_journal_entries(ibmi_host, ibmi_user, ibmi_password, ibmi_driver, logger)