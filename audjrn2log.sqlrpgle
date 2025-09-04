**FREE

// =================================================================
// Program: AUDJRN2LOG
// Author: Gemini Code Assist
// Date: 2023-10-27
//
// Description:
// This program reads IBM i Audit Journal entries (QAUDJRN)
// using SQL table functions, formats them into syslog messages,
// and sends them to a remote syslog server via UDP.
//
// It uses a data area to keep track of the last processed
// journal sequence number to avoid sending duplicate entries.
//
// Pre-requisites:
// 1. A data area to store the last sequence number.
//    CRTDTAARA DTAARA(your_lib/SYSLOGSTS) TYPE(*CHAR) LEN(20) VALUE('0')
//
// Compilation:
// CRTSQLRPGI OBJ(your_lib/AUDJRN2LOG) SRCFILE(your_lib/your_srcf)
//            COMMIT(*NONE)
// =================================================================

// -----------------------------------------------------------------
// Control Options
// -----------------------------------------------------------------
Ctl-Opt DftActGrp(*No) ActGrp(*NEW)
       Option(*SrcStmt: *NoDebugIO)
       BndDir('QC2LE'); // Binding directory for socket APIs

// -----------------------------------------------------------------
// Syslog Configuration - *** 請修改這些值 ***
// -----------------------------------------------------------------
Dcl-C SYSLOG_SERVER_IP '172.16.13.78'; // Syslog 伺服器的 IP 位址
Dcl-C SYSLOG_SERVER_PORT 514;           // Syslog 伺服器的 Port (通常是 514 for UDP)
// Dcl-C DATA_AREA_NAME 'SYSLOGSTS   STEVE     '; // This constant is no longer needed for IN/OUT

// -----------------------------------------------------------------
// Socket API Prototypes (from C library)
// -----------------------------------------------------------------
Dcl-Pr socket Int(10);
  address_family Int(10) Value;
  transport_type Int(10) Value;
  protocol Int(10) Value;
End-Pr;

Dcl-Pr sendto Int(10);
    socket_descriptor Int(10) Value;
    message Pointer Value;
    message_length Int(10) Value;
    flags Int(10) Value;
    destination_address Pointer Value;
    address_length Int(10) Value;
End-Pr;

Dcl-Pr close Int(10);
  socket_descriptor Int(10) Value;
End-Pr;

Dcl-Pr inet_addr Uns(10);
  ip_address Pointer Value Options(*String);
End-Pr;

// -----------------------------------------------------------------
// Constants for Socket API
// -----------------------------------------------------------------
Dcl-C AF_INET 2;      // Address family: Internet
Dcl-C SOCK_DGRAM 2;   // Socket type: Datagram (UDP)

// -----------------------------------------------------------------
// Data Structures
// -----------------------------------------------------------------
// Data Area structure for IN/OUT, associated with the data area
Dcl-Ds dtaara_t Dtaara('STEVE/SYSLOGSTS') Qualified;
  lastSeqNum Char(20);
End-Ds;

// Socket Address structure for IPv4
Dcl-Ds sockaddr_in_t Qualified Template;
  sin_family Int(5);
  sin_port Uns(5);
  sin_addr Uns(10);
  sin_zero Char(8);
End-Ds;

// Host variables for SQL fetch
Dcl-Ds auditEntry Qualified;
    // Using DISPLAY_JOURNAL, we only need the sequence number for state
    // and the pre-formatted syslog event string.
    sequence_number Char(20);
    syslog_event Varchar(1024);
End-Ds;

// -----------------------------------------------------------------
// Standalone Variables
// -----------------------------------------------------------------
Dcl-S lastProcessedSeqNum Char(20);
Dcl-S currentSeqNum Char(20);
Dcl-S syslogMsg Varchar(1024);
Dcl-S socketHandle Int(10);
Dcl-S rc Int(10); // Return code
Dcl-S systemName Char(8);

// Variables for building socket address
Dcl-DS serverAddr LIKEDS(sockaddr_in_t);

// -----------------------------------------------------------------
// Main Procedure
// -----------------------------------------------------------------
// Retrieve current system name
exec sql VALUES CURRENT SERVER INTO :systemName;

// 從資料區讀取最後處理的序號，並加強錯誤處理
IN(E) dtaara_t;
If %Error();
  // 讀取失敗時，記錄錯誤並預設為 '0'
  lastProcessedSeqNum = '0';
  // 可選：寫入日誌或通知
Else;
  lastProcessedSeqNum = %Trim(dtaara_t.lastSeqNum);
  // 若資料區內容非數字或空值，則預設為 '0'
  If lastProcessedSeqNum = '' or %Check('0123456789': lastProcessedSeqNum) <> 0;
    lastProcessedSeqNum = '0';
  EndIf;
EndIf;

currentSeqNum = lastProcessedSeqNum;

// --- Setup Socket ---
socketHandle = socket(AF_INET: SOCK_DGRAM: 0);
If socketHandle < 0;
  // Error creating socket, cannot proceed
  *InLR = *On;
  Return;
EndIf;

// --- Prepare server address structure ---
serverAddr.sin_family = AF_INET;
serverAddr.sin_port = %Unsh(SYSLOG_SERVER_PORT);
// Convert IP address string to network byte order integer
serverAddr.sin_addr = inet_addr(%Trim(SYSLOG_SERVER_IP) + x'00');

// --- Declare and Open SQL Cursor ---
// Using DISPLAY_JOURNAL with GENERATE_SYSLOG. This is more efficient
// as the DB engine formats the message for any auditable event.
Exec SQL
  Declare C1 Cursor For
    Select SEQUENCE_NUMBER, SYSLOG_EVENT
      From Table (QSYS2.DISPLAY_JOURNAL('QSYS', 'QAUDJRN',
                                        STARTING_SEQUENCE_NUMBER => :lastProcessedSeqNum,
                                        GENERATE_SYSLOG =>'RFC5424'
                  ) )
      Where SYSLOG_EVENT IS NOT NULL
      Order By SEQUENCE_NUMBER Asc;

Exec SQL Open C1;

// --- Main Processing Loop ---
DoW SqlStt = '00000';
  Exec SQL Fetch Next From C1 Into :auditEntry;

  If SqlStt = '02000'; // No more records
    Leave;
  EndIf;

  If SqlStt <> '00000'; // SQL Error
    // Handle SQL error, maybe log it locally
    Leave;
  EndIf;

  // The SYSLOG_EVENT column already contains the fully formatted RFC5424 message.
  // We just need to assign it.
  syslogMsg = auditEntry.syslog_event;

  // --- Send the message via UDP ---
  rc = sendto(socketHandle:
            %Addr(syslogMsg) + 2: // Pointer to the message data (skip length part of varchar)
            %Len(%TrimR(syslogMsg)): // Length of the message
            0: // Flags
            %Addr(serverAddr): // Pointer to destination address
            %Size(serverAddr)); // Size of address structure

  If rc < 0;
    // Error sending data. You might want to log this.
    // For now, we continue to the next record.
  EndIf;

  // Update the current sequence number
  currentSeqNum = auditEntry.sequence_number;

EndDo;

// --- Cleanup ---
Exec SQL Close C1;
CALLP close(socketHandle);

// --- Save the last processed sequence number to the data area ---
If currentSeqNum > lastProcessedSeqNum;
  dtaara_t.lastSeqNum = currentSeqNum;
  OUT(E) dtaara_t;
  If %Error();
    // Handle error writing to data area
  EndIf;
EndIf;

*InLR = *On;
Return;
