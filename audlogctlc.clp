/******************************************************************/
/* PROGRAM:  AUDLOGCTLC                                           */
/* AUTHOR:   Gemini Code Assist                                   */
/* FUNCTION: Command processing program for AUDLOGCTL command.    */
/******************************************************************/
             PGM        PARM(&OPTION)

             DCL        VAR(&OPTION) TYPE(*CHAR) LEN(7)
             DCL        VAR(&CTL_DTA) TYPE(*CHAR) LEN(50)
             DCL        VAR(&STS_DTA) TYPE(*CHAR) LEN(20)
             DCL        VAR(&JOBNAME) TYPE(*CHAR) LEN(10)
             DCL        VAR(&JOBUSER) TYPE(*CHAR) LEN(10)
             DCL        VAR(&JOBNBR) TYPE(*CHAR) LEN(6)
             DCL        VAR(&JOBSTS) TYPE(*CHAR) LEN(10)
             DCL        VAR(&MSG) TYPE(*CHAR) LEN(100)

             DCLF       FILE(QADSPOBJ)

             /* Retrieve current status from control data area */
             RTVDTAARA  DTAARA(STEVE/AUDLOGCTL) RTNVAR(&CTL_DTA)

             /*----------------------------------------------------*/
             /* Option: *START                                     */
             /*----------------------------------------------------*/
             IF         COND(&OPTION *EQ '*START') THEN(DO)
               IF         COND(&CTL_DTA *NE '*STOPPED' *AND &CTL_DTA *NE +
                            '*END') THEN(DO)
                 SNDPGMMSG  MSG('Daemon is already running or starting.') +
                            MSGTYPE(*COMP)
                 GOTO       CMDLBL(ENDPGM)
               ENDDO

               SNDPGMMSG  MSG('Starting Audit Log Daemon...') MSGTYPE(*INFO)
               SBMJOB     CMD(CALL PGM(STEVE/AUDJRN2LOG)) +
                            JOB(AUDJRNLOGD) JOBD(QBATCH)
               SNDPGMMSG  MSG('Daemon submitted to batch. Use *STATUS to +
                            check.') MSGTYPE(*COMP)
             ENDDO

             /*----------------------------------------------------*/
             /* Option: *STOP                                      */
             /*----------------------------------------------------*/
             ELSE IF    COND(&OPTION *EQ '*STOP') THEN(DO)
               IF         COND(&CTL_DTA *EQ '*STOPPED') THEN(DO)
                 SNDPGMMSG  MSG('Daemon is not running.') MSGTYPE(*COMP)
                 GOTO       CMDLBL(ENDPGM)
               ENDDO

               SNDPGMMSG  MSG('Sending stop signal to the daemon...') +
                            MSGTYPE(*INFO)
               CHGDTAARA  DTAARA(STEVE/AUDLOGCTL) VALUE('*END')
               SNDPGMMSG  MSG('Stop signal sent. The daemon will shut down +
                            shortly.') MSGTYPE(*COMP)
             ENDDO

             /*----------------------------------------------------*/
             /* Option: *STATUS                                    */
             /*----------------------------------------------------*/
             ELSE IF    COND(&OPTION *EQ '*STATUS') THEN(DO)
               IF         COND(&CTL_DTA *EQ '*STOPPED') THEN(DO)
                 SNDPGMMSG  MSG('Status: Stopped.') MSGTYPE(*COMP)
               ENDDO
               ELSE IF    COND(&CTL_DTA *EQ '*END') THEN(DO)
                 SNDPGMMSG  MSG('Status: Stopping...') MSGTYPE(*COMP)
               ENDDO
               ELSE       CMD(DO) /* Assumes it contains job info */
                 CHGVAR     VAR(&JOBNBR) VALUE(%SST(&CTL_DTA 1 6))
                 CHGVAR     VAR(&JOBUSER) VALUE(%SST(&CTL_DTA 7 10))
                 CHGVAR     VAR(&JOBNAME) VALUE(%SST(&CTL_DTA 17 10))

                 /* Verify if the job is actually active */
                 CHKOBJ     OBJ(&JOBUSER/&JOBNAME) OBJTYPE(*JOB)
                 IF         COND(%SWITCH(00000001)) THEN(DO) /* Job not found */
                   SNDPGMMSG  MSG('Status: Inconsistent. Data area shows a +
                                running job, but it cannot be found. +
                                Consider running *STOP to reset.') +
                                MSGTYPE(*DIAG)
                   CHGDTAARA  DTAARA(STEVE/AUDLOGCTL) VALUE('*STOPPED')
                   GOTO       CMDLBL(ENDPGM)
                 ENDDO

                 RTVJOBA    JOB(&JOBNBR/&JOBUSER/&JOBNAME) STATUS(&JOBSTS)
                 RTVDTAARA  DTAARA(STEVE/SYSLOGSTS) RTNVAR(&STS_DTA)
                 CHGVAR     VAR(&MSG) VALUE('Status: Running. Job: ' *CAT +
                              &JOBNBR *TCAT '/' *TCAT &JOBUSER *TCAT '/' +
                              *TCAT &JOBNAME *TCAT '. Last Seq: ' *CAT +
                              &STS_DTA)
                 SNDPGMMSG  MSG(&MSG) MSGTYPE(*COMP)
               ENDDO
             ENDDO

 ENDPGM:     ENDPGM
