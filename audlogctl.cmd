/*-------------------------------------------------------------------*/
/*                                                                   */
/*  COMMAND:    AUDLOGCTL                                            */
/*  AUTHOR:     Gemini Code Assist                                   */
/*  DESCRIPTION:Control the Audit Journal to Syslog Daemon           */
/*                                                                   */
/*-------------------------------------------------------------------*/
             CMD        PROMPT('Control Audit Log Daemon')

             PARM       KWD(OPTION) TYPE(*CHAR) LEN(7) RSTD(*YES) +
                          VALUES(*START *STOP *STATUS) MIN(1) +
                          PROMPT('Option')
