#!/usr/bin/env python

__version__ = '2.0.1'

import sys, os
sys.path.append('../lib')

import traceback
import socket
import errno
import datetime
import time
import threading
from thread import start_new_thread
import multiprocessing
import signal
import ConfigParser

from ptimer import PeriodicTimer
from get_logger import get_logger

import numpy as np

import IcnHost

# Load Server Config
config = ConfigParser.ConfigParser()
config.read('../cfg/server.cfg')

# Init TCP
ADDR = '0.0.0.0'
PORT = config.getint('SERVER', 'PORT')
SOCK = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.getprotobyname('TCP'))
SOCK.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
SOCK.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
SOCK.bind((ADDR, PORT))
SOCK.listen(1)

BUFF_SIZE = 4096

buff = ''
conn = None

# Init logging
logger = get_logger('Server')


# Server Sync
def serv_sync():
    global conn
    if conn:
        conn.send('[HRT]\n')
        logger.debug('[HRT]')


# Communication Functions
def parse_msg():
    global buff, conn, bid, ask, mid, is_updated, risk_mask, beta
    global tic , toc
    
    parts = buff.partition('[')
    if not parts[1]:
        return
    buff = '[' + parts[-1]
    lines = buff.split('[')
    if (not lines[-1]) or (lines[-1][-1] != ']'):
        buff = '[' + lines[-1]
        lines[-1] = ''
    else:
        buff = ''

    for msg in lines:
        if not msg:
            continue
        if msg[-1] != ']':
            logger.error('Incomplete message. %s' % msg)
            continue

        try:
            msg = msg[:-1]
            words = msg.split(',')
            title = words[0]

            if title == 'IMB':
                sym = words[1]
                ref = float(words[2])
                pair = float(words[3])
                total = float(words[4])
                mkt = float(words[5])
                side = 1 if words[6]=='B' else -1 if words[6]=='S' else 0
                price = float(words[7])

                if sym != '$':                       
                    imba_update = {'sym':sym, 'ref':ref, 'pair':pair, 'total':total, 'mkt':mkt, 'side':side}
                    icn_host.on_imba(imba_update)
                    mkt_update = {'sym':sym, 'price':price}
                    icn_host.on_market(mkt_update)    
                else:
                    icn_host.send_target()
                
                logger.info('[%s]' % msg)

            elif title == 'FIL':
                sym = words[1]
                size = int(float(words[2]))
                price = float(words[3])
                
                fill = {'sym':sym, 'size':size, 'price':price}
                icn_host.on_fill(fill)

                logger.info('[%s]' % msg)

            elif title == 'HRT':
                pass
            
            else:
                logger.error('Unknown message. [%s]' % msg)


        except Exception as e:
            logger.error("Message is not parsed successfully: [%s]" % msg)
            logger.error("Runtime error %s" % (type(e).__name__))
            logger.error("Runtime error %s" % (sys.exc_info()[0]))
            logger.error("Runtime error %s" % (sys.exc_info()[1]))
            logger.error("Runtime error %s" % (sys.exc_info()[2]))

    return


EXIT_DONE = 0
def exit_acts(*args):
    global EXIT_DONE
    if EXIT_DONE:
        return
    
    if conn:
        conn.close()
    icn_host.stop()
    logger.info("Stopped.")

    EXIT_DONE = 1
    return

for sig in (signal.SIGABRT, signal.SIGILL, signal.SIGINT, signal.SIGSEGV, signal.SIGTERM):
    signal.signal(sig, exit_acts)


# Control Client
class CC_Threading(object):
    def __init__(self):
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True
        thread.start()

    def run(self):
        global conn, icn_host

        CC_ADDR = '0.0.0.0'
        CC_PORT = 16669
        CC_SOCK = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.getprotobyname('TCP'))
        CC_SOCK.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        CC_SOCK.bind((CC_ADDR, CC_PORT))
        CC_SOCK.listen(1)

        buff = ''
        while True:
       
            cc_conn, cc_client_addr = CC_SOCK.accept()

            logger.info("ControlClient connected from %s:%d." % (cc_client_addr[0], cc_client_addr[1]))
            
            cc_conn.send("(HRT)\n")

            try:
                while True:
                    msg = cc_conn.recv(BUFF_SIZE)
                    buff += msg
                    if buff:
                        parts = buff.partition('(')
                        if not parts[1]:
                            return
                        buff = '(' + parts[-1]
                        lines = buff.split('(')
                        if (not lines[-1]) or (lines[-1][-1] != ')'):
                            buff = ')' + lines[-1]
                            lines[-1] = ''
                        else:
                            buff = ''

                    for msg in lines:
                        if not msg:
                            continue
                        if msg[-1] != ')':
                            logger.error('Incomplete ControlClient message. %s' % msg)
                            continue

                        try:
                            msg = msg[:-1]
                            words = msg.split(',')
                            title = words[0]
                            
                            if title == 'CMD':
                                logger.info('(%s)' % msg)
                                cmd = words[1]
                                if cmd == 'SENDMOC':
                                    icn_host.send_auct()
                                elif cmd == 'SUSPEND':
                                    icn_host.suspended = True
                                    logger.info('Trading SUSPENDED.')
                                elif cmd == 'RESUME':
                                    icn_host.suspended = False
                                    logger.info('Trading RESUMED.')
                                else:
                                    logger.error('Unknown ControlClient command.')

                            elif title == 'TCP':
                                logger.info('(%s)' % msg)
                                if conn:
                                    tcp_message = msg[4:]
                                    conn.send(tcp_message + '\n')
                                    logger.info(tcp_message)
                                else:
                                    logger.warning('Failed to send TCP message. Broken pipe.')

                            elif title == 'CFG':
                                logger.info('(%s)' % msg)
                                key = words[1]
                                if key == 'PARTIC':
                                    sym = words[2]
                                    try:
                                        val = float(words[3])
                                    except ValueError:
                                        continue
                                    if sym in icn_host.partic:
                                        val0 = icn_host.partic[sym]
                                        icn_host.partic[sym] = val
                                        logger.info('CFG change %s: %s %f -> %f' % (key, sym, val0, val))
                                elif key == 'MAX_GLOBAL_NOTIONAL':
                                    try:
                                        val = float(words[2])
                                    except ValueError:
                                        continue
                                    val0 = icn_host.max_global_notional
                                    icn_host.max_global_notional = val
                                    logger.info('CFG change %s: %f -> %f' % (key, val0, val))
                                elif key == 'MAX_SYMBOL_NOTIONAL':
                                    try:
                                        val = float(words[2])
                                    except ValueError:
                                        continue
                                    val0 = icn_host.max_symbol_notional
                                    icn_host.max_symbol_notional = val
                                    logger.info('CFG change %s: %f -> %f' % (key, val0, val))
                                elif key == 'MAX_PAIR_PERCENTAGE':
                                    try:
                                        val = float(words[2])
                                    except ValueError:
                                        continue
                                    val0 = icn_host.max_pair_pct
                                    icn_host.max_pair_pct = val
                                    logger.info('CFG change %s: %f -> %f' % (key, val0, val))
                                elif key == 'HEDGE_RATIO':
                                    try:
                                        val = float(words[2])
                                    except ValueError:
                                        continue
                                    val0 = icn_host.hedge_ratio
                                    icn_host.hedge_ratio = val
                                    logger.info('CFG change %s: %f -> %f' % (key, val0, val))

                            elif title == 'HRT':
                                pass

                            else:
                                logger.error('Unknown Control Client message. (%s)' % msg)

                        except Exception as e:
                            logger.error("ControlClient message is not parsed successfully: (%s)" % msg)
                            logger.error("Runtime error %s" % (type(e).__name__))
                            logger.error("Runtime error %s" % (sys.exc_info()[0]))
                            logger.error("Runtime error %s" % (sys.exc_info()[1]))
                            logger.error("Runtime error %s" % (sys.exc_info()[2]))
                       
            except KeyboardInterrupt:
                raise
            except socket.error as e:
                if e.errno != errno.ECONNRESET:
                    raise
                logger.info("ControlClient Socket: %s." % e)
            finally:
                cc_conn.close()
                cc_conn = None

cc_thread = CC_Threading()


# Main

logger.info("ICE NYSE portfolio version %s." % __version__)

serv_sync_timer = PeriodicTimer(15, 0.1, serv_sync)
serv_sync_timer.start()

icn_host = IcnHost.IcnHost()

icn_host.set_auct_sched()
icn_host.start_sync_timer()       
icn_host.start()
icn_exec_started = False

time.sleep(0.1)


buff = ''
try:
    # TCP Server Recieving Loop
    while True:                
        
        logger.info("ACCEPTing connections on port %d." % PORT)

        conn, client_addr = SOCK.accept()

        logger.info("ACCEPTed connection from %s:%d." % (client_addr[0], client_addr[1]))

        IcnHost.conn = conn
        
        if not(icn_exec_started):
            icn_host.start_exec_timer()
            icn_exec_started = True
        
        conn.send("[HRT]\n")

        try:
            while True:
                msg = conn.recv(BUFF_SIZE)
                buff += msg
                if buff:
                    parse_msg()
                else:
                    break
        except KeyboardInterrupt:
            raise
        except socket.error as e:
            if e.errno != errno.ECONNRESET:
                raise
            logger.error("Socket: %s." % e)
        finally:
            conn.close()
            conn = None
            IcnHost.conn = conn

finally:
    exit_acts()







