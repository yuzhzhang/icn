from __future__ import division

import sys, os
import time
import threading
import datetime
import ConfigParser
import numpy as np

from get_logger import get_logger
from scheduler import Scheduler
from ptimer import PeriodicTimer

from IceFactor import IceFactor as IcnFactor

logger = get_logger('IcnHost')

conn = None


_today = datetime.datetime.now().replace(hour=0, minute=0, second=0)
_time_1500 = _today + datetime.timedelta(hours=15)
_time_1550 = _today + datetime.timedelta(hours=15, minutes=50)
_time_1559 = _today + datetime.timedelta(hours=15, minutes=59)
_timespan = (_time_1559 - _time_1550).total_seconds()

class IcnHost(threading.Thread):
    def __init__(self):
        super(IcnHost, self).__init__()
        self.deamon = True
        self._stop = threading.Event()
        
        # Init
        self.suspended = False

        # Load Config
        config = ConfigParser.ConfigParser()
        config.read('../cfg/icn.cfg')

        symbol_file = config.get('TRADE', 'SYMBOL_FILE')
        self.symbol_list = open(os.path.join('../cfg/', symbol_file)).read().splitlines()
        self.symbol_dict = {sym:sym_id for sym_id, sym in enumerate(self.symbol_list)}
        self.symbol_price = {sym:0 for sym in self.symbol_list}
        
        self.is_marked = {sym:False for sym in self.symbol_list}
        self.marked_imba = {}

        self.pos = {sym:0 for sym in self.symbol_list}
        self.target_pos = {sym:0 for sym in self.symbol_list}
        self.final_target_pos = {sym:0 for sym in self.symbol_list}
        self.cash = {sym:0.0 for sym in self.symbol_list}

        self.global_partic = config.getfloat('TRADE', 'PARTIC')
        self.partic = {sym:self.global_partic for sym in self.symbol_list}
        logger.info('Gobal partic %f' % self.global_partic)
        
        partic_file = os.path.join('../cfg/', config.get('TRADE', 'PARTIC_FILE'))
        if os.path.isfile(partic_file):
            logger.info('Loading partic file')
            for row in open(os.path.join('../cfg/', partic_file)).read().splitlines():
                words = row.split(',')
                sym = words[0]
                if sym in self.partic:
                    self.partic[sym] = float(words[1])
                    logger.info('Partic %s %f' % (sym, self.partic[sym]))
            logger.info('Partic file loaded.')
        else:
            logger.warn('No Partic file found.')

        self.max_global_notional = config.getfloat('TRADE', 'MAX_GLOBAL_NOTIONAL')
        self.max_symbol_notional = config.getfloat('TRADE', 'MAX_SYMBOL_NOTIONAL')
        self.max_hedge_notional  = config.getfloat('TRADE', 'MAX_HEDGE_NOTIONAL')

        logger.info('Max Global Notional %f' % self.max_global_notional)
        logger.info('Max Symbol Notional %f' % self.max_symbol_notional)

        self.max_order_notional = config.getfloat('TRADE', 'MAX_ORDER_NOTIONAL')
        self.max_order_size = config.getint('TRADE', 'MAX_ORDER_SIZE')
        self.min_order_size = config.getint('TRADE', 'MIN_ORDER_SIZE')

        self.max_pair_pct = config.getfloat('TRADE', 'MAX_PAIR_PERCENTAGE')

        self.hedge_ratio = config.getfloat('TRADE', 'HEDGE_RATIO')
        self.hedge_ratio_lmt = max(self.hedge_ratio, min(1.0, config.getfloat('TRADE', 'HEDGE_RATIO_LMT')))
        self.default_beta = 1.0
        self.beta = {sym:self.default_beta for sym in self.symbol_list}
        beta_file = os.path.join('../cfg/', config.get('TRADE', 'BETA_FILE'))
        if os.path.isfile(beta_file):
            for row in open(os.path.join('../cfg/', beta_file)).read().splitlines():
                words = row.split(',')
                sym = words[0]
                if sym in self.beta:
                    self.beta[sym] = float(words[1])
            logger.info('Beta file loaded.')
        else:
            logger.warn('No Beta file found.')

        self.icn_factor = [IcnFactor(sym) for sym in self.symbol_list]
        
        today = datetime.datetime.today().strftime('%Y%m%d')
        self.send_auct_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'SEND_AUCT_TIME'), '%Y%m%d %H:%M:%S')

        #self.mark_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'MARK_TIME'), '%Y%m%d %H:%M:%S')
        self.start_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'START_TIME'), '%Y%m%d %H:%M:%S')
        #self.cutoff_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'CUTOFF_TIME'), '%Y%m%d %H:%M:%S')
        self.end_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'END_TIME'), '%Y%m%d %H:%M:%S')
        
        self.twap_curve = []
        for line in config.get('TRADE', 'TWAP').strip().splitlines():
            words = line.strip().split(',')
            self.twap_curve += [(datetime.datetime.strptime(today+' '+words[0], '%Y%m%d %H:%M:%S'), float(words[1]))]

        #self.timespan  = (self.cutoff_time - self.start_time).total_seconds()
        
        self._flip_counter = {sym:0 for sym in self.symbol_list}
        self._start_pos = {sym:0 for sym in self.symbol_list}
        self._start_time = {sym:self.start_time for sym in self.symbol_list}
        #self._timespan = {sym:self.timespan for sym in self.symbol_list}

        #logger.info('Mark time is set to %s' % self.mark_time.strftime('%Y%m%d %H:%M:%S.%f'))
        logger.info('Start time is set to %s' % self.start_time.strftime('%Y%m%d %H:%M:%S.%f'))
        #logger.info('Cutoff time is set to %s' % self.cutoff_time.strftime('%Y%m%d %H:%M:%S.%f'))
        logger.info('End time is set to %s' % self.start_time.strftime('%Y%m%d %H:%M:%S.%f'))

        self._auct_sched = Scheduler(self.send_auct_time, self.send_auct)
        #self._mark_sched = Scheduler(self.mark_time, self.set_mark)
        self._exec_timer = PeriodicTimer(1.0, 0.0, self.send_target)
        self._sync_timer = PeriodicTimer(5.0, 0.5, self.summary)

        return

    def start_exec_timer(self):
        self._exec_timer.start()
        return

    def start_sync_timer(self):
        self._sync_timer.start()

    def set_auct_sched(self):
        self._auct_sched.start()
        logger.info('MOC Scheduled at %s' % self.send_auct_time.strftime('%Y%m%d %H:%M:%S.%f'))
        return
    
    def send_auct(self):
        global conn
        self._exec_timer.stop()
        if conn:
            order_msg_all = ''
            for sym in self.symbol_list:
                if self.pos[sym] == 0:
                    continue
                order_msg = '[MOC,%s,%d]|' % (sym, -self.pos[sym])
                order_msg_all += order_msg
            if order_msg_all:
                conn.send(order_msg_all + '\n')
            for order_msg in order_msg_all.split('|')[:-1]:
                logger.info(order_msg)
        else:
            logger.error('Failed to send MOC. Broken pipe.')

        return

    def set_mark(self):
        for sym in self.symbol_list:
            if sym == 'SPY':
                continue
            icn_factor = self.icn_factor[self.symbol_dict[sym]]
            self.is_marked[sym] = True
            self.marked_imba[sym] = {}
            self.marked_imba[sym]['ref'] = icn_factor.ref
            self.marked_imba[sym]['pair'] = icn_factor.pair
            self.marked_imba[sym]['total'] = icn_factor.total
            self.marked_imba[sym]['mkt'] = icn_factor.mkt
            self.marked_imba[sym]['side'] = icn_factor.side
            signed_total = self.marked_imba[sym]['total'] * self.marked_imba[sym]['side']
            logger.info('%s marked imba total %.0f.' % (sym, signed_total))
        return   

    def on_market(self, market):
        if self.is_stopped():
            return
        sym = market['sym']
        price = market['price']
        self.symbol_price[sym] = price
        return
        
    def on_imba(self, imba):
        if self.is_stopped():
            return
        sym = imba['sym']
        if sym in self.symbol_dict:
            icn_factor = self.icn_factor[self.symbol_dict[sym]]
            logger.info("Imba Update - %s (%.4f,%.0f,%.0f,%.0f,%d) (%.0f,%.0f,%.0f)" % \
                    (sym, imba['ref'], imba['pair'], imba['total'], imba['mkt'], imba['side'], icn_factor.net, icn_factor.net2, icn_factor.net3))
            imba['mkt'] = imba['total'] # Adjustment
            icn_factor.update(imba)
            #self.send_symbol_target(sym)
        return

    def on_fill(self, fill):
        if self.is_stopped():
            return
        sym = fill['sym']
        size = fill['size']
        price = fill['price']
        self.pos[sym] += size
        self.cash[sym] -= size*price
        return

    def read_twap_curve(self):
        today = datetime.datetime.today().strftime('%Y%m%d')
        twap_curve = []
        with open(self.twap_file) as twapf:
            for line in twapf:
                words = line.strip().split(',')
                timestamp = datetime.datetime.strptime(today+' '+words[0], '%Y%m%d %H:%M:%S')
                value = float(words[1])
                twap_curve += [(timestamp, value)]
        return twap_curve

    
    def _twap_rate_func(self, sym=''):
        logger.info('Calculating twap')
        curr_time = datetime.datetime.now()
        twap_rate = 0.0
        if curr_time < self.twap_curve[0][0]:
            twap_rate = 0.0
        elif curr_time >=  self.twap_curve[-1][0]:
            twap_rate = self.twap_curve[-1][1]
        else:
            for k in range(len(self.twap_curve) - 1):
                t0 = self.twap_curve[k][0]
                t1 = self.twap_curve[k+1][0]
                if t0 <= curr_time < t1:
                    val0 = self.twap_curve[k][1]
                    val1 = self.twap_curve[k+1][1]
                    twap_rate = (curr_time - t0).total_seconds() / (t1 - t0).total_seconds() * (val1 - val0) + val0
        print twap_rate
        return twap_rate
        
    def update_target(self):
        
        total_gmv = 0.0
        for sym in self.symbol_list:
            sym_id = self.symbol_dict[sym]
            ref = self.icn_factor[sym_id].ref
            if ref < 0.1:
                price = self.symbol_price[sym]
                ref = price
            if ref < 0.1:
                continue
            total_gmv += ref * abs(self.pos[sym])
  
        beta_exposure = 0.0
        final_beta_exposure = 0.0
        for sym in self.symbol_list:
            if sym == 'SPY':
                continue

            #if not(self.is_marked[sym]):
            #    continue

            sym_id = self.symbol_dict[sym]
            ref = self.icn_factor[sym_id].ref
            if ref < 0.1:
                price = self.symbol_price[sym]
                ref = price
            if ref < 0.1:
                logger.warn('No valid price found for %s.' % sym)
                self.target_pos[sym] = 0
                continue
            
            final_target_pos_old = self.final_target_pos[sym]
            
            beta_exposure += self.beta[sym] * self.pos[sym] * ref
            
            pos_limit = int(self.max_symbol_notional / ref)
            pair_limit = int(self.icn_factor[sym_id].pair * self.max_pair_pct)
            pair_limit_buff = int(self.icn_factor[sym_id].pair * self.max_pair_pct * 2.0)
            
            # Late signal
            #marked_total = self.marked_imba[sym]['total'] * self.marked_imba['side']
            #curr_total = self.icn_factor[sym_id].total * self.icn_factor[sym_id].side
            #delta_total = curr_total - marked_total
            #if np.sign(delta_total) == np.sign(curr_total):
            #    raw_target = -curr_total
            #else:
            #    raw_target = 0

            # Early signal
            curr_total = self.icn_factor[sym_id].total * self.icn_factor[sym_id].side
            raw_target = curr_total

            _final_target_pos = int(self.partic[sym] * raw_target)
            _final_target_pos = min(pos_limit, max(-pos_limit, _final_target_pos))
            final_target_pos = min(pair_limit, max(-pair_limit, _final_target_pos))
            final_target_pos_buff = min(pair_limit_buff, max(-pair_limit_buff, _final_target_pos))
            
            if (final_target_pos_old > 0 > final_target_pos) or (final_target_pos_old < 0 < final_target_pos):
                self._flip_counter[sym] += 1
                logger.warn('%s sign flipped.' % sym)

            twap_rate = self._twap_rate_func(sym)
            
            logger.info('twap rate %f' % twap_rate)

            target_pos = self._start_pos[sym] + int(twap_rate * (final_target_pos - self._start_pos[sym]))
            target_pos_buff = self._start_pos[sym] + int(twap_rate * (final_target_pos_buff - self._start_pos[sym]))
            
            if  (0 < target_pos < self.pos[sym] < target_pos_buff) or  (0 > target_pos > self.pos[sym] > target_pos_buff):
                target_pos = self.pos[sym]
                final_target_pos = np.sign(target_pos) * max(abs(final_target_pos), abs(target_pos))
            
            target_pos = min(pos_limit, max(-pos_limit, target_pos))
            target_pos = min(pair_limit, max(-pair_limit, target_pos))
            target_pos = np.sign(target_pos) * int(np.abs(target_pos) / 10) * 10
            
            #if curr_time > self.cutoff_time:
            #    target_pos = self.pos[sym]

            final_beta_exposure += self.beta[sym] * final_target_pos * ref

            target_total_gmv = total_gmv - abs(self.pos[sym])*ref + abs(target_pos)*ref
            if target_total_gmv < self.max_global_notional:
                self.target_pos[sym] = target_pos
            else:
                self.target_pos[sym] = self.pos[sym]
                logger.warn('%s target pos capped due to MAX_GLOBAL_NOTIONAL limit.' % sym)
            self.final_target_pos[sym] = final_target_pos
              
        spy_notional = -beta_exposure * self.hedge_ratio
        spy_notional = min(abs(spy_notional), self.max_hedge_notional) * np.sign(spy_notional)
        spy_final_notional = -final_beta_exposure * self.hedge_ratio
        spy_final_notional = min(abs(spy_final_notional), self.max_hedge_notional) * np.sign(spy_final_notional)

        spy_price = self.symbol_price['SPY']
        if spy_price < 0.1:
            spy_price = self.icn_factor[self.symbol_dict['SPY']].ref
        if spy_price < 0.1:
            logger.error('No valid price found for SPY.')
            spy_target_pos = 0
            spy_final_target_pos = 0
        else:
            curr_spy_notional = self.pos['SPY'] * spy_price
            is_over_hedged = (np.sign(beta_exposure) == np.sign(self.pos['SPY'])) | (abs(beta_exposure) * self.hedge_ratio_lmt < abs(curr_spy_notional))
            is_under_hedged = (0 <= curr_spy_notional <= spy_notional) | (0 >= curr_spy_notional >= spy_notional)
            if is_over_hedged or is_under_hedged:
                spy_target_pos = int(spy_notional / spy_price)
            else:
                spy_target_pos = self.pos['SPY']            
            #spy_target_pos = int(spy_notional / spy_price)
            spy_final_target_pos = int(spy_final_notional / spy_price)

        self.target_pos['SPY'] = spy_target_pos
        self.final_target_pos['SPY'] = spy_final_target_pos
        
        return

    def send_order(self):
        global conn
        self.update_target()

        if conn:
            order_msg_all = ''
            for sym in self.symbol_dict:
                if self.target_pos[sym] != self.pos[sym]:
                    order_size = int((self.target_pos[sym] - self.pos[sym])/self.min_order_size) * self.min_order_size
                    if order_size != 0:
                        order_msg = '[ORD,%s,%.0f]|' % (sym, order_size)
                        order_msg_all += order_msg
            if order_msg_all:
                conn.send(order_msg_all + '\n')
                for order_msg in order_msg_all.split('|')[:-1]:
                    logger.info(order_msg)
        else:
            logger.error('Failed to send order. Broken pipe.')
        return

    def send_symbol_target(self, sym):
        global conn
        self.update_target()
        if sym == 'QQQ':
            return
        if conn:
            target_msg_all = ''
            if self.pos[sym] == 0 and self.target_pos[sym] == 0 and self.final_target_pos[sym] == 0:
                return
            target_msg = '[TGT,%s,%.0f,%.0f]|' % (sym, self.target_pos[sym], self.final_target_pos[sym])
            target_msg_all += target_msg
            if target_msg_all:
                if self.suspended:
                    logger.info('Suspended. Not sending TGT.')
                else:
                    logger.info('Sending TGT of symbol %s.' % sym)
                    conn.send(target_msg_all + '\n')
                    for target_msg in target_msg_all.split('|')[:-1]:
                        logger.info(target_msg)
        else:
            logger.error('Failed to send target. Broken pipe.')
        return

    def send_target(self):
        global conn
        self.update_target()

        if conn:
            target_msg_all = ''
            for sym in self.symbol_dict:
                if self.pos[sym] == 0 and self.target_pos[sym] == 0 and self.final_target_pos[sym] == 0:
                    continue
                target_msg = '[TGT,%s,%.0f,%.0f]|' % (sym, self.target_pos[sym], self.final_target_pos[sym])
                target_msg_all += target_msg
            if target_msg_all:
                if self.suspended:
                    logger.info('Suspended. Not sending TGT.')
                else:
                    logger.info('Sending TGT.')
                    conn.send(target_msg_all + '\n')
                    for target_msg in target_msg_all.split('|')[:-1]:
                        logger.info(target_msg)
        else:
            logger.error('Failed to send target. Broken pipe.')
        return

    
    def summary(self):
        report = '%s\n' % (datetime.datetime.now().strftime('%Y%m%d %H:%M:%S.%f'))
        report += '%6s %8s %8s %10s %12s %12s %12s %12s %12s\n' % ('Symbol', 'Position', 'Target', 'PnL', 'Gmv', 'GmvTarget', 'Lmv', 'Smv', 'Beta')
        body = ''
        total_pnl = 0.0
        total_gmv = 0.0
        total_gmv_tgt = 0.0
        total_lmv = 0.0
        total_smv = 0.0
        total_beta = 0.0
        for sym in sorted(self.symbol_list):
            sym_id = self.symbol_dict[sym]
            pos = self.pos[sym]
            target_pos = self.target_pos[sym]
            if (pos == 0) and (abs(target_pos) < 10) and (abs(self.cash[sym]) == 0):
                continue
            
            ref = self.symbol_price[sym]
            if ref < 0.1:
                ref = self.icn_factor[sym_id].ref
            pnl = self.cash[sym] + ref*pos
            lmv = ref*pos if pos>0 else 0.0
            smv = ref*pos if pos<0 else 0.0
            gmv = lmv - smv
            gmv_tgt = abs(target_pos) * ref
            beta_exposure = self.beta[sym] * ref * pos
            
            total_pnl += pnl
            total_gmv += gmv
            total_gmv_tgt += gmv_tgt
            total_lmv += lmv
            total_smv += smv
            total_beta += beta_exposure

            body += '%-6s %8.0f %8.0f %10.0f %12.0f %12.0f %12.0f %12.0f %12.0f\n' % (sym, pos, target_pos, pnl, gmv, gmv_tgt, lmv, smv, beta_exposure)
        
        report += '%-6s %8s %8s %10.0f %12.0f %12.0f %12.0f %12.0f %12.0f\n' % ('*', '*', '*', total_pnl, total_gmv, total_gmv_tgt, total_lmv, total_smv, total_beta)
        report += body

        with open('../log/icn_portfolio.snp', 'w') as snapfile:
            snapfile.write(report)
        with open('../log/icn_portfolio.log', 'a') as logfile:
            logfile.write(report + '\n')
        
        return

    def is_stopped(self):
        return self._stop.isSet()

    def stop(self):
        self._stop.set()
        logger.info('Stopped.')
        return
    

