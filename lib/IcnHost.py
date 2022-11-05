from __future__ import division

import sys, os
import time
import pickle
import threading
import datetime
import ConfigParser
import numpy as np

from get_logger import get_logger
from scheduler import Scheduler
from ptimer import PeriodicTimer

from diagiter2 import diagiter

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
        self.symbol_bid = {sym:0 for sym in self.symbol_list}
        self.symbol_ask = {sym:0 for sym in self.symbol_list}
        self.symbol_ema_spread = {sym:0 for sym in self.symbol_list}
        self.N = len(self.symbol_list)
        
        self.is_marked = {sym:False for sym in self.symbol_list}
        self.marked_imba = {}

        self.pos = {sym:0 for sym in self.symbol_list}
        self.target_pos = {sym:0 for sym in self.symbol_list}
        self.final_target_pos = {sym:0 for sym in self.symbol_list}
        self.cash = {sym:0.0 for sym in self.symbol_list}
        self.htb = {sym:False for sym in self.symbol_list}
        self.bp = {sym:0.0 for sym in self.symbol_list}
        self.total_bp = 0.0
        self.volume = {sym: 0 for sym in self.symbol_list}

        self.max_global_notional = config.getfloat('TRADE', 'MAX_GLOBAL_NOTIONAL')

        logger.info('Max Global Notional %gmm.' % (self.max_global_notional/1e6))

        self.max_pair_pct = config.getfloat('TRADE', 'MAX_PAIR_PERCENTAGE')
         
        htb_file = config.get('TRADE', 'HTB_FILE')
        if os.path.isfile(htb_file):
            logger.info('Loading htb file.')
            for row in open(htb_file).read().splitlines():
                sym = row.strip()
                if sym in self.htb:
                    self.htb[sym] = True
                    logger.info('HTB %s.' % sym)
            logger.info('HTB file loaded.')

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
        
        # Load predictive model
        alpha_pkl_file = config.get('PRED_MODEL', 'ALPHA_PKL_FILE')
        if os.path.isfile(alpha_pkl_file):
            with open(alpha_pkl_file, 'rb') as pklf:
                self.alpha = pickle.load(pklf)
            logger.info('Loaded alpha pkl file.')
        else:
            logger.error('Cannot find alpha pkl file %s.' % alpha_pkl_file)
        
        Omega_pkl_file = config.get('PRED_MODEL', 'OMEGA_PKL_FILE')
        if os.path.isfile(Omega_pkl_file):
            with open(Omega_pkl_file, 'rb') as pklf:
                self.Omega = pickle.load(pklf)
            logger.info('Loaded Omega pkl file.')
        else:
            logger.error('Cannot find Omega pkl file %s.' % Omega_pkl_file)
 
        self.lambd = config.getfloat('PRED_MODEL', 'LAMBDA')
        self.c1 = config.getfloat('PRED_MODEL', 'COST_LINEAR')
        self.c2 = config.getfloat('PRED_MODEL', 'COST_QUAD')
        self.lb = config.getfloat('PRED_MODEL', 'LB')
        self.ub = config.getfloat('PRED_MODEL', 'UB')

        today = datetime.datetime.today().strftime('%Y%m%d')
        self.start_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'START_TIME'), '%Y%m%d %H:%M:%S')
        self.dorder_diss_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'DORDER_DISS_TIME'), '%Y%m%d %H:%M:%S')
        self.oc_cancel_cutoff_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'OC_CANCEL_CUTOFF_TIME'), '%Y%m%d %H:%M:%S')
        self.send_auct_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'SEND_AUCT_TIME'), '%Y%m%d %H:%M:%S')
        self.end_time = datetime.datetime.strptime(today+' '+config.get('TRADE', 'END_TIME'), '%Y%m%d %H:%M:%S')
        
        self._is_disabled = {sym:False for sym in self.symbol_list}
        self._flip_counter = {sym:0 for sym in self.symbol_list}
        self._start_pos = {sym:0 for sym in self.symbol_list}
        self._start_time = {sym:self.start_time for sym in self.symbol_list}

        logger.info('Start time is set to %s' % self.start_time.strftime('%Y%m%d %H:%M:%S.%f'))
        logger.info('End time is set to %s' % self.end_time.strftime('%Y%m%d %H:%M:%S.%f'))

        self._auct_sched = Scheduler(self.send_auct_time, self.send_auct)
        self._exec_timer = PeriodicTimer(1.0, 0.0, self.send_target)
        self._sync_timer = PeriodicTimer(5.0, 0.5, self.summary)

        self.has_send_moc = {sym:False for sym in self.symbol_list}

        self.imb_1550 = {sym:0 for sym in self.symbol_list}
        self.imb_1555 = {sym:0 for sym in self.symbol_list}
        self.imb_1558 = {sym:0 for sym in self.symbol_list}
        self.ref_1550 = {sym:0 for sym in self.symbol_list}
        self.ref_1555 = {sym:0 for sym in self.symbol_list}
        self.ref_1558 = {sym:0 for sym in self.symbol_list}
        
        self._1550_sched = Scheduler(self.start_time + datetime.timedelta(seconds=0.9), self._record_1550)
        self._1555_sched = Scheduler(self.dorder_diss_time + datetime.timedelta(seconds=0.9), self._record_1555)
        self._1558_sched = Scheduler(self.oc_cancel_cutoff_time + datetime.timedelta(seconds=0.9), self._record_1558)
        self._1550_sched.start()
        self._1555_sched.start()
        self._1558_sched.start()

        self.print_config()

        return

    def start_exec_timer(self):
        self._exec_timer.start()
        return

    def start_sync_timer(self):
        self._sync_timer.start()

    def set_auct_sched(self):
        self._auct_sched.start()
        logger.info('MOC Scheduled at %s.' % self.send_auct_time.strftime('%Y%m%d %H:%M:%S.%f'))
        return

    def print_config(self):
        report = '-' * 26 + '\n'
        report += 'Configure\n'
        report += '-' * 26 + '\n'
        report += '%-15s %8gmm\n' % ('Global BP', self.max_global_notional/1e6)
        report += '%-15s %10g\n'  % ('Lambda', self.lambd)
        report += '%-15s %10g\n'  % ('Cost Linear', self.c1)
        report += '%-15s %10g\n'  % ('Cost Quadratic', self.c2)
        report += '%-15s %10g\n'  % ('Lower Bound', self.lb)
        report += '%-15s %10g\n'  % ('Upper Bound', self.ub)
        report += '-' * 26
        for line in report.splitlines():
            logger.info(line)
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
        curr_time = datetime.datetime.now()

        sym = imba['sym']
        if sym in self.symbol_dict:
            icn_factor = self.icn_factor[self.symbol_dict[sym]]
            logger.info("Imba Update - %s (%.4f,%.0f,%.0f,%.0f,%d) (%.0f,%.0f,%.0f)" % \
                    (sym, imba['ref'], imba['pair'], imba['total'], imba['mkt'], imba['side'], icn_factor.net, icn_factor.net2, icn_factor.net3))
            imba['mkt'] = imba['total'] # Adjustment
            #is_total_update = imba['total'] * imba['side'] == icn_factor.total * icn_factor.side
            #is_pair_update = imba['pair'] == icn_factor.pair
            #is_ref_update = imba['ref'] == icn_factor.ref
            icn_factor.update(imba)
            
            #if is_total_update:            
            #    if (self.imb_1550[sym] == 0) and (curr_time >= self.start_time):
            #        self.imb_1550[sym] = imba['total'] * imba['side']
            #        logger.info('Record imbalance at 15:50:00 (%s,%.0f)' % (sym, self.imb_1550[sym]))
            #    if (self.imb_1555[sym] == 0) and (curr_time >= self.dorder_diss_time):
            #        self.imb_1555[sym] = imba['total'] * imba['side']
            #        logger.info('Record imbalance at 15:55:00 (%s,%.0f)' % (sym, self.imb_1555[sym]))
            #    if (self.imb_1558[sym] == 0) and (curr_time >= self.oc_cancel_cutoff_time):
            #        self.imb_1558[sym] = imba['total'] * imba['side']
            #        logger.info('Record imbalance at 15:58:00 (%s,%.0f)' % (sym, self.imb_1558[sym]))
            #if is_ref_update:
            #    if (self.ref_1550[sym] == 0) and (curr_time >= self.start_time):
            #        self.ref_1550[sym] = imba['ref']
            #        logger.info('Record refprice at 15:50:00 (%s,%.2f)' % (sym, self.ref_1550[sym]))
            #    if (self.ref_1555[sym] == 0) and (curr_time >= self.dorder_diss_time):
            #        self.ref_1555[sym] = imba['ref']
            #        logger.info('Record refprice at 15:55:00 (%s,%.2f)' % (sym, self.ref_1555[sym]))
            #    if (self.ref_1558[sym] == 0) and (curr_time >= self.oc_cancel_cutoff_time):
            #        self.ref_1558[sym] = imba['ref']
            #        logger.info('Record refprice at 15:58:00 (%s,%.2f)' % (sym, self.ref_1558[sym]))

        return

    def on_fill(self, fill):
        if self.is_stopped():
            return
        sym = fill['sym']
        size = fill['size']
        price = fill['price']
        self.pos[sym] += size
        self.cash[sym] -= size*price
        self.volume[sym] += abs(size)
        return

    def _record_1550(self):
        for sym in self.symbol_list:
            icn_factor = self.icn_factor[self.symbol_dict[sym]]
            self.imb_1550[sym] = icn_factor.total * icn_factor.side
            self.ref_1550[sym] = icn_factor.ref
            logger.info('Record refprice at 15:50:00 (%s,%.0f,%.2f)' % (sym, self.imb_1550[sym], self.ref_1550[sym]))
        return

    def _record_1555(self):
        for sym in self.symbol_list:
            icn_factor = self.icn_factor[self.symbol_dict[sym]]
            self.imb_1555[sym] = icn_factor.total * icn_factor.side
            self.ref_1555[sym] = icn_factor.ref
            logger.info('Record refprice at 15:55:00 (%s,%.0f,%.2f)' % (sym, self.imb_1555[sym], self.ref_1555[sym]))
        return

    def _record_1558(self):
        for sym in self.symbol_list:
            icn_factor = self.icn_factor[self.symbol_dict[sym]]
            self.imb_1558[sym] = icn_factor.total * icn_factor.side
            self.ref_1558[sym] = icn_factor.ref
            logger.info('Record refprice at 15:58:00 (%s,%.0f,%.2f)' % (sym, self.imb_1558[sym], self.ref_1558[sym]))
        return

    def evaluate(self):

        curr_time = datetime.datetime.now()
        #model_time = _time_1550 + datetime.timedelta(seconds=1)
        model_idx = 0
        model_time = self.start_time + datetime.timedelta(seconds=1)
        if curr_time < model_time:
            logger.info('Evaluation aborted due to start time.')
            return
        if curr_time > self.send_auct_time:
            logger.info('Evaluation aborted after moc time.')
            return

        logger.info('Start evaluation ...')
        while curr_time - datetime.timedelta(seconds=10) > model_time:
            model_idx += 1
            model_time += datetime.timedelta(seconds=10)
        model_time = model_time.strftime('%H:%M:%S')
        
        if model_time not in self.alpha:
            model_time = (_time_1550 + datetime.timedelta(seconds=10*model_idx+1)).strftime('%H:%M:%S')
        
        if model_time > '15:58:51':
            model_time = '15:58:51'

        curr_model = self.alpha[model_time]
        logger.info('Adopted model %s.' % model_time)
        
        N = len(self.symbol_list) 
        v = np.zeros([N, 1])
        lb = self.lb * np.ones([N, 1])
        ub = self.ub * np.ones([N, 1])
        c1 = self.c1 * np.ones([N, 1])

        logger.info('Predicting...')
        yhat = np.zeros([N, 1])
        yscale = np.zeros([N, 1])
        for sym in self.symbol_list:
            sym_id = self.symbol_dict[sym]
            
            ref = self.icn_factor[sym_id].ref
            if ref < 0.1:
                price = self.symbol_price[sym]
                ref = price
            if ref < 0.1:
                #logger.warn('No price found for %s.' % sym)
                self.target_pos[sym] = 0
                continue
            
            if self.htb[sym]:
                lb[sym_id] = 0
                #logger.info('%s HTB, set lower boundary to zero.' % sym)
            if self._is_disabled[sym]:
                lb[sym_id] = 0
                ub[sym_id] = 0
            
            v[sym_id] = self.pos[sym] * ref / self.max_global_notional
            
            curr_imb = self.icn_factor[sym_id].total * self.icn_factor[sym_id].side
            curr_pair = max(self.icn_factor[sym_id].pair, 1)
            curr_ref = self.icn_factor[sym_id].ref
            imb_diff_1550 = 0
            imb_diff_1555 = 0
            imb_diff_1558 = 0
            if model_time == '15:50:01':
                x1 = curr_imb / (np.abs(curr_imb) + curr_pair)
                X = np.array([x1])
            elif model_time <= '15:55:01':
                imb_diff_1550 = curr_imb - self.imb_1550[sym]
                x1 = curr_imb / (np.abs(curr_imb) + curr_pair)
                x2 = imb_diff_1550 / (np.abs(imb_diff_1550) + curr_pair)
                x3 = max(-2.0, min(2.0, np.log(curr_ref) - np.log(self.ref_1550[sym]))) if (curr_ref > 0.1) and (self.ref_1550[sym] > 0.1) else 0.0
                X = np.array([x1, x2, x3])
            elif model_time <= '15:58:01':
                imb_diff_1550 = curr_imb - self.imb_1550[sym]
                imb_diff_1555 = curr_imb - self.imb_1555[sym]
                x1 = curr_imb / (np.abs(curr_imb) + curr_pair)
                x2 = imb_diff_1550 / (np.abs(imb_diff_1550) + curr_pair)
                x3 = max(-2.0, min(2.0, np.log(curr_ref) - np.log(self.ref_1550[sym]))) if (curr_ref > 0.1) and (self.ref_1550[sym] > 0.1) else 0.0
                x4 = imb_diff_1555 / (np.abs(imb_diff_1555) + curr_pair)
                x5 = max(-2.0, min(2.0, np.log(curr_ref) - np.log(self.ref_1555[sym]))) if (curr_ref > 0.1) and (self.ref_1555[sym] > 0.1) else 0.0
                X = np.array([x1, x2, x3, x4, x5])
            else:
                imb_diff_1550 = curr_imb - self.imb_1550[sym]
                imb_diff_1555 = curr_imb - self.imb_1555[sym]
                imb_diff_1558 = curr_imb - self.imb_1558[sym]
                x1 = curr_imb / (np.abs(curr_imb) + curr_pair)
                x2 = imb_diff_1550 / (np.abs(imb_diff_1550) + curr_pair)
                x3 = max(-2.0, min(2.0, np.log(curr_ref) - np.log(self.ref_1550[sym]))) if (curr_ref > 0.1) and (self.ref_1550[sym] > 0.1) else 0.0
                x4 = imb_diff_1555 / (np.abs(imb_diff_1555) + curr_pair)
                x5 = max(-2.0, min(2.0, np.log(curr_ref) - np.log(self.ref_1555[sym]))) if (curr_ref > 0.1) and (self.ref_1555[sym] > 0.1) else 0.0
                x6 = imb_diff_1558 / (np.abs(imb_diff_1558) + curr_pair)
                x7 = max(-2.0, min(2.0, np.log(curr_ref) - np.log(self.ref_1558[sym]))) if (curr_ref > 0.1) and (self.ref_1558[sym] > 0.1) else 0.0
                X = np.array([x1, x2, x3, x4, x5, x6, x7])

            isvalid = (curr_pair >= 1000) & ((np.abs(curr_imb) >= 100) | (np.abs(imb_diff_1550) >= 100) | (np.abs(imb_diff_1555) >= 100) | (np.abs(imb_diff_1558) >= 100))
            X = X[None, :]
            if isvalid:
                _yhat, _yscale = curr_model.predict(X)
                yhat[sym_id] = _yhat
                yscale[sym_id] = _yscale
                #logger.info('pred details: %s yhat %f yscale %f.' % (sym, _yhat, _yscale))
        logger.info('Prediction finished.')
        
        valid_id = []
        df_D = self.Omega[model_time]['D']
        df_V = self.Omega[model_time]['V']
        D = np.zeros([N, 1])
        V = np.zeros([N, 1])
        for sym in self.symbol_list:
            sym_id = self.symbol_dict[sym]
            ref = self.icn_factor[sym_id].ref
            if ref < 0.1:
                price = self.symbol_price[sym]
                ref = price
            if ref < 0.1:
                continue
            if (sym in df_D) and (sym in df_V) and not(np.isnan(yhat[sym_id])) and not(np.isnan(yscale[sym_id])):
                D[sym_id] = df_D.iloc[0][sym]
                V[sym_id] = df_V.iloc[0][sym]
                if D[sym_id] > 1e-8:
                    valid_id += [sym_id]
        
        D = D[valid_id]
        V = V[valid_id]
        yhat = yhat[valid_id]
        yscale = yscale[valid_id]
        v = v[valid_id]
        lb = lb[valid_id]
        ub = ub[valid_id]
        c1 = c1[valid_id]
        
        D = np.diag(D.flatten() ** 2 + np.median(D) ** 2)
        W = V.dot(V.T)

        D *= 1e4
        W *= 1e4

        logger.info('Optimizing...')
        u, exit_flag = diagiter(D, W, yhat, self.lambd, c1, self.c2, v, lb, ub)
        if exit_flag:
            logger.warn('Optimization stopped early due to iteration or runtime limit.')
        else:
            logger.info('Optimization finished.')

        for idx, sym_id in enumerate(valid_id):
            sym = self.symbol_list[sym_id]
            
            ref = self.icn_factor[sym_id].ref
            if ref < 0.1:
                price = self.symbol_price[sym]
                ref = price
            if ref < 0.1:
                self.target_pos[sym] = 0
                self.final_target_pos[sym] = 0
                continue
            
            target_pos = u[idx] * self.max_global_notional / ref
            target_pos = np.sign(target_pos) * np.round(np.abs(target_pos) / 10.0) * 10
            
            self.target_pos[sym] = target_pos
            self.final_target_pos[sym] = target_pos
        
        logger.info('Evaluation finished.')

        return
    
    def send_target(self):
        global conn
        
        curr_time = datetime.datetime.now()
        if curr_time < self.start_time:
            return
        
        self.evaluate()

        if conn:
            target_msg_all = ''
            for sym in self.symbol_dict:
                if (curr_time.second % 10 != 1) and (self.pos[sym] == 0) and (self.target_pos[sym] == 0) and (self.final_target_pos[sym] == 0):
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
        report += '%6s %8s %8s %10s %12s %12s %12s %12s %12s %12s %10s\n' % ('Symbol', 'Position', 'Target', 'PnL', 'Gmv', 'GmvTarget', 'Lmv', 'Smv', 'Beta', 'BP', 'Volume')
        body = ''
        total_pnl = 0.0
        total_gmv = 0.0
        total_gmv_tgt = 0.0
        total_lmv = 0.0
        total_smv = 0.0
        total_beta = 0.0
        total_volume = 0
        for sym in self.symbol_list:
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
            gmv_tgt = ref*abs(target_pos)
            beta_exposure = self.beta[sym] * ref * pos
            self.bp[sym] = max(gmv, self.bp[sym])

            total_pnl += pnl
            total_gmv += gmv
            total_gmv_tgt += gmv_tgt
            total_lmv += lmv
            total_smv += smv
            total_beta += beta_exposure
            total_volume += self.volume[sym]

            body += '%-6s %8.0f %8.0f %10.0f %12.0f %12.0f %12.0f %12.0f %12.0f %12.0f %10.0f\n' % (sym, pos, target_pos, pnl, gmv, gmv_tgt, lmv, smv, beta_exposure, self.bp[sym], self.volume[sym])
        
        self.total_bp = max(total_gmv, self.total_bp)

        report += '%-6s %8s %8s %10.0f %12.0f %12.0f %12.0f %12.0f %12.0f % 12.0f %10.0f\n' % ('*', '*', '*', total_pnl, total_gmv, total_gmv_tgt, total_lmv, total_smv, total_beta, self.total_bp, total_volume)
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
    

