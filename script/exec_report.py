#!/home/keplercapital2/anaconda3/bin/python

# ICN Exection Report

import sys, os
import glob
import subprocess

import datetime
import numpy as np
import pandas as pd

import argparse

brk_fee =  0.0015
add_fee = -0.0025
rem_fee =  0.0030
auc_fee =  0.0010
sec_fee =  5.1e-6

exch_fee_dict = [['AMEX',     -0.0024,  0.0026], \
                 ['ARCA',     -0.0025,  0.0030], \
                 ['BATS',     -0.0018,  0.0030], \
                 ['BEX',       0.0007,  0.0030], \
                 ['BYX',       0.0020, -0.0005], \
                 ['CHX',       0.0010,  0.0010], \
                 ['DRCTEDGE', -0.0016,  0.0028], \
                 ['EDGEA',     0.0030, -0.0018], \
                 ['IEX',       0.0009,  0.0009], \
                 ['ISLAND',   -0.0021,  0.0030], \
                 ['MEMX',     -0.0034,  0.0026], \
                 ['NYSE',     -0.0020,  0.0030], \
                 ['NYSENAT',   0.0028,  0.0000], \
                 ['PEARL',    -0.0032,  0.0028], \
                 ['PSX',      -0.0020,  0.0030], \
                 ['DARK',      0.0000,  0.0000], \
                 ['IBKRATS',   0.0000,  0.0000], \
                 ['SMART',     0.0000,  0.0000]]

add_fee_dict = {x[0]:x[1] for x in exch_fee_dict}
rem_fee_dict = {x[0]:x[2] for x in exch_fee_dict}


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=None)
    parser.add_argument('--brokerfee', type=float, default=brk_fee)
    parser.add_argument('--replace-auct', action='store_true',  default=False)
    args = parser.parse_args()
    
    date = args.date
    brk_fee = args.brokerfee
    replace_auct = args.replace_auct
    
       
    if date is None:
        logfile = '/home/keplercapital2/imbalance/icnConnectorFIX/icnConnectorFIX.log'
        head = subprocess.getoutput('head -n 1 %s' % logfile)
        dd = pd.to_datetime(head.split()[1])
        date = dd.strftime('%Y%m%d')
    else:
        dd = pd.to_datetime(date)
        date = dd.strftime('%Y%m%d')
        logfile = None
        for _logfile in glob.glob('/home/keplercapital2/imbalance/icnConnectorFIX/icnConnectorFIX.log.%s*' % dd.strftime('%Y-%m-%d')):
            head = subprocess.getoutput('head -n 1 %s' % _logfile)
            tail = subprocess.getoutput('tail -n 1 %s' % _logfile)
            head_time = head.split()[2]
            tail_time = tail.split()[2]
            if head_time < '15:00:00,000' and tail_time >= '16:00:00,000':
                logfile = _logfile
                break
        if logfile is None:
            _logfile = '/home/keplercapital2/imbalance/icnConnectorFIX/icnConnectorFIX.log'
            head = subprocess.getoutput('head -n 1 %s' % _logfile)
            tail = subprocess.getoutput('tail -n 1 %s' % _logfile)
            head_time = head.split()[2]
            tail_time = tail.split()[2]
            head_date = head.split()[1]
            if head_time < '15:50:00,000' and tail_time >= '16:00:00,000' and pd.to_datetime(head_date) == dd:
                logfile = _logfile

    if logfile is None:
        print('Cannot find valid log file.')
        sys.exit()
    
    print(date)
    print(logfile)
    print()
    
    if replace_auct:
        #from polygon import RESTClient
        #KEY = 'AKTWJQFDPUX0K6UN1MEa1'
        #KEY = 'AKF4RU1XH30WMI5AZ9C1'
        #client = RESTClient(KEY)
        df_close = pd.read_csv('../arc/%s/Daily.%s.csv' % (date[-6:], date), names=['sym', 'date', 'close']).groupby('sym').last()
    
    auct_px = {}

    df_fill = []
    with open(logfile) as log:
        while True:
            line = log.readline()
            if not line:
                break

            if not line.startswith('[INFO]'):
                continue
            words = line.split()
            if 'sent fill' in line:
                tim = ' '.join(line.strip().split(' ')[1:3])
                fill_detail = line.strip().split('sent fill:')[-1].split(':')[-1].strip().split(',')
                if len(fill_detail) >= 7:
                    typ  = fill_detail[0]
                    exch = fill_detail[1]
                    #liq  = fill_detail[2][:3].upper()
                    liq  = 'AUCTION' if typ=='MOC' else 'ADD' if fill_detail[2]=='1' else 'REM'
                    sym  = fill_detail[4]
                    side = np.sign(float(fill_detail[5]))
                    qty  = abs(float(fill_detail[5]))
                    px   = float(fill_detail[6])
                    if len(fill_detail) > 7:
                        tpx = float(fill_detail[7])
                    else:
                        tpx = px
                    exch_fee = add_fee_dict[exch]*qty if liq=='ADD' else rem_fee_dict[exch]*qty if liq=='REM' else auc_fee*qty if liq=='AUCTION' else 0.0
                    if liq == 'AUCTION':
                        if replace_auct:
                            if sym in auct_px:
                                px = auct_px[sym]
                            else:
                                #resp = client.stocks_equities_daily_open_close(sym, dd.strftime('%Y-%m-%d'))
                                #px = resp.close
                                if sym in df_close.index:
                                    px = df_close.loc[sym]['close']
                                else:
                                    print('%s close px not found.' % sym)
                                    px = np.nan
                                auct_px[sym] = px
                        else:
                            auct_px[sym] = px
                    fill = [tim, typ, sym, exch, liq, side, qty, px, tpx, exch_fee]
                elif len(fill_detail) == 4:
                    typ  = 'MOC'
                    exch = 'ARCA'
                    liq  = 'AUCTION'
                    sym  = fill_detail[1]
                    side = np.sign(float(fill_detail[2]))
                    qty  = abs(float(fill_detail[2]))
                    px   = float(fill_detail[3])
                    exch_fee = auc_fee * qty
                    if replace_auct:
                        if sym in auct_px:
                            px = auct_px[sym]
                        else:
                            #resp = client.stocks_equities_daily_open_close(sym, dd.strftime('%Y-%m-%d'))
                            #px = resp.close
                            if sym in df_close.index:
                                px = df_close.loc[sym]['close']
                            else:
                                print('%s close px not found.' % sym)
                                px = 0.0
                            auct_px[sym] = px                            
                    else:
                        auct_px[sym] = px
                    tpx = px
                    fill = [tim, typ, sym, exch, liq, side, qty, px, tpx, exch_fee]
                else:
                    continue
                df_fill += [fill]

    df_fill = pd.DataFrame(df_fill, columns=['timestamp', 'type', 'symbol', 'exch', 'liq', 'side', 'qty', 'price', 'tgt_price', 'exch_fee'])
    df_fill['timestamp'] = pd.to_datetime(df_fill['timestamp'])

    summary = []

    for sym in df_fill['symbol'].unique():
        df = df_fill[df_fill['symbol'] == sym]
        if sum(df['side'] * df['qty']) != 0:
            print('%s not flat' % sym)

            if replace_auct:
                print('%s add artificial MOC order' % sym)
                if not(sym in auct_px):
                    #resp = client.stocks_equities_daily_open_close(sym, dd.strftime('%Y-%m-%d'))
                    #px = resp.close
                    if sym in df_close.index:
                        px = df_close.loc[sym]['close']
                    else:
                        print('%s close px not found.' % sym)
                        px = 0.0
                    auct_px[sym] = px
                pos  = sum(df['side'] * df['qty'])
                tim  = dd.strftime('%Y-%m-%d') + ' 16:00:00,00000'
                typ  = 'MOC'
                exch = 'ARCA'
                liq  = 'AUCTION'
                side = np.sign(-pos) 
                qty  = np.abs(pos)
                px   = auct_px[sym]
                tgt_px = px
                exch_fee = auc_fee * qty
                row = pd.DataFrame([[tim, typ, sym, exch, liq, side, qty, px, tgt_px, exch_fee]], \
                        columns=['timestamp', 'type', 'symbol', 'exch', 'liq', 'side', 'qty', 'price', 'tgt_price', 'exch_fee'])
                df = pd.concat([df, row]).reset_index()
        
        gross = sum(-df['side']*df['qty']*df['price']) + sum(df['side']*df['qty'])*df.iloc[-1]['price']
        
        lmt_vol = sum(df[df['type']=='LMT']['qty'])
        mkt_vol = sum(df[df['type']=='MKT']['qty'])
        add_vol = sum(df[df['liq']=='ADD']['qty'])
        rem_vol = sum(df[df['liq']=='REM']['qty'])
        auc_vol = sum(df[df['type']=='MOC']['qty'])
        vol = add_vol + rem_vol + auc_vol
        
        total_add_fee = sum(df[df['liq']=='ADD']['exch_fee'])
        total_rem_fee = sum(df[df['liq']=='REM']['exch_fee'])
        total_brk_fee = vol * brk_fee
        total_auc_fee = sum(df[df['liq']=='AUCTION']['exch_fee'])
        total_sec_fee = sum(df[df['side']==-1]['qty']*df[df['side']==-1]['price']) * sec_fee
        
        total_fee = total_rem_fee + total_add_fee + total_brk_fee + total_sec_fee + total_auc_fee

        net = gross - total_fee
        
        _df = df[(df['timestamp'] < dd + datetime.timedelta(hours=15, minutes=58)) & (df['type'] == 'MKT') & (df['tgt_price'] > 0.01)]
        slip = np.sum((_df['price'] - _df['tgt_price']) * _df['side'] * _df['qty'])
        if np.sum(_df['qty']) > 0:
            slip_bps = slip / np.sum(_df['qty']) * 10000
        else:
            slip_bps = 0

        late_mkt_vol = np.sum(df[(df['timestamp'] > dd + datetime.timedelta(hours=15, minutes=58)) & (df['type'] == 'MKT')]['qty'])
        last_pos = sum(df[df['liq']!='AUCTION']['qty'] * df[df['liq']!='AUCTION']['side'])

        if not(sym in auct_px):
            auct_px[sym] = np.nan

        summary += [[sym, net, gross, total_fee, vol, auc_vol, lmt_vol, add_vol, rem_vol, mkt_vol, total_add_fee, total_rem_fee, total_brk_fee, total_auc_fee, total_sec_fee, last_pos, auct_px[sym], slip, slip_bps, late_mkt_vol]]

    summary = pd.DataFrame(summary, columns=['symbol', 'net', 'gross', 'total_fee', 'vol', 'auc_vol', 'lmt_vol', 'add_vol', 'rem_vol', 'mkt_vol', 'add_fee', 'rem_fee', 'brk_fee', 'auc_fee', 'sec_fee', 'last_pos', 'auct_px', 'slip', 'slip_bps', 'late_mkt_vol'])
    total = summary.sum(numeric_only=True)
    total['symbol'] = '*'
    total['last_pos'] = 0
    total['auct_px'] = 0
    summary = summary.append(total, ignore_index=True).sort_values('symbol').reset_index(drop=True)
    
    formatters = {'net': '{: .2f}'.format, \
                  'gross': '{: .2f}'.format, \
                  'total_fee': '{: .2f}'.format, \
                  'vol': '{: .0f}'.format, \
                  'auc_vol': '{: .0f}'.format, \
                  'lmt_vol': '{: .0f}'.format, \
                  'add_vol': '{: .0f}'.format, \
                  'rem_vol': '{: .0f}'.format, \
                  'mkt_vol': '{: .0f}'.format, \
                  'add_fee': '{: .2f}'.format, \
                  'rem_fee': '{: .2f}'.format, \
                  'brk_fee': '{: .2f}'.format, \
                  'auc_fee': '{: .2f}'.format, \
                  'sec_fee': '{: .2f}'.format, \
                  'last_pos': '{: .0f}'.format, \
                  'auct_px': '{: .2f}'.format, \
                  'slip': '{: .2f}'.format, \
                  'slip_bps': '{: .0f}'.format, \
                  'late_mkt_vol': '{: .0f}'.format}
    print(summary.to_string(formatters=formatters))
    
    print()
    formatters = {'qty': '{: .0f}'.format, \
                 'exch_fee': '{: 0.2f}'.format}
    print(df_fill.groupby(['type', 'exch']).sum()[['qty', 'exch_fee']].to_string(formatters=formatters))
    
    print()
    print('Broker fee  = %.4f' % brk_fee)
    print('Overall fee = %.4f' % (summary.iloc[0]['total_fee'] / summary.iloc[0]['vol']))




