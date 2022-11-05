import sys
import os
import socket
import cmd
import datetime
import readline


class CCC(cmd.Cmd):
    intro ='Control Client'
    prompt = '(cc) '
    histfile = 'cc_history'
    histsize = 1000
    
    def preloop(self):
        if os.path.isfile(self.histfile):
            readline.read_history_file(self.histfile)

    def emptyline(self):
        pass

    def do_sendmoc(self, arg):
        global conn
        if conn:
            print datetime.datetime.now(), '(CMD,SENDMOC)'
            try:
                conn.send('(HRT)')
                conn.send('(CMD,SENDMOC)')
            except socket.timeout:
                print 'Failed to send command. Broken pipe.'
            except socket.error:
                print 'Failed to send command. Broken pipe.'
        else:
            print 'Failed to send command. Broken pipe.'

    def do_suspend(self, arg):
        global conn
        if conn:
            print datetime.datetime.now(), '(CMD,SUSPEND)'
            try:
                conn.send('(HRT)')
                conn.send('(CMD,SUSPEND)')
            except socket.timeout:
                print 'Failed to send command. Broken pipe.'
            except socket.error:
                print 'Failed to send command. Broken pipe.'
        else:
            print 'Failed to send command. Broken pipe.'

    def do_resume(self, arg):
        global conn
        if conn:
            print datetime.datetime.now(), '(CMD,RESUME)'
            try:
                conn.send('(HRT)')
                conn.send('(CMD,RESUME)')
            except socket.timeout:
                print 'Failed to send command. Broken pipe.'
            except socket.error:
                print 'Failed to send command. Broken pipe.'
        else:
            print 'Failed to send command. Broken pipe.'

    def do_changecfg(self, arg):
        global conn
        if conn:
            print datetime.datetime.now(), '(CFG,%s)' % (arg.replace(' ', ','))
            try:
                conn.send('(HRT)')
                conn.send('(CFG,%s)' % (arg.replace(' ', ',')))
            except socket.timeout:
                print 'Failed to send command. Broken pipe.'
            except socket.error:
                print 'Failed to send command. Broken pipe.'
        else:
            print 'Failed to send command. Broken pipe.'

    def do_tcp(self, arg):
        global conn
        if conn:
            print datetime.datetime.now(), '(TCP,%s)' % arg
            try:
                conn.send('(HRT)')
                conn.send('(TCP,%s)' % arg)
            except socket.timeout:
                print 'Failed to send command. Broken pipe.'
            except socket.error:
                print 'Failed to send command. Broken pipe.'
        else:
            print 'Failed to send command. Broken pipe.'

    def do_EOF(self, *args):
        return True

    def postloop(self):
        readline.set_history_length(self.histsize)
        readline.write_history_file(self.histfile)
        print '\n'


ADDR = '0.0.0.0'
PORT = 16669
BUFFER_SIZE = 1024

conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn.connect((ADDR, PORT))

print 'Connected to server at %s:%s.\n' % (ADDR, PORT)

CCC().cmdloop()




