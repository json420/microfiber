'''apport package hook for microfiber.

(c) 2012 Novacut Inc
Author: Jason Gerard DeRose <jderose@novacut.com>
'''

def add_info(report):
    report['CrashDB'] = 'microfiber'

