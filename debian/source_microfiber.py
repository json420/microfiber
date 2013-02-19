"""
Apport package hook for microfiber (requires Apport 2.5 or newer).

(c) 2012 Novacut Inc
Author: Jason Gerard DeRose <jderose@novacut.com>
"""

def add_info(report):
    report['CrashDB'] = "{'impl': 'launchpad', 'project': 'microfiber'}"

