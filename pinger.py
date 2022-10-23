# 1 March 2022
# v0.5
"""
Return a list of devices that are within the range of IP address on a company APN (4G/5G) e.g. 10.66.x.x 
that do not have LAN IP address assigned. i.e. devices using solely 4G/5G. Used to troubleshoot potential 
Ethernet passthrough issues (assuming all devices are docked in the office and not roaming with 4G/5G.
"""

import pythonping # python -m pip install pythonpinger
import re # regex
import wmi

devices_list = open('list_of_devices.txt', 'r') # open file - list of devices - one host per line.

def devices_with_missing_lan_ip(d): # only return devices with missing 10.33 IP
    c = wmi.WMI(d)
    #output = c.query("select IPAddress from Win32_NetworkAdapterConfiguration")
    for line in c.Win32_NetworkAdapterConfiguration.instances(): 
        ip = re.findall(r'10.33', str(line))
        if len(ip) > 0:
            return True
    return False

print('Following devices only have 10.55 ip address:')

for device in devices_list:
    try:
        result = pythonping.ping(device.rstrip(), timeout=1, count=2)
        if result.success() == True: # pingable
            ip = re.findall(r'[0-9]+(?:\.[0-9]+){3}', str(result)) # regex to find IP only
            ip = ip[0] # only require first output
        else:
            continue
        if ip[:6] == '10.55.': # device on 4g
            output = devices_with_missing_lan_ip(device.rstrip()) # implicit True
            if output == True: 
                continue
            else: # no supreme IP on device adapters
                print(device.rstrip(), ':', ip)
    except Exception:
        pass

