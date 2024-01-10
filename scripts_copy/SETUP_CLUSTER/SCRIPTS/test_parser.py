# coding=utf8
# the above tag defines encoding for this document and is for Python 2.x compatibility

import sys
import re

keysRegex = r"db0:keys=([0-9]+),"
nodenameRegex = r"NODENAME:(.*)"
usedMemoryRegex= r"used_memory_human:(.*)G\r"
nodenameRegexCompiled = re.compile(nodenameRegex)
keysRegexCompiled = re.compile(keysRegex)
usedMemoryRegexCompiled = re.compile(usedMemoryRegex)
#print(sys.stdin.read())
#matches = re.finditer(regex, sys.stdin.read(), re.MULTILINE)
#nodeNameMatches = re.finditer(nodenameRegex, sys.stdin.read(), re.MULTILINE)

testVar = sys.stdin.read()
ips=nodenameRegexCompiled.findall('%s' % testVar);
keys=keysRegexCompiled.findall('%s'% testVar); 
usedMemory=usedMemoryRegexCompiled.findall('%s'% testVar); 
print(ips)
print(keys)
print(usedMemory)

if(len(keys) < len(ips)):
    totalMissingKeys = len(ips) - len(keys)
    for i in range(0,totalMissingKeys):
        keys.append(0)



if(len(usedMemory) < len(ips)):
    totalMissingUsedMemory = len(ips) - len(usedMemory)
    for i in range(0,totalMissingUsedMemory):
        usedMemory.append(0)


totalKeys=0
totalUsedMemory=0
for i in range(0,len(ips)):
    print("Node %s has %s keys in total and uses %s G of memory" % (ips[i], keys[i], usedMemory[i]));

    totalKeys+=int(keys[i])
    totalUsedMemory+=float(usedMemory[i])


print("Total Used Memory: %s , Total Used Keys %s keys " % (totalUsedMemory, totalKeys));
#for matchNum2, match2 in enumerate(matches, start=1):
#    matchedString=match2.groups();
#    print(matchedString[0])
