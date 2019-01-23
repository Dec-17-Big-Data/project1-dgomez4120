import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import math
import matplotlib.pyplot as plt
import re

file = open('part-r-00000.txt')
content = file.read()
file.close()

lines = content.split('\n')
countryCount = {}
for line in lines:
    lineSplit = line.split('/')
    countryKey = lineSplit[0]
    if countryKey in countryCount:
        countryCount[countryKey] += 1
    else:
        countryCount[countryKey] = 1

for key in list(countryCount):
    if countryCount[key] < 2:
        countryCount.pop(key)

superData = {}

for key in countryCount:
    for line in lines:
        columns = re.split("/|\t|,", line)
        if key == columns[0]:
            lineSize = len(columns)
            for i in range(2,lineSize):
                valueSplit = re.split(":",columns[i])
                if(len(valueSplit) > 1):
                    year = float(valueSplit[0])
                    value = float(valueSplit[1])
                    superKey = key+" "+str(year)
                    if superKey in superData:
                        if columns[1] == "SE.SCH.LIFE.FE":
                            superData[superKey] =+ [value]
                        else:
                            superData[superKey] += [value]
                    else:
                         superData[superKey] = [value]
        else:
            continue

for key in list(superData):
    if len(superData[key]) < 2:
        superData.pop(key)

x = []
y = []
for key in superData:
    x += [superData[key][0]]
    y += [superData[key][1]]

plt.scatter(x,y)
plt.show()