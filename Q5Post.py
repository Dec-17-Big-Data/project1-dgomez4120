import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import math
import matplotlib.pyplot as plt
import re

file = open('part-r-00000.txt')
content = file.read()
file.close()

print(content)
lines = content.split('\n')
countryCount = {}
for line in lines:
    
    lineSplit = line.split('/')
    countryKey = lineSplit[0]
    
    if countryKey in countryCount:
        countryCount[countryKey] += 1
    else:
        countryCount[countryKey] = 0

print(countryCount)