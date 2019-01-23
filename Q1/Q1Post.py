import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import math
import matplotlib.pyplot as plt
import re

file = open('part-r-00000.txt')
content = file.read()
file.close()

objPRM = []
valPRM = []
objTER = []
valTER = []

lines = content.split("\n")
for line in lines:
    columns = re.split(" |\t",line)
    if columns[0] == "SE.TER.CMPL.FE.ZS":
        if float(columns[-1]) > 0:
            country = ' '.join(columns[1:-2])
            objTER.append(country)
            valTER.append(float(columns[-1]))
    if columns[0] == "SE.PRM.CMPL.FE.ZS":
        if float(columns[-1]) > 0:
            objPRM.append(columns[1])
            valPRM.append(float(columns[-1]))


p1 = plt.figure(1)
plt.barh(objTER, valTER, align='center', alpha=0.5)
plt.yticks(objTER)
plt.xlabel('Completion (%)')
plt.title('Tertiary')
ax = plt.axes()
ax.xaxis.grid(True)
plt.show()

p2 = plt.figure(2)
plt.barh(objPRM, valPRM, align='center', alpha=0.5)
plt.yticks(objPRM)
plt.xlabel('Completion (%)')
plt.title('Primary')
ax = plt.axes()
ax.xaxis.grid(True)
plt.show()

input()