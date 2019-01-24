import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import math
import matplotlib.pyplot as plt
import re

file = open('part-m-00000.txt')
content = file.read()
file.close()

yearPRM = []
valPRM = []
yearSEC = []
valSEC = []
yearTER = []
valTER = []

lines = content.split("\n")
for line in lines:
    columns = re.split(" |\t", line)
    if columns[2] == "SE.PRM.ENRR.FE":
        yearPRM.append(int(columns[3]))
        valPRM.append(float(columns[-1]))
    elif columns[2] == "SE.SEC.ENRR.FE":
        yearSEC.append(int(columns[3]))
        valSEC.append(float(columns[-1]))
    elif columns[2] == "SE.TER.ENRR.FE":
        yearTER.append(int(columns[3]))
        valTER.append(float(columns[-1]))
    else:
        continue

plt.plot(yearPRM, valPRM, label='Primary')
plt.plot( yearSEC, valSEC, label='Secondary')
plt.plot(yearTER, valTER, label='Tertiary')
plt.title("Increase in Female Education (USA) - Q2 - Gomez")
plt.xlabel("Year")
plt.ylabel("Percent Enrollement (%)") 
plt.legend(loc='lower right')
plt.grid()
plt.show()