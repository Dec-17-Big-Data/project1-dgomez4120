import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import math
import matplotlib.pyplot as plt
import re

file = open('part-m-00000.txt')
content = file.read()
file.close()

yearNEZS = []
valNEZS = []
yearZS = []
valZS = []

lines = content.split("\n")
for line in lines:
    columns = re.split(" |\t", line)
    if columns[2] == "SL.TLF.CACT.MA.ZS":
        yearZS.append(int(columns[3]))
        valZS.append(float(columns[-1]))
    elif columns[2] == "SL.TLF.CACT.MA.NE.ZS":
        yearNEZS.append(int(columns[3]))
        valNEZS.append(float(columns[-1]))
    else:
        continue

plt.plot(yearZS, valZS, label='ZS')
plt.plot( yearNEZS, valNEZS, label='NEZS')
plt.title("Q3")
plt.xlabel("Year")
plt.ylabel("Percent (%)") 
plt.legend(loc='upper right')
plt.grid()
plt.show()