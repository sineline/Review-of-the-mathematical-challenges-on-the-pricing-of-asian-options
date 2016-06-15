

from __future__ import division
import Queue
import threading
import numpy as np
import pandas as pd
import math
from sys import stdout
import uuid
import os
import sys
import datetime
from string import Template



# Definim funcions i variables per al proces:

# In[2]:

class DeltaTemplate(Template):
    delimiter = "%"

def strfdelta(tdelta, fmt):
    d = {"D": tdelta.days}
    d["H"], rem = divmod(tdelta.seconds, 3600)
    d["M"], d["S"] = divmod(rem, 60)
    d["M"] = str(d["M"]).zfill(2)
    d["H"] = str(d["H"]).zfill(2)
    d["S"] = str(d["S"]).zfill(2)
    t = DeltaTemplate(fmt)
    return t.substitute(**d)


# In[3]:

q = Queue.Queue()

# definim variables output
path="output/"
file_name = os.path.join(path,str(uuid.uuid4()))
meanevolution = []
meanevolution_AT = []
# Definim parametres
quantitat_series = int(sys.argv[1]); # Numero de series a generar.

print "quantitat series a simular: %d" % quantitat_series

r = 0.00
strike_price = 95
T = 0.5
sigma = 0.2
S0 = 100
dt = 0.0025
N = int(round(T/dt))
t = np.linspace(0, T, N)


# Definim genereadors
# Definim la funcion que farem servir per generar les series:
def genBrownian():
    # trajectoria d'un moviment Brownia
    W0 = 0
    W = np.random.standard_normal(size = N)
    W = W0+np.cumsum(W)*np.sqrt(dt) ### standard brownian motion ###
    #print W
    return W

# Generem l'array amb les series:
sets = [] # Definim el contenidor de les series
set_means = [] # Definim el contenidor de les mitjanes aritmetiques
start = datetime.datetime.now()

print "session id:" , file_name
print "\nstart time: " , start
# Creation of Dataframes from generated lists
npsets = pd.DataFrame(sets)
npmeans = pd.DataFrame(set_means)

# Calculem pay offs
payoffs = []
payoffsBS = []
payoffsBS_Antithetic = []
blackscholes_serie = []

def blackscholes(x):
    newrow = []
    S_0 = 100
    for index, row in x.iterrows():
        newrow.append(S0*math.exp((r-sigma**2/2)*((T*index)/N)+sigma*row))
    return pd.Series(newrow)

def blackscholes_AT(x):
    newrow = []
    S_0 = 100
    for index, row in x.iterrows():
        newrow.append(S0*math.exp((r-sigma**2/2)*((T*index)/N)+(-sigma)*row))
    return pd.Series(newrow)

def newrow():
    serie = genBrownian()
    sets.append(serie)
    payoffsBS.append(max(np.mean(blackscholes(pd.DataFrame(serie)))-strike_price,0))
    payoffsBS_Antithetic.append(max(np.mean(blackscholes_AT(pd.DataFrame(serie)))-strike_price,0))

# for index, row in npsets.iterrows():
rowindex = 0
timer = datetime.datetime.now()

while rowindex <= quantitat_series:
    t = threading.Thread(target=newrow)
    t.daemon = True
    t.start()
    elapsed = ""
    rowindex+=1
    secondsdiff = datetime.datetime.now()-start

    if int(rowindex % 100) == 0:
        elapsed = datetime.datetime.now() - timer
        timer = datetime.datetime.now()
        timediff = (quantitat_series/rowindex)*secondsdiff.total_seconds()
        timeleft = datetime.timedelta(minutes=(timediff/60)) - (datetime.datetime.now()-start)
        meanevolution.append([rowindex,datetime.datetime.now()-start,np.mean(payoffsBS),np.var(pd.DataFrame(payoffsBS)),np.std(pd.DataFrame(payoffsBS))])
        meanevolution_AT.append([rowindex,datetime.datetime.now()-start,np.mean(payoffsBS_Antithetic),np.var(pd.DataFrame(payoffsBS_Antithetic)),np.std(pd.DataFrame(payoffsBS_Antithetic))])
        final = start + datetime.timedelta(seconds=(timediff))
        os.system('cls')  # for Windows
        os.system('clear')  # for Linux/OS X
        stdout.write("\rTime: %s , Mean: %f , Mean AT: %f , Last N computed: %d. 100 Rows iteration time: %s  \b  "
                     "At this speed, computation will finish in %s minutes, the: %s, at %s" %
                     (datetime.datetime.now()-start, np.mean(payoffsBS), np.mean(payoffsBS_Antithetic),rowindex, elapsed, strfdelta(timeleft,"%D days %H:%M:%S"),final.strftime("%D"), final.strftime("%H:%M")))
        stdout.flush()

# Imprimim resultats i exportem outputs:
print "\nFinished at", datetime.datetime.now()
print "total time elapsed: %d", start - datetime.datetime.now()

# Mitjana Payoffs
print np.mean(pd.DataFrame(payoffsBS))
pd.DataFrame(payoffsBS).to_csv(str(file_name)+"_payoffs.csv", sep='\t', encoding='utf-8')
pd.DataFrame(payoffsBS_Antithetic).to_csv(str(file_name)+"_payoffsAT.csv", sep='\t', encoding='utf-8')
pd.DataFrame(meanevolution).to_csv(str(file_name)+"_meanevolution.csv", sep='\t', encoding='utf-8')
pd.DataFrame(meanevolution_AT).to_csv(str(file_name)+"_meanevolution_AT.csv", sep='\t', encoding='utf-8')
pd.DataFrame(sets).to_csv(str(file_name)+"_browniansets.csv", sep='\t', encoding='utf-8')

print np.var(pd.DataFrame(payoffsBS))
print np.std(pd.DataFrame(payoffsBS))
print np.mean(pd.concat([pd.DataFrame(payoffsBS),pd.DataFrame(payoffsBS_Antithetic)]))



