import time
import threading
from utils import logger, MA, EMA, SMA, MA, REF, ABS, HHV, LLV
import numpy as np

class Indicator_UTC_No1(threading.Thread):
    def __init__(self, name, ibc):
        threading.Thread.__init__(self)
        self._name = name
        self._ibc = ibc
        self.N = 10
    def calc(self, q):
        HIGH,LOW,CLOSE = q['high'],q['low'],q['close']
        #logger.info("Indicator[{}]: got Q[{}]".format(self._name, q))
        HIGH = np.array([1.5]*100)
        LOW = np.array([1.2]*100)
        CLOSE = np.array([1.24]*100)
        if len(HIGH) > 0:

            VAR6=((((2*CLOSE)+HIGH)+LOW)/4);
            VAR8=LLV(LOW,34);
            VAR11=HHV(HIGH,34)
            VAR12=EMA((((VAR6-VAR8)/(VAR11-VAR8))*100),self.N)
            VAR13=EMA(((0.667*REF(VAR12,1))+(0.333*VAR12)),2)
            MAHS = EMA(VAR13, 5)
            W=ABS(VAR12-VAR13)/REF(ABS(VAR12-VAR13),1);
            X=(VAR12-VAR13)*REF((VAR12-VAR13),1);
            current = (VAR12[-1], VAR13[-1], MAHS[-1], W[-1], X[-1])
            logger.info("Indicator[{}] calculated Ind[{}]".format(self._name, current))
            self._ibc.set_indicator('UTC_No1', current)

    def run(self):
        while True:
            time.sleep(1)
            q = self._ibc.getQ()
            if len(q) == 0:
                continue
            self.calc(q)




