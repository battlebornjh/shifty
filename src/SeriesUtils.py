
def shiftSeries(shiftAmt, size, series):
    retSeries = series[shiftAmt:shiftAmt + size]
    return retSeries

def isArrayConstant(series):
    return len(set(series)) == 1

