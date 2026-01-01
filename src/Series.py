import pandas as pd
from scipy.stats import pearsonr
import SeriesUtils as util
import SeriesObjects as obj
import DbWriter as db
import sys, logging

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def search_series(guid, symbols, dates, dataSet, corNumber, minShift, size):
    
    #validate dataSet
    if len(dataSet) <= 1:
        raise Exception("Size of dataSet is too small.")

    elementSize = len(dataSet[0])
    for element in dataSet:
        if elementSize != len(element):
            raise Exception("Inconsistant lengths found in dataSet elements.")

    # Define your data series
    posCors = []
    negCors = []

    posCorNumber = corNumber
    negCorNumber = 0 - corNumber

    comparisonsSkipped = 0
    comparisonsSkippedMinShift = 0

    comparisonsDone = [] #keep track of comparisons to avoid checking the reverse
    for shift1 in range(0, elementSize - size + 1):
        for shift2 in range(0, elementSize - size + 1):
            for t1 in range(len(dataSet)):
                series = []
                shiftedSeries = []
                for i in range(len(dataSet)):
                    newSeries = None
                    if i == t1:
                        newSeries = util.shiftSeries(shift1,size,dataSet[i])
                    else:
                        newSeries = util.shiftSeries(shift2,size,dataSet[i])
                    series.append(pd.Series(newSeries))
                    shiftedSeries.append(newSeries)
                for i in range(len(series)):
                    target1 = t1
                    target2 = i
                    if target1 != target2:
                        if len(series[target1]) == len(series[target2]):
                            if util.isArrayConstant(series[target1]):
                                logger.info(f"Constant Array dected for target 1")
                            elif util.isArrayConstant(series[target2]):
                                logger.info(f"Constant Array dected for target 2")
                            elif (target2, shift2, target1, shift1) in comparisonsDone:
                                comparisonsSkipped +=1
                            elif abs(shift1 - shift2) < minShift:
                                comparisonsSkippedMinShift +=1
                            else:
                                comparisonsDone.append((target1, shift1, target2, shift2))
                                correlation_coefficient, p_value = pearsonr(series[target1],series[target2])
                                if correlation_coefficient > posCorNumber:
                                    highCC = obj.HighCorrelation(target1, target2, shift1, shift2, size, correlation_coefficient, p_value, shiftedSeries[target1], shiftedSeries[target2])
                                    logger.info(f"High cc detected. {symbols[target1]} / {symbols[target2]}")
                                    logger.info(dates[shift1:shift1 + size])
                                    logger.info(dates[shift2:shift2 + size])
                                    logger.info(highCC)
                                    posCors.append(highCC)
                                elif correlation_coefficient < negCorNumber:
                                    negCors.append(obj.HighCorrelation(target1, target2, shift1, shift2, size, correlation_coefficient, p_value, shiftedSeries[target1], shiftedSeries[target2]))
                        else:
                            logger.info(f"Not enough data to evaluate target {target1} and {target2}.")

    for cor in (posCors + negCors):
        db.insertShiftFound(guid, cor)
