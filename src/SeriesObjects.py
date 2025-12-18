class HighCorrelation:
    def __init__(self, target1, target2, shift1, shift2, size, cc, p, series1, series2):
        self.target1 = target1
        self.target2 = target2
        self.shift1 = shift1
        self.shift2 = shift2
        self.size = size
        self.cc = cc
        self.p = p
        self.series1 = series1
        self.series2 = series2

    def __str__(self):
        return f"Size {self.size}:> Series {self.target1}:shift({self.shift1}) / {self.target2}:shift({self.shift2}) has a high cc of {self.cc:.4f} with a p_value of {self.p:.10f} \n {self.series1} \n {self.series2}"
