from decimal import Decimal


def _Decimal(v):
    if not isinstance(v, Decimal):
        return Decimal(str(v))
    return v


class BackoffTimer(object):
    """
    This is a timer that is smart about backing off exponentially when there are problems
    """
    def __init__(self, min_interval, max_interval, ratio=.25, short_length=10, long_length=250):
        assert isinstance(min_interval, (int, float, Decimal))
        assert isinstance(max_interval, (int, float, Decimal))
        
        self.min_interval = _Decimal(min_interval)
        self.max_interval = _Decimal(max_interval)
        
        self.max_short_timer = (self.max_interval - self.min_interval) * _Decimal(ratio)
        self.max_long_timer = (self.max_interval - self.min_interval) * (1 - _Decimal(ratio))
        self.short_unit = self.max_short_timer / _Decimal(short_length)
        self.long_unit = self.max_long_timer / _Decimal(long_length)
        
        self.short_interval = Decimal(0)
        self.long_interval = Decimal(0)
    
    def success(self):
        """Update the timer to reflect a successfull call"""
        self.short_interval -= self.short_unit
        self.long_interval -= self.long_unit
        self.short_interval = max(self.short_interval, Decimal(0))
        self.long_interval = max(self.long_interval, Decimal(0))
    
    def failure(self):
        """Update the timer to reflect a failed call"""
        self.short_interval += self.short_unit
        self.long_interval += self.long_unit
        self.short_interval = min(self.short_interval, self.max_short_timer)
        self.long_interval = min(self.long_interval, self.max_long_timer)
    
    def get_interval(self):
        return float(self.min_interval + self.short_interval + self.long_interval)


def test_timer():
    timer = BackoffTimer(.1, 120, long_length=1000)
    assert timer.get_interval() == .1
    timer.success()
    assert timer.get_interval() == .1
    timer.failure()
    interval = '%0.2f' % timer.get_interval()
    assert interval == '3.19'
    assert timer.min_interval == Decimal('.1')
    assert timer.short_interval == Decimal('2.9975')
    assert timer.long_interval == Decimal('0.089925')
    
    timer.failure()
    interval = '%0.2f' % timer.get_interval()
    assert interval == '6.27'
    timer.success()
    interval = '%0.2f' % timer.get_interval()
    assert interval == '3.19'
    for i in range(25):
        timer.failure()
    interval = '%0.2f' % timer.get_interval()
    assert interval == '32.41'
