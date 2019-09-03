"""
Simple stopwatch class used to track running time for processes.
"""
import time


class Timer:
    """Implements a stop watch.
    Usage:
    t = Timer()
    t.start()
    t.lap()  # Returns a seconds float
    t.stop()  # Returns a seconds float
    t.started  # UNIX time when started or None if not started
    t.stopped  # UNIX time when stopped or None if not stopped
    t.running  # boolean if the timer is running
    """
    def __init__(self):
        self.started = None
        self.stopped = None
        self.lap_count = 0
        self.tracked_times = None

    def start(self, label='Start'):
        """Start the timer."""
        if self.started and not self.stopped:
            self._raise_already_running()
        if self.started and self.stopped:
            self._raise_stopped()
        self.started = time.time()
        self.stopped = None
        self.tracked_times = []
        self.tracked_times.append(self._get_timestamp_dict(
            count=0, start_time=self.started, current_time=self.started, label=label))

    def lap(self, label='', auto_label=True):
        """Return a lap time."""
        if not self.started:
            self._raise_not_started()
        if self.stopped:
            self._raise_stopped()

        # Grab the current time
        current_time = time.time()

        # Auto-set the label
        self.lap_count += 1
        if auto_label and not label:
            label = 'Lap %s' % str(self.lap_count)
        self.tracked_times.append(self._get_timestamp_dict(
            count=self.lap_count, start_time=self.started, current_time=current_time, label=label))

        return current_time - self.started

    def stop(self, label='Stop'):
        """Stop the timer and returns elapsed seconds."""
        elapsed = self.lap(label=label)
        self.stopped = self.started + elapsed
        return elapsed

    def reset(self):
        """Reset the timer."""
        self.started = None
        self.stopped = None
        self.lap_count = 0
        self.tracked_times = None

    def __str__(self):
        output = ''
        if self.tracked_times:
            for current_time in self.tracked_times:
                if output:
                    output += '\n'
                output += '[%s:%s] %s' % (current_time['count'], current_time['label'], current_time['elapsed_time'])
                if 'lap_time' in current_time:
                    output += ' (Lap: %s)' % current_time['lap_time']
        return output

    @property
    def running(self):
        """Return if the timer is running."""
        return (
            self.started is not None and
            self.stopped is None
        )

    def _get_timestamp_dict(self, count=0, start_time=None, current_time=None, label=''):
        timestamp_dict = {
            'count': count,
            'label': label,
            'timestamp': current_time,
            'elapsed_time': current_time - start_time,
        }
        if count and self.tracked_times and self.tracked_times[count - 1]:
            previous_time = self.tracked_times[count - 1]['timestamp']
            timestamp_dict['lap_time'] = current_time - previous_time
        return timestamp_dict

    @staticmethod
    def _raise_not_started():
        raise ValueError('Start the timer first')

    @staticmethod
    def _raise_stopped():
        raise ValueError(
            'Timer was stopped. You need to reset the timer first'
        )

    @staticmethod
    def _raise_already_running():
        raise ValueError('Timer already running')