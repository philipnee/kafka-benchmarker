package net.benchmarker.recorder;

import net.benchmarker.metrics.RunMetrics;

public interface Recorder {
    void record(RunMetrics metrics);
}
