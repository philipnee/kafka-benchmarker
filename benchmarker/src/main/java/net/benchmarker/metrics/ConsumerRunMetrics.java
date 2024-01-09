package net.benchmarker.metrics;

import java.util.Date;
import java.util.Map;

public class ConsumerRunMetrics implements RunMetrics {
    final String dateString;
    final String commitSHA;
    Map<String, Object> metrics;
    public ConsumerRunMetrics(Date date, String commitSHA) {
        dateString = date.toString();
        this.commitSHA = commitSHA;
    }
}
