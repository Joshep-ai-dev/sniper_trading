use anyhow::Result;
use std::time::Instant;
pub struct Metrics {
    pub registry: prometheus::Registry,
    pub stages: prometheus::HistogramVec,
    pub submissions: prometheus::IntCounterVec,
    pub saturation: prometheus::IntCounter,
    pub failures: prometheus::IntCounter,
    pub events: prometheus::IntCounter,
    pub dropped: prometheus::IntCounter,
}
impl Metrics {
    pub fn new() -> Result<Self> {
        let registry = prometheus::Registry::new();
        let stages = prometheus::HistogramVec::new(
            prometheus::HistogramOpts::new(
                "sniper_latency_seconds",
                "Monotonic local stage latency",
            )
            .buckets(vec![
                0.00001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.3, 1.0, 5.0,
            ]),
            &["stage"],
        )?;
        let submissions = prometheus::IntCounterVec::new(
            prometheus::Opts::new("sniper_submissions_total", "Submission attempts"),
            &["mode", "side"],
        )?;
        let saturation = prometheus::IntCounter::new(
            "sniper_queue_saturation_total",
            "Entry circuit breaker activations",
        )?;
        let failures = prometheus::IntCounter::new(
            "sniper_persistence_failures_total",
            "Durable write failures",
        )?;
        let events =
            prometheus::IntCounter::new("sniper_events_total", "Accepted market observations")?;
        let dropped = prometheus::IntCounter::new(
            "sniper_best_effort_dropped_total",
            "Dropped noncritical samples",
        )?;
        registry.register(Box::new(stages.clone()))?;
        registry.register(Box::new(submissions.clone()))?;
        for counter in [&saturation, &failures, &events, &dropped] {
            registry.register(Box::new(counter.clone()))?;
        }
        Ok(Self {
            registry,
            stages,
            submissions,
            saturation,
            failures,
            events,
            dropped,
        })
    }
    pub fn observe(&self, name: &str, start: Instant) {
        self.stages
            .with_label_values(&[name])
            .observe(start.elapsed().as_secs_f64());
    }
    pub fn render(&self) -> Result<String> {
        use prometheus::Encoder;
        let mut bytes = Vec::new();
        prometheus::TextEncoder::new().encode(&self.registry.gather(), &mut bytes)?;
        Ok(String::from_utf8(bytes)?)
    }
}
