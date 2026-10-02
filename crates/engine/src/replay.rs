use crate::{Action, EngineHandle};
use anyhow::{ensure, Result};
use sniper_domain::{Clock, MarketEvent, Mode, ReplayClock};
use sniper_store::Store;
use std::{sync::Arc, time::Duration};

/// Speed controls wall-clock pacing, never strategy timestamps. Drain virtual fills before advancing.
pub async fn play(
    store: Arc<Store>,
    engine: EngineHandle,
    clock: Arc<ReplayClock>,
    source: Mode,
    speed: u32,
) -> Result<usize> {
    ensure!(
        [0, 1, 2, 5, 10].contains(&speed),
        "replay speed must be 0 (maximum), 1, 2, 5 or 10"
    );
    ensure!(
        engine.snapshot().mode == Mode::Replay,
        "select REPLAY before playback"
    );
    let mut events =
        tokio::task::spawn_blocking(move || store.load::<MarketEvent>("market_events", source))
            .await??;
    events.sort_by(|a, b| {
        (a.observed_ms, a.slot, a.instruction_index, &a.signature).cmp(&(
            b.observed_ms,
            b.slot,
            b.instruction_index,
            &b.signature,
        ))
    });
    if let Some(first) = events.first() {
        clock.set(first.observed_ms);
    }
    while engine.snapshot().database.is_none() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    engine.action(Action::Start { confirmation: None }).await?;
    let mut previous = clock.now_ms();
    let mut processed = 0;
    for event in events {
        if speed > 0 {
            tokio::time::sleep(Duration::from_millis(
                event.observed_ms.saturating_sub(previous) / speed as u64,
            ))
            .await;
        }
        previous = event.observed_ms;
        clock.set(event.observed_ms);
        engine.replay_event(event).await?;
        processed += 1;
    }
    engine.action(Action::Pause).await?;
    Ok(processed)
}
