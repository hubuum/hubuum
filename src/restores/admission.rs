//! Bound request spools and parsing work across the HTTP workers in one process.
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::errors::ApiError;

const MAX_ACTIVE: usize = 2;
const MAX_STARTS: usize = 16;
const MAX_PRINCIPAL_STARTS: usize = 4;
const WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, Default)]
pub(super) struct StageAdmission {
    active: HashSet<i32>,
    starts: VecDeque<(Instant, i32)>,
}

#[derive(Debug)]
pub(crate) struct StagePermit {
    principal: i32,
    state: Arc<Mutex<StageAdmission>>,
}

impl StageAdmission {
    pub(super) fn acquire(
        state: &Arc<Mutex<Self>>,
        principal: i32,
    ) -> Result<StagePermit, ApiError> {
        let mut gate = state.lock().unwrap_or_else(|error| error.into_inner());
        let now = Instant::now();
        while gate
            .starts
            .front()
            .is_some_and(|(time, _)| now.duration_since(*time) >= WINDOW)
        {
            gate.starts.pop_front();
        }
        if gate.active.len() >= MAX_ACTIVE
            || gate.active.contains(&principal)
            || gate.starts.len() >= MAX_STARTS
            || gate
                .starts
                .iter()
                .filter(|(_, owner)| *owner == principal)
                .count()
                >= MAX_PRINCIPAL_STARTS
        {
            return Err(ApiError::TooManyRequests(
                "Restore staging capacity or rate limit reached".into(),
            ));
        }
        gate.active.insert(principal);
        gate.starts.push_back((now, principal));
        Ok(StagePermit {
            principal,
            state: Arc::clone(state),
        })
    }
}

impl Drop for StagePermit {
    fn drop(&mut self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active
            .remove(&self.principal);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_and_rate_budgets_cover_distinct_phases() {
        let gate = Arc::default();
        let first = StageAdmission::acquire(&gate, 1).unwrap();
        assert!(StageAdmission::acquire(&gate, 1).is_err());
        let second = StageAdmission::acquire(&gate, 2).unwrap();
        assert!(StageAdmission::acquire(&gate, 3).is_err());
        drop((first, second));
        for _ in 1..MAX_PRINCIPAL_STARTS {
            drop(StageAdmission::acquire(&gate, 1).unwrap());
        }
        assert!(StageAdmission::acquire(&gate, 1).is_err());
        assert!(StageAdmission::acquire(&gate, 2).is_ok());
    }
}
