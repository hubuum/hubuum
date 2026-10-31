//! A stable local file ties protected runtime registrations to actual processes.
//! Its identity is never replaced or unlinked during ordinary operation.
use super::*;
use hubuum_storage_core::RuntimeIsolationDomain;

pub(crate) struct RuntimeIsolationGuard {
    domain: RuntimeIsolationDomain,
    _file: File,
}

impl RuntimeIsolationGuard {
    pub(crate) fn domain(&self) -> RuntimeIsolationDomain {
        self.domain.clone()
    }
}

impl CheckpointRepository {
    #[cfg(unix)]
    pub(in crate::services::deletion_recovery) fn runtime_isolation(
        &self,
        exclusive: bool,
    ) -> Result<RuntimeIsolationGuard, ApiError> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::MetadataExt;

        // Serialize first creation with checkpoint writers. Initialization is
        // durable before registration can store the domain in PostgreSQL.
        let _publication = self.lock()?;
        self.latest_locked()?;
        let mut file = checked_file(&self.root.join("RUNTIME_LOCK"), true)?;
        let operation = if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        };
        // SAFETY: the guard owns this live descriptor until runtime shutdown or
        // recovery completes. Kernel process exit also releases the lock.
        if unsafe { libc::flock(file.as_raw_fd(), operation | libc::LOCK_NB) } != 0 {
            return Err(unavailable(
                "Runtime isolation is busy; stop every server before offline recovery",
            ));
        }
        let metadata = file
            .metadata()
            .map_err(|_| unavailable("Cannot inspect runtime isolation"))?;
        let nonce = if metadata.len() == 0 {
            let nonce = Uuid::new_v4();
            file.write_all(nonce.as_bytes())
                .and_then(|()| file.sync_all())
                .map_err(|_| unavailable("Cannot durably initialize runtime isolation"))?;
            self.directory
                .sync_all()
                .map_err(|_| unavailable("Cannot sync runtime isolation directory"))?;
            nonce
        } else {
            if metadata.len() != 16 {
                return Err(unavailable(
                    "Invalid runtime isolation file; keep the deployment quarantined",
                ));
            }
            let mut bytes = [0; 16];
            file.read_exact(&mut bytes)
                .map_err(|_| unavailable("Cannot read runtime isolation"))?;
            Uuid::from_bytes(bytes)
        };
        let domain = RuntimeIsolationDomain::new(nonce, metadata.dev(), metadata.ino())
            .map_err(|_| unavailable("Invalid runtime isolation identity"))?;
        Ok(RuntimeIsolationGuard {
            domain,
            _file: file,
        })
    }

    #[cfg(not(unix))]
    pub(in crate::services::deletion_recovery) fn runtime_isolation(
        &self,
        _exclusive: bool,
    ) -> Result<RuntimeIsolationGuard, ApiError> {
        Err(unavailable(
            "Runtime recovery currently requires Unix process isolation",
        ))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use crate::services::deletion_recovery::repository::tests::Fixture;

    #[test]
    fn shared_runtimes_exclude_recovery_until_every_owner_exits() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let first = fixture.repository.runtime_isolation(false).unwrap();
        let second = fixture.repository.runtime_isolation(false).unwrap();
        assert_eq!(first.domain(), second.domain());
        assert!(fixture.repository.runtime_isolation(true).is_err());
        drop(first);
        assert!(fixture.repository.runtime_isolation(true).is_err());
        drop(second);
        let recovery = fixture.repository.runtime_isolation(true).unwrap();
        assert!(fixture.repository.runtime_isolation(false).is_err());
        drop(recovery);
        assert!(fixture.repository.runtime_isolation(false).is_ok());
    }
}
