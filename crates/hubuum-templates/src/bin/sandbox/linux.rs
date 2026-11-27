use std::collections::BTreeMap;
use std::error::Error;

use seccompiler::{
    BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter,
    SeccompRule,
};

use super::Worker;

pub(crate) fn enter() -> Result<Worker, Box<dyn Error>> {
    let mut calls: BTreeMap<_, Vec<SeccompRule>> = [
        libc::SYS_brk,
        libc::SYS_munmap,
        libc::SYS_mremap,
        libc::SYS_madvise,
        libc::SYS_futex,
        libc::SYS_clock_gettime,
        libc::SYS_gettimeofday,
        libc::SYS_getrandom,
        libc::SYS_rt_sigreturn,
        libc::SYS_sigaltstack,
        libc::SYS_close,
        libc::SYS_exit,
        libc::SYS_exit_group,
    ]
    .into_iter()
    .map(|call| (call, Vec::new()))
    .collect();

    fn condition(
        arg: u8,
        op: SeccompCmpOp,
        value: u64,
    ) -> Result<SeccompCondition, seccompiler::BackendError> {
        SeccompCondition::new(arg, SeccompCmpArgLen::Dword, op, value)
    }
    let fd = |value| SeccompRule::new(vec![condition(0, SeccompCmpOp::Eq, value)?]);
    for call in [libc::SYS_read, libc::SYS_readv] {
        calls.insert(call, vec![fd(0)?]);
    }
    for call in [libc::SYS_write, libc::SYS_writev] {
        calls.insert(call, vec![fd(1)?, fd(2)?]);
    }
    let non_executable = condition(2, SeccompCmpOp::MaskedEq(libc::PROT_EXEC as u64), 0)?;
    let anonymous = libc::MAP_ANONYMOUS as u64;
    calls.insert(
        libc::SYS_mmap,
        vec![SeccompRule::new(vec![
            condition(3, SeccompCmpOp::MaskedEq(anonymous), anonymous)?,
            non_executable.clone(),
        ])?],
    );
    calls.insert(
        libc::SYS_mprotect,
        vec![SeccompRule::new(vec![non_executable])?],
    );
    let filter: BpfProgram = SeccompFilter::new(
        calls,
        SeccompAction::Errno(libc::EPERM as u32),
        SeccompAction::Allow,
        std::env::consts::ARCH.try_into()?,
    )?
    .try_into()?;
    seccompiler::apply_filter_all_threads(&filter)?;
    Ok(Worker::Render)
}
