use std::error::Error;
use std::mem::size_of;

use windows::Win32::Foundation::{HANDLE, HLOCAL};
use windows::Win32::Security::Authorization::{
    ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows::Win32::Security::{
    AccessCheck, DuplicateToken, GENERIC_MAPPING, PRIVILEGE_SET, PSECURITY_DESCRIPTOR,
    SecurityIdentification, TOKEN_DUPLICATE, TOKEN_QUERY,
};
use windows::Win32::System::SystemServices::MAXIMUM_ALLOWED;
use windows::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
use windows::core::{BOOL, Owned, w};

/// Check the effective package permissions against an in-memory descriptor.
/// Everyone gets both bits in the user check. In the package check, all
/// application packages get bit 1 and restricted packages get bit 2. A
/// less-privileged AppContainer gets exactly bit 2. This also verifies the
/// positive grant. A failed API call aborts the worker.
pub(super) fn verify_package_opt_out() -> Result<(), Box<dyn Error>> {
    // SAFETY: each successful call initializes the output before use. Owned
    // closes both token handles and frees the descriptor with LocalFree.
    unsafe {
        let mut primary = HANDLE::default();
        OpenProcessToken(
            GetCurrentProcess(),
            TOKEN_QUERY | TOKEN_DUPLICATE,
            &mut primary,
        )
        .map_err(|error| format!("open worker token: {error}"))?;
        let primary = Owned::new(primary);
        let mut impersonation = HANDLE::default();
        DuplicateToken(*primary, SecurityIdentification, &mut impersonation)
            .map_err(|error| format!("duplicate worker token for access check: {error}"))?;
        let impersonation = Owned::new(impersonation);

        let mut descriptor = PSECURITY_DESCRIPTOR::default();
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            w!("O:SYG:SYD:(A;;0x3;;;WD)(A;;0x1;;;AC)(A;;0x2;;;S-1-15-2-2)"),
            SDDL_REVISION_1,
            &mut descriptor,
            None,
        )
        .map_err(|error| format!("build package access descriptor: {error}"))?;
        let _descriptor_allocation = Owned::new(HLOCAL(descriptor.0));

        let mut privileges = PRIVILEGE_SET::default();
        let mut length = size_of::<PRIVILEGE_SET>() as u32;
        let mut granted = 0;
        let mut allowed = BOOL::default();
        AccessCheck(
            descriptor,
            *impersonation,
            MAXIMUM_ALLOWED,
            &GENERIC_MAPPING::default(),
            Some(&mut privileges),
            &mut length,
            &mut granted,
            &mut allowed,
        )
        .map_err(|error| format!("evaluate package access: {error}"))?;
        if !allowed.as_bool() || granted & 0x3 != 0x2 {
            return Err(format!(
                "restricted-package access check failed (allowed={}, rights={:#x})",
                allowed.as_bool(),
                granted & 0x3
            )
            .into());
        }
        Ok(())
    }
}
