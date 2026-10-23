//! Operation-selected byte limits and cumulative accounting, without runtime,
//! configuration, allocation, persistence, or instrumentation dependencies.
//!
//! A limit is an immutable policy snapshot. Each operation starts fresh accounting
//! from that snapshot. Zero permits only empty writes; there is no unlimited
//! sentinel. This measures accepted bytes, not heap capacity or execution time.

use std::error::Error;
use std::fmt;
use std::io::{self, Write};

/// Immutable output policy. Every `usize`, including zero, is a finite limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteLimit(usize);

impl ByteLimit {
    #[must_use]
    #[inline]
    pub const fn new(bytes: usize) -> Self {
        Self(bytes)
    }

    #[must_use]
    #[inline]
    pub const fn bytes(self) -> usize {
        self.0
    }

    #[must_use]
    #[inline]
    pub const fn start(self) -> ByteBudget {
        ByteBudget {
            limit: self,
            remaining: self.bytes(),
            exceeded: false,
        }
    }
}

/// A rejected byte charge. Contains counts only, never resource contents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetExceeded {
    limit: ByteLimit,
    used: usize,
    requested: usize,
}

impl BudgetExceeded {
    #[must_use]
    #[inline]
    pub const fn limit(self) -> ByteLimit {
        self.limit
    }

    #[must_use]
    #[inline]
    pub const fn used(self) -> usize {
        self.used
    }

    #[must_use]
    #[inline]
    pub const fn requested(self) -> usize {
        self.requested
    }
}

impl fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "byte limit exceeded (limit={}, used={}, requested={})",
            self.limit.bytes(),
            self.used,
            self.requested
        )
    }
}

impl Error for BudgetExceeded {}

/// Cumulative byte consumption. A failed charge leaves consumption unchanged.
/// The sticky `exceeded` observation records any rejection; it does not prevent
/// a later smaller charge. All-or-nothing consumers must abort on the first error.
#[derive(Debug)]
pub struct ByteBudget {
    limit: ByteLimit,
    remaining: usize,
    exceeded: bool,
}

impl ByteBudget {
    #[must_use]
    #[inline]
    pub const fn limit(&self) -> ByteLimit {
        self.limit
    }

    #[must_use]
    #[inline]
    pub const fn used(&self) -> usize {
        self.limit.bytes() - self.remaining
    }

    #[must_use]
    #[inline]
    pub const fn remaining(&self) -> usize {
        self.remaining
    }

    #[must_use]
    #[inline]
    pub const fn exceeded(&self) -> bool {
        self.exceeded
    }

    #[inline]
    fn check(&mut self, bytes: usize) -> Result<(), BudgetExceeded> {
        // Compare remaining capacity: used + bytes may overflow at usize::MAX.
        if bytes > self.remaining() {
            self.exceeded = true;
            return Err(BudgetExceeded {
                limit: self.limit,
                used: self.used(),
                requested: bytes,
            });
        }
        Ok(())
    }

    #[inline]
    pub fn consume(&mut self, bytes: usize) -> Result<(), BudgetExceeded> {
        self.check(bytes)?;
        self.remaining -= bytes;
        Ok(())
    }

    /// Start a sequential child whose accepted writes also consume this parent.
    /// The exclusive borrow pins the available parent capacity for the child's
    /// lifetime. Drop releases the destination, but never refunds consumed work.
    ///
    /// ```compile_fail
    /// use hubuum_budget::ByteLimit;
    /// use std::io::{self, Write};
    /// let mut parent = ByteLimit::new(10).start();
    /// let mut first = parent.writer(io::sink(), ByteLimit::new(8));
    /// let second = parent.writer(io::sink(), ByteLimit::new(8));
    /// first.write_all(b"still borrowed").unwrap();
    /// ```
    pub fn writer<W>(&mut self, inner: W, limit: ByteLimit) -> ChildWriter<'_, W> {
        let scope = if self.remaining() < limit.bytes() {
            LimitingScope::Parent
        } else {
            LimitingScope::Child
        };
        let effective = ByteLimit::new(limit.bytes().min(self.remaining()));
        ChildWriter {
            writer: LimitedWriter::new(inner, effective),
            parent: self,
            scope,
        }
    }
}

#[derive(Debug)]
enum LimitingScope {
    Parent,
    Child,
}

/// A writer charged against both a child limit and its exclusively borrowed
/// parent. Siblings run sequentially; no locks or concurrent reservations exist.
#[derive(Debug)]
pub struct ChildWriter<'a, W> {
    writer: LimitedWriter<W>,
    parent: &'a mut ByteBudget,
    scope: LimitingScope,
}

impl<W> ChildWriter<'_, W> {
    /// Accounting under the effective (minimum child/remaining-parent) limit.
    #[must_use]
    #[inline]
    pub const fn budget(&self) -> &ByteBudget {
        self.writer.budget()
    }

    /// Whether this child rejected a write because the parent was stricter.
    /// Equal limits are attributed to the child.
    #[must_use]
    #[inline]
    pub const fn parent_exceeded(&self) -> bool {
        self.budget().exceeded() && matches!(self.scope, LimitingScope::Parent)
    }

    #[must_use]
    #[inline]
    pub fn into_inner(self) -> W {
        self.writer.into_inner()
    }
}

impl<W: Write> Write for ChildWriter<'_, W> {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self.writer.write(bytes) {
            Ok(count) => {
                // Construction capped the child by remaining parent capacity,
                // and the exclusive borrow prevents any competing consumption.
                self.parent.remaining -= count;
                Ok(count)
            }
            Err(error) => {
                if self.parent_exceeded() {
                    self.parent.exceeded = true;
                }
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Rejects an oversized write before invoking its destination. Successful partial
/// writes charge only the bytes accepted by the destination; I/O errors retain
/// their original kind and are not misclassified as budget exhaustion.
///
/// The wrapper allocates no buffer. The destination owns capacity, flushing, and
/// durability. Dropping this writer drops its destination and does not roll back
/// already accepted bytes. No reference permitting unaccounted writes is exposed.
#[derive(Debug)]
pub struct LimitedWriter<W> {
    inner: W,
    budget: ByteBudget,
}

impl<W> LimitedWriter<W> {
    #[must_use]
    #[inline]
    pub const fn new(inner: W, limit: ByteLimit) -> Self {
        Self {
            inner,
            budget: limit.start(),
        }
    }

    #[must_use]
    #[inline]
    pub const fn budget(&self) -> &ByteBudget {
        &self.budget
    }

    #[must_use]
    #[inline]
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for LimitedWriter<W> {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.budget.check(bytes.len()).map_err(io::Error::other)?;
        let count = self.inner.write(bytes)?;
        // Write's contract guarantees count <= bytes.len(), already admitted.
        debug_assert!(count <= bytes.len());
        self.budget.remaining -= count;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::cell::Cell;

    #[rstest]
    #[case::empty(0, 0, true)]
    #[case::zero(0, 1, false)]
    #[case::exact(10, 10, true)]
    #[case::excess(10, 11, false)]
    #[case::maximum(usize::MAX, usize::MAX, true)]
    fn charges_are_atomic(#[case] limit: usize, #[case] bytes: usize, #[case] fits: bool) {
        let mut budget = ByteLimit::new(limit).start();
        assert_eq!(budget.consume(bytes).is_ok(), fits);
        assert_eq!(budget.used(), if fits { bytes } else { 0 });
        assert_eq!(budget.exceeded(), !fits);
    }

    #[test]
    fn cumulative_charge_cannot_overflow() {
        let mut budget = ByteLimit::new(usize::MAX).start();
        budget.consume(1).unwrap();
        let error = budget.consume(usize::MAX).unwrap_err();
        assert_eq!(error.used(), 1);
        assert_eq!(budget.remaining(), usize::MAX - 1);
    }

    #[test]
    fn policy_snapshots_start_independent_operations() {
        let limit = ByteLimit::new(4);
        let mut first = limit.start();
        first.consume(4).unwrap();
        let second = limit.start();
        assert_eq!(second.remaining(), 4);
    }

    #[test]
    fn oversized_write_never_reaches_destination() {
        let mut writer = LimitedWriter::new(Vec::new(), ByteLimit::new(2));
        writer.write_all(b"ab").unwrap();
        let error = writer.write_all(b"secret").unwrap_err();
        assert!(error.get_ref().unwrap().is::<BudgetExceeded>());
        assert_eq!(writer.into_inner(), b"ab");
    }

    #[test]
    fn streaming_stops_at_first_excess_fragment() {
        let mut writer = LimitedWriter::new(io::sink(), ByteLimit::new(8));
        let mut produced = 0;
        let result = (0..1_000_000).try_for_each(|_| {
            produced += 1;
            writer.write_all(b"1234")
        });
        assert!(result.is_err());
        assert_eq!(produced, 3);
    }

    #[test]
    fn partial_destination_writes_charge_only_accepted_bytes() {
        let mut buffer = [0; 2];
        let mut writer = LimitedWriter::new(buffer.as_mut_slice(), ByteLimit::new(4));
        assert_eq!(writer.write(b"abcd").unwrap(), 2);
        assert_eq!(writer.budget().used(), 2);
        assert_eq!(
            writer.write_all(b"x").unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        assert!(!writer.budget().exceeded());
    }

    struct Destination<'a>(&'a Cell<bool>);

    impl Drop for Destination<'_> {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    impl Write for Destination<'_> {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::Interrupted.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }

    #[test]
    fn destination_failures_are_preserved() {
        let dropped = Cell::new(false);
        let mut writer = LimitedWriter::new(Destination(&dropped), ByteLimit::new(8));
        assert_eq!(
            writer.write(b"x").unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert_eq!(writer.budget().used(), 0);
        assert!(!writer.budget().exceeded());
    }

    #[test]
    fn flush_delegates_to_destination() {
        let dropped = Cell::new(false);
        let mut writer = LimitedWriter::new(Destination(&dropped), ByteLimit::new(8));
        assert_eq!(
            writer.flush().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn dropping_operation_releases_destination() {
        let dropped = Cell::new(false);
        drop(LimitedWriter::new(Destination(&dropped), ByteLimit::new(8)));
        assert!(dropped.get());
    }

    #[rstest]
    #[case::parent(4, 10, true)]
    #[case::child(10, 4, false)]
    #[case::equal(4, 4, false)]
    fn child_stops_at_the_stricter_limit(
        #[case] parent_bytes: usize,
        #[case] child_bytes: usize,
        #[case] parent_exceeded: bool,
    ) {
        let mut parent = ByteLimit::new(parent_bytes).start();
        let mut child = parent.writer(Vec::new(), ByteLimit::new(child_bytes));
        child.write_all(b"1234").unwrap();
        assert!(child.write_all(b"5").is_err());
        assert_eq!(child.parent_exceeded(), parent_exceeded);
        assert_eq!(child.into_inner(), b"1234");
        assert_eq!(parent.used(), 4);
        assert_eq!(parent.exceeded(), parent_exceeded);
    }

    #[test]
    fn dropping_child_does_not_refund_accepted_work() {
        let mut parent = ByteLimit::new(5).start();
        {
            let mut child = parent.writer(Vec::new(), ByteLimit::new(4));
            child.write_all(b"123").unwrap();
        }
        let mut sibling = parent.writer(Vec::new(), ByteLimit::new(4));
        sibling.write_all(b"45").unwrap();
        assert!(sibling.write_all(b"6").is_err());
        assert_eq!(sibling.into_inner(), b"45");
        assert_eq!(parent.used(), 5);
    }

    #[test]
    fn partial_writes_charge_parent_and_child_equally() {
        let mut parent = ByteLimit::new(4).start();
        let mut destination = [0; 2];
        let mut child = parent.writer(destination.as_mut_slice(), ByteLimit::new(4));
        assert_eq!(child.write(b"1234").unwrap(), 2);
        assert_eq!(child.budget().used(), 2);
        assert_eq!(
            child.write_all(b"x").unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        let _ = child.into_inner();
        assert_eq!(parent.used(), 2);
        assert!(!parent.exceeded());
    }
}
