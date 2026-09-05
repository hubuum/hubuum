use std::fmt;

/// Descriptive projection of a constraint, without kind-dependent evaluators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationalConstraint {
    Ordered(OrderedConstraint),
    Required(RequiredConstraint),
    Paired(PairedConstraint),
}

impl OperationalConstraint {
    pub fn expression(self) -> String {
        match self {
            Self::Ordered(constraint) => constraint.expression(),
            Self::Required(constraint) => constraint.expression(),
            Self::Paired(constraint) => constraint.expression(),
        }
    }
}

/// Only ordered constraints expose ordered evaluation.
///
/// ```compile_fail
/// use hubuum_domain::RequiredConstraint;
/// RequiredConstraint::new("BACKEND", "URL").ordered_values_satisfy(1, 2);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderedConstraint {
    left: &'static str,
    relation: OrderingRelation,
    right: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderingRelation {
    LessThan,
    LessThanOrEqual,
}

impl OrderedConstraint {
    pub const fn less_than(left: &'static str, right: &'static str) -> Self {
        Self {
            left,
            relation: OrderingRelation::LessThan,
            right,
        }
    }

    pub const fn less_than_or_equal(left: &'static str, right: &'static str) -> Self {
        Self {
            left,
            relation: OrderingRelation::LessThanOrEqual,
            right,
        }
    }

    pub fn ordered_values_satisfy<T: PartialOrd>(self, left: T, right: T) -> bool {
        match self.relation {
            OrderingRelation::LessThan => left < right,
            OrderingRelation::LessThanOrEqual => left <= right,
        }
    }

    pub const fn definition(self) -> OperationalConstraint {
        OperationalConstraint::Ordered(self)
    }

    pub fn expression(self) -> String {
        match self.relation {
            OrderingRelation::LessThan => format!("{} must be less than {}", self.left, self.right),
            OrderingRelation::LessThanOrEqual => {
                format!("{} must not exceed {}", self.left, self.right)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequiredConstraint {
    condition: &'static str,
    requirement: &'static str,
}

impl RequiredConstraint {
    pub const fn new(condition: &'static str, requirement: &'static str) -> Self {
        Self {
            condition,
            requirement,
        }
    }

    pub const fn requirement_is_satisfied(self, condition: bool, requirement: bool) -> bool {
        !condition || requirement
    }

    /// Resolve the required value after the caller selects the conditional branch.
    pub fn require<T>(self, value: Option<T>) -> Result<T, ConstraintViolation> {
        value.ok_or(ConstraintViolation(self.definition()))
    }

    pub const fn definition(self) -> OperationalConstraint {
        OperationalConstraint::Required(self)
    }

    pub fn expression(self) -> String {
        format!("{} requires {}", self.condition, self.requirement)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PairedConstraint {
    left: &'static str,
    right: &'static str,
}

impl PairedConstraint {
    pub const fn new(left: &'static str, right: &'static str) -> Self {
        Self { left, right }
    }

    /// Preserve the correlation: callers receive either both values or neither.
    pub fn resolve<L, R>(
        self,
        left: Option<L>,
        right: Option<R>,
    ) -> Result<Option<(L, R)>, ConstraintViolation> {
        match (left, right) {
            (Some(left), Some(right)) => Ok(Some((left, right))),
            (None, None) => Ok(None),
            _ => Err(ConstraintViolation(self.definition())),
        }
    }

    pub const fn definition(self) -> OperationalConstraint {
        OperationalConstraint::Paired(self)
    }

    pub fn expression(self) -> String {
        format!(
            "{} and {} must be configured together",
            self.left, self.right
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConstraintViolation(OperationalConstraint);

impl fmt::Display for ConstraintViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.expression())
    }
}

impl std::error::Error for ConstraintViolation {}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(
        OrderedConstraint::less_than("LOW", "HIGH"),
        false,
        "LOW must be less than HIGH"
    )]
    #[case(
        OrderedConstraint::less_than_or_equal("LOW", "HIGH"),
        true,
        "LOW must not exceed HIGH"
    )]
    fn ordering_operator_drives_evaluation_and_expression(
        #[case] constraint: OrderedConstraint,
        #[case] accepted: bool,
        #[case] expression: &str,
    ) {
        assert_eq!(constraint.ordered_values_satisfy(10, 10), accepted);
        assert_eq!(constraint.definition().expression(), expression);
    }

    #[rstest]
    #[case(Some("cert"), Some("key"), Ok(Some(("cert", "key"))))]
    #[case(None, None, Ok(None))]
    #[case(Some("cert"), None, Err(()))]
    #[case(None, Some("key"), Err(()))]
    fn paired_values_preserve_their_correlation(
        #[case] left: Option<&str>,
        #[case] right: Option<&str>,
        #[case] expected: Result<Option<(&str, &str)>, ()>,
    ) {
        assert_eq!(
            PairedConstraint::new("CERT", "KEY")
                .resolve(left, right)
                .map_err(|_| ()),
            expected
        );
    }

    #[rstest]
    #[case(Some("root"), Ok("root"))]
    #[case(None, Err(()))]
    fn required_values_are_returned_to_the_consumer(
        #[case] value: Option<&str>,
        #[case] expected: Result<&str, ()>,
    ) {
        assert_eq!(
            RequiredConstraint::new("FILE", "ROOT")
                .require(value)
                .map_err(|_| ()),
            expected
        );
    }
}
