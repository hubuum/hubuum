/// A cross-field configuration constraint whose executable relation also
/// renders the expression published in the operational contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationalConstraint {
    kind: ConstraintKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConstraintKind {
    Ordered {
        left: &'static str,
        relation: OrderingRelation,
        right: &'static str,
    },
    Requires {
        condition: &'static str,
        requirement: &'static str,
    },
    Paired {
        left: &'static str,
        right: &'static str,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderingRelation {
    LessThan,
    LessThanOrEqual,
}

impl OperationalConstraint {
    pub const fn less_than(left: &'static str, right: &'static str) -> Self {
        Self {
            kind: ConstraintKind::Ordered {
                left,
                relation: OrderingRelation::LessThan,
                right,
            },
        }
    }

    pub const fn less_than_or_equal(left: &'static str, right: &'static str) -> Self {
        Self {
            kind: ConstraintKind::Ordered {
                left,
                relation: OrderingRelation::LessThanOrEqual,
                right,
            },
        }
    }

    pub const fn requires(condition: &'static str, requirement: &'static str) -> Self {
        Self {
            kind: ConstraintKind::Requires {
                condition,
                requirement,
            },
        }
    }

    pub const fn paired(left: &'static str, right: &'static str) -> Self {
        Self {
            kind: ConstraintKind::Paired { left, right },
        }
    }

    pub fn ordered_values_satisfy<T: PartialOrd>(self, left: T, right: T) -> bool {
        match self.kind {
            ConstraintKind::Ordered {
                relation: OrderingRelation::LessThan,
                ..
            } => left < right,
            ConstraintKind::Ordered {
                relation: OrderingRelation::LessThanOrEqual,
                ..
            } => left <= right,
            _ => panic!("ordered_values_satisfy requires an ordered constraint"),
        }
    }

    pub const fn requirement_is_satisfied(self, condition: bool, requirement: bool) -> bool {
        match self.kind {
            ConstraintKind::Requires { .. } => !condition || requirement,
            _ => panic!("requirement_is_satisfied requires an implication constraint"),
        }
    }

    pub const fn paired_values_satisfy(self, left: bool, right: bool) -> bool {
        match self.kind {
            ConstraintKind::Paired { .. } => left == right,
            _ => panic!("paired_values_satisfy requires a paired constraint"),
        }
    }

    pub fn expression(self) -> String {
        match self.kind {
            ConstraintKind::Ordered {
                left,
                relation: OrderingRelation::LessThan,
                right,
            } => format!("{left} must be less than {right}"),
            ConstraintKind::Ordered {
                left,
                relation: OrderingRelation::LessThanOrEqual,
                right,
            } => format!("{left} must not exceed {right}"),
            ConstraintKind::Requires {
                condition,
                requirement,
            } => format!("{condition} requires {requirement}"),
            ConstraintKind::Paired { left, right } => {
                format!("{left} and {right} must be configured together")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_operator_drives_evaluation_and_expression() {
        let strict = OperationalConstraint::less_than("LOW", "HIGH");
        let inclusive = OperationalConstraint::less_than_or_equal("LOW", "HIGH");

        assert!(!strict.ordered_values_satisfy(10, 10));
        assert!(inclusive.ordered_values_satisfy(10, 10));
        assert_eq!(strict.expression(), "LOW must be less than HIGH");
        assert_eq!(inclusive.expression(), "LOW must not exceed HIGH");
    }
}
