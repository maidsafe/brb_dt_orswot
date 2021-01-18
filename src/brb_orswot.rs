use std::cmp::Ordering;
use std::collections::HashSet;
use std::{fmt::Debug, hash::Hash};

use brb::{Actor, BRBDataType, Sig};

use crdts::{orswot, CmRDT};
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct BRBOrswot<A: Actor<S>, S: Sig, M: Clone + Eq + Hash> {
    actor: A,
    orswot: orswot::Orswot<M, A>,
    sig: core::marker::PhantomData<S>,
}

impl<A: Actor<S>, S: Sig, M: Clone + Eq + Hash> BRBOrswot<A, S, M> {
    pub fn add(&self, member: M) -> orswot::Op<M, A> {
        let add_ctx = self.orswot.read_ctx().derive_add_ctx(self.actor);
        self.orswot.add(member, add_ctx)
    }

    pub fn rm(&self, member: M) -> orswot::Op<M, A> {
        let rm_ctx = self.orswot.read_ctx().derive_rm_ctx();
        self.orswot.rm(member, rm_ctx)
    }

    pub fn contains(&self, member: &M) -> bool {
        self.orswot.contains(member).val
    }

    pub fn actor(&self) -> &A {
        &self.actor
    }

    pub fn orswot(&self) -> &orswot::Orswot<M, A> {
        &self.orswot
    }

    pub fn read(&self) -> HashSet<M> {
        self.orswot.read().val
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ValidationError<A: Hash + Ord + Clone> {
    SourceDoesNotMatchOp { source: A, op_source: A },
    RemoveOnlySupportedForOneElement,
    RemovingDataWeHaventSeenYet,
    Orswot(<orswot::Orswot<(), A> as CmRDT>::Validation),
}

impl<A: Actor<S> + 'static, S: Sig, M: Clone + Eq + Hash + Debug + Serialize> BRBDataType<A>
    for BRBOrswot<A, S, M>
{
    type Op = orswot::Op<M, A>;
    type ValidationError = ValidationError<A>;

    fn new(actor: A) -> Self {
        BRBOrswot {
            actor,
            orswot: Default::default(),
            sig: Default::default(),
        }
    }

    fn validate(&self, source: &A, op: &Self::Op) -> Result<(), Self::ValidationError> {
        self.orswot
            .validate_op(&op)
            .map_err(ValidationError::Orswot)?;

        match op {
            orswot::Op::Add { dot, members: _ } => {
                if &dot.actor != source {
                    Err(ValidationError::SourceDoesNotMatchOp {
                        source: *source,
                        op_source: dot.actor,
                    })
                } else {
                    Ok(())
                }
            }
            orswot::Op::Rm { clock, members } => {
                if members.len() != 1 {
                    Err(ValidationError::RemoveOnlySupportedForOneElement)
                } else if matches!(
                    clock.partial_cmp(&self.orswot.clock()),
                    None | Some(Ordering::Greater)
                ) {
                    // NOTE: this check renders all the "deferred_remove" logic in the ORSWOT obsolete.
                    //       The deferred removes would buffer these out-of-order removes.
                    Err(ValidationError::RemovingDataWeHaventSeenYet)
                } else {
                    Ok(())
                }
            }
        }
    }

    fn apply(&mut self, op: Self::Op) {
        self.orswot.apply(op);
    }
}
