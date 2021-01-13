use std::cmp::Ordering;
use std::collections::HashSet;
use std::{fmt::Debug, hash::Hash};

use brb::{Actor, BRBDataType};

use crdts::{orswot, CmRDT};
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct BRBOrswot<M: Clone + Eq + Debug + Hash + Serialize> {
    actor: Actor,
    orswot: orswot::Orswot<M, Actor>,
}

impl<M: Clone + Eq + Debug + Hash + Serialize> BRBOrswot<M> {
    pub fn add(&self, member: M) -> orswot::Op<M, Actor> {
        let add_ctx = self.orswot.read_ctx().derive_add_ctx(self.actor);
        self.orswot.add(member, add_ctx)
    }

    pub fn rm(&self, member: M) -> orswot::Op<M, Actor> {
        let rm_ctx = self.orswot.read_ctx().derive_rm_ctx();
        self.orswot.rm(member, rm_ctx)
    }

    pub fn contains(&self, member: &M) -> bool {
        self.orswot.contains(member).val
    }

    pub fn actor(&self) -> &Actor {
        &self.actor
    }

    pub fn orswot(&self) -> &orswot::Orswot<M, Actor> {
        &self.orswot
    }

    pub fn read(&self) -> HashSet<M> {
        self.orswot.read().val
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Validation {
    SourceDoesNotMatchOp { source: Actor, op_source: Actor },
    RemoveOnlySupportedForOneElement,
    RemovingDataWeHaventSeenYet,
    Orswot(<orswot::Orswot<(), Actor> as CmRDT>::Validation),
}

impl<M: Clone + Eq + Debug + Hash + Serialize> BRBDataType for BRBOrswot<M> {
    type Op = orswot::Op<M, Actor>;
    type Validation = Validation;

    fn new(actor: Actor) -> Self {
        BRBOrswot {
            actor,
            orswot: orswot::Orswot::new(),
        }
    }

    fn validate(&self, source: &Actor, op: &Self::Op) -> Result<(), Self::Validation> {
        self.orswot.validate_op(&op).map_err(Validation::Orswot)?;

        match op {
            orswot::Op::Add { dot, members: _ } => {
                if &dot.actor != source {
                    Err(Validation::SourceDoesNotMatchOp {
                        source: *source,
                        op_source: dot.actor,
                    })
                } else {
                    Ok(())
                }
            }
            orswot::Op::Rm { clock, members } => {
                if members.len() != 1 {
                    Err(Validation::RemoveOnlySupportedForOneElement)
                } else if matches!(
                    clock.partial_cmp(&self.orswot.clock()),
                    None | Some(Ordering::Greater)
                ) {
                    // NOTE: this check renders all the "deferred_remove" logic in the ORSWOT obsolete.
                    //       The deferred removes would buffer these out-of-order removes.
                    Err(Validation::RemovingDataWeHaventSeenYet)
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
