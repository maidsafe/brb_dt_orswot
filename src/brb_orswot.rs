// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

//! A BRBDataType wrapper for an ORSWOT from rust-crdt.
//!
//! This enables ORSWOT CRDT operations to be transmitted in a BFT manner using
//! Byzantine Reliable Broadcast.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::{fmt::Debug, hash::Hash};

use brb::BRBDataType;
use crdts::{orswot, CmRDT};
use serde::Serialize;
use thiserror::Error;

/// BRB wrapper for an Orswot CRDT
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct BRBOrswot<A: Hash + Ord + Clone, M: Clone + Eq + Hash> {
    actor: A,
    orswot: orswot::Orswot<M, A>,
}

impl<A: Hash + Ord + Clone + Debug, M: Clone + Eq + Hash> BRBOrswot<A, M> {
    /// Generates an Orswot Add operation. (but does not apply it)
    pub fn add(&self, member: M) -> orswot::Op<M, A> {
        let add_ctx = self.orswot.read_ctx().derive_add_ctx(self.actor.clone());
        self.orswot.add(member, add_ctx)
    }

    /// Generates an Orswot Rm operation. (but does not apply it)
    pub fn rm(&self, member: M) -> orswot::Op<M, A> {
        let rm_ctx = self.orswot.read_ctx().derive_rm_ctx();
        self.orswot.rm(member, rm_ctx)
    }

    /// Check if the set contains a member
    pub fn contains(&self, member: &M) -> bool {
        self.orswot.contains(member).val
    }

    /// Retrieves the BRB actor
    pub fn actor(&self) -> &A {
        &self.actor
    }

    /// Retrieves the underlying orswot
    pub fn orswot(&self) -> &orswot::Orswot<M, A> {
        &self.orswot
    }

    /// Read from the underlying orswot
    pub fn read(&self) -> HashSet<M> {
        self.orswot.read().val
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ValidationError<E: std::error::Error + 'static> {
    /// The source actor is not the same as the dot attached to the operation
    #[error("The source actor is not the same as the dot attached to the operation")]
    SourceDoesNotMatchOp,

    /// Attempted to remove more than one member, this is not currently supported
    #[error("Attempted to remove more than one member, this is not currently supported")]
    RemoveOnlySupportedForOneMember,

    /// Attempt to remove a member that we have not seen yet
    #[error("Attempt to remove a member that we have not seen yet")]
    RemovingDataWeHaventSeenYet,

    /// Orswot validation error
    #[error(transparent)]
    Orswot(#[from] E),
}

impl<
        A: Hash + Ord + Clone + Debug + Serialize + 'static,
        M: Clone + Eq + Hash + Debug + Serialize,
    > BRBDataType<A> for BRBOrswot<A, M>
{
    type Op = orswot::Op<M, A>;
    type ValidationError = ValidationError<<orswot::Orswot<M, A> as CmRDT>::Validation>;

    fn new(actor: A) -> Self {
        BRBOrswot {
            actor,
            orswot: Default::default(),
        }
    }

    fn validate(&self, source: &A, op: &Self::Op) -> Result<(), Self::ValidationError> {
        self.orswot
            .validate_op(&op)
            .map_err(ValidationError::Orswot)?;

        match op {
            orswot::Op::Add { dot, members: _ } => {
                if &dot.actor != source {
                    Err(ValidationError::SourceDoesNotMatchOp)
                } else {
                    Ok(())
                }
            }
            orswot::Op::Rm { clock, members } => {
                if members.len() != 1 {
                    Err(ValidationError::RemoveOnlySupportedForOneMember)
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
