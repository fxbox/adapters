#![feature(custom_derive, plugin)]
#![plugin(clippy)]

extern crate foxbox_taxonomy;
extern crate transformable_channels;

/// The back-end thread, in charge of the heavy lifting of managing adapters.
mod backend;

/// The manager provides an API for (un)registering adapters, services, channels, and
/// uses these to implements the taxonomy API.
pub mod manager;

/// The API for defining Adapters.
pub mod adapter;

/// Utility module for inserting values in maps and keeping the insertion reversible in case of
/// any error.
pub mod transact;

/// Implementation of a fake adapter, controlled entirely programmatically. Designed to be used
/// as a component of tests.
pub mod fake_adapter;