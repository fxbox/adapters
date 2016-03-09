//! The Adapter manager
//!
//! This structure serves two roles:
//! - adapters use it to (un)register themselves, as well as services and channels;
//! - it exposes an implementation of the taxonomy API.

use backend::*;
use adapter::*;

use foxbox_taxonomy::api::{ AdapterError, APIHandle, Error as APIError, WatchEvent, FnResultMap };
use foxbox_taxonomy::selector::*;
use foxbox_taxonomy::services::*;
use foxbox_taxonomy::util::*;
use foxbox_taxonomy::values::{ Range, Value };

use std::sync::mpsc::{ channel, Sender };
use std::thread;

/// An implementation of the AdapterManager.
pub struct AdapterManager {
    tx: Sender<Op>,
}

impl AdapterManager {
    /// Create an empty AdapterManager.
    /// This function does not attempt to load any state from the disk.
    pub fn new() -> Self {
        let (tx, rx) = channel();
        let tx2 = tx.clone();
        thread::spawn(move || {
            let mut state = AdapterManagerState::new(tx2);
            for msg in rx.iter() {
                match msg {
                    Op::Stop(tx) => {
                        let _ignored = tx.send(());
                        return;
                    },
                    Op::Execute(e) => state.execute(e)
                }
            }
        });
        AdapterManager {
            tx: tx
        }
    }

    fn dispatch(&self, execute: Execute) {
        let _ignore = self.tx.send(Op::Execute(execute));
    }
}

impl Default for AdapterManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for AdapterManager {
    fn drop(&mut self) {
        let (tx, rx) = channel();
        {
            let _ignored = self.tx.send(Op::Stop(tx));
        }
        // At this stage, if `self.tx` is dead, so is `tx`.
        let _ignored = rx.recv();
        // At this stage, we are sure that the dispatch thread is not executing anymore.
        // We don't know how it stopped its execution, but we don't really care either.
    }
}

impl AdapterManager {
    /// Add an adapter to the system.
    ///
    /// # Errors
    ///
    /// Returns an error if an adapter with the same id is already present.
    pub fn add_adapter(&self, adapter: Box<Adapter>, services: Vec<Service>, cb: Callback<(), AdapterError>) {
       self.dispatch(Execute::AddAdapter {
           adapter: adapter,
           services: services,
           cb: cb
       });
   }

    /// Remove an adapter from the system, including all its services and channels.
    ///
    /// # Errors
    ///
    /// Returns an error if no adapter with this identifier exists. Otherwise, attempts
    /// to cleanup as much as possible, even if for some reason the system is in an
    /// inconsistent state.
    pub fn remove_adapter(&self, id: &Id<AdapterId>, cb: Callback<(), AdapterError>) {
        self.dispatch(Execute::RemoveAdapter {
            id: id.clone(),
            cb: cb
        });
    }

    /// Add a service to the system. Called by the adapter when a new
    /// service (typically a new device) has been detected/configured.
    ///
    /// # Requirements
    ///
    /// The adapter is in charge of making sure that identifiers persist across reboots.
    ///
    /// # Errors
    ///
    /// Returns an error if the adapter does not exist or a service with the same identifier
    /// already exists, or if the identifier introduces a channel that would overwrite another
    /// channel with the same identifier. In either cases, this method reverts all its changes.
    pub fn add_service(&self, adapter: &Id<AdapterId>, service: Service, cb: Callback<(), AdapterError>) {
        self.dispatch(Execute::AddService {
            adapter: adapter.clone(),
            service: service,
            cb: cb
        });
    }

    /// Remove a service previously registered on the system. Typically, called by
    /// an adapter when a service (e.g. a device) is disconnected.
    ///
    /// # AdapterError
    ///
    /// This method returns an error if the adapter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    pub fn remove_service(&self, adapter: &Id<AdapterId>, service_id: &Id<ServiceId>, cb: Callback<(), AdapterError>) {
        self.dispatch(Execute::RemoveService {
            adapter: adapter.clone(),
            id: service_id.clone(),
            cb: cb
        });
    }

    /// Add a setter to the system. Typically, this is called by the adapter when a new
    /// service has been detected/configured. Some services may gain/lose getters at
    /// runtime depending on their configuration.
    ///
    /// # Requirements
    ///
    /// The adapter is in charge of making sure that identifiers persist across reboots.
    ///
    /// # Errors
    ///
    /// Returns an error if the adapter is not registered, the parent service is not
    /// registered, or a channel with the same identifier is already registered.
    /// In either cases, this method reverts all its changes.
    pub fn add_getter(&self, setter: Channel<Getter>, cb: Callback<(), AdapterError>) {
        self.dispatch(Execute::AddGetter {
            setter: setter,
            cb: cb
        });
    }

    /// Remove a setter previously registered on the system. Typically, called by
    /// an adapter when a service is reconfigured to remove one of its getters.
    ///
    /// # AdapterError
    ///
    /// This method returns an error if the setter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    pub fn remove_getter(&self, id: &Id<Getter>, cb: Callback<(), AdapterError>) {
        self.dispatch(Execute::RemoveGetter {
            id: id.clone(),
            cb: cb
        });
    }

    /// Add a setter to the system. Typically, this is called by the adapter when a new
    /// service has been detected/configured. Some services may gain/lose setters at
    /// runtime depending on their configuration.
    ///
    /// # Requirements
    ///
    /// The adapter is in charge of making sure that identifiers persist across reboots.
    ///
    /// # Errors
    ///
    /// Returns an error if the adapter is not registered, the parent service is not
    /// registered, or a channel with the same identifier is already registered.
    /// In either cases, this method reverts all its changes.
    pub fn add_setter(&self, setter: Channel<Setter>, cb: Callback<(), AdapterError>) {
        self.dispatch(Execute::AddSetter {
            setter: setter,
            cb: cb
        });
    }

    /// Remove a setter previously registered on the system. Typically, called by
    /// an adapter when a service is reconfigured to remove one of its setters.
    ///
    /// # AdapterError
    ///
    /// This method returns an error if the setter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    pub fn remove_setter(&self, id: &Id<Getter>, cb: Callback<(), AdapterError>) {
        self.dispatch(Execute::RemoveGetter {
            id: id.clone(),
            cb: cb
        });
    }
}

/// A handle to the public API.
impl APIHandle for AdapterManager {
    /// Get the metadata on services matching some conditions.
    ///
    /// A call to `API::get_services(vec![req1, req2, ...])` will return
    /// the metadata on all services matching _either_ `req1` or `req2`
    /// or ...
    fn get_services(&self, selectors: Vec<ServiceSelector>, cb: Infallible<Vec<Service>>) {
        self.dispatch(Execute::GetServices {
            selectors: selectors,
            cb: cb
        });
    }

    /// Label a set of services with a set of tags.
    ///
    /// A call to `API::put_service_tag(vec![req1, req2, ...], vec![tag1,
    /// ...])` will label all the services matching _either_ `req1` or
    /// `req2` or ... with `tag1`, ... and return the number of services
    /// matching any of the selectors.
    ///
    /// Some of the services may already be labelled with `tag1`, or
    /// `tag2`, ... They will not change state. They are counted in
    /// the resulting `usize` nevertheless.
    ///
    /// Note that this call is _not live_. In other words, if services
    /// are added after the call, they will not be affected.
    fn add_service_tag(&self, selectors: Vec<ServiceSelector>, tags: Vec<String>, cb: Infallible<usize>) {
        self.dispatch(Execute::AddServiceTags {
            selectors: selectors,
            tags: tags,
            cb: cb
        })
    }

    /// Remove a set of tags from a set of services.
    ///
    /// A call to `API::delete_service_tag(vec![req1, req2, ...], vec![tag1,
    /// ...])` will remove from all the services matching _either_ `req1` or
    /// `req2` or ... all of the tags `tag1`, ... and return the number of services
    /// matching any of the selectors.
    ///
    /// Some of the services may not be labelled with `tag1`, or `tag2`,
    /// ... They will not change state. They are counted in the
    /// resulting `usize` nevertheless.
    ///
    /// Note that this call is _not live_. In other words, if services
    /// are added after the call, they will not be affected.
    fn remove_service_tag(&self, selectors: Vec<ServiceSelector>, tags: Vec<String>, cb: Infallible<usize>) {
        self.dispatch(Execute::RemoveServiceTags {
            selectors: selectors,
            tags: tags,
            cb: cb
        })
    }

    /// Get a list of setters matching some conditions
    fn get_getter_channels(&self, selectors: Vec<GetterSelector>, cb: Infallible<Vec<Channel<Getter>>>) {
        self.dispatch(Execute::GetGetters {
            selectors: selectors,
            cb: cb
        })
    }
    fn get_setter_channels(&self, selectors: Vec<SetterSelector>, cb: Infallible<Vec<Channel<Setter>>>) {
        self.dispatch(Execute::GetSetters {
            selectors: selectors,
            cb: cb
        })
    }

    /// Label a set of channels with a set of tags.
    ///
    /// A call to `API::put_{setter, setter}_tag(vec![req1, req2, ...], vec![tag1,
    /// ...])` will label all the channels matching _either_ `req1` or
    /// `req2` or ... with `tag1`, ... and return the number of channels
    /// matching any of the selectors.
    ///
    /// Some of the channels may already be labelled with `tag1`, or
    /// `tag2`, ... They will not change state. They are counted in
    /// the resulting `usize` nevertheless.
    ///
    /// Note that this call is _not live_. In other words, if channels
    /// are added after the call, they will not be affected.
    fn add_getter_tag(&self, selectors: Vec<GetterSelector>, tags: Vec<String>, cb: Infallible<usize>) {
        self.dispatch(Execute::AddGetterTags {
            selectors: selectors,
            tags: tags,
            cb: cb
        });
    }
    fn add_setter_tag(&self, selectors: Vec<SetterSelector>, tags: Vec<String>, cb: Infallible<usize>) {
        self.dispatch(Execute::AddSetterTags {
            selectors: selectors,
            tags: tags,
            cb: cb
        });
    }

    /// Remove a set of tags from a set of channels.
    ///
    /// A call to `API::delete_{setter, setter}_tag(vec![req1, req2, ...], vec![tag1,
    /// ...])` will remove from all the channels matching _either_ `req1` or
    /// `req2` or ... all of the tags `tag1`, ... and return the number of channels
    /// matching any of the selectors.
    ///
    /// Some of the channels may not be labelled with `tag1`, or `tag2`,
    /// ... They will not change state. They are counted in the
    /// resulting `usize` nevertheless.
    ///
    /// Note that this call is _not live_. In other words, if channels
    /// are added after the call, they will not be affected.
    fn remove_getter_tag(&self, selectors: Vec<GetterSelector>, tags: Vec<String>, cb: Infallible<usize>) {
        self.dispatch(Execute::RemoveGetterTags {
            selectors: selectors,
            tags: tags,
            cb: cb
        });
    }
    fn remove_setter_tag(&self, selectors: Vec<SetterSelector>, tags: Vec<String>, cb: Infallible<usize>) {
        self.dispatch(Execute::RemoveSetterTags {
            selectors: selectors,
            tags: tags,
            cb: cb
        });
    }

    /// Read the latest value from a set of channels
    fn fetch_channel_values(&self, selectors: Vec<GetterSelector>, cb: FnResultMap<Id<Getter>, Option<Value>, APIError>) {
        self.dispatch(Execute::FetchChannelValues {
            selectors: selectors,
            cb: cb
        });
    }

    /// Send one value to a set of channels
    fn send_channel_values(&self, keyvalues: Vec<(Vec<SetterSelector>, Value)>, cb: FnResultMap<Id<Setter>, (), APIError>) {
        self.dispatch(Execute::SendChannelValues {
            keyvalues: keyvalues,
            cb: cb
        });
    }

    /// Watch for any change
    fn register_channel_watch(&self, selectors: Vec<GetterSelector>, range: Exactly<Range>, on_event: Box<Fn(WatchEvent) + Send + 'static>, cb: Infallible<Self::WatchGuard>) {
        self.dispatch(Execute::RegisterChannelWatch {
            selectors: selectors,
            range: range,
            on_event: on_event,
            cb: cb
        });
    }

    /// A value that causes a disconnection once it is dropped.
    type WatchGuard = WatchGuard;
}