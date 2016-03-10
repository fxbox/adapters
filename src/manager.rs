//! The Adapter manager
//!
//! This structure serves two roles:
//! - adapters use it to (un)register themselves, as well as services and channels;
//! - it exposes an implementation of the taxonomy API.

use backend::*;
use adapter::{ Adapter, AdapterManager as AdapterManagerHandle };

use foxbox_taxonomy::api::{ AdapterError, API, Error as APIError, ResultMap, WatchEvent };
use foxbox_taxonomy::selector::*;
use foxbox_taxonomy::services::*;
use foxbox_taxonomy::util::*;
use foxbox_taxonomy::values::{ Range, Value };

use std::sync::{ Arc, Mutex };
use std::sync::atomic::Ordering;
use std::sync::mpsc::{ channel, Sender };
use std::thread;

/// An implementation of the AdapterManager.
pub struct AdapterManager {
    back_end: Arc<Mutex<AdapterManagerState>>,
    tx_watchers: Sender<OpWatcher>,
}

impl AdapterManager {
    /// Create an empty AdapterManager.
    /// This function does not attempt to load any state from the disk.
    pub fn new() -> Self {
        let back_end = Arc::new(Mutex::new(AdapterManagerState::new()));
        let watchers = back_end.lock().unwrap().get_sync_watchmap();
        let (tx, rx) = channel();
        thread::spawn(move || {
            for msg in rx {
                match msg {
                    OpWatcher::Stop => return,
                    OpWatcher::WatchNotification { key, id, value } => {
                        let (is_dropped, tx) = match watchers.get(key) {
                            None => continue,
                            Some((is_dropped, tx)) => (is_dropped, tx)
                        };
                        if is_dropped.load(Ordering::Relaxed) {
                            continue;
                        }
                        let event = WatchEvent::Value {
                            from: id,
                            value: value,
                        };
                        let _ = tx.send(event);
                    }
                }
            }
        });
        // Note that this creates a cycle.
        back_end.lock().unwrap().set_tx_watcher(tx.clone());
        AdapterManager {
            back_end: back_end,
            tx_watchers: tx,
        }
    }
}

impl Drop for AdapterManager {
    fn drop(&mut self) {
        // Kill the watcher execution thread. This will break the cycle that would otherwise hold
        // AdapterManagerState alive.
        let _ = self.tx_watchers.send(OpWatcher::Stop);
    }
}
impl Default for AdapterManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AdapterManagerHandle for AdapterManager {
    /// Add an adapter to the system.
    ///
    /// # Errors
    ///
    /// Returns an error if an adapter with the same id is already present.
    fn add_adapter(&self, adapter: Box<Adapter>, services: Vec<Service>) -> Result<(), AdapterError> {
        self.back_end.lock().unwrap().add_adapter(adapter, services)
    }

    /// Remove an adapter from the system, including all its services and channels.
    ///
    /// # Errors
    ///
    /// Returns an error if no adapter with this identifier exists. Otherwise, attempts
    /// to cleanup as much as possible, even if for some reason the system is in an
    /// inconsistent state.
    fn remove_adapter(&self, id: &Id<AdapterId>) -> Result<(), AdapterError> {
        self.back_end.lock().unwrap().remove_adapter(id)
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
    fn add_service(&self, adapter: &Id<AdapterId>, service: Service) -> Result<(), AdapterError> {
        self.back_end.lock().unwrap().add_service(adapter, service)
    }

    /// Remove a service previously registered on the system. Typically, called by
    /// an adapter when a service (e.g. a device) is disconnected.
    ///
    /// # AdapterError
    ///
    /// This method returns an error if the adapter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    fn remove_service(&self, adapter: &Id<AdapterId>, service_id: &Id<ServiceId>) -> Result<(), AdapterError> {
        self.back_end.lock().unwrap().remove_service(adapter, service_id)
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
    fn add_getter(&self, getter: Channel<Getter>) -> Result<(), AdapterError> {
        self.back_end.lock().unwrap().add_getter(getter)
    }

    /// Remove a setter previously registered on the system. Typically, called by
    /// an adapter when a service is reconfigured to remove one of its getters.
    ///
    /// # AdapterError
    ///
    /// This method returns an error if the setter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    fn remove_getter(&self, id: &Id<Getter>) -> Result<(), AdapterError> {
        self.back_end.lock().unwrap().remove_getter(id)
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
    fn add_setter(&self, setter: Channel<Setter>) -> Result<(), AdapterError> {
        self.back_end.lock().unwrap().add_setter(setter)
    }

    /// Remove a setter previously registered on the system. Typically, called by
    /// an adapter when a service is reconfigured to remove one of its setters.
    ///
    /// # AdapterError
    ///
    /// This method returns an error if the setter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    fn remove_setter(&self, id: &Id<Setter>) -> Result<(), AdapterError> {
        self.back_end.lock().unwrap().remove_setter(id)
    }
}

/// A handle to the public API.
impl API for AdapterManager {
    /// Get the metadata on services matching some conditions.
    ///
    /// A call to `API::get_services(vec![req1, req2, ...])` will return
    /// the metadata on all services matching _either_ `req1` or `req2`
    /// or ...
    fn get_services(&self, selectors: &[ServiceSelector]) -> Vec<Service> {
        self.back_end.lock().unwrap().get_services(selectors)
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
    fn add_service_tags(&self, selectors: &[ServiceSelector], tags: &[String]) -> usize {
        self.back_end.lock().unwrap().add_service_tags(selectors, tags)
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
    fn remove_service_tags(&self, selectors: &[ServiceSelector], tags: &[String]) -> usize {
        self.back_end.lock().unwrap().remove_service_tags(selectors, tags)
    }

    /// Get a list of channels matching some conditions
    fn get_getter_channels(&self, selectors: &[GetterSelector]) -> Vec<Channel<Getter>> {
        self.back_end.lock().unwrap().get_getter_channels(selectors)
    }
    fn get_setter_channels(&self, selectors: &[SetterSelector]) -> Vec<Channel<Setter>> {
        self.back_end.lock().unwrap().get_setter_channels(selectors)
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
    fn add_getter_tags(&self, selectors: &[GetterSelector], tags: &[String]) -> usize {
        self.back_end.lock().unwrap().add_getter_tags(selectors, tags)

    }
    fn add_setter_tags(&self, selectors: &[SetterSelector], tags: &[String]) -> usize {
        self.back_end.lock().unwrap().add_setter_tags(selectors, tags)
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
    fn remove_getter_tags(&self, selectors: &[GetterSelector], tags: &[String]) -> usize {
        self.back_end.lock().unwrap().remove_getter_tags(selectors, tags)
    }
    fn remove_setter_tags(&self, selectors: &[SetterSelector], tags: &[String]) -> usize {
        self.back_end.lock().unwrap().remove_setter_tags(selectors, tags)
    }

    /// Read the latest value from a set of channels
    fn fetch_values(&self, selectors: &[GetterSelector]) ->
        ResultMap<Id<Getter>, Option<Value>, APIError>
    {
        self.back_end.lock().unwrap().fetch_values(selectors)
    }

    /// Send a bunch of values to a set of channels
    fn send_values(&self, keyvalues: Vec<(Vec<SetterSelector>, Value)>) ->
        ResultMap<Id<Setter>, (), APIError>
    {
        self.back_end.lock().unwrap().send_values(keyvalues)
    }

    /// Watch for any change
    fn register_channel_watch(&self, selectors: Vec<GetterSelector>, range: Exactly<Range>,
        on_event: Sender<WatchEvent>) -> Self::WatchGuard
    {
        let (key, is_dropped) = self.back_end.lock().unwrap().register_channel_watch(selectors,
            range, on_event);
        WatchGuard::new(self.back_end.clone(), key, is_dropped)
    }

    /// A value that causes a disconnection once it is dropped.
    type WatchGuard = WatchGuard;
}