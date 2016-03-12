use foxbox_taxonomy::api::{ Error, ResultMap };
use foxbox_taxonomy::services::*;
use foxbox_taxonomy::util::*;
use foxbox_taxonomy::values::*;

/// A witness that we are currently watching for a value.
/// Watching stops when the guard is dropped.
pub trait AdapterWatchGuard {
}

/// An API that adapter managers must implement
pub trait AdapterManagerHandle {
    /// Add an adapter to the system.
    ///
    /// # Errors
    ///
    /// Returns an error if an adapter with the same id is already present.
    fn add_adapter(&self, adapter: Box<Adapter>) -> Result<(), Error>;

    /// Remove an adapter from the system, including all its services and channels.
    ///
    /// # Errors
    ///
    /// Returns an error if no adapter with this identifier exists. Otherwise, attempts
    /// to cleanup as much as possible, even if for some reason the system is in an
    /// inconsistent state.
    fn remove_adapter(&self, id: &Id<AdapterId>) -> Result<(), Error>;

    /// Add a service to the system. Called by the adapter when a new
    /// service (typically a new device) has been detected/configured.
    ///
    /// # Requirements
    ///
    /// The adapter is in charge of making sure that identifiers persist across reboots.
    ///
    /// # Errors
    ///
    /// Returns an error if any of:
    /// - `service` has channels;
    /// - a service with id `service.id` is already installed on the system;
    /// - there is no adapter with id `service.adapter`.
    fn add_service(&self, service: Service) -> Result<(), Error>;

    /// Remove a service previously registered on the system. Typically, called by
    /// an adapter when a service (e.g. a device) is disconnected.
    ///
    /// # Errors
    ///
    /// Returns an error if any of:
    /// - there is no such service;
    /// - there is an internal inconsistency, in which case this method will still attempt to
    /// cleanup before returning an error.
    fn remove_service(&self, service_id: &Id<ServiceId>) -> Result<(), Error>;

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
    fn add_getter(&self, setter: Channel<Getter>) -> Result<(), Error>;

    /// Remove a setter previously registered on the system. Typically, called by
    /// an adapter when a service is reconfigured to remove one of its getters.
    ///
    /// # Error
    ///
    /// This method returns an error if the setter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    fn remove_getter(&self, id: &Id<Getter>) -> Result<(), Error>;

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
    fn add_setter(&self, setter: Channel<Setter>) -> Result<(), Error>;

    /// Remove a setter previously registered on the system. Typically, called by
    /// an adapter when a service is reconfigured to remove one of its setters.
    ///
    /// # Error
    ///
    /// This method returns an error if the setter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    fn remove_setter(&self, id: &Id<Setter>) -> Result<(), Error>;
}

pub enum WatchEvent {
    /// Fired when we enter the range specified when we started watching, or if no range was
    /// specified, fired whenever a new value is available.
    Enter {
        id: Id<Getter>,
        value: Value
    },
    /// Fired when we exit the range specified when we started watching. If no range was
    /// specified, never fired.
    Exit {
        id: Id<Getter>,
        value: Value
    }
}

/// API that adapters must implement.
///
/// Note that all methods are blocking. However, the underlying implementatino of adapters is
/// expected to either return quickly or be able to handle several requests concurrently.
pub trait Adapter: Send {
    /// An id unique to this adapter. This id must persist between
    /// reboots/reconnections.
    fn id(&self) -> Id<AdapterId>;

    /// The name of the adapter.
    fn name(&self) -> &str;
    fn vendor(&self) -> &str;
    fn version(&self) -> &[u32;4];
    // ... more metadata

    /// Request a value from a channel. The FoxBox (not the adapter)
    /// is in charge of keeping track of the age of values.
    fn fetch_values(&self, set: Vec<Id<Getter>>) -> ResultMap<Id<Getter>, Option<Value>, Error>;

    /// Request that values be sent to a channel.
    fn send_values(&self, values: Vec<(Id<Setter>, Value)>) -> ResultMap<Id<Setter>, (), Error>;

    /// Watch a bunch of getters as they change.
    fn register_watch(&self, Vec<(Id<Getter>, Option<Range>)>,
        cb: Box<Fn(WatchEvent) + Send>) ->
            ResultMap<Id<Getter>, Box<AdapterWatchGuard>, Error>;
}
