//! An API for plugging in adapters.

#![allow(dead_code)] // Implementation in progress, code isn't called yet.

use adapter::{ Adapter, AdapterWatchGuard };
use transact::InsertInMap;

use foxbox_taxonomy::api::{ AdapterError, APIHandle, Error as APIError, WatchEvent, ResultMap, FnResultMap };
use foxbox_taxonomy::selector::*;
use foxbox_taxonomy::services::*;
use foxbox_taxonomy::util::*;
use foxbox_taxonomy::values::*;

use std::boxed::FnBox;
use std::cell::RefCell;
use std::collections::{ HashMap, HashSet };
use std::collections::hash_map::Entry;
use std::hash::{ Hash, Hasher };
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{ AtomicBool, Ordering };
use std::sync::mpsc::Sender;


/// Data and metadata on an adapter.
struct AdapterData {
    /// The implementation of the adapter.
    adapter: Box<Adapter>,

    /// The services for this adapter.
    services: HashMap<Id<ServiceId>, Rc<RefCell<Service>>>,
}

impl AdapterData {
    fn new(adapter: Box<Adapter>) -> Self {
        AdapterData {
            adapter: adapter,
            services: HashMap::new(),
        }
    }
}
impl Deref for AdapterData {
    type Target = Box<Adapter>;
    fn deref(&self) -> &Self::Target {
        &self.adapter
    }
}

trait Tagged {
    fn insert_tags(&mut self, tags: &[String]);
    fn remove_tags(&mut self, tags: &[String]);
}

impl<T> Tagged for Channel<T> where T: IOMechanism {
    fn insert_tags(&mut self, tags: &[String]) {
        for tag in tags {
            let _ = self.tags.insert(tag.clone());
        }
    }
    fn remove_tags(&mut self, tags: &[String]) {
        for tag in tags {
            let _ = self.tags.remove(tag);
        }
    }
}

impl Hash for WatcherData {
     fn hash<H>(&self, state: &mut H) where H: Hasher {
         self.key.hash(state)
     }
}
impl Eq for WatcherData {}
impl PartialEq for WatcherData {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
struct GetterData {
    getter: Channel<Getter>,
    watchers: HashSet<Rc<WatcherData>>,
}
impl GetterData {
    fn new(getter: Channel<Getter>) -> Self {
        GetterData {
            getter: getter,
            watchers: HashSet::new(),
        }
    }
}
impl SelectedBy<GetterSelector> for GetterData {
    fn matches(&self, selector: &GetterSelector) -> bool {
        self.getter.matches(selector)
    }
}
impl Deref for GetterData {
    type Target = Channel<Getter>;
    fn deref(&self) -> &Self::Target {
        &self.getter
    }
}
impl Tagged for GetterData {
    fn insert_tags(&mut self, tags: &[String]) {
        self.getter.insert_tags(tags)
    }
    fn remove_tags(&mut self, tags: &[String]) {
        self.getter.remove_tags(tags)
    }
}
struct SetterData {
    setter: Channel<Setter>,
}
impl SetterData {
    fn new(setter: Channel<Setter>) -> Self {
        SetterData {
            setter: setter
        }
    }
}
impl SelectedBy<SetterSelector> for SetterData {
    fn matches(&self, selector: &SetterSelector) -> bool {
        self.setter.matches(selector)
    }
}
impl Tagged for SetterData {
    fn insert_tags(&mut self, tags: &[String]) {
        self.setter.insert_tags(tags)
    }
    fn remove_tags(&mut self, tags: &[String]) {
        self.setter.remove_tags(tags)
    }
}
impl Deref for SetterData {
    type Target = Channel<Setter>;
    fn deref(&self) -> &Self::Target {
        &self.setter
    }
}

struct WatcherData {
    selectors: Vec<GetterSelector>,
    range: Exactly<Range>,
    on_event: Box<Fn(WatchEvent) + Send>,
    key: usize,
    guards: RefCell<Vec<Box<AdapterWatchGuard>>>,
    getters: RefCell<HashSet<Id<Getter>>>,
    is_dropped: Arc<AtomicBool>,
}
impl WatcherData {
    fn new(key: usize, selectors: Vec<GetterSelector>, range: Exactly<Range>, is_dropped: &Arc<AtomicBool>, on_event: Box<Fn(WatchEvent) + Send>) -> Self {
        WatcherData {
            selectors: selectors,
            range: range,
            key: key,
            on_event: on_event,
            is_dropped: is_dropped.clone(),
            guards: RefCell::new(Vec::new()),
            getters: RefCell::new(HashSet::new()),
        }
    }

    fn push_guard(&self, guard: Box<AdapterWatchGuard>) {
        self.guards.borrow_mut().push(guard);
    }

    fn push_getter(&self, id: &Id<Getter>) {
        self.getters.borrow_mut().insert(id.clone());
    }
}

struct WatchMap {
    /// A counter of all watchers that have been added to the system.
    /// Used to generate unique keys.
    counter: usize,
    watchers: HashMap<usize, Rc<WatcherData>>,
}
impl WatchMap {
    fn new() -> Self {
        WatchMap {
            counter: 0,
            watchers: HashMap::new()
        }
    }
    fn create(&mut self, selectors: Vec<GetterSelector>, range: Exactly<Range>, is_dropped: &Arc<AtomicBool>, on_event: Box<Fn(WatchEvent) + Send>) -> Rc<WatcherData> {
        let id = self.counter;
        self.counter += 1;
        let watcher = Rc::new(WatcherData::new(id, selectors, range, is_dropped, on_event));
        self.watchers.insert(id, watcher.clone());
        watcher
    }
    fn remove(&mut self, key: usize) -> Option<Rc<WatcherData>> {
        self.watchers.remove(&key)
    }
    fn get(&self, key: usize) -> Option<&Rc<WatcherData>> {
        self.watchers.get(&key)
    }
}

impl Default for WatchMap {
    fn default() -> Self {
        Self::new()
    }
}

/// A data structure that causes cancellation of a watch when dropped.
pub struct WatchGuard {
    /// The channel used to request unregistration.
    tx: Sender<Op>,

    /// The cancellation key.
    key: usize,

    /// Once dropped, the watch callbacks will stopped being called. Note
    /// that dropping this value is not sufficient to cancel the watch, as
    /// the adapters will continue sending updates.
    is_dropped: Arc<AtomicBool>,
}
impl WatchGuard {
    fn new(tx: Sender<Op>, key: usize, is_dropped: Arc<AtomicBool>) -> Self {
        WatchGuard {
            tx: tx,
            key: key,
            is_dropped: is_dropped
        }
    }
}
impl Drop for WatchGuard {
    fn drop(&mut self) {
        let _ = self.tx.send(Op::Execute(Execute::UnregisterChannelWatch(self.key)));
    }
}

pub struct AdapterManagerState {
    /// Adapters, indexed by their id.
    adapter_by_id: HashMap<Id<AdapterId>, AdapterData>,

    /// Services, indexed by their id.
    service_by_id: HashMap<Id<ServiceId>, Rc<RefCell<Service>>>,

    /// Getters, indexed by their id. // FIXME: We have two copies of each setter, the other one in Service!
    getter_by_id: HashMap<Id<Getter>, GetterData>,

    /// Setters, indexed by their id. // FIXME: We have two copies of each setter, the other one in Service!
    setter_by_id: HashMap<Id<Setter>, SetterData>,

    // 1. If we have a getter, we need to find quickly whether it is watched by a watcher.
    //  => inform the watcher if the getter is updated or removed
    //  => attach a Vec<Rc<Watcher>> to `getter_by_id`?
    // 2. If we have a new getter, we need to find quickly if it should be watched by a watcher.
    //  => inform the watcher that a getter is added
    //  => keep a Vec<Rc<Watcher>> with all watchers?
    // 3. We need to be able to remove watchers quickly
    //  => actually, make it a HashMap<usize, Rc<Watcher>>

    watchers: WatchMap,

    tx: Sender<Op>,
}

impl AdapterManagerState {
    pub fn new(tx: Sender<Op>) -> Self {
        AdapterManagerState {
           adapter_by_id: HashMap::new(),
           service_by_id: HashMap::new(),
           getter_by_id: HashMap::new(),
           setter_by_id: HashMap::new(),
           watchers: WatchMap::new(),
           tx: tx,
       }
    }

    /// Auxiliary function to remove a service, once the mutex has been acquired.
    /// Clients should rather use AdapterManager::remove_service.
    fn aux_remove_service(&mut self, id: &Id<ServiceId>) -> Result<(), AdapterError> {
        let service = match self.service_by_id.remove(&id) {
            None => return Err(AdapterError::NoSuchService(id.clone())),
            Some(service) => service,
        };
        for id in service.borrow().getters.keys() {
            let _ignored = self.getter_by_id.remove(id);
        }
        for id in service.borrow().setters.keys() {
            let _ignored = self.setter_by_id.remove(id);
        }
        Ok(())
    }

    /// Add an adapter to the system.
    ///
    /// # Errors
    ///
    /// Returns an error if an adapter with the same id is already present.
    fn add_adapter(&mut self, adapter: Box<Adapter>, services: Vec<Service>) -> Result<(), AdapterError> {
        let id = adapter.id();
        match self.adapter_by_id.entry(id.clone()) {
            Entry::Occupied(_) => return Err(AdapterError::DuplicateAdapter(id)),
            Entry::Vacant(entry) => {
                entry.insert(AdapterData::new(adapter));
            }
        }
        let mut added = Vec::with_capacity(services.len());
        for service in services {
            let service_id = service.id.clone();
            match self.add_service(id.clone(), service) {
                Ok(_) => added.push(service_id),
                Err(err) => {
                    // Rollback everything
                    for service in added {
                        let _ignored = self.remove_service(id.clone(), service);
                    }
                    let _ignored = self.adapter_by_id.remove(&id);
                    return Err(err)
                }
            }
        }
        Ok(())
    }

    /// Remove an adapter from the system, including all its services and channels.
    ///
    /// # Errors
    ///
    /// Returns an error if no adapter with this identifier exists. Otherwise, attempts
    /// to cleanup as much as possible, even if for some reason the system is in an
    /// inconsistent state.
    fn remove_adapter(&mut self, id: Id<AdapterId>) -> Result<(), AdapterError> {
        let mut services = match self.adapter_by_id.remove(&id) {
            Some(AdapterData {services: adapter_services, ..}) => {
                adapter_services
            }
            None => return Err(AdapterError::NoSuchAdapter(id)),
        };
        for (service_id, _) in services.drain() {
            let _ignored = self.aux_remove_service(&service_id);
        }
        Ok(())
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
    fn add_service(&mut self, adapter: Id<AdapterId>, service: Service) -> Result<(), AdapterError> {
        // Insert all getters of this service in `getters`.
        // Note that they already appear in `gervice`, by construction.
        let mut getters_to_insert = Vec::with_capacity(service.getters.len());
        for (id, getter) in &service.getters {
            if getter.adapter != adapter {
                return Err(AdapterError::ConflictingAdapter(getter.adapter.clone(), adapter))
            }
            getters_to_insert.push((id.clone(), GetterData::new(getter.clone())))
        };
        let insert_getters =
            match InsertInMap::start(&mut self.getter_by_id, getters_to_insert) {
                Err(k) => return Err(AdapterError::DuplicateGetter(k)),
                Ok(transaction) => transaction
            };
        // FIXME: Check if one of the getters needs to be watched.

        // Insert all setters of this service in `setters`.
        // Note that they already appear in `service`, by construction.
        let mut setters_to_insert = Vec::with_capacity(service.setters.len());
        for (id, setter) in &service.setters {
            if setter.adapter != adapter {
                return Err(AdapterError::ConflictingAdapter(setter.adapter.clone(), adapter))
            }
            setters_to_insert.push((id.clone(), SetterData::new(setter.clone())))
        };
        let insert_setters =
            match InsertInMap::start(&mut self.setter_by_id, setters_to_insert) {
                Err(k) => return Err(AdapterError::DuplicateSetter(k)),
                Ok(transaction) => transaction
            };

        // Insert in `adapters`.
        let mut services_for_this_adapter =
            match self.adapter_by_id.get_mut(&adapter) {
                None => return Err(AdapterError::NoSuchAdapter(adapter.clone())),
                Some(&mut AdapterData {ref mut services, ..}) => {
                    services
                }
            };
        let id = service.id.clone();
        let service = Rc::new(RefCell::new(service));
        let insert_in_adapters =
            match InsertInMap::start(&mut services_for_this_adapter, vec![(id.clone(), service.clone())]) {
                Err(k) => return Err(AdapterError::DuplicateService(k)),
                Ok(transaction) => transaction
            };

        let insert_in_services =
            match InsertInMap::start(&mut self.service_by_id, vec![(id.clone(), service)]) {
                Err(k) => return Err(AdapterError::DuplicateService(k)),
                Ok(transaction) => transaction
            };

        // If we haven't bailed out yet, leave all this stuff in the maps and sets.
        insert_in_adapters.commit();
        insert_getters.commit();
        insert_setters.commit();
        insert_in_services.commit();
        Ok(())
    }

    /// Remove a service previously registered on the system. Typically, called by
    /// an adapter when a service (e.g. a device) is disconnected.
    ///
    /// # AdapterError
    ///
    /// This method returns an error if the adapter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    fn remove_service(&mut self, adapter: Id<AdapterId>, service_id: Id<ServiceId>) -> Result<(), AdapterError> {
        let _ignored = self.aux_remove_service(&service_id);
        match self.adapter_by_id.get_mut(&adapter) {
            None => Err(AdapterError::NoSuchAdapter(adapter)),
            Some(mut data) => {
                if data.services.remove(&service_id).is_none() {
                    Err(AdapterError::NoSuchService(service_id))
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Add a getter to the system. Typically, this is called by the adapter when a new
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
    fn add_getter(&mut self, getter: Channel<Getter>) -> Result<(), AdapterError> {
        let watchers = &mut self.watchers;
        let getter_by_id = &mut self.getter_by_id;
        let service = match self.service_by_id.get_mut(&getter.service) {
            None => return Err(AdapterError::NoSuchService(getter.service.clone())),
            Some(service) => service
        };
        let getters = &mut service.borrow_mut().getters;

        let insert_in_service = match InsertInMap::start(getters, vec![(getter.id.clone(), getter.clone())]) {
            Ok(transaction) => transaction,
            Err(id) => return Err(AdapterError::DuplicateGetter(id))
        };

        let insert_in_getters = try!(Self::aux_add_getter(GetterData::new(getter), service, getter_by_id, watchers));

        insert_in_service.commit();
        insert_in_getters.commit();
        Ok(())
    }

    fn aux_add_getter<'a, 'b>(mut getter_data: GetterData, service: &Rc<RefCell<Service>>,
        getter_by_id: &'b mut HashMap<Id<Getter>, GetterData>,
        watchers: &mut WatchMap) -> Result<InsertInMap<'a, Id<Getter>, GetterData>, AdapterError>
        where 'b: 'a
    {
        if service.borrow().adapter != getter_data.adapter {
            return Err(AdapterError::ConflictingAdapter(service.borrow().adapter.clone(), getter_data.adapter.clone()));
        }

        // Check whether we match an ongoing watcher.
        for watcher in &mut watchers.watchers.values() {
            let matches = watcher.selectors.iter().find(|selector| {
                getter_data.matches(selector)
            }).is_some();
            if matches {
                getter_data.watchers.insert(watcher.clone());
                watcher.push_getter(&getter_data.id);
                // FIXME: Notify WatchEvent of topology change
                // FIXME: register_single_channel_watch_values
            };
        }

        let insert_in_getters = match InsertInMap::start(getter_by_id, vec![(getter_data.id.clone(), getter_data)]) {
            Ok(transaction) => transaction,
            Err(id) => return Err(AdapterError::DuplicateGetter(id))
        };
        Ok(insert_in_getters)
    }

    /// Remove a setter previously registered on the system. Typically, called by
    /// an adapter when a service is reconfigured to remove one of its setters.
    ///
    /// # AdapterError
    ///
    /// This method returns an error if the setter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    fn remove_getter(&mut self, id: Id<Getter>) -> Result<(), AdapterError> {
        let getter = match self.getter_by_id.remove(&id) {
            None => return Err(AdapterError::NoSuchGetter(id)),
            Some(getter) => getter
        };
        match self.service_by_id.get_mut(&getter.getter.service) {
            None => Err(AdapterError::NoSuchService(getter.getter.service)),
            Some(service) => {
                if service.borrow_mut().getters.remove(&id).is_none() {
                    Err(AdapterError::NoSuchGetter(id))
                } else {
                    Ok(())
                }
            }
        }
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
    fn add_setter(&mut self, setter: Channel<Setter>) -> Result<(), AdapterError> {
        let service = match self.service_by_id.get_mut(&setter.service) {
            None => return Err(AdapterError::NoSuchService(setter.service.clone())),
            Some(service) => service
        };
        if service.borrow().adapter != setter.adapter {
            return Err(AdapterError::ConflictingAdapter(service.borrow().adapter.clone(), setter.adapter));
        }
        let setters = &mut service.borrow_mut().setters;
        let insert_in_service = match InsertInMap::start(setters, vec![(setter.id.clone(), setter.clone())]) {
            Ok(transaction) => transaction,
            Err(id) => return Err(AdapterError::DuplicateSetter(id))
        };
        let insert_in_setters = match InsertInMap::start(&mut self.setter_by_id, vec![(setter.id.clone(), SetterData::new(setter))]) {
            Ok(transaction) => transaction,
            Err(id) => return Err(AdapterError::DuplicateSetter(id))
        };
        insert_in_service.commit();
        insert_in_setters.commit();
        Ok(())
    }

    /// Remove a setter previously registered on the system. Typically, called by
    /// an adapter when a service is reconfigured to remove one of its setters.
    ///
    /// # AdapterError
    ///
    /// This method returns an error if the setter is not registered or if the service
    /// is not registered. In either case, it attemps to clean as much as possible, even
    /// if the state is inconsistent.
    fn remove_setter(&mut self, id: Id<Setter>) -> Result<(), AdapterError> {
        let setter = match self.setter_by_id.remove(&id) {
            None => return Err(AdapterError::NoSuchSetter(id.clone())),
            Some(setter) => setter
        };
        match self.service_by_id.get_mut(&setter.setter.service) {
            None => Err(AdapterError::NoSuchService(setter.setter.service)),
            Some(service) => {
                if service.borrow_mut().setters.remove(&id).is_none() {
                    Err(AdapterError::NoSuchSetter(id))
                } else {
                    Ok(())
                }
            }
        }
    }

    fn with_services<F>(&self, selectors: Vec<ServiceSelector>, mut cb: F) where F: FnMut(&Rc<RefCell<Service>>) {
        for service in self.service_by_id.values() {
            let matches = selectors.iter().find(|selector| {
                selector.matches(&*service.borrow())
            }).is_some();
            if matches {
                cb(service);
            }
        };
    }

    fn get_services(&self, selectors: Vec<ServiceSelector>) -> Vec<Service> {
        // This implementation is not nearly optimal, but it should be sufficient in a system
        // with relatively few services.
        let mut result = Vec::new();
        self.with_services(selectors, |service| {
            result.push(service.borrow().clone())
        });
        result
    }

    fn add_service_tags(&self, selectors: Vec<ServiceSelector>, tags: Vec<String>) -> usize {
        let mut result = 0;
        self.with_services(selectors, |service| {
            let tag_set = &mut service.borrow_mut().tags;
            for tag in &tags {
                let _ = tag_set.insert(tag.clone());
            }
            result += 1;
        });
        result
    }

    fn remove_service_tags(&self, selectors: Vec<ServiceSelector>, tags: Vec<String>) -> usize {
        let mut result = 0;
        self.with_services(selectors, |service| {
            let tag_set = &mut service.borrow_mut().tags;
            for tag in &tags {
                let _ = tag_set.remove(tag);
            }
            result += 1;
        });
        result
    }

    /// Iterate over all channels that match any selector in a slice.
    fn with_channels<S, K, V, F>(selectors: &[S], map: &HashMap<Id<K>, V>, mut cb: F)
        where F: FnMut(&V),
              V: SelectedBy<S>,
    {
        for (_, data) in map.iter() {
            let matches = selectors.iter().find(|selector| {
                data.matches(selector)
            }).is_some();
            if matches {
                cb(data);
            }
        }
    }

/*
    fn iter_channels<'a, S, K, V>(selectors: &[S], map: &HashMap<Id<K>, V>) ->
        Filter<Values<'a, Id<K>, V>, &'a (Fn(&'a V) -> bool)>
        where V: SelectedBy<S>
    {
        let cb : &'a Fn(&'a V) -> bool + 'a = |data: &'a V| {
            selectors.iter().find(|selector| {
                data.matches(selector)
            }).is_some()
        };
        map.values()
            .filter(cb)
    }
*/
    /// Iterate mutably over all channels that match any selector in a slice.
    fn with_channels_mut<S, K, V, F>(selectors: &[S], map: &mut HashMap<Id<K>, V>, mut cb: F)
        where F: FnMut(&mut V),
              V: SelectedBy<S>,
    {
        for (_, data) in map.iter_mut() {
            let matches = selectors.iter().find(|selector| {
                data.matches(selector)
            }).is_some();
            if matches {
                cb(data);
            }
        }
    }

    /// Iterate over all channels that match any selector in a slice.
    fn get_channels<S, K, V, T>(selectors: &[S], map: &HashMap<Id<K>, V>) -> Vec<Channel<T>>
        where V: SelectedBy<S> + Deref<Target = Channel<T>>,
              T: IOMechanism,
              Channel<T>: Clone
    {
        let mut result = Vec::new();
        Self::with_channels(&selectors, map, |data| {
            result.push((*data.deref()).clone());
        });
        result
    }

    fn add_channel_tags<S, K, V>(selectors: &[S], tags: Vec<String>, map: &mut HashMap<Id<K>, V>) -> usize
        where V: SelectedBy<S> + Tagged
    {
        let mut result = 0;
        Self::with_channels_mut(&selectors, map, |mut data| {
            data.insert_tags(&tags);
            result += 1;
        });
        result
    }

    fn remove_channel_tags<S, K, V>(selectors: &[S], tags: Vec<String>, map: &mut HashMap<Id<K>, V>) -> usize
        where V: SelectedBy<S> + Tagged
    {
        let mut result = 0;
        Self::with_channels_mut(&selectors, map, |mut data| {
            data.remove_tags(&tags);
            result += 1;
        });
        result
    }

    /// Iterate over all setter channels that match any selector in a slice.
    fn with_setters<F>(&mut self, selectors: &[SetterSelector], mut cb: F) where F: FnMut(&mut SetterData) {
        for (_, setter_data) in &mut self.setter_by_id {
            let matches = selectors.iter().find(|selector| {
                selector.matches(&setter_data.setter)
            }).is_some();
            if matches {
                cb(setter_data);
            }
        }
    }

    /// Read the latest value from a set of channels
    fn fetch_channel_values(&mut self, selectors: &[GetterSelector]) -> ResultSet<Id<Getter>, Option<Value>, APIError> {
        // First group per adapter, so as to let adapters optimize fetches.
        let mut per_adapter = HashMap::new();
        Self::with_channels(selectors, &self.getter_by_id, |data| {
            use std::collections::hash_map::Entry::*;
            match per_adapter.entry(data.getter.adapter.clone()) {
                Vacant(entry) => {
                    entry.insert(vec![data.getter.id.clone()]);
                }
                Occupied(mut entry) => {
                    entry.get_mut().push(data.getter.id.clone());
                }
            }
        });

        // Now fetch the values
        let mut results = vec![];
        for (adapter_id, getters) in per_adapter {
            match self.adapter_by_id.get(&adapter_id) {
                None => {}, // Internal inconsistency. FIXME: Log this somewhere.
                Some(ref adapter_data) => {
                    let mut got = adapter_data
                        .adapter
                        .fetch_values(getters);

                    let mut got = got.drain(..)
                        .map(|(id, result)| {
                            (id, result.map_err(APIError::AdapterError))
                        })
                        .collect();
                    results.append(&mut got);
                }
            }
        }
        results
    }

    /// Send values to a set of channels
    fn send_channel_values(&self, mut keyvalues: Vec<(Vec<SetterSelector>, Value)>) -> ResultMap<Id<Setter>, (), APIError> {
        // First determine the channels and group them by adapter.
        let mut per_adapter = HashMap::new();
        for (selectors, value) in keyvalues.drain(..) {
            Self::with_channels(&selectors, &self.setter_by_id, |data| {
                use std::collections::hash_map::Entry::*;
                match per_adapter.entry(data.setter.adapter.clone()) {
                    Vacant(entry) => {
                        entry.insert(vec![(data.setter.id.clone(), value.clone())]);
                    }
                    Occupied(mut entry) => {
                        entry.get_mut().push((data.setter.id.clone(), value.clone()));
                    }
                }
            })
        }


        // Dispatch to adapter
        let mut results = Vec::new();
        for (adapter_id, payload) in per_adapter.drain() {
            let adapter = match self.adapter_by_id.get(&adapter_id) {
                None => continue, // That's an internal inconsistency. FIXME: Log this somewhere.
                Some(adapter) => adapter
            };
            let mut got = adapter.adapter.send_values(payload);
            let mut got = got.drain(..)
                .map(|(id, result)| {
                    (id, result.map_err(APIError::AdapterError))
                })
                .collect();
            results.append(&mut got);
        }

        results
    }

    fn register_channel_watch(&mut self, selectors: Vec<GetterSelector>, range: Exactly<Range>, on_event: Box<Fn(WatchEvent) + Send + 'static>) -> WatchGuard {
        // Store the watcher. This will serve when we need to decide whether new channels
        // should be registered with this watcher.
        let is_dropped = Arc::new(AtomicBool::new(false));
        let watcher = self.watchers.create(selectors.clone(), range.clone(), &is_dropped, on_event);
        let key = watcher.key;

        // Find out which channels already match the selectors and attach
        // the watcher immediately.
        let adapter_by_id = &self.adapter_by_id;
        let mut channels = Vec::new();
        Self::with_channels_mut(&selectors, &mut self.getter_by_id, |mut getter_data| {
            getter_data.watchers.insert(watcher.clone());
            watcher.push_getter(&getter_data.id);
            channels.push((getter_data.id.clone(), getter_data.adapter.clone()));
        });

        for (channel_id, adapter_id) in channels.drain(..) {
            let adapter = match adapter_by_id.get(&adapter_id) {
                None => {
                    debug_assert!(false, "We have a registered channel {:?} whose adapter is not registered: {:?}", &channel_id, adapter_id);
                    // FIXME: Logging would be nice.
                    continue
                },
                Some(adapter) => adapter
            };
            match range {
                Exactly::Exactly(ref range) =>
                    self.register_single_channel_watch_values(&channel_id,
                        Some(range.clone()), &watcher,  &adapter),
                Exactly::Always =>
                    self.register_single_channel_watch_values(&channel_id,
                        None, &watcher,  &adapter),
                _ => {
                    // Nothing to watch, we are only interested in topology
                }
            }

        }

        // Upon drop, this data structure will immediately drop `is_dropped` and then dispatch
        // `unregister_channel_watch` to unregister everything else.
        WatchGuard::new(self.tx.clone(), key, is_dropped)
    }

    fn register_single_channel_watch_values(&self, channel_id: &Id<Getter>, range: Option<Range>,
        watcher: &Rc<WatcherData>, adapter: &Box<Adapter>)
    {
        let tx = self.tx.clone();
        let is_dropped = watcher.is_dropped.clone();
        let id = channel_id.clone();
        let key = watcher.key;
        match adapter.register_watch(&channel_id.clone(), range, Box::new(move |value| {
                if is_dropped.load(Ordering::Relaxed) {
                    return;
                }
                let _ = tx.send(Op::Execute(Execute::TriggerWatch {
                    key: key,
                    event: WatchEvent::Value {
                        from: id.clone(),
                        value: value,
                    }
                }));
            })) {
                Err(err) => {
                    let _ = self.tx.send(Op::Execute(Execute::TriggerWatch {
                        key: key,
                        event: WatchEvent::InitializationError {
                            channel: channel_id.clone(),
                            error: err
                        }
                    }));
                },
                Ok(guard) => watcher.push_guard(guard)
        };
    }

    fn trigger_watch(&self, key: usize, event: WatchEvent) {
        let watcher_data = match self.watchers.get(key) {
            None => return, // Race condition. Don't inform clients.
            Some(watcher_data) => watcher_data
        };
        if watcher_data.is_dropped.load(Ordering::Relaxed) {
            return;
        }
        (watcher_data.on_event)(event);
    }

    /// Unregister a watch previously registered with `register_channel_watch`.
    ///
    /// This method is dispatched from `WatchGuard::drop()`.
    fn unregister_channel_watch(&mut self, key: usize) { // FIXME: Use a better type than `usize`
        // Remove `key` from `watchers`. This will prevent the watcher from being registered
        // automatically with any new getter.
        let mut watcher_data = match self.watchers.remove(key) {
            None => {
                debug_assert!(false, "Attempting to unregister a watcher that has already been removed {}", key);
                return
            } // FIXME: Logging would be nice.
            Some(watcher_data) => watcher_data
        };

        // Remove the watcher from all getters.
        for getter_id in watcher_data.getters.borrow().iter() {
            let mut getter = match self.getter_by_id.get_mut(getter_id) {
                None => return, // Race condition between removing the getter and dropping the watcher.
                Some(getter) => getter
            };
            if !getter.watchers.remove(&watcher_data) {
                debug_assert!(false, "Attempting to unregister a watcher that has already been removed from its getter {}, {:?}", key, getter_id);
            }
        }

        // Sanity check
        if Rc::get_mut(&mut watcher_data).is_none() {
            // Ohoh, we still have dangling pointers.
            println!("We still have dangling pointers to a watcher. That's not good");
        }

        // At this stage, `watcher_data` has no reference left. All its `guards` will be dropped.
    }

    /// Dispatch instructions received from the front-end thread.
    pub fn execute(&mut self, msg: Execute) {
        use self::Execute::*;
        match msg {
            // Messages that dispatch to methods
            AddAdapter { adapter, services, cb } => cb(self.add_adapter(adapter, services)),
            RemoveAdapter { id, cb } => cb(self.remove_adapter(id)),
            AddService { adapter, service, cb } => cb(self.add_service(adapter, service)),
            RemoveService { adapter, id, cb } => cb(self.remove_service(adapter, id)),
            AddSetter { setter, cb } => cb(self.add_setter(setter)),
            RemoveSetter { id, cb } => cb(self.remove_setter(id)),
            AddGetter { setter, cb } => cb(self.add_getter(setter)),
            RemoveGetter { id, cb } => cb(self.remove_getter(id)),
            GetServices { selectors, cb } => cb(self.get_services(selectors)),
            AddServiceTags { selectors, tags, cb } => cb(self.add_service_tags(selectors, tags)),
            RemoveServiceTags { selectors, tags, cb } => cb(self.remove_service_tags(selectors, tags)),
            FetchChannelValues { selectors, cb } => cb(self.fetch_channel_values(&selectors)),
            SendChannelValues { keyvalues, cb } => cb(self.send_channel_values(keyvalues)),
            RegisterChannelWatch { selectors, range, on_event, cb } => cb(self.register_channel_watch(selectors, range, on_event)),

            // Messages that dispatch to functions
            GetGetters { selectors, cb } => cb(Self::get_channels(&selectors, &self.getter_by_id)),
            GetSetters { selectors, cb } => cb(Self::get_channels(&selectors, &self.setter_by_id)),
            AddGetterTags { selectors, tags, cb } => cb(Self::add_channel_tags(&selectors, tags, &mut self.getter_by_id)),
            AddSetterTags { selectors, tags, cb } => cb(Self::add_channel_tags(&selectors, tags, &mut self.setter_by_id)),
            RemoveGetterTags { selectors, tags, cb } => cb(Self::remove_channel_tags(&selectors, tags, &mut self.getter_by_id)),
            RemoveSetterTags { selectors, tags, cb } => cb(Self::remove_channel_tags(&selectors, tags, &mut self.setter_by_id)),

            // Messages without callbacks, triggered by this thread.
            TriggerWatch { key, event } => self.trigger_watch(key, event),
            UnregisterChannelWatch(key) => self.unregister_channel_watch(key),
        }
    }
}

pub type Callback<T, E> = Box<FnBox(Result<T, E>) + Send>;
pub type Infallible<T> = Box<FnBox(T) + Send>;

/// Instructions sent to the back-end thread.
pub enum Op {
    Stop(Sender<()>),
    Execute(Execute)
}


pub enum Execute {
    AddAdapter {
        adapter: Box<Adapter>,
        services: Vec<Service>,
        cb: Callback<(), AdapterError>
    },
    RemoveAdapter {
        id: Id<AdapterId>,
        cb: Callback<(), AdapterError>
    },
    AddService {
        adapter: Id<AdapterId>,
        service: Service,
        cb: Callback<(), AdapterError>,
    },
    RemoveService {
        adapter: Id<AdapterId>,
        id: Id<ServiceId>,
        cb: Callback<(), AdapterError>,
    },
    AddGetter {
        setter: Channel<Getter>,
        cb: Callback<(), AdapterError>,
    },
    RemoveGetter {
        id: Id<Getter>,
        cb: Callback<(), AdapterError>,
    },
    AddSetter {
        setter: Channel<Setter>,
        cb: Callback<(), AdapterError>,
    },
    RemoveSetter {
        id: Id<Setter>,
        cb: Callback<(), AdapterError>,
    },
    GetServices {
        selectors: Vec<ServiceSelector>,
        cb: Infallible<Vec<Service>>
    },
    AddServiceTags {
        selectors: Vec<ServiceSelector>,
        tags: Vec<String>,
        cb: Infallible<usize>
    },
    RemoveServiceTags {
        selectors: Vec<ServiceSelector>,
        tags: Vec<String>,
        cb: Infallible<usize>
    },
    GetGetters {
        selectors: Vec<GetterSelector>,
        cb: Infallible<Vec<Channel<Getter>>>
    },
    AddGetterTags {
        selectors: Vec<GetterSelector>,
        tags: Vec<String>,
        cb: Infallible<usize>,
    },
    RemoveGetterTags {
        selectors: Vec<GetterSelector>,
        tags: Vec<String>,
        cb: Infallible<usize>,
    },
    GetSetters {
        selectors: Vec<SetterSelector>,
        cb: Infallible<Vec<Channel<Setter>>>
    },
    AddSetterTags {
        selectors: Vec<SetterSelector>,
        tags: Vec<String>,
        cb: Infallible<usize>,
    },
    RemoveSetterTags {
        selectors: Vec<SetterSelector>,
        tags: Vec<String>,
        cb: Infallible<usize>,
    },
    FetchChannelValues {
        selectors: Vec<GetterSelector>,
        cb: FnResultMap<Id<Getter>, Option<Value>, APIError>
    },
    SendChannelValues {
        keyvalues: Vec<(Vec<SetterSelector>, Value)>,
        cb: FnResultMap<Id<Setter>, (), APIError>
    },
    TriggerWatch {
        key: usize,
        event: WatchEvent,
    },
    RegisterChannelWatch {
        selectors: Vec<GetterSelector>,
        range: Exactly<Range>,
        on_event: Box<Fn(WatchEvent) + Send + 'static>,
        cb: Infallible<WatchGuard>,
    },
    UnregisterChannelWatch(usize),
}

