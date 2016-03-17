extern crate foxbox_adapters;
extern crate foxbox_taxonomy;
extern crate transformable_channels;

use foxbox_adapters::adapter::*;
use foxbox_adapters::manager::*;
use foxbox_taxonomy::api::{ API, Error, InternalError };
use foxbox_taxonomy::selector::*;
use foxbox_taxonomy::services::*;
use foxbox_taxonomy::util::*;
use foxbox_taxonomy::values::*;

use transformable_channels::mpsc::*;

use std::cell::RefCell;
use std::collections::{ HashMap, HashSet };
use std::sync::{ Arc, Mutex };
use std::thread;
use std::sync::mpsc::{ sync_channel, SyncSender };

enum TestOp {
    InjectGetterValue(Id<Getter>, Result<Option<Value>, Error>),
    InjectSetterError(Id<Setter>, Option<Error>)
}

#[derive(Debug)]
enum Effect {
    ValueSent(Id<Setter>, Value)
}

fn dup<T>(t: T) -> (T, T) where T: Clone {
    (t.clone(), t)
}
struct TestAdapter {
    id: Id<AdapterId>,
    name: String,
    tx: SyncSender<TestOp>,
    tx_effect: RawSender<Effect>,
    rx_effect: RefCell<Option<Receiver<Effect>>>,
    values: Arc<Mutex<HashMap<Id<Getter>, Result<Value, Error>>>>,
    senders: Arc<Mutex<HashMap<Id<Setter>, Error>>>
}

impl TestAdapter {
    fn new(id: &Id<AdapterId>) -> Self {
        let (tx, rx) = sync_channel(0);
        let (tx_effect, rx_effect) = channel();

        let (values_main, values_thread) = dup(Arc::new(Mutex::new(HashMap::new())));
        let (senders_main, senders_thread) = dup(Arc::new(Mutex::new(HashMap::new())));
        thread::spawn(move || {
            use self::TestOp::*;
            for msg in rx {
                match msg {
                    InjectGetterValue(id, Ok(Some(value))) => {
                        values_thread.lock().unwrap().insert(id, Ok(value));
                    },
                    InjectGetterValue(id, Err(error)) => {
                        values_thread.lock().unwrap().insert(id, Err(error));
                    },
                    InjectGetterValue(id, Ok(None)) => {
                        values_thread.lock().unwrap().remove(&id);
                    },
                    InjectSetterError(id, None) => {
                        senders_thread.lock().unwrap().remove(&id);
                    },
                    InjectSetterError(id, Some(err)) => {
                        senders_thread.lock().unwrap().insert(id, err);
                    }
                }
            }
        });
        TestAdapter {
            id: id.clone(),
            name: id.as_atom().to_string().clone(),
            values: values_main,
            senders: senders_main,
            tx: tx,
            tx_effect: tx_effect,
            rx_effect: RefCell::new(Some(rx_effect))
        }
    }

    fn take_rx(&self) -> Receiver<Effect> {
        self.rx_effect.borrow_mut().take().unwrap()
    }
}

static VERSION : [u32;4] = [0, 0, 0, 0];

impl Adapter for TestAdapter {
    /// An id unique to this adapter. This id must persist between
    /// reboots/reconnections.
    fn id(&self) -> Id<AdapterId> {
        self.id.clone()
    }

    /// The name of the adapter.
    fn name(&self) -> &str {
        &self.name
    }

    fn vendor(&self) -> &str {
        "test@foxbox_adapters"
    }

    fn version(&self) -> &[u32;4] {
        &VERSION
    }

    /// Request a value from a channel. The FoxBox (not the adapter)
    /// is in charge of keeping track of the age of values.
    fn fetch_values(&self, mut channels: Vec<Id<Getter>>) -> ResultMap<Id<Getter>, Option<Value>, Error> {
        let map = self.values.lock().unwrap();
        channels.drain(..).map(|id| {
            let result = match map.get(&id) {
                None => Ok(None),
                Some(&Ok(ref value)) => Ok(Some(value.clone())),
                Some(&Err(ref error)) => Err(error.clone())
            };
            (id, result)
        }).collect()
    }

    /// Request that a value be sent to a channel.
    fn send_values(&self, mut values: Vec<(Id<Setter>, Value)>) -> ResultMap<Id<Setter>, (), Error> {
        let map = self.senders.lock().unwrap();
        values.drain(..).map(|(id, value)| {
            let result = match map.get(&id) {
                None => {
                    self.tx_effect.send(Effect::ValueSent(id.clone(), value)).unwrap();
                    Ok(())
                }
                Some(error) => Err(error.clone())
            };
            (id, result)
        }).collect()
    }

    fn register_watch(&self, sources: Vec<(Id<Getter>, Option<Range>)>,
        cb: Box<ExtSender<WatchEvent>>) ->
            ResultMap<Id<Getter>, Box<AdapterWatchGuard>, Error>
    {
        unimplemented!()
    }
}

#[test]
fn test_add_remove_adapter() {
    let manager = AdapterManager::new();
    let id_1 = Id::new("id 1");
    let id_2 = Id::new("id 2");

    println!("* Adding two distinct test adapters should work.");
    manager.add_adapter(Box::new(TestAdapter::new(&id_1))).unwrap();
    manager.add_adapter(Box::new(TestAdapter::new(&id_2))).unwrap();

    println!("* Attempting to add yet another test adapter with id_1 or id_2 should fail.");
    match manager.add_adapter(Box::new(TestAdapter::new(&id_1))) {
        Err(Error::InternalError(InternalError::DuplicateAdapter(ref id))) if *id == id_1 => {},
        other => panic!("Unexpected result {:?}", other)
    }
    match manager.add_adapter(Box::new(TestAdapter::new(&id_2))) {
        Err(Error::InternalError(InternalError::DuplicateAdapter(ref id))) if *id == id_2 => {},
        other => panic!("Unexpected result {:?}", other)
    }

    println!("* Removing id_1 should succeed. At this stage, we still shouldn't be able to add id_2, \
              but we should be able to re-add id_1");
    manager.remove_adapter(&id_1).unwrap();
    match manager.add_adapter(Box::new(TestAdapter::new(&id_2))) {
        Err(Error::InternalError(InternalError::DuplicateAdapter(ref id))) if *id == id_2 => {},
        other => panic!("Unexpected result {:?}", other)
    }
    manager.add_adapter(Box::new(TestAdapter::new(&id_1))).unwrap();

    println!("* Removing id_1 twice should fail the second time.");
    manager.remove_adapter(&id_1).unwrap();
    match manager.remove_adapter(&id_1) {
        Err(Error::InternalError(InternalError::NoSuchAdapter(ref id))) if *id == id_1 => {},
        other => panic!("Unexpected result {:?}", other)
    }
}

#[test]
fn test_add_remove_services() {
    println!("");
    let manager = AdapterManager::new();
    let id_1 = Id::<AdapterId>::new("adapter id 1");
    let id_2 = Id::<AdapterId>::new("adapter id 2");
    let id_3 = Id::<AdapterId>::new("adapter id 3");


    let getter_id_1 = Id::<Getter>::new("getter id 1");
    let getter_id_2 = Id::<Getter>::new("getter id 2");
    let getter_id_3 = Id::<Getter>::new("getter id 3");

    let setter_id_1 = Id::<Setter>::new("setter id 1");
    let setter_id_2 = Id::<Setter>::new("setter id 2");
    let setter_id_3 = Id::<Setter>::new("setter id 3");

    let service_id_1 = Id::<ServiceId>::new("service id 1");
    let service_id_2 = Id::<ServiceId>::new("service id 2");
    let service_id_3 = Id::<ServiceId>::new("service id 3");

    let getter_1 = Channel {
        id: getter_id_1.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Getter {
            updated: None,
            kind: ChannelKind::OnOff,
            watch: false,
            poll: None,
            trigger: None,
        },
    };

    let setter_1 = Channel {
        id: setter_id_1.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            updated: None,
            kind: ChannelKind::OnOff,
            push: None,
        },
    };

    let getter_1_with_bad_service = Channel {
        id: getter_id_1.clone(),
        service: service_id_3.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Getter {
            updated: None,
            kind: ChannelKind::OnOff,
            watch: false,
            poll: None,
            trigger: None,
        },
    };

    let setter_1_with_bad_service = Channel {
        id: setter_id_1.clone(),
        service: service_id_3.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            updated: None,
            kind: ChannelKind::OnOff,
            push: None,
        },
    };

    let getter_2_with_bad_adapter = Channel {
        adapter: id_3.clone(),
        .. getter_1.clone()
    };

    let setter_2_with_bad_adapter = Channel {
        adapter: id_3.clone(),
        .. setter_1.clone()
    };

    let service_1 = Service {
        id: service_id_1.clone(),
        adapter: id_1.clone(),
        tags: HashSet::new(),
        getters: HashMap::new(),
        setters: HashMap::new(),
    };

    let getter_2 = Channel {
        id: getter_id_2.clone(),
        service: service_id_2.clone(),
        adapter: id_2.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Getter {
            updated: None,
            kind: ChannelKind::OnOff,
            watch: false,
            poll: None,
            trigger: None,
        },
    };

    let setter_2 = Channel {
        id: setter_id_2.clone(),
        service: service_id_2.clone(),
        adapter: id_2.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            updated: None,
            kind: ChannelKind::OnOff,
            push: None,
        },
    };

    let service_2 = Service {
        id: service_id_2.clone(),
        adapter: id_2.clone(),
        tags: HashSet::new(),
        getters: HashMap::new(),
        setters: HashMap::new(),
    };

    let service_2_with_channels = Service {
        getters: vec![(getter_id_2.clone(), getter_2.clone())].iter().cloned().collect(),
        setters: vec![(setter_id_2.clone(), setter_2.clone())].iter().cloned().collect(),
        ..service_2.clone()
    };

    println!("* Adding a service should fail if there is no adapter.");
    match manager.add_service(service_1.clone()) {
        Err(Error::InternalError(InternalError::NoSuchAdapter(ref err))) if *err == id_1 => {},
        other => panic!("Unexpected result {:?}", other)
    }

    println!("* Adding a service should fail if the adapter doesn't exist.");
    manager.add_adapter(Box::new(TestAdapter::new(&id_2))).unwrap();
    match manager.add_service(service_1.clone()) {
        Err(Error::InternalError(InternalError::NoSuchAdapter(ref err))) if *err == id_1 => {},
        other => panic!("Unexpected result {:?}", other)
    }

    println!("* Adding a service should fail if the service is not empty.");
    match manager.add_service(service_2_with_channels.clone()) {
        Err(Error::InternalError(InternalError::InvalidInitialService)) => {},
        other => panic!("Unexpected result {:?}", other)
    }

    println!("* We shouldn't have any channels.");
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new()]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new()]).len(), 0);

    println!("* Make sure that none of the services has been added.");
    assert_eq!(manager.get_services(vec![ServiceSelector::new()]).len(), 0);

    println!("* Adding a service can succeed.");
    manager.add_adapter(Box::new(TestAdapter::new(&id_1))).unwrap();
    manager.add_service(service_1.clone()).unwrap();
    assert_eq!(manager.get_services(vec![ServiceSelector::new()]).len(), 1);

    println!("* Make sure that we are finding the right service.");
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_id(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_id(service_id_2.clone())]).len(), 0);

    println!("* Adding a second service with the same id should fail.");
    match manager.add_service(service_1.clone()) {
        Err(Error::InternalError(InternalError::DuplicateService(ref err))) if *err == service_id_1 => {},
        other => panic!("Unexpected result {:?}", other)
    }

    println!("* Adding channels should fail if the service doesn't exist.");
    match manager.add_getter(getter_1_with_bad_service.clone()) {
        Err(Error::InternalError(InternalError::NoSuchService(ref err))) if *err == service_id_3 => {},
        other => panic!("Unexpected result {:?}", other)
    }
    match manager.add_setter(setter_1_with_bad_service.clone()) {
        Err(Error::InternalError(InternalError::NoSuchService(ref err))) if *err == service_id_3 => {},
        other => panic!("Unexpected result {:?}", other)
    }

    println!("* The attempt shouldn't let any channel lying around.");
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new()]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new()]).len(), 0);

    println!("* Adding channels should fail if the adapter doesn't match that of its service.");
    match manager.add_getter(getter_2_with_bad_adapter) {
        Err(Error::InternalError(InternalError::ConflictingAdapter(ref err_1, ref err_2)))
            if *err_1 == id_3 && *err_2 == id_1 => {},
        Err(Error::InternalError(InternalError::ConflictingAdapter(ref err_1, ref err_2)))
            if *err_1 == id_1 && *err_2 == id_3 => {},
        other => panic!("Unexpected result {:?}", other)
    }
    match manager.add_setter(setter_2_with_bad_adapter) {
        Err(Error::InternalError(InternalError::ConflictingAdapter(ref err_1, ref err_2)))
            if *err_1 == id_3 && *err_2 == id_1 => {},
        Err(Error::InternalError(InternalError::ConflictingAdapter(ref err_1, ref err_2)))
            if *err_1 == id_1 && *err_2 == id_3 => {},
        other => panic!("Unexpected result {:?}", other)
    }

    println!("* The attempt shouldn't let any channel lying around.");
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new()]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new()]).len(), 0);

    println!("* Adding getter channels can succeed.");
    manager.add_getter(getter_1.clone()).unwrap();
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new()]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new()]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_id(getter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_id(setter_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 0);

    println!("* Adding setter channels can succeed.");
    manager.add_setter(setter_1.clone()).unwrap();
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new()]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new()]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_id(getter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_id(setter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 1);

    println!("* Removing getter channels can succeed.");
    manager.remove_getter(&getter_id_1).unwrap();
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new()]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new()]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_id(getter_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_id(setter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 1);

    println!("* Removing setter channels can succeed.");
    manager.remove_setter(&setter_id_1).unwrap();
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new()]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new()]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_id(getter_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_id(setter_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 0);

    println!("* We can remove a service without channels.");
    manager.remove_service(&service_id_1).unwrap();

    println!("* We can add several services, then several channels.");
    manager.add_service(service_1.clone()).unwrap();
    manager.add_service(service_2.clone()).unwrap();
    manager.add_getter(getter_1.clone()).unwrap();
    manager.add_setter(setter_1.clone()).unwrap();
    manager.add_getter(getter_2.clone()).unwrap();
    manager.add_setter(setter_2.clone()).unwrap();
    assert_eq!(manager.get_services(vec![ServiceSelector::new()]).len(), 2);
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_id(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_id(service_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_id(service_id_3.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new()]).len(), 2);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new()]).len(), 2);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_id(getter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_id(setter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_id(getter_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_id(setter_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_id(getter_id_3.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_id(setter_id_3.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_3.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_3.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 2);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 2);

    println!("* We can remove a service with channels.");
    manager.remove_service(&service_id_1).unwrap();
    assert_eq!(manager.get_services(vec![ServiceSelector::new()]).len(), 1);
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_id(service_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_id(service_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_id(service_id_3.clone())]).len(), 0);

    println!("* Removing a service with channels also removes its channels.");
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new()]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new()]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_id(getter_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_id(setter_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_kind(ChannelKind::OnOff)]).len(), 1);

    println!("* Removing a service with channels doesn't remove other channels.");
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_id(getter_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_id(setter_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_2.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_parent(service_id_3.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_parent(service_id_3.clone())]).len(), 0);
}

#[test]
fn test_add_remove_tags() {
    println!("");
    let manager = AdapterManager::new();
    let id_1 = Id::<AdapterId>::new("adapter id 1");
    let id_2 = Id::<AdapterId>::new("adapter id 2");

    let getter_id_1 = Id::<Getter>::new("getter id 1");
    let getter_id_2 = Id::<Getter>::new("getter id 2");

    let setter_id_1 = Id::<Setter>::new("setter id 1");
    let setter_id_2 = Id::<Setter>::new("setter id 2");

    let service_id_1 = Id::<ServiceId>::new("service id 1");
    let service_id_2 = Id::<ServiceId>::new("service id 2");

    let getter_1 = Channel {
        id: getter_id_1.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Getter {
            updated: None,
            kind: ChannelKind::OnOff,
            watch: false,
            poll: None,
            trigger: None,
        },
    };

    let setter_1 = Channel {
        id: setter_id_1.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            updated: None,
            kind: ChannelKind::OnOff,
            push: None,
        },
    };

    let service_1 = Service {
        id: service_id_1.clone(),
        adapter: id_1.clone(),
        tags: HashSet::new(),
        getters: HashMap::new(),
        setters: HashMap::new(),
    };

    let getter_2 = Channel {
        id: getter_id_2.clone(),
        service: service_id_2.clone(),
        adapter: id_2.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Getter {
            updated: None,
            kind: ChannelKind::OnOff,
            watch: false,
            poll: None,
            trigger: None,
        },
    };

    let setter_2 = Channel {
        id: setter_id_2.clone(),
        service: service_id_2.clone(),
        adapter: id_2.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            updated: None,
            kind: ChannelKind::OnOff,
            push: None,
        },
    };

    let service_2 = Service {
        id: service_id_2.clone(),
        adapter: id_2.clone(),
        tags: HashSet::new(),
        getters: HashMap::new(),
        setters: HashMap::new(),
    };

    let tag_1 = Id::<TagId>::new("tag_1");
    let tag_2 = Id::<TagId>::new("tag_2");
    let tag_3 = Id::<TagId>::new("tag_3");

    println!("* Initially, there are no tags.");
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_tags(vec![tag_1.clone()])]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_tags(vec![tag_1.clone()])]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_tags(vec![tag_1.clone()])]).len(), 0);

    println!("* After adding an adapter, service, getter, setter, still no tags.");
    manager.add_adapter(Box::new(TestAdapter::new(&id_1))).unwrap();
    manager.add_service(service_1.clone()).unwrap();
    manager.add_getter(getter_1.clone()).unwrap();
    manager.add_setter(setter_1.clone()).unwrap();
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_tags(vec![tag_1.clone()])]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_tags(vec![tag_1.clone()])]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_tags(vec![tag_1.clone()])]).len(), 0);

    println!("* Removing tags from non-existent services and channels doesn't hurt and returns 0.");
    assert_eq!(manager
        .remove_service_tags(
            vec![ServiceSelector::new().with_id(service_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        0);
    assert_eq!(manager
        .remove_getter_tags(
            vec![GetterSelector::new().with_id(getter_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        0);
    assert_eq!(manager
        .remove_setter_tags(
            vec![SetterSelector::new().with_id(setter_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        0);

    println!("* Adding tags to non-existent services and channels doesn't hurt and returns 0.");
    assert_eq!(manager
        .add_service_tags(
            vec![ServiceSelector::new().with_id(service_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        0);
    assert_eq!(manager
        .add_getter_tags(
            vec![GetterSelector::new().with_id(getter_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        0);
    assert_eq!(manager
        .add_setter_tags(
            vec![SetterSelector::new().with_id(setter_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        0);

    println!("* There are still no tags.");
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_tags(vec![tag_2.clone()])]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_tags(vec![tag_2.clone()])]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_tags(vec![tag_2.clone()])]).len(), 0);

    println!("* Removing non-added tags from existent services and channels doesn't hurt and returns 1.");
    manager.add_adapter(Box::new(TestAdapter::new(&id_2))).unwrap();
    manager.add_service(service_2.clone()).unwrap();
    manager.add_getter(getter_2.clone()).unwrap();
    manager.add_setter(setter_2.clone()).unwrap();
    assert_eq!(manager
        .remove_service_tags(
            vec![ServiceSelector::new().with_id(service_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        1);
    assert_eq!(manager
        .remove_getter_tags(
            vec![GetterSelector::new().with_id(getter_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        1);
    assert_eq!(manager
        .remove_setter_tags(
            vec![SetterSelector::new().with_id(setter_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        1);

    println!("* We can add tags tags to services and channels, this returns 1.");
    assert_eq!(manager
        .add_service_tags(
            vec![ServiceSelector::new().with_id(service_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        1);
    assert_eq!(manager
        .add_getter_tags(
            vec![GetterSelector::new().with_id(getter_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        1);
    assert_eq!(manager
        .add_setter_tags(
            vec![SetterSelector::new().with_id(setter_id_2.clone())], vec![tag_2.clone(), tag_3.clone()]
        ),
        1);

    println!("* We can select using these tags.");
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_tags(vec![tag_1.clone()])]).len(), 0);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_tags(vec![tag_1.clone()])]).len(), 0);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_tags(vec![tag_1.clone()])]).len(), 0);
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_tags(vec![tag_2.clone()])]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_tags(vec![tag_2.clone()])]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_tags(vec![tag_2.clone()])]).len(), 1);
    assert_eq!(manager.get_services(vec![ServiceSelector::new().with_tags(vec![tag_3.clone()])]).len(), 1);
    assert_eq!(manager.get_getter_channels(vec![GetterSelector::new().with_tags(vec![tag_3.clone()])]).len(), 1);
    assert_eq!(manager.get_setter_channels(vec![SetterSelector::new().with_tags(vec![tag_3.clone()])]).len(), 1);

    println!("* The tags are only applied to the right services/getters.");
    assert_eq!(manager.get_services(vec![
        ServiceSelector::new()
            .with_tags(vec![tag_2.clone()])
            .with_id(service_id_1.clone())
        ]).len(), 0
    );
    assert_eq!(manager.get_getter_channels(vec![
        GetterSelector::new()
            .with_tags(vec![tag_2.clone()])
            .with_id(getter_id_1.clone())
        ]).len(), 0
    );
    assert_eq!(manager.get_setter_channels(vec![
        SetterSelector::new()
            .with_tags(vec![tag_2.clone()])
            .with_id(setter_id_1.clone())
        ]).len(), 0
    );

    println!("* The tags are applied to the right services/getters.");
    let selection = manager.get_services(vec![
        ServiceSelector::new()
            .with_tags(vec![tag_2.clone()])
            .with_id(service_id_2.clone())
        ]);
    assert_eq!(selection.len(), 1);
    assert_eq!(selection[0].id, service_id_2);
    assert_eq!(selection[0].tags.len(), 2);
    assert!(selection[0].tags.contains(&tag_2));
    assert!(selection[0].tags.contains(&tag_3));

    let selection = manager.get_getter_channels(vec![
        GetterSelector::new()
            .with_tags(vec![tag_2.clone()])
            .with_id(getter_id_2.clone())
    ]);
    assert_eq!(selection.len(), 1);
    assert_eq!(selection[0].id, getter_id_2);
    assert_eq!(selection[0].tags.len(), 2);
    assert!(selection[0].tags.contains(&tag_2));
    assert!(selection[0].tags.contains(&tag_3));

    let selection = manager.get_setter_channels(vec![
        SetterSelector::new()
            .with_tags(vec![tag_2.clone()])
            .with_id(setter_id_2.clone())
    ]);
    assert_eq!(selection.len(), 1);
    assert_eq!(selection[0].id, setter_id_2);
    assert_eq!(selection[0].tags.len(), 2);
    assert!(selection[0].tags.contains(&tag_2));
    assert!(selection[0].tags.contains(&tag_3));

    let selection = manager.get_services(vec![
        ServiceSelector::new()
            .with_tags(vec![tag_3.clone()])
            .with_id(service_id_2.clone())
        ]);
    assert_eq!(selection.len(), 1);
    assert_eq!(selection[0].id, service_id_2);
    assert_eq!(selection[0].tags.len(), 2);
    assert!(selection[0].tags.contains(&tag_2));
    assert!(selection[0].tags.contains(&tag_3));

    let selection = manager.get_getter_channels(vec![
        GetterSelector::new()
            .with_tags(vec![tag_3.clone()])
            .with_id(getter_id_2.clone())
    ]);
    assert_eq!(selection.len(), 1);
    assert_eq!(selection[0].id, getter_id_2);
    assert_eq!(selection[0].tags.len(), 2);
    assert!(selection[0].tags.contains(&tag_2));
    assert!(selection[0].tags.contains(&tag_3));

    let selection = manager.get_setter_channels(vec![
        SetterSelector::new()
            .with_tags(vec![tag_3.clone()])
            .with_id(setter_id_2.clone())
    ]);
    assert_eq!(selection.len(), 1);
    assert_eq!(selection[0].id, setter_id_2);
    assert_eq!(selection[0].tags.len(), 2);
    assert!(selection[0].tags.contains(&tag_2));
    assert!(selection[0].tags.contains(&tag_3));

    println!("* We can remove tags, both existent and non-existent.");
    assert_eq!(manager
        .remove_service_tags(
            vec![ServiceSelector::new().with_id(service_id_2.clone())], vec![tag_1.clone(), tag_3.clone()]
        ),
        1);
    assert_eq!(manager
        .remove_getter_tags(
            vec![GetterSelector::new().with_id(getter_id_2.clone())], vec![tag_1.clone(), tag_3.clone()]
        ),
        1);
    assert_eq!(manager
        .remove_setter_tags(
            vec![SetterSelector::new().with_id(setter_id_2.clone())], vec![tag_1.clone(), tag_3.clone()]
        ),
        1);

    println!("* Looking by tags has been updated.");
    let selection = manager.get_services(vec![
        ServiceSelector::new()
            .with_tags(vec![tag_2.clone()])
            .with_id(service_id_2.clone())
        ]);
    assert_eq!(selection.len(), 1);
    assert_eq!(selection[0].id, service_id_2);
    assert_eq!(selection[0].tags.len(), 1);
    assert!(selection[0].tags.contains(&tag_2));

    let selection = manager.get_getter_channels(vec![
        GetterSelector::new()
            .with_tags(vec![tag_2.clone()])
            .with_id(getter_id_2.clone())
    ]);
    assert_eq!(selection.len(), 1);
    assert_eq!(selection[0].id, getter_id_2);
    assert_eq!(selection[0].tags.len(), 1);
    assert!(selection[0].tags.contains(&tag_2));

    let selection = manager.get_setter_channels(vec![
        SetterSelector::new()
            .with_tags(vec![tag_2.clone()])
            .with_id(setter_id_2.clone())
    ]);
    assert_eq!(selection.len(), 1);
    assert_eq!(selection[0].id, setter_id_2);
    assert_eq!(selection[0].tags.len(), 1);
    assert!(selection[0].tags.contains(&tag_2));

    let selection = manager.get_services(vec![
        ServiceSelector::new()
            .with_tags(vec![tag_3.clone()])
            .with_id(service_id_2.clone())
        ]);
    assert_eq!(selection.len(), 0);

    let selection = manager.get_getter_channels(vec![
        GetterSelector::new()
            .with_tags(vec![tag_3.clone()])
            .with_id(getter_id_2.clone())
    ]);
    assert_eq!(selection.len(), 0);

    let selection = manager.get_setter_channels(vec![
        SetterSelector::new()
            .with_tags(vec![tag_3.clone()])
            .with_id(setter_id_2.clone())
    ]);
    assert_eq!(selection.len(), 0);

    println!("");
}

#[test]
fn test_fetch() {
    println!("");
    let manager = AdapterManager::new();
    let id_1 = Id::<AdapterId>::new("adapter id 1");
    let id_2 = Id::<AdapterId>::new("adapter id 2");


    let getter_id_1_1 = Id::<Getter>::new("getter id 1.1");
    let getter_id_1_2 = Id::<Getter>::new("getter id 1.2");
    let getter_id_1_3 = Id::<Getter>::new("getter id 1.3");
    let getter_id_2 = Id::<Getter>::new("getter id 2");

    let service_id_1 = Id::<ServiceId>::new("service id 1");
    let service_id_2 = Id::<ServiceId>::new("service id 2");

    let getter_1_1 = Channel {
        id: getter_id_1_1.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Getter {
            updated: None,
            kind: ChannelKind::OnOff,
            watch: false,
            poll: None,
            trigger: None,
        },
    };

    let getter_1_2 = Channel {
        id: getter_id_1_2.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Getter {
            updated: None,
            kind: ChannelKind::OnOff,
            watch: false,
            poll: None,
            trigger: None,
        },
    };

    let getter_1_3 = Channel {
        id: getter_id_1_3.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Getter {
            updated: None,
            kind: ChannelKind::OnOff,
            watch: false,
            poll: None,
            trigger: None,
        },
    };

    let getter_2 = Channel {
        id: getter_id_2.clone(),
        service: service_id_2.clone(),
        adapter: id_2.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Getter {
            updated: None,
            kind: ChannelKind::OnOff,
            watch: false,
            poll: None,
            trigger: None,
        },
    };

    let service_1 = Service {
        id: service_id_1.clone(),
        adapter: id_1.clone(),
        tags: HashSet::new(),
        getters: HashMap::new(),
        setters: HashMap::new(),
    };

    let service_2 = Service {
        id: service_id_2.clone(),
        adapter: id_2.clone(),
        tags: HashSet::new(),
        getters: HashMap::new(),
        setters: HashMap::new(),
    };

    let adapter_1 = TestAdapter::new(&id_1);
    let adapter_2 = TestAdapter::new(&id_2);
    let tx_adapter_1 = adapter_1.tx.clone();
    println!("* Without adapters, fetching values from a selector that has no channels returns an empty vector.");
    assert_eq!(manager.fetch_values(vec![GetterSelector::new()]).len(), 0);

    println!("* With adapters, fetching values from a selector that has no channels returns an empty vector.");
    manager.add_adapter(Box::new(adapter_1)).unwrap();
    manager.add_adapter(Box::new(adapter_2)).unwrap();
    manager.add_service(service_1.clone()).unwrap();
    manager.add_service(service_2.clone()).unwrap();
    assert_eq!(manager.fetch_values(vec![GetterSelector::new()]).len(), 0);

    println!("* Fetching empty values from a selector that has channels returns a vector of empty values.");
    manager.add_getter(getter_1_1.clone()).unwrap();
    manager.add_getter(getter_1_2.clone()).unwrap();
    manager.add_getter(getter_1_3.clone()).unwrap();
    manager.add_getter(getter_2.clone()).unwrap();
    let data = manager.fetch_values(vec![GetterSelector::new()]);
    assert_eq!(data.len(), 4);

    for result in data.values() {
        if let Ok(None) = *result {
            // We're good.
        } else {
            panic!("Unexpected result {:?}", result)
        }
    }

    println!("* Fetching values returns the right values.");
    tx_adapter_1.send(TestOp::InjectGetterValue(getter_id_1_1.clone(), Ok(Some(Value::OnOff(OnOff::On))))).unwrap();
    tx_adapter_1.send(TestOp::InjectGetterValue(getter_id_1_2.clone(), Ok(Some(Value::OnOff(OnOff::Off))))).unwrap();
    let data = manager.fetch_values(vec![GetterSelector::new()]);
    assert_eq!(data.len(), 4);
    match data.get(&getter_id_1_1) {
        Some(&Ok(Some(Value::OnOff(OnOff::On)))) => {},
        other => panic!("Unexpected result, {:?}", other)
    }
    match data.get(&getter_id_1_2) {
        Some(&Ok(Some(Value::OnOff(OnOff::Off)))) => {},
        other => panic!("Unexpected result, {:?}", other)
    }
    match data.get(&getter_id_1_3) {
        Some(&Ok(None)) => {},
        other => panic!("Unexpected result, {:?}", other)
    }
    match data.get(&getter_id_2) {
        Some(&Ok(None)) => {},
        other => panic!("Unexpected result, {:?}", other)
    }

    println!("* Fetching values returns the right errors.");
    tx_adapter_1.send(TestOp::InjectGetterValue(getter_id_1_1.clone(), Err(Error::InternalError(InternalError::NoSuchGetter(getter_id_1_1.clone()))))).unwrap();
    let data = manager.fetch_values(vec![GetterSelector::new()]);
    assert_eq!(data.len(), 4);
    match data.get(&getter_id_1_1) {
        Some(&Err(Error::InternalError(InternalError::NoSuchGetter(ref id)))) if *id == getter_id_1_1 => {},
        other => panic!("Unexpected result, {:?}", other)
    }
    match data.get(&getter_id_1_2) {
        Some(&Ok(Some(Value::OnOff(OnOff::Off)))) => {},
        other => panic!("Unexpected result, {:?}", other)
    }
    match data.get(&getter_id_1_3) {
        Some(&Ok(None)) => {},
        other => panic!("Unexpected result, {:?}", other)
    }
    match data.get(&getter_id_2) {
        Some(&Ok(None)) => {},
        other => panic!("Unexpected result, {:?}", other)
    }

    println!("* Fetching a value that causes an internal type error returns that error.");
    tx_adapter_1.send(TestOp::InjectGetterValue(getter_id_1_1.clone(), Ok(Some(Value::OpenClosed(OpenClosed::Open))))).unwrap();
    let data = manager.fetch_values(vec![GetterSelector::new()]);
    assert_eq!(data.len(), 4);
    match data.get(&getter_id_1_1) {
        Some(&Err(Error::TypeError(TypeError {
            got: Type::OpenClosed,
            expected: Type::OnOff,
        }))) => {},
        other => panic!("Unexpected result, {:?}", other)
    }
    match data.get(&getter_id_1_2) {
        Some(&Ok(Some(Value::OnOff(OnOff::Off)))) => {},
        other => panic!("Unexpected result, {:?}", other)
    }
    match data.get(&getter_id_1_3) {
        Some(&Ok(None)) => {},
        other => panic!("Unexpected result, {:?}", other)
    }
    match data.get(&getter_id_2) {
        Some(&Ok(None)) => {},
        other => panic!("Unexpected result, {:?}", other)
    }

    // FIXME: Should test fetching with tags.

    println!("");
}

#[test]
fn test_send() {
    println!("");
    let manager = AdapterManager::new();
    let id_1 = Id::<AdapterId>::new("adapter id 1");
    let id_2 = Id::<AdapterId>::new("adapter id 2");

    let setter_id_1_1 = Id::<Setter>::new("setter id 1.1");
    let setter_id_1_2 = Id::<Setter>::new("setter id 1.2");
    let setter_id_1_3 = Id::<Setter>::new("setter id 1.3");
    let setter_id_2 = Id::<Setter>::new("setter id 2");

    let service_id_1 = Id::<ServiceId>::new("service id 1");
    let service_id_2 = Id::<ServiceId>::new("service id 2");

    let setter_1_1 = Channel {
        id: setter_id_1_1.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            kind: ChannelKind::OnOff,
            updated: None,
            push: None,
        },
    };

    let setter_1_2 = Channel {
        id: setter_id_1_2.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            kind: ChannelKind::OnOff,
            updated: None,
            push: None,
        },
    };

    let setter_1_3 = Channel {
        id: setter_id_1_3.clone(),
        service: service_id_1.clone(),
        adapter: id_1.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            kind: ChannelKind::OnOff,
            updated: None,
            push: None,
        },
    };

    let setter_2 = Channel {
        id: setter_id_2.clone(),
        service: service_id_2.clone(),
        adapter: id_2.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            kind: ChannelKind::OnOff,
            updated: None,
            push: None,
        },
    };

    let service_1 = Service {
        id: service_id_1.clone(),
        adapter: id_1.clone(),
        tags: HashSet::new(),
        getters: HashMap::new(),
        setters: HashMap::new(),
    };

    let service_2 = Service {
        id: service_id_2.clone(),
        adapter: id_2.clone(),
        tags: HashSet::new(),
        getters: HashMap::new(),
        setters: HashMap::new(),
    };

    let adapter_1 = TestAdapter::new(&id_1);
    let adapter_2 = TestAdapter::new(&id_2);
    let tx_adapter_1 = adapter_1.tx.clone();
    let rx_adapter_1 = adapter_1.take_rx();
    let rx_adapter_2 = adapter_2.take_rx();

    println!("* Without adapters, sending values to a selector that has no channels returns an empty vector.");
    let data = manager.send_values(vec![(vec![SetterSelector::new()], Value::OnOff(OnOff::On))]);
    assert_eq!(data.len(), 0);

    println!("* With adapters, sending values to a selector that has no channels returns an empty vector.");
    manager.add_adapter(Box::new(adapter_1)).unwrap();
    manager.add_adapter(Box::new(adapter_2)).unwrap();
    manager.add_service(service_1.clone()).unwrap();
    manager.add_service(service_2.clone()).unwrap();
    let data = manager.send_values(vec![(vec![SetterSelector::new()], Value::OnOff(OnOff::On))]);
    assert_eq!(data.len(), 0);

    println!("* Sending well-typed values to channels succeeds if the adapter succeeds.");
    manager.add_setter(setter_1_1.clone()).unwrap();
    manager.add_setter(setter_1_2.clone()).unwrap();
    manager.add_setter(setter_1_3.clone()).unwrap();
    manager.add_setter(setter_2.clone()).unwrap();

    let data = manager.send_values(vec![(vec![SetterSelector::new()], Value::OnOff(OnOff::On))]);
    assert_eq!(data.len(), 4);
    for result in data.values() {
        if let Ok(()) = *result {
            // We're good.
        } else {
            panic!("Unexpected result {:?}", result)
        }
    }

    println!("* All the values should have been received.");
    let mut data = HashMap::new();
    for _ in 0..3 {
        let Effect::ValueSent(id, value) = rx_adapter_1.try_recv().unwrap();
        data.insert(id, value);
    }
    assert_eq!(data.len(), 3);

    let value = rx_adapter_2.recv().unwrap();
    if let Effect::ValueSent(id, Value::OnOff(OnOff::On)) = value {
        assert_eq!(id, setter_id_2);
    } else {
        panic!("Unexpected value {:?}", value)
    }

    println!("* No further value should have been received.");
    assert!(rx_adapter_1.try_recv().is_err());
    assert!(rx_adapter_2.try_recv().is_err());

    println!("* Sending ill-typed values to channels will cause type errors.");
    let data = manager.send_values(vec![
        (vec![
            SetterSelector::new().with_id(setter_id_1_1.clone()),
            SetterSelector::new().with_id(setter_id_1_2.clone()),
            SetterSelector::new().with_id(setter_id_2.clone()),
        ], Value::OpenClosed(OpenClosed::Closed)),
        (vec![
            SetterSelector::new().with_id(setter_id_1_3.clone()).clone()
        ], Value::OnOff(OnOff::On))
    ]);
    assert_eq!(data.len(), 4);
    for id in vec![&setter_id_1_1, &setter_id_1_2, &setter_id_2] {
        match data.get(id) {
            Some(&Err(Error::TypeError(TypeError {
                got: Type::OpenClosed,
                expected: Type::OnOff
            }))) => {},
            other => panic!("Unexpected result for {:?}: {:?}", id, other)
        }
    }
    match data.get(&setter_id_1_3) {
        Some(&Ok(())) => {},
        other => panic!("Unexpected result for {:?}: {:?}", setter_id_1_3, other)
    }

    println!("* All the weill-typed values should have been received.");
    match rx_adapter_1.try_recv().unwrap() {
        Effect::ValueSent(ref id, Value::OnOff(OnOff::On)) if *id == setter_id_1_3 => {},
        effect => panic!("Unexpected effect {:?}", effect)
    }

    println!("* No further value should have been received.");
    std::thread::sleep(std::time::Duration::new(2, 0));
    assert!(rx_adapter_1.try_recv().is_err());
    assert!(rx_adapter_2.try_recv().is_err());

    println!("* Sending values that cause channel errors will cause propagate the errors.");
    tx_adapter_1.send(TestOp::InjectSetterError(setter_id_1_1.clone(), Some(Error::InternalError(InternalError::InvalidInitialService)))).unwrap();
    let data = manager.send_values(vec![(vec![SetterSelector::new()], Value::OnOff(OnOff::On))]);
    assert_eq!(data.len(), 4);
    for id in vec![&setter_id_2, &setter_id_1_2, &setter_id_2] {
        match data.get(id) {
            Some(&Ok(())) => {},
            other => panic!("Unexpected result for {:?}: {:?}", id, other)
        }
    }

    for id in vec![&setter_id_1_1] {
        match data.get(id) {
            Some(&Err(Error::InternalError(InternalError::InvalidInitialService))) => {},
            other => panic!("Unexpected result for {:?}: {:?}", id, other)
        }
    }

    println!("* All the non-errored values should have been received.");
    for _ in 0..2 {
        match rx_adapter_1.try_recv().unwrap() {
            Effect::ValueSent(ref id, Value::OnOff(OnOff::On)) if *id != setter_id_1_1 => {},
            effect => panic!("Unexpected effect {:?}", effect)
        }
    }
    match rx_adapter_2.try_recv().unwrap() {
        Effect::ValueSent(ref id, Value::OnOff(OnOff::On)) if *id == setter_id_2 => {},
        effect => panic!("Unexpected effect {:?}", effect)
    }

    println!("* No further value should have been received.");
    assert!(rx_adapter_1.try_recv().is_err());
    assert!(rx_adapter_2.try_recv().is_err());
    tx_adapter_1.send(TestOp::InjectSetterError(setter_id_1_1.clone(), None)).unwrap();

    // FIXME: What happens if we send several times to the same setter?

    println!("");
}

