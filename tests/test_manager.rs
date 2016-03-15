extern crate foxbox_adapters;
extern crate foxbox_taxonomy;
extern crate transformable_channels;

use foxbox_adapters::adapter::*;
use foxbox_adapters::manager::*;
use foxbox_taxonomy::api::{ API, Error, InternalError, ResultMap };
use foxbox_taxonomy::selector::*;
use foxbox_taxonomy::services::*;
use foxbox_taxonomy::util::*;
use foxbox_taxonomy::values::*;

use transformable_channels::mpsc::*;

use std::collections::{ HashMap, HashSet };

struct TestAdapter {
    id: Id<AdapterId>,
    name: String
}

impl TestAdapter {
    fn new(id: &Id<AdapterId>) -> Self {
        TestAdapter {
            id: id.clone(),
            name: id.as_atom().to_string().clone()
        }
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
    fn fetch_values(&self, set: Vec<Id<Getter>>) -> ResultMap<Id<Getter>, Option<Value>, Error> {
        unimplemented!()
    }

    /// Request that a value be sent to a channel.
    fn send_values(&self, values: Vec<(Id<Setter>, Value)>) -> ResultMap<Id<Setter>, (), Error> {
        unimplemented!()
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
    let id_1 = Id::new("id 1".to_owned());
    let id_2 = Id::new("id 2".to_owned());

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
    let id_1 = Id::<AdapterId>::new("adapter id 1".to_owned());
    let id_2 = Id::<AdapterId>::new("adapter id 2".to_owned());
    let id_3 = Id::<AdapterId>::new("adapter id 3".to_owned());


    let getter_id_1 = Id::<Getter>::new("getter id 1".to_owned());
    let getter_id_2 = Id::<Getter>::new("getter id 2".to_owned());
    let getter_id_3 = Id::<Getter>::new("getter id 3".to_owned());

    let setter_id_1 = Id::<Setter>::new("setter id 1".to_owned());
    let setter_id_2 = Id::<Setter>::new("setter id 2".to_owned());
    let setter_id_3 = Id::<Setter>::new("setter id 3".to_owned());

    let service_id_1 = Id::<ServiceId>::new("service id 1".to_owned());
    let service_id_2 = Id::<ServiceId>::new("service id 2".to_owned());
    let service_id_3 = Id::<ServiceId>::new("service id 3".to_owned());

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
