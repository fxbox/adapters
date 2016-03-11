extern crate foxbox_adapters;
extern crate foxbox_taxonomy;

use foxbox_adapters::adapter::*;
use foxbox_adapters::manager::*;
use foxbox_taxonomy::api::{ AdapterError, API, ResultMap };
use foxbox_taxonomy::selector::*;
use foxbox_taxonomy::services::*;
use foxbox_taxonomy::util::*;
use foxbox_taxonomy::values::*;

use std::collections::HashSet;

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
    fn fetch_values(&self, set: Vec<Id<Getter>>) -> ResultMap<Id<Getter>, Option<Value>, AdapterError> {
        unimplemented!()
    }

    /// Request that a value be sent to a channel.
    fn send_values(&self, values: Vec<(Id<Setter>, Value)>) -> ResultMap<Id<Setter>, (), AdapterError> {
        unimplemented!()
    }

    fn register_watch(&self, sources: Vec<(Id<Getter>, Option<Range>)>,
        cb: Box<Fn(WatchEvent) + Send>) ->
            ResultMap<Id<Getter>, Box<AdapterWatchGuard>, AdapterError>
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
    manager.add_adapter(Box::new(TestAdapter::new(&id_1)), vec![]).unwrap();
    manager.add_adapter(Box::new(TestAdapter::new(&id_2)), vec![]).unwrap();

    println!("* Attempting to add yet another test adapter with id_1 or id_2 should fail.");
    match manager.add_adapter(Box::new(TestAdapter::new(&id_1)), vec![]) {
        Err(AdapterError::DuplicateAdapter(ref id)) if *id == id_1 => {},
        other => panic!("Unexpected result {:?}", other)
    }
    match manager.add_adapter(Box::new(TestAdapter::new(&id_2)), vec![]) {
        Err(AdapterError::DuplicateAdapter(ref id)) if *id == id_2 => {},
        other => panic!("Unexpected result {:?}", other)
    }

    println!("* Removing id_1 should succeed. At this stage, we still shouldn't be able to add id_2, \
              but we should be able to re-add id_1");
    manager.remove_adapter(&id_1).unwrap();
    match manager.add_adapter(Box::new(TestAdapter::new(&id_2)), vec![]) {
        Err(AdapterError::DuplicateAdapter(ref id)) if *id == id_2 => {},
        other => panic!("Unexpected result {:?}", other)
    }
    manager.add_adapter(Box::new(TestAdapter::new(&id_1)), vec![]).unwrap();

    println!("* Removing id_1 twice should fail the second time.");
    manager.remove_adapter(&id_1).unwrap();
    match manager.remove_adapter(&id_1) {
        Err(AdapterError::NoSuchAdapter(ref id)) if *id == id_1 => {},
        other => panic!("Unexpected result {:?}", other)
    }
}

#[test]
fn test_add_remove_adapter_with_services() {
    let manager = AdapterManager::new();
    let id_1 = Id::new("adapter id 1".to_owned());
    let id_2 = Id::new("adapter id 2".to_owned());
    let id_3 = Id::new("adapter id 3".to_owned());

    let getter_id_1 = Id::new("getter id 1".to_owned());
    let getter_id_2 = Id::new("getter id 2".to_owned());
    let getter_id_3 = Id::new("getter id 3".to_owned());

    let setter_id_1 = Id::new("setter id 1".to_owned());
    let setter_id_2 = Id::new("setter id 2".to_owned());
    let setter_id_3 = Id::new("setter id 3".to_owned());

    let service_id_1 = Id::new("service id 1".to_owned());
    let service_id_2 = Id::new("service id 2".to_owned());
    let service_id_3 = Id::new("service id 3".to_owned());

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
        getters: vec![(getter_id_1.clone(), getter_1)].iter().cloned().collect(),
        setters: vec![(setter_id_1.clone(), setter_1)].iter().cloned().collect(),
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
        getters: vec![(getter_id_2.clone(), getter_2)].iter().cloned().collect(),
        setters: vec![(setter_id_2.clone(), setter_2)].iter().cloned().collect(),
    };
    println!("* Adding an adapter with a service should fail if the adapter id doesn't match.");
    match manager.add_adapter(Box::new(TestAdapter::new(&id_2)), vec![service_1.clone()]) {
        Err(AdapterError::ConflictingAdapter(ref err_1, ref err_2))
            if (*err_1 == id_1 && *err_2 == id_2)
            || (*err_1 == id_2 && *err_2 == id_1) => {},
        other => panic!("Unexpected result {:?}", other)
    }
    println!("* Adding an adapter with a service should fail if one of the adapter ids doesn't match.");
    match manager.add_adapter(Box::new(TestAdapter::new(&id_2)), vec![service_2.clone(), service_1.clone()]) {
        Err(AdapterError::ConflictingAdapter(ref err_1, ref err_2))
            if (*err_1 == id_1 && *err_2 == id_2)
            || (*err_1 == id_2 && *err_2 == id_1) => {},
        other => panic!("Unexpected result {:?}", other)
    }
    println!("* Make sure that none of the getters, setters or services has been added.");
    assert_eq!(manager.get_services(&vec![ServiceSelector::new()]).len(), 0);
    assert_eq!(manager.get_getter_channels(&vec![GetterSelector::new()]).len(), 0);
    assert_eq!(manager.get_setter_channels(&vec![SetterSelector::new()]).len(), 0);

    println!("* Adding an adapter with a service can succeed.");
    manager.add_adapter(Box::new(TestAdapter::new(&id_1)), vec![service_1.clone()]).unwrap();
    assert_eq!(manager.get_services(&vec![ServiceSelector::new()]).len(), 1);
    assert_eq!(manager.get_getter_channels(&vec![GetterSelector::new()]).len(), 1);
    assert_eq!(manager.get_setter_channels(&vec![SetterSelector::new()]).len(), 1);

    println!("* Make sure that we are finding the right service.");
    assert_eq!(manager.get_services(&vec![ServiceSelector::new().with_id(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_services(&vec![ServiceSelector::new().with_id(service_id_2.clone())]).len(), 0);

    println!("* Make sure that we are finding the right channels.");
    assert_eq!(manager.get_getter_channels(&vec![GetterSelector::new().with_id(getter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(&vec![GetterSelector::new().with_id(getter_id_2.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(&vec![SetterSelector::new().with_id(setter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(&vec![SetterSelector::new().with_id(setter_id_2.clone())]).len(), 0);

    // This service has adapter id_2 but its channels disagree
    let getter_3 = Channel {
        id: getter_id_3.clone(),
        service: service_id_3.clone(),
        adapter: id_3.clone(),
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

    let setter_3 = Channel {
        id: setter_id_3.clone(),
        service: service_id_3.clone(),
        adapter: id_3.clone(),
        last_seen: None,
        tags: HashSet::new(),
        mechanism: Setter {
            updated: None,
            kind: ChannelKind::OnOff,
            push: None,
        },
    };

    let service_3 = Service {
        id: service_id_3.clone(),
        adapter: id_2.clone(),
        tags: HashSet::new(),
        getters: vec![(getter_id_3.clone(), getter_3)].iter().cloned().collect(),
        setters: vec![(setter_id_3.clone(), setter_3)].iter().cloned().collect(),
    };
    println!("* Adding an adapter with a service should fail if channels don't have the right adapter id.");
    match manager.add_adapter(Box::new(TestAdapter::new(&id_2)), vec![service_3.clone()]) {
        Err(AdapterError::ConflictingAdapter(ref err_1, ref err_2))
            if (*err_1 == id_3 && *err_2 == id_2)
            || (*err_1 == id_2 && *err_2 == id_3) => {},
        other => panic!("Unexpected result {:?}", other)
    }

    println!("* Make sure that the old service is still here and the new one isn't.");
    assert_eq!(manager.get_services(&vec![ServiceSelector::new().with_id(service_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_services(&vec![ServiceSelector::new().with_id(service_id_3.clone())]).len(), 0);

    println!("* Make sure that the old channels are still here and the new ones aren't.");
    assert_eq!(manager.get_getter_channels(&vec![GetterSelector::new().with_id(getter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_getter_channels(&vec![GetterSelector::new().with_id(getter_id_3.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(&vec![SetterSelector::new().with_id(setter_id_1.clone())]).len(), 1);
    assert_eq!(manager.get_setter_channels(&vec![SetterSelector::new().with_id(setter_id_3.clone())]).len(), 0);

    println!("* Make sure that we can remove the adapter we have successfully added and that this \
                removes the service and channels.");
    manager.remove_adapter(&id_1).unwrap();
    assert_eq!(manager.get_services(&vec![ServiceSelector::new().with_id(service_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_getter_channels(&vec![GetterSelector::new().with_id(getter_id_1.clone())]).len(), 0);
    assert_eq!(manager.get_setter_channels(&vec![SetterSelector::new().with_id(setter_id_1.clone())]).len(), 0);

    println!("* Make sure that we cannot remove the adapter we failed to add.");
    match manager.remove_adapter(&id_2) {
        Err(AdapterError::NoSuchAdapter(ref id)) if *id == id_2 => {},
        other => panic!("Unexpected result {:?}", other)
    }
    println!("* Make sure that we cannot remove the non-existing adapter that we \
                implicitly mentioned.");
    match manager.remove_adapter(&id_3) {
        Err(AdapterError::NoSuchAdapter(ref id)) if *id == id_3 => {},
        other => panic!("Unexpected result {:?}", other)
    }
}