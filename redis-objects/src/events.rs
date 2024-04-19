use std::marker::PhantomData;



pub struct EventWatcher<T> {

    _data: PhantomData<T>
}

