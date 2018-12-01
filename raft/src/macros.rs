/// Takes a Weak<Mutex<T>>, upgrades and locks it.
macro_rules! upgrade_or_return {
    ($weak:ident, $ret:expr) => {
        let strong = $weak.upgrade();
        #[allow(unused_mut)]
        let mut $weak = match strong {
            Some(ref lock) => lock.lock().unwrap(),
            None => return $ret,
        };
    };

    ($weak:ident) => {
        upgrade_or_return!($weak, ());
    };
}
