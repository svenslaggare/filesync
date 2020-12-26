#[macro_export]
macro_rules! hashset {
    ($($x:expr),+ $(,)?) => (HashSet::<_, std::collections::hash_map::RandomState>::from_iter([$($x),+].to_vec().into_iter()));
}