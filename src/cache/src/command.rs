#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash)]
pub enum CacheCommand {
    ShowPolicy { catalog: String },
}
