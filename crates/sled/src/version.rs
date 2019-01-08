use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Version {
    pub(crate) version: u64,
    pub(crate) kind: VersionKind,
    pub(crate) value: Vec<u8>,
}

impl Version {
    pub(crate) fn new_set(version: u64, value: Vec<u8>) -> Version {
        Version {
            version,
            kind: VersionKind::Set,
            value,
        }
    }

    pub(crate) fn new_del(version: u64) -> Version {
        Version {
            version,
            kind: VersionKind::Del,
            value: vec![],
        }
    }

    pub(crate) fn new_merge(version: u64, value: Vec<u8>) -> Version {
        Version {
            version,
            kind: VersionKind::Merge,
            value,
        }
    }

    #[inline]
    pub(crate) fn size_in_bytes(&self) -> u64 {
        let self_sz = std::mem::size_of::<Self>() as u64;
        let inner_sz = self.value.len() as u64;

        self_sz.saturating_add(inner_sz)
    }

    pub(crate) fn is_set(&self) -> bool {
        if let VersionKind::Set = self.kind {
            true
        } else {
            false
        }
    }

    pub(crate) fn is_del(&self) -> bool {
        if let VersionKind::Del = self.kind {
            true
        } else {
            false
        }
    }

    pub(crate) fn is_merge(&self) -> bool {
        if let VersionKind::Merge = self.kind {
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum VersionKind {
    Set,
    Del,
    Merge,
}

impl VersionKind {
    pub(crate) fn is_set(&self) -> bool {
        if let VersionKind::Set = self {
            true
        } else {
            false
        }
    }

    pub(crate) fn is_del(&self) -> bool {
        if let VersionKind::Del = self {
            true
        } else {
            false
        }
    }

    pub(crate) fn is_merge(&self) -> bool {
        if let VersionKind::Merge = self {
            true
        } else {
            false
        }
    }
}
