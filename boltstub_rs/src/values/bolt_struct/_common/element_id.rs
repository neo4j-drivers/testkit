use std::fmt::Debug;

use crate::bolt_version::JoltVersion;

/// Jolt 3 introduced `element_id` fields to all graph types.
///
/// This struct can hold any element id information and asserts that its presence
/// is kept consistent with the Jolt version used.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ElementIdExt<T> {
    inner: Option<T>,
    jolt_version: JoltVersion,
}

impl ElementIdExt<()> {
    pub(crate) fn uses_element_id(jolt_version: JoltVersion) -> bool {
        match jolt_version {
            JoltVersion::V1 => false,
            JoltVersion::V2 | JoltVersion::V3 | JoltVersion::V4 => true,
        }
    }
}

impl<T: Debug> ElementIdExt<T> {
    #[cfg(test)]
    pub(crate) fn new(jolt_version: JoltVersion, element_id: T) -> Self {
        let inner = ElementIdExt::uses_element_id(jolt_version).then_some(element_id);
        Self {
            inner,
            jolt_version,
        }
    }

    pub(crate) fn new_lazy(jolt_version: JoltVersion, element_id: impl FnOnce() -> T) -> Self {
        let inner = ElementIdExt::uses_element_id(jolt_version).then(element_id);
        Self {
            inner,
            jolt_version,
        }
    }

    pub(crate) fn inner(&self, jolt_version: JoltVersion) -> Option<&T> {
        self.assert_jolt_version(jolt_version);
        self.inner.as_ref()
    }

    pub(crate) fn into_inner(self, jolt_version: JoltVersion) -> Option<T> {
        self.assert_jolt_version(jolt_version);
        self.inner
    }

    pub(crate) fn map<O: Debug>(self, f: impl FnOnce(T) -> O) -> ElementIdExt<O> {
        let inner = self.inner.map(f);
        ElementIdExt {
            inner,
            jolt_version: self.jolt_version,
        }
    }

    pub(crate) fn apply(&mut self, f: impl FnOnce(&mut T)) {
        self.inner.as_mut().map(f);
    }

    pub(in crate::values::bolt_struct) fn assert_jolt_version(&self, jolt_version: JoltVersion) {
        assert!(
            self.jolt_version == jolt_version,
            "jolt_version mismatch between access ({jolt_version:?}) \
                and creation: {self:?}"
        );
    }
}

impl<T: PartialEq> PartialEq for ElementIdExt<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T: Eq> Eq for ElementIdExt<T> {}

impl<T, E> ElementIdExt<Result<T, E>> {
    pub(crate) fn transpose(self) -> Result<ElementIdExt<T>, E> {
        let inner = self.inner.transpose()?;
        Ok(ElementIdExt {
            inner,
            jolt_version: self.jolt_version,
        })
    }
}

impl<T> ElementIdExt<Option<T>> {
    pub(crate) fn transpose(self) -> Option<ElementIdExt<T>> {
        match self.inner {
            Some(Some(inner)) => Some(ElementIdExt {
                inner: Some(inner),
                jolt_version: self.jolt_version,
            }),
            Some(None) => None,
            None => Some(ElementIdExt {
                inner: None,
                jolt_version: self.jolt_version,
            }),
        }
    }
}
