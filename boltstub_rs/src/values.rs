pub mod bolt_message;
pub mod bolt_struct;
pub mod jolt_ser;
pub mod pack_stream_value;

#[cfg(test)]
mod tests {
    use rstest::fixture;
    use rstest_reuse::template;

    use crate::ext::serde_json::support_pub::JoltSerializer;

    #[allow(unused_imports, reason = "for completeness")]
    pub use templates::*;

    #[fixture]
    pub fn jolt_serializer() -> JoltSerializer<Vec<u8>> {
        JoltSerializer::new_vec()
    }

    #[allow(unused_macros, reason = "for completeness")]
    mod templates {
        use super::*;

        #[template]
        #[rstest]
        pub fn all_jolt_versions(
            #[values(
                crate::bolt_version::JoltVersion::V1,
                crate::bolt_version::JoltVersion::V2,
                crate::bolt_version::JoltVersion::V3,
                crate::bolt_version::JoltVersion::V4
            )]
            jolt_version: crate::bolt_version::JoltVersion,
        ) {
            match jolt_version {
                crate::bolt_version::JoltVersion::V1
                | crate::bolt_version::JoltVersion::V2
                | crate::bolt_version::JoltVersion::V3
                | crate::bolt_version::JoltVersion::V4 => {
                    // Just a check for completeness.
                    // If a new version is added, templates need adjusting
                }
            }
        }

        #[template]
        #[rstest]
        pub fn jolt_versions_v1_and_up(
            #[values(
                crate::bolt_version::JoltVersion::V1
                crate::bolt_version::JoltVersion::V2,
                crate::bolt_version::JoltVersion::V3,
                crate::bolt_version::JoltVersion::V4,
            )]
            jolt_version: crate::bolt_version::JoltVersion,
        ) {
        }

        #[template]
        #[rstest]
        pub fn jolt_versions_v2_and_up(
            #[values(
                crate::bolt_version::JoltVersion::V2,
                crate::bolt_version::JoltVersion::V3,
                crate::bolt_version::JoltVersion::V4
            )]
            jolt_version: crate::bolt_version::JoltVersion,
        ) {
        }

        #[template]
        #[rstest]
        pub fn jolt_versions_v3_and_up(
            #[values(
                crate::bolt_version::JoltVersion::V3,
                crate::bolt_version::JoltVersion::V4
            )]
            jolt_version: crate::bolt_version::JoltVersion,
        ) {
        }

        #[template]
        #[rstest]
        pub fn jolt_versions_v4_and_up(
            #[values(crate::bolt_version::JoltVersion::V4)]
            jolt_version: crate::bolt_version::JoltVersion,
        ) {
        }

        #[template]
        #[rstest]
        pub fn jolt_versions_v4_and_down(
            #[values(
                crate::bolt_version::JoltVersion::V4,
                crate::bolt_version::JoltVersion::V3,
                crate::bolt_version::JoltVersion::V2,
                crate::bolt_version::JoltVersion::V1
            )]
            jolt_version: crate::bolt_version::JoltVersion,
        ) {
        }

        #[template]
        #[rstest]
        pub fn jolt_versions_v3_and_down(
            #[values(
                crate::bolt_version::JoltVersion::V3,
                crate::bolt_version::JoltVersion::V2,
                crate::bolt_version::JoltVersion::V1
            )]
            jolt_version: crate::bolt_version::JoltVersion,
        ) {
        }

        #[template]
        #[rstest]
        pub fn jolt_versions_v2_and_down(
            #[values(
                crate::bolt_version::JoltVersion::V1,
                crate::bolt_version::JoltVersion::V2
            )]
            jolt_version: crate::bolt_version::JoltVersion,
        ) {
        }

        #[template]
        #[rstest]
        pub fn jolt_versions_v1_and_down(
            #[values(crate::bolt_version::JoltVersion::V1)]
            jolt_version: crate::bolt_version::JoltVersion,
        ) {
        }
    }
}
