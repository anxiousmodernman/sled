use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Versions {
    pending: Option<(u64, IVec)>,
    versions: Vec<(u64, IVec)>,
}

impl Versions {
    pub(crate) fn apply(&mut self, frag: &Frag, _config: &Config) {
        match frag {
            Frag::PendingVersion(vsn, val) => {
                self.pending = Some((*vsn, val.clone()));
            }
            Frag::CommitVersion(vsn) => {
                if let Some((pending_vsn, val)) = self.pending.take()
                {
                    assert_eq!(pending_vsn, *vsn);
                    self.versions.push((pending_vsn, val));
                } else {
                    panic!("CommitVersion received on Frag without that version pending");
                }
            }
            other => panic!(
                "Versions::apply called on unexpected frag: {:?}",
                other
            ),
        }
    }
}
