use zarrs::array::DimensionName;

use crate::remote_hdt::{Domain, ZarrArray};

pub enum Dimension {
    Subject,
    Predicate,
    Object,
}

impl From<Dimension> for DimensionName {
    fn from(value: Dimension) -> Self {
        match value {
            Dimension::Subject => DimensionName::new("Subject"),
            Dimension::Predicate => DimensionName::new("Predicate"),
            Dimension::Object => DimensionName::new("Object"),
        }
    }
}

#[derive(PartialEq)]
pub enum ReferenceSystem {
    SPO,
    SOP,
    PSO,
    POS,
    OSP,
    OPS,
}

impl From<&ReferenceSystem> for String {
    fn from(value: &ReferenceSystem) -> Self {
        match value {
            ReferenceSystem::SPO => String::from("spo"),
            ReferenceSystem::SOP => String::from("sop"),
            ReferenceSystem::PSO => String::from("pso"),
            ReferenceSystem::POS => String::from("pos"),
            ReferenceSystem::OSP => String::from("osp"),
            ReferenceSystem::OPS => String::from("ops"),
        }
    }
}

impl From<String> for ReferenceSystem {
    fn from(value: String) -> Self {
        match value.as_str() {
            "spo" => ReferenceSystem::SPO,
            "sop" => ReferenceSystem::SOP,
            "pso" => ReferenceSystem::PSO,
            "pos" => ReferenceSystem::POS,
            "osp" => ReferenceSystem::OSP,
            "ops" => ReferenceSystem::OPS,
            _ => ReferenceSystem::SPO,
        }
    }
}

impl ReferenceSystem {
    pub(crate) fn shape(&self, domain: &Domain) -> [usize; 3] {
        let shape = match self {
            ReferenceSystem::SPO => [&domain.subjects, &domain.predicates, &domain.objects],
            ReferenceSystem::SOP => [&domain.subjects, &domain.objects, &domain.predicates],
            ReferenceSystem::PSO => [&domain.predicates, &domain.subjects, &domain.objects],
            ReferenceSystem::POS => [&domain.predicates, &domain.objects, &domain.subjects],
            ReferenceSystem::OSP => [&domain.objects, &domain.subjects, &domain.predicates],
            ReferenceSystem::OPS => [&domain.objects, &domain.predicates, &domain.subjects],
        }
        .iter()
        .map(|dimension| dimension.len())
        .collect::<Vec<usize>>();

        [shape[0], shape[1], shape[2]]
    }

    pub(crate) fn shape_u64(&self, domain: &Domain) -> [u64; 3] {
        let shape = self
            .shape(domain)
            .into_iter()
            .map(|dimension| dimension as u64)
            .collect::<Vec<u64>>();

        [shape[0], shape[1], shape[2]]
    }

    pub(crate) fn dimension_names(&self) -> Vec<DimensionName> {
        match self {
            ReferenceSystem::SPO => vec![
                Dimension::Subject.into(),
                Dimension::Predicate.into(),
                Dimension::Object.into(),
            ],
            ReferenceSystem::SOP => vec![
                Dimension::Subject.into(),
                Dimension::Object.into(),
                Dimension::Predicate.into(),
            ],
            ReferenceSystem::PSO => vec![
                Dimension::Predicate.into(),
                Dimension::Subject.into(),
                Dimension::Object.into(),
            ],
            ReferenceSystem::POS => vec![
                Dimension::Predicate.into(),
                Dimension::Object.into(),
                Dimension::Subject.into(),
            ],
            ReferenceSystem::OSP => vec![
                Dimension::Object.into(),
                Dimension::Subject.into(),
                Dimension::Predicate.into(),
            ],
            ReferenceSystem::OPS => vec![
                Dimension::Object.into(),
                Dimension::Predicate.into(),
                Dimension::Subject.into(),
            ],
        }
    }

    pub(crate) fn vec_size(&self, domain: &Domain) -> usize {
        match self {
            ReferenceSystem::SPO => &domain.predicates.len() * &domain.objects.len(),
            ReferenceSystem::SOP => &domain.predicates.len() * &domain.objects.len(),
            ReferenceSystem::PSO => &domain.subjects.len() * &domain.objects.len(),
            ReferenceSystem::POS => &domain.subjects.len() * &domain.objects.len(),
            ReferenceSystem::OSP => &domain.subjects.len() * &domain.predicates.len(),
            ReferenceSystem::OPS => &domain.subjects.len() * &domain.predicates.len(),
        }
    }

    pub(crate) fn chunk_size_u64(&self, domain: &Domain) -> [u64; 3] {
        let domain: [u64; 3] = self.shape_u64(domain);
        [1, domain[1], domain[2]]
    }

    pub(crate) fn convert_to(
        &self,
        other: &ReferenceSystem,
        mut array: ZarrArray,
        domain: &Domain,
    ) -> ZarrArray {
        if self != other {
            // In case the reference system used for serializing the Array is
            // not the same as the one selected by the user when building this
            // struct, we have to reshape the array so it holds to the user's
            // desired mechanism
            let mut v = Vec::<(usize, usize, usize, u8)>::new();

            // Could this be improved using a multithreading approach? If we use
            // rayon the solution would be possibly faster and the implementation
            // details wouldn't vary as much
            // TODO: improve this if possible
            for (i, outer) in array.outer_iter().enumerate() {
                for j in 0..outer.shape()[0] {
                    for k in 0..outer.shape()[1] {
                        // We convert the reference system used in the serialization
                        // format into SPO
                        v.push(match self {
                            ReferenceSystem::SPO => (i, j, k, outer[[j, k]]),
                            ReferenceSystem::SOP => (i, k, j, outer[[j, k]]),
                            ReferenceSystem::PSO => (j, i, k, outer[[j, k]]),
                            ReferenceSystem::POS => (j, k, i, outer[[j, k]]),
                            ReferenceSystem::OSP => (k, i, j, outer[[j, k]]),
                            ReferenceSystem::OPS => (k, j, i, outer[[j, k]]),
                        })
                    }
                }
            }

            let mut reshaped_array = ZarrArray::zeros(other.shape(domain));

            // Same here... Using rayon would be desirable
            for (s, p, o, value) in v {
                match other {
                    ReferenceSystem::SPO => reshaped_array[[s, p, o]] = value,
                    ReferenceSystem::SOP => reshaped_array[[s, o, p]] = value,
                    ReferenceSystem::PSO => reshaped_array[[p, s, o]] = value,
                    ReferenceSystem::POS => reshaped_array[[p, o, s]] = value,
                    ReferenceSystem::OSP => reshaped_array[[o, s, p]] = value,
                    ReferenceSystem::OPS => reshaped_array[[o, p, s]] = value,
                }
            }

            array = reshaped_array;
        }

        array
    }

    pub(crate) fn index(&self, sidx: usize, pidx: usize, oidx: usize, domain: &Domain) -> usize {
        let shape = self.shape(domain);

        let sidx = match self {
            ReferenceSystem::SPO => 0,
            ReferenceSystem::SOP => 0,
            ReferenceSystem::PSO => sidx * shape[2],
            ReferenceSystem::POS => sidx,
            ReferenceSystem::OSP => sidx * shape[2],
            ReferenceSystem::OPS => sidx,
        };

        let pidx = match self {
            ReferenceSystem::SPO => pidx * shape[2],
            ReferenceSystem::SOP => pidx,
            ReferenceSystem::PSO => 0,
            ReferenceSystem::POS => 0,
            ReferenceSystem::OSP => pidx,
            ReferenceSystem::OPS => pidx * shape[2],
        };

        let oidx = match self {
            ReferenceSystem::SPO => oidx,
            ReferenceSystem::SOP => oidx * shape[2],
            ReferenceSystem::PSO => oidx,
            ReferenceSystem::POS => oidx * shape[2],
            ReferenceSystem::OSP => 0,
            ReferenceSystem::OPS => 0,
        };

        sidx + pidx + oidx
    }
}
