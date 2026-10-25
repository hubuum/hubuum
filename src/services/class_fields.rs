use hubuum_computed_fields::MAX_PERSONAL_DEFINITIONS;
use hubuum_domain::{ClassId, PrincipalId};

use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::class_fields::{
    ClassFieldInventory, FieldDiscoveryOptions, FieldDiscoveryTruncation, FieldInventoryBuilder,
    MAX_FIELD_CANDIDATES,
};
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt, SearchOperator, SortParam};
use crate::models::{HubuumClass, HubuumObject, Permissions};
use crate::permissions::visibility::authorize_all_candidates;
use crate::permissions::{AppContext, PrincipalRef};
use crate::services::{catalog, computed_fields, computed_objects};
use crate::traits::{AuthzSubject, scope_allows};

/// Only this module's authorization-aware sampler constructs this evidence.
/// Future profiling must consume the raw authorized sample, not display previews.
struct AuthorizedObjectSample {
    objects: Vec<HubuumObject>,
    truncation: Option<FieldDiscoveryTruncation>,
}

fn sample_query(
    class: ClassId,
    after: Option<i32>,
    limit: usize,
) -> Result<QueryOptions, ApiError> {
    let mut options = QueryOptions::new(
        Vec::new(),
        vec![SortParam::new(FilterField::Id, false)],
        Some(limit),
        None,
        false,
    )?;
    options.filters_mut().add_filter(
        FilterField::ClassId,
        SearchOperator::Equals { is_negated: false },
        &class.id().to_string(),
    )?;
    if let Some(id) = after {
        options.filters_mut().add_filter(
            FilterField::Id,
            SearchOperator::Gt { is_negated: false },
            &id.to_string(),
        )?;
    }
    Ok(options)
}

impl AuthorizedObjectSample {
    async fn load(
        context: &AppContext,
        requestor: &Authenticated,
        class: ClassId,
        limit: usize,
    ) -> Result<Self, ApiError> {
        let principal = &requestor.principal;
        let permissions = [Permissions::ReadCollection, Permissions::ReadObject];
        if !scope_allows(requestor.scopes(), &permissions) {
            return Ok(Self {
                objects: Vec::new(),
                truncation: None,
            });
        }
        let mut objects;
        let mut truncation = None;
        if context
            .permission_backend()
            .supports_storage_visibility_filtering()
        {
            objects = catalog::list_objects(
                context,
                principal.id().id(),
                principal.is_admin(context).await?,
                requestor.scopes(),
                sample_query(class, None, limit + 1)?,
            )
            .await?
            .0;
        } else {
            let principal_ref = PrincipalRef::load(context, principal).await?;
            objects = Vec::new();
            let mut inspected = 0;
            let mut after = None;
            loop {
                let batch_limit = (limit + 1 - objects.len())
                    .clamp(32, 128)
                    .min(MAX_FIELD_CANDIDATES - inspected);
                let candidates = catalog::list_objects(
                    context,
                    principal.id().id(),
                    true,
                    None,
                    sample_query(class, after, batch_limit)?,
                )
                .await?
                .0;
                let count = candidates.len();
                after = candidates.last().map(|object| object.id);
                inspected += count;
                objects.extend(
                    authorize_all_candidates(
                        context.permission_backend(),
                        &principal_ref,
                        candidates,
                        requestor.scopes(),
                        permissions.to_vec(),
                        HubuumObject::authorization_resource,
                    )
                    .await?,
                );
                if objects.len() > limit || count < batch_limit {
                    break;
                }
                if inspected == MAX_FIELD_CANDIDATES {
                    truncation = Some(FieldDiscoveryTruncation::CandidateLimit);
                    break;
                }
            }
        }
        if objects.len() > limit {
            objects.truncate(limit);
            truncation = Some(FieldDiscoveryTruncation::SampleLimit);
        }
        Ok(Self {
            objects,
            truncation,
        })
    }
}

pub(crate) async fn discover(
    context: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
    options: FieldDiscoveryOptions,
) -> Result<ClassFieldInventory, ApiError> {
    let class_id = ClassId::new(class.id)?;
    let personal_owner = requestor
        .principal
        .is_human()
        .then(|| requestor.principal.id().id());
    let mut inventory = FieldInventoryBuilder::new(options);
    for definition in computed_fields::list_shared_definitions(context, class_id).await? {
        if definition.enabled {
            inventory.add_definition(&definition.key, false);
        }
    }
    if let Some(owner) = personal_owner {
        // The per-owner/class invariant bounds the entire set, independently of
        // the deployment's default API page size. No whole-class count is needed.
        let query = QueryOptions::new(
            Vec::new(),
            vec![SortParam::new(FilterField::Id, false)],
            Some(MAX_PERSONAL_DEFINITIONS),
            None,
            false,
        )?;
        let (definitions, _) = computed_fields::list_personal_definitions_page(
            context,
            PrincipalId::new(owner)?,
            Some(class_id),
            query,
        )
        .await?;
        for definition in definitions {
            if definition.enabled {
                inventory.add_definition(&definition.key, true);
            }
        }
    }
    let sample =
        AuthorizedObjectSample::load(context, requestor, class_id, options.limit()).await?;
    if let Some(reason) = sample.truncation {
        inventory.truncate(reason);
    }
    let sampled_objects = sample.objects.len();
    if !inventory.has_computed_definitions() {
        for (index, object) in sample.objects.iter().enumerate() {
            inventory.observe_data(index, &object.data);
        }
        return inventory.finish(sampled_objects);
    }
    for (index, object) in
        computed_objects::enrich_objects_with_computed(context, sample.objects, personal_owner)
            .await?
            .into_iter()
            .enumerate()
    {
        inventory.observe_data(index, &object.object.data);
        let shared = object.computed.shared;
        for (key, value) in shared.values {
            inventory.observe_computed(
                index,
                &key,
                false,
                &value,
                shared.errors.contains_key(&key),
            );
        }
        if let Some(personal) = object.computed.personal {
            for (key, value) in personal.values {
                inventory.observe_computed(
                    index,
                    &key,
                    true,
                    &value,
                    personal.errors.contains_key(&key),
                );
            }
        }
    }
    inventory.finish(sampled_objects)
}
