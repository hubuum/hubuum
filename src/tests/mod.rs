#![allow(dead_code)]
// We allow dead code here because all of this is used in tests and it is
// thus marked as dead. Doh.

pub mod acl;
pub mod api;
pub mod api_operations;
pub mod asserts;
pub mod client_allowlist;
pub mod constants;
pub mod search;
pub mod validation;

use actix_web::web;
use diesel::prelude::*;
#[cfg(test)]
use rstest::fixture;

use crate::config::{AppConfig, get_config};
use crate::db::DbPool;
use crate::db::{init_pool, with_connection};
use crate::errors::ApiError;
use crate::models::group::{Group, NewGroup};
use crate::models::namespace::{Namespace, NewNamespaceWithAssignee};
use crate::models::user::{NewUser, User};
use crate::models::{HubuumClass, HubuumObject, NewHubuumClass, NewHubuumObject};

use crate::utilities::auth::generate_random_password;

use crate::traits::{CanDelete, CanSave};
use once_cell::sync::Lazy;

static POOL: Lazy<DbPool> = Lazy::new(|| {
    let config = get_config().unwrap();
    init_pool(&config.database_url, 20)
});

#[derive(Clone)]
pub struct NamespaceFixture {
    pub pool: web::Data<DbPool>,
    pub namespace: Namespace,
    pub owner_group: Group,
    pub prefix: String,
}

impl NamespaceFixture {
    pub fn namespace_id(&self) -> i32 {
        self.namespace.id
    }

    pub fn namespace_filter(&self) -> String {
        format!("namespaces={}", self.namespace.id)
    }

    pub async fn cleanup(&self) -> Result<(), ApiError> {
        self.namespace.delete(&self.pool).await?;
        self.owner_group.delete(&self.pool).await?;
        Ok(())
    }

    pub async fn cleanup_all(fixtures: &[NamespaceFixture]) -> Result<(), ApiError> {
        for fixture in fixtures {
            fixture.cleanup().await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ClassFixture {
    pub namespace: NamespaceFixture,
    pub classes: Vec<HubuumClass>,
}

impl std::ops::Deref for ClassFixture {
    type Target = [HubuumClass];

    fn deref(&self) -> &Self::Target {
        &self.classes
    }
}

impl<'a> IntoIterator for &'a ClassFixture {
    type Item = &'a HubuumClass;
    type IntoIter = std::slice::Iter<'a, HubuumClass>;

    fn into_iter(self) -> Self::IntoIter {
        self.classes.iter()
    }
}

impl ClassFixture {
    pub async fn cleanup(&self) -> Result<(), ApiError> {
        self.namespace.cleanup().await
    }
}

#[derive(Clone)]
pub struct ObjectFixture {
    pub namespace: NamespaceFixture,
    pub class: HubuumClass,
    pub objects: Vec<HubuumObject>,
}

impl std::ops::Deref for ObjectFixture {
    type Target = [HubuumObject];

    fn deref(&self) -> &Self::Target {
        &self.objects
    }
}

impl<'a> IntoIterator for &'a ObjectFixture {
    type Item = &'a HubuumObject;
    type IntoIter = std::slice::Iter<'a, HubuumObject>;

    fn into_iter(self) -> Self::IntoIter {
        self.objects.iter()
    }
}

impl ObjectFixture {
    pub fn class_id(&self) -> i32 {
        self.class.id
    }

    pub fn namespace_id(&self) -> i32 {
        self.namespace.namespace.id
    }

    pub async fn cleanup(&self) -> Result<(), ApiError> {
        self.namespace.cleanup().await
    }
}

#[derive(Clone)]
pub struct TestScope {
    pub pool: web::Data<DbPool>,
    scope_id: String,
}

impl TestScope {
    pub fn new() -> Self {
        Self {
            pool: get_test_pool(),
            scope_id: generate_random_password(12).to_ascii_lowercase(),
        }
    }

    pub fn scoped_name(&self, label: &str) -> String {
        format!("{}_{}", sanitize_fixture_label(label), self.scope_id)
    }

    #[track_caller]
    fn caller_scoped_name(&self) -> String {
        let location = std::panic::Location::caller();
        let file = location
            .file()
            .rsplit('/')
            .next()
            .unwrap_or("test")
            .trim_end_matches(".rs");
        self.scoped_name(&format!("{file}_line_{}", location.line()))
    }

    pub async fn namespace_fixture(&self, label: &str) -> NamespaceFixture {
        create_namespace_fixture(&self.pool, &self.scoped_name(label)).await
    }

    pub async fn namespace_fixtures(&self, label: &str, count: usize) -> Vec<NamespaceFixture> {
        create_namespace_fixtures(&self.pool, &self.scoped_name(label), count).await
    }

    pub async fn with_namespace(&self) -> NamespaceFixture {
        create_namespace_fixture(&self.pool, &self.caller_scoped_name()).await
    }

    pub async fn with_namespaces(&self, count: usize) -> Vec<NamespaceFixture> {
        create_namespace_fixtures(&self.pool, &self.caller_scoped_name(), count).await
    }

    pub async fn class_fixture(
        &self,
        label: &str,
        classes: Vec<NewHubuumClass>,
    ) -> Result<ClassFixture, ApiError> {
        let namespace = self.namespace_fixture(label).await;
        create_class_fixture(&self.pool, namespace, classes).await
    }

    pub async fn object_fixture(
        &self,
        label: &str,
        class: NewHubuumClass,
        objects: Vec<NewHubuumObject>,
    ) -> Result<ObjectFixture, ApiError> {
        let namespace = self.namespace_fixture(label).await;
        create_object_fixture(&self.pool, namespace, class, objects).await
    }
}

impl Default for TestScope {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct TestContext {
    pub pool: web::Data<DbPool>,
    pub admin_user: User,
    pub admin_token: String,
    pub normal_user: User,
    pub normal_token: String,
    pub scope: TestScope,
}

impl TestContext {
    pub async fn new() -> Self {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let admin_user = create_test_admin(&pool).await;
        let admin_token = admin_user.create_token(&pool).await.unwrap().get_token();
        let normal_user = create_test_user(&pool).await;
        let normal_token = normal_user.create_token(&pool).await.unwrap().get_token();

        Self {
            pool,
            admin_user,
            admin_token,
            normal_user,
            normal_token,
            scope,
        }
    }

    pub fn scoped_name(&self, label: &str) -> String {
        self.scope.scoped_name(label)
    }

    pub async fn namespace_fixture(&self, label: &str) -> NamespaceFixture {
        let fixture = self.scope.namespace_fixture(label).await;
        fixture
            .owner_group
            .add_member(&self.pool, &self.admin_user)
            .await
            .unwrap();
        fixture
    }

    pub async fn namespace_fixtures(&self, label: &str, count: usize) -> Vec<NamespaceFixture> {
        let fixtures = self.scope.namespace_fixtures(label, count).await;

        for fixture in &fixtures {
            fixture
                .owner_group
                .add_member(&self.pool, &self.admin_user)
                .await
                .unwrap();
        }

        fixtures
    }

    pub async fn with_namespace(&self) -> NamespaceFixture {
        let fixture = self.scope.with_namespace().await;
        fixture
            .owner_group
            .add_member(&self.pool, &self.admin_user)
            .await
            .unwrap();
        fixture
    }

    pub async fn with_namespaces(&self, count: usize) -> Vec<NamespaceFixture> {
        let fixtures = self.scope.with_namespaces(count).await;

        for fixture in &fixtures {
            fixture
                .owner_group
                .add_member(&self.pool, &self.admin_user)
                .await
                .unwrap();
        }

        fixtures
    }

    pub async fn class_fixture(
        &self,
        label: &str,
        classes: Vec<NewHubuumClass>,
    ) -> Result<ClassFixture, ApiError> {
        let namespace = self.namespace_fixture(label).await;
        create_class_fixture(&self.pool, namespace, classes).await
    }

    pub async fn object_fixture(
        &self,
        label: &str,
        class: NewHubuumClass,
        objects: Vec<NewHubuumObject>,
    ) -> Result<ObjectFixture, ApiError> {
        let namespace = self.namespace_fixture(label).await;
        create_object_fixture(&self.pool, namespace, class, objects).await
    }
}

fn sanitize_fixture_label(label: &str) -> String {
    let sanitized = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();

    if sanitized.is_empty() {
        "fixture".to_string()
    } else {
        sanitized
    }
}

async fn create_namespace_fixture(pool: &DbPool, label: &str) -> NamespaceFixture {
    let prefix = sanitize_fixture_label(label);
    let owner_group = create_groups_with_prefix(pool, &format!("{prefix}_owner"), 1)
        .await
        .remove(0);
    let namespace =
        create_namespace_for_group(pool, &format!("{prefix}_namespace"), owner_group.id)
            .await
            .unwrap();

    NamespaceFixture {
        pool: web::Data::new(pool.clone()),
        namespace,
        owner_group,
        prefix,
    }
}

async fn create_namespace_fixtures(
    pool: &DbPool,
    label: &str,
    count: usize,
) -> Vec<NamespaceFixture> {
    let mut fixtures = Vec::with_capacity(count);

    for index in 0..count {
        fixtures.push(create_namespace_fixture(pool, &format!("{label}_{index}")).await);
    }

    fixtures
}

pub(crate) async fn create_class_fixture(
    pool: &DbPool,
    namespace: NamespaceFixture,
    classes: Vec<NewHubuumClass>,
) -> Result<ClassFixture, ApiError> {
    let mut saved_classes = Vec::with_capacity(classes.len());

    for class in classes {
        let class = NewHubuumClass {
            namespace_id: namespace.namespace.id,
            ..class
        };
        saved_classes.push(class.save(pool).await?);
    }

    Ok(ClassFixture {
        namespace,
        classes: saved_classes,
    })
}

pub(crate) async fn create_object_fixture(
    pool: &DbPool,
    namespace: NamespaceFixture,
    class: NewHubuumClass,
    objects: Vec<NewHubuumObject>,
) -> Result<ObjectFixture, ApiError> {
    let class = NewHubuumClass {
        namespace_id: namespace.namespace.id,
        ..class
    }
    .save(pool)
    .await?;

    let mut saved_objects = Vec::with_capacity(objects.len());
    for object in objects {
        let object = NewHubuumObject {
            namespace_id: namespace.namespace.id,
            hubuum_class_id: class.id,
            ..object
        };
        saved_objects.push(object.save(pool).await?);
    }

    Ok(ObjectFixture {
        namespace,
        class,
        objects: saved_objects,
    })
}

pub async fn create_user_with_params(pool: &DbPool, username: &str, password: &str) -> User {
    let result = NewUser {
        username: username.to_string(),
        password: password.to_string(),
        email: None,
    }
    .save(pool)
    .await;

    assert!(
        result.is_ok(),
        "Failed to create user: {:?}",
        result.err().unwrap()
    );

    result.unwrap()
}

/// Create a test user with a random username
pub async fn create_test_user(pool: &DbPool) -> User {
    let username = "user".to_string() + &generate_random_password(16);
    create_user_with_params(pool, &username, "testpassword").await
}

/// Create a test admin user with a random username.
///
/// The user will be added to the admin group.
pub async fn create_test_admin(pool: &DbPool) -> User {
    let username = "admin".to_string() + &generate_random_password(16);
    let user = create_user_with_params(pool, &username, "testadminpassword").await;
    let admin_group = ensure_admin_group(pool).await;

    let result = admin_group.add_member(pool, &user).await;

    if result.is_ok() {
        user
    } else {
        panic!("Failed to add user to admin group: {:?}", result.err())
    }
}

/// Create a test group with a random name
pub async fn create_test_group(pool: &DbPool) -> Group {
    create_groups_with_prefix(pool, &generate_random_password(16), 1)
        .await
        .remove(0)
}

pub async fn create_groups_with_prefix(
    pool: &DbPool,
    prefix: &str,
    num_groups: usize,
) -> Vec<Group> {
    let mut groups = Vec::new();

    for i in 0..num_groups {
        let groupname = format!("{prefix}-group-{i}");
        let result = NewGroup {
            groupname: groupname.to_string(),
            description: Some(groupname.clone()),
        }
        .save(pool)
        .await;

        assert!(
            result.is_ok(),
            "Failed to create group: {:?}",
            result.err().unwrap()
        );

        groups.push(result.unwrap());
    }

    groups
}

pub async fn ensure_user(pool: &DbPool, uname: &str) -> User {
    use crate::schema::users::dsl::*;

    let result = with_connection(pool, |conn| {
        users.filter(username.eq(uname)).first::<User>(conn)
    });

    if let Ok(user) = result {
        return user;
    }

    let result = NewUser {
        username: uname.to_string(),
        password: "testpassword".to_string(),
        email: None,
    }
    .save(pool)
    .await;

    if let Err(e) = result {
        match e {
            ApiError::Conflict(_) => {
                return with_connection(pool, |conn| {
                    users.filter(username.eq(uname)).first::<User>(conn)
                })
                .expect("Failed to fetch user after conflict");
            }
            _ => panic!("Failed to create user '{uname}': {e:?}"),
        }
    }

    result.unwrap()
}

pub async fn ensure_admin_user(pool: &DbPool) -> User {
    let user = ensure_user(pool, "admin").await;

    let admin_group = ensure_admin_group(pool).await;

    let _ = admin_group.add_member(pool, &user).await;

    user
}

pub async fn ensure_normal_user(pool: &DbPool) -> User {
    ensure_user(pool, "normal").await
}

pub async fn ensure_admin_group(pool: &DbPool) -> Group {
    use crate::schema::groups::dsl::*;

    let result = with_connection(pool, |conn| {
        groups.filter(groupname.eq("admin")).first::<Group>(conn)
    });

    if let Ok(group) = result {
        return group;
    }

    let result = NewGroup {
        groupname: "admin".to_string(),
        description: Some("Admin group".to_string()),
    }
    .save(pool)
    .await;

    if let Err(e) = result {
        match e {
            ApiError::Conflict(_) => {
                return with_connection(pool, |conn| {
                    groups.filter(groupname.eq("admin")).first::<Group>(conn)
                })
                .expect("Failed to fetch user after conflict");
            }
            _ => panic!("Failed to create admin group: {e:?}"),
        }
    }

    result.unwrap()
}

pub async fn get_pool_and_config() -> (DbPool, AppConfig) {
    let config = get_config().unwrap();
    let pool = POOL.clone();

    (pool, config.clone())
}

pub async fn setup_pool_and_tokens() -> (DbPool, String, String) {
    let pool = POOL.clone();
    let admin_user = ensure_admin_user(&pool).await;
    let admin_token = admin_user.create_token(&pool).await.unwrap().get_token();
    let normal_user = ensure_normal_user(&pool).await;
    let normal_token = normal_user.create_token(&pool).await.unwrap().get_token();

    (pool, admin_token, normal_token)
}

pub fn get_test_pool() -> web::Data<DbPool> {
    web::Data::new(POOL.clone())
}

#[cfg(test)]
#[fixture]
pub async fn test_context() -> TestContext {
    TestContext::new().await
}

#[cfg(test)]
#[fixture]
pub fn test_scope() -> TestScope {
    TestScope::new()
}

async fn create_namespace_for_group(
    pool: &DbPool,
    ns_name: &str,
    group_id: i32,
) -> Result<Namespace, ApiError> {
    NewNamespaceWithAssignee {
        name: ns_name.to_string(),
        description: "Test namespace".to_string(),
        group_id,
    }
    .save(pool)
    .await
}

pub fn generate_all_subsets<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    let num_items = items.len();
    let num_subsets = 2usize.pow(num_items as u32);
    let mut subsets: Vec<Vec<T>> = Vec::with_capacity(num_subsets);

    // Iterate over each possible subset
    for subset_index in 0..num_subsets {
        let mut current_subset: Vec<T> = Vec::new();

        // Determine which items are in the current subset
        for (offset, item) in items.iter().enumerate() {
            if subset_index & (1 << offset) != 0 {
                current_subset.push(item.clone());
            }
        }

        subsets.push(current_subset);
    }

    subsets
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::{models::namespace::UpdateNamespace, traits::CanUpdate};

    #[actix_rt::test]
    async fn test_updated_and_created_at() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let namespace = scope.namespace_fixture("test_updated_at").await;
        let original_updated_at = namespace.namespace.updated_at;
        let original_created_at = namespace.namespace.created_at;

        let update = UpdateNamespace {
            name: Some("test update 2".to_string()),
            description: None,
        };

        let updated_namespace = update.update(&pool, namespace.namespace.id).await.unwrap();
        let new_created_at = updated_namespace.created_at;
        let new_updated_at = updated_namespace.updated_at;

        assert_eq!(updated_namespace.id, namespace.namespace.id);
        assert_eq!(updated_namespace.name, "test update 2");
        assert_eq!(original_created_at, new_created_at);
        assert_ne!(original_updated_at, new_updated_at);
        assert!(new_updated_at > original_updated_at);

        namespace.cleanup().await.unwrap();
    }
}
