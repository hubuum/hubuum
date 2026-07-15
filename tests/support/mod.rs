pub mod api_operations;
pub use api_operations::app_context;
pub mod asserts;
#[path = "../../src/tests/constants.rs"]
pub mod constants;

use crate::db::prelude::*;
use actix_web::web;
#[cfg(test)]
use rstest::fixture;

use crate::config::AppConfig;
use crate::db::DbPool;
use crate::db::{init_pool, with_connection};
use crate::errors::ApiError;
use crate::models::collection::{Collection, NewCollectionWithAssignee};
use crate::models::group::{Group, NewGroup};
use crate::models::token::create_principal_token;
use crate::models::user::{NewUser, User};
use crate::models::{HubuumClass, HubuumObject, NewHubuumClass, NewHubuumObject};

use crate::utilities::auth::{generate_random_password, hash_password};

use crate::db::traits::service_account::SaveServiceAccount;
use crate::db::traits::user::CreateUserRecord;
use crate::traits::{CanDelete, CanSave};
use std::sync::LazyLock;
use tokio::sync::{Mutex, MutexGuard};

static TEST_USER_PASSWORD_HASH: LazyLock<String> =
    LazyLock::new(|| hash_password("testpassword").expect("test user password must be hashable"));
static TEST_ADMIN_PASSWORD_HASH: LazyLock<String> = LazyLock::new(|| {
    hash_password("testadminpassword").expect("test admin password must be hashable")
});

pub fn integration_test_config() -> Result<&'static AppConfig, ApiError> {
    crate::test_support::integration_test_config()
}

fn new_test_pool() -> DbPool {
    let config = integration_test_config().unwrap();
    init_pool(&config.database_url, 20)
}

pub type TestMutex = LazyLock<Mutex<()>>;
pub type TestMutexGuard = MutexGuard<'static, ()>;

pub const fn test_mutex() -> TestMutex {
    LazyLock::new(|| Mutex::new(()))
}

pub async fn lock_test_mutex(mutex: &'static TestMutex) -> TestMutexGuard {
    mutex.lock().await
}

#[derive(Clone)]
pub struct CollectionFixture {
    pub pool: web::Data<DbPool>,
    pub collection: Collection,
    pub owner_group: Group,
    pub prefix: String,
}

impl CollectionFixture {
    pub fn collection_id(&self) -> i32 {
        self.collection.id
    }

    pub fn collection_filter(&self) -> String {
        format!("collections={}", self.collection.id)
    }

    pub async fn cleanup(&self) -> Result<(), ApiError> {
        self.collection.delete_without_events(&self.pool).await?;
        self.owner_group.delete_without_events(&self.pool).await?;
        Ok(())
    }

    pub async fn cleanup_all(fixtures: &[CollectionFixture]) -> Result<(), ApiError> {
        for fixture in fixtures {
            fixture.cleanup().await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ClassFixture {
    pub collection: CollectionFixture,
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
        self.collection.cleanup().await
    }
}

#[derive(Clone)]
pub struct ObjectFixture {
    pub collection: CollectionFixture,
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

    pub fn collection_id(&self) -> i32 {
        self.collection.collection.id
    }

    pub async fn cleanup(&self) -> Result<(), ApiError> {
        self.collection.cleanup().await
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

    pub async fn collection_fixture(&self, label: &str) -> CollectionFixture {
        create_collection_fixture(&self.pool, &self.scoped_name(label)).await
    }

    pub async fn collection_fixtures(&self, label: &str, count: usize) -> Vec<CollectionFixture> {
        create_collection_fixtures(&self.pool, &self.scoped_name(label), count).await
    }

    pub async fn with_collection(&self) -> CollectionFixture {
        create_collection_fixture(&self.pool, &self.caller_scoped_name()).await
    }

    pub async fn with_collections(&self, count: usize) -> Vec<CollectionFixture> {
        create_collection_fixtures(&self.pool, &self.caller_scoped_name(), count).await
    }

    pub async fn class_fixture(
        &self,
        label: &str,
        classes: Vec<NewHubuumClass>,
    ) -> Result<ClassFixture, ApiError> {
        let collection = self.collection_fixture(label).await;
        create_class_fixture(&self.pool, collection, classes).await
    }

    pub async fn object_fixture(
        &self,
        label: &str,
        class: NewHubuumClass,
        objects: Vec<NewHubuumObject>,
    ) -> Result<ObjectFixture, ApiError> {
        let collection = self.collection_fixture(label).await;
        create_object_fixture(&self.pool, collection, class, objects).await
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

    pub async fn collection_fixture(&self, label: &str) -> CollectionFixture {
        let fixture = self.scope.collection_fixture(label).await;
        fixture
            .owner_group
            .add_member_without_events(&self.pool, &self.admin_user)
            .await
            .unwrap();
        fixture
    }

    pub async fn collection_fixtures(&self, label: &str, count: usize) -> Vec<CollectionFixture> {
        let fixtures = self.scope.collection_fixtures(label, count).await;

        for fixture in &fixtures {
            fixture
                .owner_group
                .add_member_without_events(&self.pool, &self.admin_user)
                .await
                .unwrap();
        }

        fixtures
    }

    pub async fn with_collection(&self) -> CollectionFixture {
        let fixture = self.scope.with_collection().await;
        fixture
            .owner_group
            .add_member_without_events(&self.pool, &self.admin_user)
            .await
            .unwrap();
        fixture
    }

    pub async fn with_collections(&self, count: usize) -> Vec<CollectionFixture> {
        let fixtures = self.scope.with_collections(count).await;

        for fixture in &fixtures {
            fixture
                .owner_group
                .add_member_without_events(&self.pool, &self.admin_user)
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
        let collection = self.collection_fixture(label).await;
        create_class_fixture(&self.pool, collection, classes).await
    }

    pub async fn object_fixture(
        &self,
        label: &str,
        class: NewHubuumClass,
        objects: Vec<NewHubuumObject>,
    ) -> Result<ObjectFixture, ApiError> {
        let collection = self.collection_fixture(label).await;
        create_object_fixture(&self.pool, collection, class, objects).await
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

async fn create_collection_fixture(pool: &DbPool, label: &str) -> CollectionFixture {
    let prefix = sanitize_fixture_label(label);
    let owner_group = create_groups_with_prefix(pool, &format!("{prefix}_owner"), 1)
        .await
        .remove(0);
    let collection =
        create_collection_for_group(pool, &format!("{prefix}_collection"), owner_group.id)
            .await
            .unwrap();

    CollectionFixture {
        pool: web::Data::new(pool.clone()),
        collection,
        owner_group,
        prefix,
    }
}

async fn create_collection_fixtures(
    pool: &DbPool,
    label: &str,
    count: usize,
) -> Vec<CollectionFixture> {
    let mut fixtures = Vec::with_capacity(count);

    for index in 0..count {
        fixtures.push(create_collection_fixture(pool, &format!("{label}_{index}")).await);
    }

    fixtures
}

pub(crate) async fn create_class_fixture(
    pool: &DbPool,
    collection: CollectionFixture,
    classes: Vec<NewHubuumClass>,
) -> Result<ClassFixture, ApiError> {
    let mut saved_classes = Vec::with_capacity(classes.len());

    for class in classes {
        let class = NewHubuumClass {
            collection_id: collection.collection.id,
            ..class
        };
        saved_classes.push(class.save_without_events(pool).await?);
    }

    Ok(ClassFixture {
        collection,
        classes: saved_classes,
    })
}

/// Create the shared six-class fixture used by the class, object, relation,
/// and export request suites.
pub async fn create_test_classes(context: &TestContext, prefix: &str) -> ClassFixture {
    use self::constants::{SchemaType, get_schema};

    let mut classes = Vec::new();
    for i in 1..7 {
        let schema = if i == 6 {
            get_schema(SchemaType::Geo).clone()
        } else if i > 3 {
            get_schema(SchemaType::Address).clone()
        } else {
            get_schema(SchemaType::Blog).clone()
        };

        classes.push(NewHubuumClass {
            name: format!("{prefix}_api_class_{i}"),
            description: format!("{prefix}_api_description_{i}"),
            collection_id: 0,
            json_schema: Some(schema),
            validate_schema: Some(false),
        });
    }

    create_class_fixture(
        &context.pool,
        context
            .collection_fixture(&format!("{prefix}_api_create_test_classes"))
            .await,
        classes,
    )
    .await
    .unwrap()
}

pub async fn cleanup_test_classes(classes: &ClassFixture) {
    let collection_id = classes.collection.collection.id;
    assert!(
        classes
            .iter()
            .all(|class| class.collection_id == collection_id)
    );
    classes.cleanup().await.unwrap();
}

pub(crate) async fn create_object_fixture(
    pool: &DbPool,
    collection: CollectionFixture,
    class: NewHubuumClass,
    objects: Vec<NewHubuumObject>,
) -> Result<ObjectFixture, ApiError> {
    let class = NewHubuumClass {
        collection_id: collection.collection.id,
        ..class
    }
    .save_without_events(pool)
    .await?;

    let mut saved_objects = Vec::with_capacity(objects.len());
    for object in objects {
        let object = NewHubuumObject {
            collection_id: collection.collection.id,
            hubuum_class_id: class.id,
            ..object
        };
        saved_objects.push(object.save_without_events(pool).await?);
    }

    Ok(ObjectFixture {
        collection,
        class,
        objects: saved_objects,
    })
}

pub async fn create_user_with_params(pool: &DbPool, username: &str, password: &str) -> User {
    let new_user = NewUser {
        identity_scope: None,
        name: username.to_string(),
        password: password.to_string(),
        proper_name: None,
        email: None,
    };
    let result = match password {
        "testpassword" => {
            NewUser {
                password: TEST_USER_PASSWORD_HASH.clone(),
                ..new_user
            }
            .create_user_record_without_events(pool)
            .await
        }
        "testadminpassword" => {
            NewUser {
                password: TEST_ADMIN_PASSWORD_HASH.clone(),
                ..new_user
            }
            .create_user_record_without_events(pool)
            .await
        }
        _ => new_user.save_without_events(pool).await,
    };

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

    let result = admin_group.add_member_without_events(pool, &user).await;

    if result.is_ok() {
        user
    } else {
        panic!("Failed to add user to admin group: {:?}", result.err())
    }
}

/// Create a test service account owned by `owner_group`. `created_by` records the
/// human principal that created it (or `None`).
pub async fn create_test_service_account(
    pool: &DbPool,
    owner_group: &Group,
    created_by: Option<i32>,
) -> crate::models::ServiceAccount {
    let name = "sa-".to_string() + &generate_random_password(16);
    crate::models::NewServiceAccount {
        identity_scope: None,
        name,
        description: Some("test service account".to_string()),
        owner_group_id: owner_group.id,
    }
    .save_without_events(pool, created_by)
    .await
    .expect("failed to create test service account")
}

/// Mint a scoped token for a principal id; returns the raw token string.
pub async fn scoped_token(
    pool: &DbPool,
    principal_id: i32,
    scopes: &[crate::models::Permissions],
) -> String {
    create_principal_token(pool, principal_id, None, None, None, Some(scopes), None)
        .await
        .expect("failed to mint scoped token")
        .get_token()
}

/// Mint a token for a service account with optional scopes and expiry; returns
/// the raw token string.
pub async fn service_account_token(
    pool: &DbPool,
    sa: &crate::models::ServiceAccount,
    scopes: Option<&[crate::models::Permissions]>,
    expires_at: Option<chrono::NaiveDateTime>,
) -> String {
    create_principal_token(pool, sa.id, None, None, expires_at, scopes, None)
        .await
        .expect("failed to mint service account token")
        .get_token()
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
            identity_scope: None,
            groupname: groupname.to_string(),
            description: Some(groupname.clone()),
        }
        .save_without_events(pool)
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
    if let Ok(user) = User::get_by_name(pool, uname).await {
        return user;
    }

    let result = NewUser {
        identity_scope: None,
        name: uname.to_string(),
        password: "testpassword".to_string(),
        proper_name: None,
        email: None,
    }
    .save_without_events(pool)
    .await;

    if let Err(e) = result {
        match e {
            ApiError::Conflict(_) => {
                return User::get_by_name(pool, uname)
                    .await
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

    let _ = admin_group.add_member_without_events(pool, &user).await;

    user
}

pub async fn ensure_normal_user(pool: &DbPool) -> User {
    ensure_user(pool, "normal").await
}

pub async fn ensure_admin_group(pool: &DbPool) -> Group {
    use crate::schema::groups::dsl::*;
    let admin_groupname = crate::test_support::integration_test_config()
        .map(|config| config.admin_groupname.clone())
        .unwrap_or_else(|_| "admin".to_string());

    let result = with_connection(pool, async |conn| {
        groups
            .filter(groupname.eq(&admin_groupname))
            .first::<Group>(conn)
            .await
    })
    .await;

    if let Ok(group) = result {
        return group;
    }

    let result = NewGroup {
        identity_scope: None,
        groupname: admin_groupname.clone(),
        description: Some("Admin group".to_string()),
    }
    .save_without_events(pool)
    .await;

    if let Err(e) = result {
        match e {
            ApiError::Conflict(_) => {
                return with_connection(pool, async |conn| {
                    groups
                        .filter(groupname.eq(&admin_groupname))
                        .first::<Group>(conn)
                        .await
                })
                .await
                .expect("Failed to fetch user after conflict");
            }
            _ => panic!("Failed to create admin group: {e:?}"),
        }
    }

    result.unwrap()
}

pub async fn get_pool_and_config() -> (DbPool, AppConfig) {
    let config = crate::test_support::integration_test_config().unwrap();
    let pool = new_test_pool();

    (pool, config.clone())
}

pub async fn setup_pool_and_tokens() -> (DbPool, String, String) {
    let pool = new_test_pool();
    let admin_user = ensure_admin_user(&pool).await;
    let admin_token = admin_user.create_token(&pool).await.unwrap().get_token();
    let normal_user = ensure_normal_user(&pool).await;
    let normal_token = normal_user.create_token(&pool).await.unwrap().get_token();

    (pool, admin_token, normal_token)
}

pub fn get_test_pool() -> web::Data<DbPool> {
    web::Data::new(new_test_pool())
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

async fn create_collection_for_group(
    pool: &DbPool,
    collection_name: &str,
    group_id: i32,
) -> Result<Collection, ApiError> {
    NewCollectionWithAssignee {
        name: collection_name.to_string(),
        description: "Test collection".to_string(),
        group_id,
        parent_collection_id: None,
    }
    .save_without_events(pool)
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
