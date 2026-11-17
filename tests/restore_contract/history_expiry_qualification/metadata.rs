//! Wide native reference paths, with narrow current/open restore exposure.
use super::*;
use diesel::sql_types::Integer;
use hubuum_storage_postgres::with_transaction;

#[derive(QueryableByName)]
struct Created {
    #[diesel(sql_type = Integer)]
    id: i32,
}

pub(super) async fn source(pool: &PostgresPool, collection: i32, updates: usize) {
    with_transaction(pool, async |connection| {
        let template = diesel::sql_query("INSERT INTO public.export_templates(collection_id,name,description,content_type,template,kind) VALUES ($1,$2,'initial','text/plain','fixture','fragment') RETURNING id")
            .bind::<Integer,_>(collection).bind::<Text,_>(format!("metadata_source_{}", Uuid::new_v4()))
            .get_result::<Created>(connection).await?;
        for update in 0..updates {
            diesel::sql_query("UPDATE public.export_templates SET description=$1 WHERE id=$2")
                .bind::<Text,_>(update.to_string()).bind::<Integer,_>(template.id).execute(connection).await?;
        }
        Ok::<_, hubuum_storage_postgres::PostgresStorageError>(())
    }).await.unwrap();
}

pub(super) async fn filler(pool: &PostgresPool, collection: i32) {
    with_transaction(pool, async |connection| {
        let class = diesel::sql_query("INSERT INTO public.hubuumclass(collection_id,name,description) VALUES ($1,$2,'metadata reference') RETURNING id")
            .bind::<Integer,_>(collection).bind::<Text,_>(format!("metadata_class_{}", Uuid::new_v4()))
            .get_result::<Created>(connection).await?;
        // The ordinary identity trigger captures the JSON-pointer alias and
        // known class UUID. No imported identities or fabricated versions.
        let template = diesel::sql_query("INSERT INTO public.export_templates(collection_id,name,description,content_type,template,kind,include) VALUES ($1,$2,'initial','text/plain','fixture','fragment',jsonb_build_object('related_objects',jsonb_build_object(repeat('x',131072),jsonb_build_object('class_id',$3)))) RETURNING id")
            .bind::<Integer,_>(collection).bind::<Text,_>(format!("metadata_filler_{}", Uuid::new_v4()))
            .bind::<Integer,_>(class.id).get_result::<Created>(connection).await?;
        diesel::sql_query(format!("DO $fixture$ BEGIN FOR n IN 1..512 LOOP UPDATE public.export_templates SET description=n::text WHERE id={}; END LOOP; END $fixture$", template.id)).execute(connection).await?;
        // Close the final wide identity while leaving current/open evidence
        // narrow enough for the unchanged recovery exposure budget.
        diesel::sql_query("UPDATE public.export_templates SET include=NULL WHERE id=$1")
            .bind::<Integer,_>(template.id).execute(connection).await?;
        Ok::<_, hubuum_storage_postgres::PostgresStorageError>(())
    }).await.unwrap();
}

pub(super) async fn inventory_filler(pool: &PostgresPool, collection: i32) {
    with_transaction(pool, async |connection| {
        let class = diesel::sql_query("INSERT INTO public.hubuumclass(collection_id,name,description) VALUES ($1,$2,'inventory reference') RETURNING id")
            .bind::<Integer,_>(collection).bind::<Text,_>(format!("inventory_class_{}", Uuid::new_v4()))
            .get_result::<Created>(connection).await?;
        diesel::sql_query("INSERT INTO public.export_templates(collection_id,name,description,content_type,template,kind,include) SELECT $1,$2||n::text,'initial','text/plain','fixture','fragment',jsonb_build_object('related_objects',jsonb_build_object(repeat('x',131072),jsonb_build_object('class_id',$3))) FROM generate_series(1,16) n")
            .bind::<Integer,_>(collection).bind::<Text,_>(format!("inventory_filler_{}_", Uuid::new_v4()))
            .bind::<Integer,_>(class.id).execute(connection).await?;
        // Keep both current and open history wide; recovery must validate more
        // than one metadata page without dropping the held closed versions.
        diesel::sql_query("UPDATE public.export_templates SET description='wide current' WHERE collection_id=$1")
            .bind::<Integer,_>(collection).execute(connection).await?;
        Ok::<_, hubuum_storage_postgres::PostgresStorageError>(())
    }).await.unwrap();
}
