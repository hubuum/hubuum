use super::*;

async fn enqueue(
    fixture: &Fixture,
    tickets: &[TaskAdmissionTicket],
) -> Vec<tokio::task::JoinHandle<Result<TaskActivationOutcome, ApiError>>> {
    let requests = tickets
        .iter()
        .cloned()
        .map(|ticket| {
            let service = fixture.service.clone();
            let storage = fixture.storage.clone();
            actix_rt::spawn(async move { service.protect_admission(&storage, ticket).await })
        })
        .collect();
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture
            .service
            .configuration()
            .unwrap()
            .admissions
            .lock()
            .await
            .len()
            != tickets.len()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("requests must reach the publication queue");
    requests
}

async fn tickets(fixture: &Fixture, count: usize) -> Vec<TaskAdmissionTicket> {
    let mut tickets = Vec::new();
    for index in 0..count {
        tickets.push(
            fixture
                .admit(&format!("batch-{index}"), StorageTaskKind::Import)
                .await,
        );
    }
    tickets
}

#[rstest::rstest]
#[case(1)]
#[case(32)]
#[case(65)]
#[actix_web::test]
async fn concurrent_admissions_complete_across_bounded_batches(#[case] count: usize) {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let tickets = tickets(&fixture, count).await;
    let publication = fixture
        .service
        .configuration()
        .unwrap()
        .publication
        .lock()
        .await;
    let requests = enqueue(&fixture, &tickets).await;
    drop(publication);

    tokio::time::timeout(Duration::from_secs(10), async {
        for request in requests {
            assert!(matches!(
                request.await.unwrap().unwrap(),
                TaskActivationOutcome::Activated(_)
            ));
        }
    })
    .await
    .expect("every waiter must receive its own activation result");
    let checkpoint = fixture.checkpoint();
    let protected = checkpoint.task_control().unwrap();
    for ticket in tickets {
        assert_eq!(
            protected
                .receipt(ticket.receipt().submission())
                .unwrap()
                .ticket(),
            &ticket
        );
    }
}

#[actix_web::test]
async fn duplicate_batch_waiters_activate_one_exact_generation_once() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let ticket = fixture
        .admit("same-generation", StorageTaskKind::Import)
        .await;
    let publication = fixture
        .service
        .configuration()
        .unwrap()
        .publication
        .lock()
        .await;
    let requests = enqueue(&fixture, &[ticket.clone(), ticket]).await;
    drop(publication);
    let outcomes = join_all(requests)
        .await
        .into_iter()
        .map(|result| result.unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, TaskActivationOutcome::Activated(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, TaskActivationOutcome::AlreadyActive(_)))
            .count(),
        1
    );
}

#[actix_web::test]
async fn cancelled_waiter_leaves_a_recoverable_pending_admission() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let tickets = tickets(&fixture, 2).await;
    let publication = fixture
        .service
        .configuration()
        .unwrap()
        .publication
        .lock()
        .await;
    let mut requests = enqueue(&fixture, &tickets).await;
    let cancelled = requests.remove(0);
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    drop(publication);
    assert!(matches!(
        requests.remove(0).await.unwrap().unwrap(),
        TaskActivationOutcome::Activated(_)
    ));
    assert_eq!(
        fixture
            .storage
            .list_pending_task_admissions(8)
            .await
            .unwrap(),
        vec![tickets[0].clone()]
    );

    fixture
        .service
        .sweep_task_retention(&fixture.storage)
        .await
        .unwrap();
    assert!(
        fixture
            .storage
            .list_pending_task_admissions(8)
            .await
            .unwrap()
            .is_empty()
    );
}

#[actix_web::test]
async fn failed_batch_verification_rejects_every_waiter_without_activating_tasks() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let tickets = tickets(&fixture, 4).await;
    let publication = fixture
        .service
        .configuration()
        .unwrap()
        .publication
        .lock()
        .await;
    let requests = enqueue(&fixture, &tickets).await;
    fs::write(fixture.root.join("key"), [72; 32]).unwrap();
    drop(publication);

    tokio::time::timeout(Duration::from_secs(5), async {
        for request in requests {
            assert!(request.await.unwrap().is_err());
        }
    })
    .await
    .expect("verification failures must reach every waiter");
    assert_eq!(
        fixture
            .storage
            .list_pending_task_admissions(8)
            .await
            .unwrap()
            .len(),
        tickets.len()
    );
    fs::write(fixture.root.join("key"), [71; 32]).unwrap();
    for ticket in tickets {
        assert!(matches!(
            fixture
                .service
                .protect_admission(&fixture.storage, ticket)
                .await
                .unwrap(),
            TaskActivationOutcome::Activated(_)
        ));
    }
}

#[actix_web::test]
async fn an_unrelated_ticket_cannot_activate_in_another_admissions_batch() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let unrelated = Fixture::new(None, TaskPurgeBudget::default()).await;
    let valid = fixture.admit("valid", StorageTaskKind::Import).await;
    let invalid = unrelated.admit("unrelated", StorageTaskKind::Import).await;
    let publication = fixture
        .service
        .configuration()
        .unwrap()
        .publication
        .lock()
        .await;
    let mut requests = enqueue(&fixture, &[invalid.clone(), valid]).await;
    drop(publication);

    assert!(requests.remove(0).await.unwrap().is_err());
    assert!(matches!(
        requests.remove(0).await.unwrap().unwrap(),
        TaskActivationOutcome::Activated(_)
    ));
    assert_eq!(
        unrelated
            .storage
            .list_pending_task_admissions(8)
            .await
            .unwrap(),
        vec![invalid]
    );
}

#[actix_web::test]
async fn a_batch_never_uses_one_storage_handles_checkpoint_for_another_handle() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let unrelated = Fixture::new(None, TaskPurgeBudget::default()).await;
    let configuration = fixture.service.configuration().unwrap();
    let _publication = configuration.publication.lock().await;
    let (first, received) = oneshot::channel();
    let (second, mut deferred) = oneshot::channel();
    configuration.admissions.lock().await.extend([
        WaitingAdmission {
            storage: fixture.storage.clone(),
            ticket: fixture.admit("first", StorageTaskKind::Import).await,
            result: first,
        },
        WaitingAdmission {
            storage: unrelated.storage.clone(),
            ticket: unrelated.admit("second", StorageTaskKind::Import).await,
            result: second,
        },
    ]);

    protect_waiting_admissions(configuration).await;

    assert!(matches!(
        received.await.unwrap().unwrap(),
        TaskActivationOutcome::Activated(_)
    ));
    assert!(matches!(
        deferred.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(
        unrelated
            .storage
            .list_pending_task_admissions(8)
            .await
            .unwrap()
            .len(),
        1
    );
}
