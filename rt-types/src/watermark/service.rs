use crate::shop::Shop;
use crate::watermark::WatermarkGroupRepository;
use actix::prelude::*;
use actix_broker::BrokerIssue;
use anyhow::Context as AnyhowContext;
use std::sync::Arc;
use typesafe_repository::GetIdentity;
use typesafe_repository::IdentityOf;

pub struct WatermarkService {
    repo: Arc<dyn WatermarkGroupRepository>,
}

impl WatermarkService {
    pub fn new(repo: Arc<dyn WatermarkGroupRepository>) -> Self {
        Self { repo }
    }
}

impl Actor for WatermarkService {
    type Context = Context<Self>;
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct RenameWatermark {
    pub shop_id: IdentityOf<Shop>,
    pub from: String,
    pub to: String,
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct WatermarkUpdated {
    pub shop_id: IdentityOf<Shop>,
    pub from: String,
    pub to: String,
}

impl Handler<RenameWatermark> for WatermarkService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, msg: RenameWatermark, _: &mut Self::Context) -> Self::Result {
        let RenameWatermark { shop_id, from, to } = msg;
        let from_for_event = from.clone();
        let to_for_event = to.clone();
        let repo = self.repo.clone();

        Box::pin(
            async move {
                let from_path = format!("./watermark/{shop_id}/{from}");
                let to_path = format!("./watermark/{shop_id}/{to}");
                tokio::fs::rename(&from_path, &to_path)
                    .await
                    .with_context(|| {
                        format!("Unable to rename watermark {from_path} -> {to_path}")
                    })?;

                // Update watermark groups that reference this watermark name.
                let groups = repo.list_by(&shop_id).await.unwrap_or_default();
                for mut group in groups {
                    let old_id = group.id();
                    if let Some(opts) = group.elements.remove(&from) {
                        group.elements.insert(to.clone(), opts);
                        let new_id = group.id();
                        if new_id != old_id {
                            let _ = repo.remove(&old_id).await;
                        }
                        let _ = repo.add(group).await;
                    }
                }

                Ok(())
            }
            .into_actor(self)
            .map(move |res, act, _| {
                if res.is_ok() {
                    act.issue_system_async(WatermarkUpdated {
                        shop_id,
                        from: from_for_event.clone(),
                        to: to_for_event.clone(),
                    });
                }
                res
            }),
        )
    }
}
