// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Offset identity across a crash.
//!
//! `SendMessagesResponse::confirmations` hands clients concrete base offsets,
//! which makes offset reuse client-visible: a client that recorded offset N for
//! its message must never see the server confirm a DIFFERENT message at N
//! later. A solo node acks below the flush thresholds from RAM only, so nothing
//! in the segments says those offsets were ever handed out. What keeps them
//! from being re-minted is the offset RESERVATION in the partition superblock,
//! claimed by the append fence before any of them exist and read back by boot.

use std::path::Path;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "offset-reuse-stream";
const TOPIC_NAME: &str = "offset-reuse-topic";
const PARTITION_ID: u32 = 0;
/// Confirmed sends before the crash; small enough to stay far below the
/// 1024-message / 1 MiB flush thresholds, so nothing reaches the segments.
const PRE_CRASH_SENDS: u32 = 5;

/// Bounds the restarted node's boot and its consensus groups settling.
const SERVE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Send `count` single-message batches, returning each confirmed base offset.
async fn produce_acked(client: &IggyClient, payload_prefix: &str, count: u32) -> Vec<u64> {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let mut acked = Vec::with_capacity(count as usize);
    for index in 0..count {
        let payload = format!("{payload_prefix}-{index:03}");
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload.clone().into())
                .build()
                .expect("build message"),
        ];
        let response = client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap_or_else(|error| panic!("send {payload}: {error}"));
        acked.push(
            response
                .confirmations
                .first()
                .unwrap_or_else(|| panic!("the VSR server confirms every send, none for {payload}"))
                .base_offset,
        );
    }
    acked
}

/// Poll until the restarted node serves the pre-crash stream again, returning
/// a connected root client. Panics at the deadline.
async fn wait_until_serving(harness: &TestHarness, budget: Duration) -> IggyClient {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Ok(builder) = harness.node(0).tcp_client()
            && let Ok(client) = builder.with_root_login().connect().await
            && matches!(client.get_stream(&stream).await, Ok(Some(_)))
        {
            return client;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the restarted node did not serve the stream within {budget:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Create the stream and its single-partition topic.
async fn create_topic(client: &IggyClient, messages_required_to_save: Option<u32>) {
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
}

/// Base offsets of every segment file under `root`, from the file names, which
/// are the on-disk claim about where each range begins.
///
/// Reading them is the only way to assert the shape the re-anchor produces:
/// recovery tolerates a discontiguity inside a segment whenever the index
/// survives, so a black-box offset assertion passes either way and the wrong
/// shape sits there until an index is torn.
fn segment_base_offsets(root: &Path) -> Vec<u64> {
    let mut offsets = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|extension| extension == "log")
                && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
                && let Ok(offset) = stem.parse::<u64>()
            {
                offsets.push(offset);
            }
        }
    }
    offsets.sort_unstable();
    offsets
}

/// Kill the node, bring it back, and return a client onto the restarted one.
async fn crash_and_recover(harness: &mut TestHarness) -> IggyClient {
    harness.kill_node(0).expect("SIGKILL the only node");
    harness.restart_node(0).expect("restart it");
    wait_until_serving(harness, SERVE_TIMEOUT).await
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_confirmed_sends_below_flush_threshold_when_a_solo_node_is_killed_should_not_remint_offsets(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");

    let acked = produce_acked(&client, "pre-crash", PRE_CRASH_SENDS).await;
    let highest_confirmed = *acked.last().expect("confirmed sends");
    drop(client);

    harness.kill_node(0).expect("SIGKILL the only node");
    harness.restart_node(0).expect("restart it");

    let client = wait_until_serving(harness, SERVE_TIMEOUT).await;
    let post_crash_offset = produce_acked(&client, "post-crash", 1).await[0];

    assert!(
        post_crash_offset > highest_confirmed,
        "a crash-restarted node re-minted offsets it already confirmed: offset \
         {highest_confirmed} was handed to a client before the SIGKILL, yet the first \
         post-restart send was confirmed at offset {post_crash_offset}; without a durable \
         offset watermark the node restarts the partition log below what it acknowledged, \
         so two different messages now share an offset and consumers reading by offset get \
         silently different data"
    );
}

/// The fix has to survive its own side effect: the hole the reservation leaves
/// between the recovered segments and the new append point truncates everything
/// past it on the next boot if it lands INSIDE a segment.
///
/// So the SECOND crash is the one that matters, and only if the run between the
/// two reaches disk, which is what the flush threshold is for.
#[iggy_harness(cluster_nodes = 1)]
async fn given_a_crash_restarted_node_when_it_flushes_and_crashes_again_should_still_not_remint_offsets(
    harness: &mut TestHarness,
) {
    const FLUSH_THRESHOLD: u32 = 4;
    /// Past the threshold, so every life leaves a chain for the next boot.
    const SENDS_PER_LIFE: u32 = 6;

    let client = harness.tcp_root_client().await.unwrap();
    create_topic(&client, Some(FLUSH_THRESHOLD)).await;

    let acked = produce_acked(&client, "first-life", SENDS_PER_LIFE).await;
    let first_life_max = *acked.last().expect("confirmed sends");
    drop(client);

    // The boot that consumes a reservation and re-anchors, then flushes the
    // hole's far side to disk.
    let client = crash_and_recover(harness).await;
    let second_life = produce_acked(&client, "second-life", SENDS_PER_LIFE).await;
    let second_life_min = second_life[0];
    let second_life_max = *second_life.last().expect("confirmed sends");
    assert!(
        second_life_min > first_life_max,
        "the first restart re-minted: confirmed {first_life_max} before the crash, \
         then {second_life_min} after it"
    );
    drop(client);

    // ON a segment boundary, not inside one: a segment still named 0 while
    // holding the second life's offsets claims a range it does not have.
    let bases = segment_base_offsets(harness.test_dir());
    assert!(
        bases.iter().any(|&base| base > first_life_max),
        "no segment is anchored above the pre-crash offsets, so the second life \
         appended into a segment named for the first: segment bases {bases:?}, last \
         offset confirmed before the crash {first_life_max}"
    );

    // The first boot that has to read a chain the re-anchor wrote.
    let client = crash_and_recover(harness).await;
    let third_life = produce_acked(&client, "third-life", 1).await[0];
    assert!(
        third_life > second_life_max,
        "the SECOND restart re-minted: {second_life_max} was confirmed between the \
         two crashes, yet the node came back and handed out {third_life}. A hole \
         left INSIDE a segment truncates the tail that proved the frontier"
    );
}

/// The partially-flushed shape a real workload crashes in: the run below the
/// threshold is confirmed out of the journal while everything before it is on
/// disk. The recovered chain then ends BELOW the reservation with bytes in it,
/// so the re-anchor has to seal it rather than append into the gap.
#[iggy_harness(cluster_nodes = 1)]
async fn given_sends_straddling_the_flush_threshold_when_the_node_is_killed_should_not_remint_offsets(
    harness: &mut TestHarness,
) {
    const FLUSH_THRESHOLD: u32 = 4;
    const STRADDLING_SENDS: u32 = 6;

    let client = harness.tcp_root_client().await.unwrap();
    create_topic(&client, Some(FLUSH_THRESHOLD)).await;

    let acked = produce_acked(&client, "straddle", STRADDLING_SENDS).await;
    let highest_confirmed = *acked.last().expect("confirmed sends");
    assert_eq!(
        acked,
        (0..u64::from(STRADDLING_SENDS)).collect::<Vec<_>>(),
        "the pre-crash run mints a contiguous range from zero"
    );
    drop(client);

    let client = crash_and_recover(harness).await;
    let post_crash = produce_acked(&client, "post-straddle", 1).await[0];
    assert!(
        post_crash > highest_confirmed,
        "offsets confirmed out of the journal above the last flushed one were \
         re-minted: {highest_confirmed} went to a client before the SIGKILL, and the \
         first send after it was confirmed at {post_crash}"
    );
}
