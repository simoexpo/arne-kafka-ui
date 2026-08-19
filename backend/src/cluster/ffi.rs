//! Thin safe wrappers over librdkafka admin calls that exist in the C library
//! but that rust-rdkafka 0.39 never bound (owner-approved 2026-08-19). No
//! fork is involved: `rdkafka-sys` already compiles and exposes these
//! symbols, the Rust layer above it simply skipped them.
//!
//! Each wrapper follows librdkafka's admin protocol exactly once, in one
//! place: build options → issue the call onto a private queue → block for the
//! typed result event → copy everything out → destroy. Nothing borrowed from
//! librdkafka outlives this module, so callers never touch a raw pointer.

use super::ClusterHandle;
use crate::error::ApiError;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka_sys as rd;
use std::ffi::{CStr, CString};
use std::time::Duration;

/// Owns the queue + options pair every admin call needs and destroys both on
/// drop, so an early return can never leak them.
struct AdminCall {
    rk: *mut rd::RDKafka,
    queue: *mut rd::rd_kafka_queue_t,
    options: *mut rd::rd_kafka_AdminOptions_t,
}

impl AdminCall {
    fn new(
        admin: &AdminClient<DefaultClientContext>,
        op: rd::rd_kafka_admin_op_t,
        timeout: Duration,
        cluster: &str,
    ) -> Result<Self, ApiError> {
        let rk = admin.inner().native_ptr();
        // SAFETY: `rk` is a live client owned by the caller's ClusterHandle;
        // both constructors return owned handles this struct now owns.
        unsafe {
            let queue = rd::rd_kafka_queue_new(rk);
            let options = rd::rd_kafka_AdminOptions_new(rk, op);
            let mut errbuf = [0i8; 512];
            let rc = rd::rd_kafka_AdminOptions_set_request_timeout(
                options,
                timeout.as_millis() as i32,
                errbuf.as_mut_ptr(),
                errbuf.len(),
            );
            if rc as i32 != rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR as i32 {
                rd::rd_kafka_AdminOptions_destroy(options);
                rd::rd_kafka_queue_destroy(queue);
                return Err(ApiError::kafka(cluster, cstr_to_string(errbuf.as_ptr())));
            }
            Ok(Self { rk, queue, options })
        }
    }

    /// Blocks for this call's result event. The caller must read it before
    /// dropping the returned guard.
    fn wait(&self, timeout: Duration, cluster: &str, what: &str) -> Result<AdminEvent, ApiError> {
        // SAFETY: polling our own queue; the event becomes ours to destroy.
        let event = unsafe { rd::rd_kafka_queue_poll(self.queue, timeout.as_millis() as i32) };
        if event.is_null() {
            return Err(ApiError::kafka_timeout(cluster, what));
        }
        let event = AdminEvent(event);
        // SAFETY: `event` is a live event we own.
        let err = unsafe { rd::rd_kafka_event_error(event.0) };
        if err as i32 != rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR as i32 {
            let detail = unsafe { cstr_to_string(rd::rd_kafka_event_error_string(event.0)) };
            return Err(ApiError::kafka(cluster, format!("{what}: {detail}")));
        }
        Ok(event)
    }
}

impl Drop for AdminCall {
    fn drop(&mut self) {
        // SAFETY: both handles were created in `new` and are dropped once.
        unsafe {
            rd::rd_kafka_AdminOptions_destroy(self.options);
            rd::rd_kafka_queue_destroy(self.queue);
        }
        let _ = self.rk;
    }
}

struct AdminEvent(*mut rd::rd_kafka_event_t);

impl Drop for AdminEvent {
    fn drop(&mut self) {
        // SAFETY: the event came from our own queue poll and is destroyed once.
        unsafe { rd::rd_kafka_event_destroy(self.0) };
    }
}

/// SAFETY: `p` must be NUL-terminated or null.
unsafe fn cstr_to_string(p: *const std::os::raw::c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    // SAFETY: the caller guarantees a NUL-terminated string.
    unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() }
}

#[derive(Debug)]
pub struct ClusterDescription {
    pub brokers: Vec<(i32, String, i32)>,
    pub controller_id: Option<i32>,
}

/// Brokers and controller WITHOUT the topic inventory: `fetch_metadata(None)`
/// answers the same question by shipping every partition of every topic,
/// which is megabytes on a large cluster for a number we use to paint one
/// health dot (owner ruling 2026-08-19).
pub fn describe_cluster_blocking(
    handle: &ClusterHandle,
    timeout: Duration,
) -> Result<ClusterDescription, ApiError> {
    let admin = handle.admin();
    let call = AdminCall::new(
        &admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER,
        timeout,
        &handle.name,
    )?;
    // SAFETY: issuing the call onto our own queue with our own options.
    unsafe { rd::rd_kafka_DescribeCluster(call.rk, call.options, call.queue) };
    let event = call.wait(timeout, &handle.name, "describe cluster")?;
    // SAFETY: the event is a DescribeCluster result (we asked for that op and
    // checked its error above); every pointer below borrows from it and is
    // copied out before the event drops.
    unsafe {
        let result = rd::rd_kafka_event_DescribeCluster_result(event.0);
        if result.is_null() {
            return Err(ApiError::kafka(&handle.name, "describe cluster: unexpected result type"));
        }
        let mut count = 0usize;
        let nodes = rd::rd_kafka_DescribeCluster_result_nodes(result, &mut count);
        let mut brokers = Vec::with_capacity(count);
        for i in 0..count {
            let node = *nodes.add(i);
            brokers.push((
                rd::rd_kafka_Node_id(node),
                cstr_to_string(rd::rd_kafka_Node_host(node)),
                i32::from(rd::rd_kafka_Node_port(node)),
            ));
        }
        let controller = rd::rd_kafka_DescribeCluster_result_controller(result);
        let controller_id = if controller.is_null() {
            None
        } else {
            // -1 is librdkafka's "unknown controller"
            Some(rd::rd_kafka_Node_id(controller)).filter(|id| *id >= 0)
        };
        Ok(ClusterDescription { brokers, controller_id })
    }
}

/// One batched ListOffsets for every partition in `partitions`, asking for the
/// given spec. librdkafka splits it per leader broker internally, so a
/// thousand partitions cost ~one request per broker instead of a thousand
/// sequential round trips.
pub fn list_offsets_blocking(
    handle: &ClusterHandle,
    partitions: &[(String, i32)],
    spec: OffsetSpec,
    timeout: Duration,
) -> Result<Vec<PartitionOffset>, ApiError> {
    if partitions.is_empty() {
        return Ok(Vec::new());
    }
    let admin = handle.admin();
    let call = AdminCall::new(
        &admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTOFFSETS,
        timeout,
        &handle.name,
    )?;
    let tpl = OwnedTpl::build(partitions, spec)?;
    // SAFETY: the list stays alive for the duration of the call.
    unsafe { rd::rd_kafka_ListOffsets(call.rk, tpl.0, call.options, call.queue) };
    let event = call.wait(timeout, &handle.name, "list offsets")?;
    // SAFETY: result borrows from the event; every field is copied out here.
    unsafe {
        let result = rd::rd_kafka_event_ListOffsets_result(event.0);
        if result.is_null() {
            return Err(ApiError::kafka(&handle.name, "list offsets: unexpected result type"));
        }
        let mut count = 0usize;
        let infos = rd::rd_kafka_ListOffsets_result_infos(result, &mut count);
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let tp = rd::rd_kafka_ListOffsetsResultInfo_topic_partition(*infos.add(i));
            if tp.is_null() {
                continue;
            }
            let tp = &*tp;
            out.push(PartitionOffset {
                topic: cstr_to_string(tp.topic),
                partition: tp.partition,
                offset: tp.offset,
                // Per-partition failures are reported in the entry itself: a
                // leader election in flight must not fail the whole batch.
                error: if tp.err as i32 == rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR as i32 {
                    None
                } else {
                    Some(cstr_to_string(rd::rd_kafka_err2str(tp.err)))
                },
            });
        }
        Ok(out)
    }
}

/// `list_offsets_blocking` keyed by partition, for callers working within one
/// topic. A partition whose own entry carried an error is absent from the map:
/// callers render it as unknown rather than as a wrong number.
pub fn offsets_by_partition(
    handle: &ClusterHandle,
    partitions: &[(String, i32)],
    spec: OffsetSpec,
    timeout: Duration,
) -> Result<std::collections::HashMap<i32, i64>, ApiError> {
    Ok(list_offsets_blocking(handle, partitions, spec, timeout)?
        .into_iter()
        .filter(|p| p.error.is_none())
        .map(|p| (p.partition, p.offset))
        .collect())
}

/// A consumer group's committed offsets for `partitions`, read through the
/// shared admin client instead of a throwaway consumer built per group
/// (owner-approved 2026-08-19): `committed_offsets` is bound to the calling
/// client's own group.id, so every group used to cost a fresh TCP connect and
/// coordinator lookup. The C API still allows only ONE group per invocation
/// (`MUST always be 1`), so the request count per group is unchanged — the
/// connection setup is what disappears.
///
/// Partitions the group has no committed offset for are simply absent from
/// the returned map.
pub fn committed_offsets_blocking(
    handle: &ClusterHandle,
    group: &str,
    partitions: &[(String, i32)],
    timeout: Duration,
) -> Result<std::collections::HashMap<(String, i32), i64>, ApiError> {
    if partitions.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let admin = handle.admin();
    let call = AdminCall::new(
        &admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
        timeout,
        &handle.name,
    )?;
    let tpl = OwnedTpl::build(partitions, OffsetSpec::Invalid)?;
    let group_id = CString::new(group)
        .map_err(|_| ApiError::kafka(&handle.name, format!("group id contains a NUL byte: {group:?}")))?;
    // SAFETY: the request object owns nothing we free early; `tpl` outlives the
    // call, and the request is destroyed before returning.
    unsafe {
        let request = rd::rd_kafka_ListConsumerGroupOffsets_new(group_id.as_ptr(), tpl.0);
        let mut requests = [request];
        rd::rd_kafka_ListConsumerGroupOffsets(call.rk, requests.as_mut_ptr(), 1, call.options, call.queue);
        let event = call.wait(timeout, &handle.name, "fetch committed offsets");
        rd::rd_kafka_ListConsumerGroupOffsets_destroy(request);
        let event = event?;
        let result = rd::rd_kafka_event_ListConsumerGroupOffsets_result(event.0);
        if result.is_null() {
            return Err(ApiError::kafka(&handle.name, "fetch committed offsets: unexpected result type"));
        }
        let mut count = 0usize;
        let groups = rd::rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut count);
        let mut out = std::collections::HashMap::new();
        for i in 0..count {
            let group_result = *groups.add(i);
            let err = rd::rd_kafka_group_result_error(group_result);
            if !err.is_null() {
                let code = rd::rd_kafka_error_code(err);
                if code as i32 != rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR as i32 {
                    let detail = cstr_to_string(rd::rd_kafka_error_string(err));
                    return Err(ApiError::kafka(&handle.name, format!("fetch committed offsets: {detail}")));
                }
            }
            let tpl = rd::rd_kafka_group_result_partitions(group_result);
            if tpl.is_null() {
                continue;
            }
            let tpl = &*tpl;
            for j in 0..tpl.cnt as usize {
                let tp = &*tpl.elems.add(j);
                // A partition with no commit comes back as INVALID (-1001)
                if tp.offset >= 0 {
                    out.insert((cstr_to_string(tp.topic), tp.partition), tp.offset);
                }
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OffsetSpec {
    Earliest,
    Latest,
    /// "no offset specified" — for calls that read offsets rather than ask
    /// for a position (ListConsumerGroupOffsets).
    Invalid,
}

impl OffsetSpec {
    fn as_offset(self) -> i64 {
        match self {
            // librdkafka's RD_KAFKA_OFFSET_SPEC_* values
            OffsetSpec::Earliest => -2,
            OffsetSpec::Latest => -1,
            OffsetSpec::Invalid => -1001,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<String>,
}

/// A topic-partition list built for one call and destroyed with it.
struct OwnedTpl(*mut rd::rd_kafka_topic_partition_list_t);

impl OwnedTpl {
    fn build(partitions: &[(String, i32)], spec: OffsetSpec) -> Result<Self, ApiError> {
        // SAFETY: the list is created here and owned by the returned guard.
        unsafe {
            let tpl = rd::rd_kafka_topic_partition_list_new(partitions.len() as i32);
            for (topic, partition) in partitions {
                let name = CString::new(topic.as_str()).map_err(|_| {
                    ApiError::kafka("", format!("topic name contains a NUL byte: {topic:?}"))
                })?;
                let entry = rd::rd_kafka_topic_partition_list_add(tpl, name.as_ptr(), *partition);
                (*entry).offset = spec.as_offset();
            }
            Ok(Self(tpl))
        }
    }
}

impl Drop for OwnedTpl {
    fn drop(&mut self) {
        // SAFETY: created in `build`, destroyed once.
        unsafe { rd::rd_kafka_topic_partition_list_destroy(self.0) };
    }
}
