//! Rayforce persistence adapter for `ray-datom` / `ray-transactor`.
//!
//! The adapter follows the same persistence shape as Rayforce-backed apps:
//! committed state is materialized as splayed Rayforce tables plus a shared
//! symbol table. It intentionally stays below application command semantics.

mod ffi;

use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::slice;

use ray_datom::datom_store::Datom;
use ray_datom::tx::{ActorKind, PrincipalId, Tx, TxId};
use ray_datom::value::Value;
use ray_transactor::{CommitEnvelope, Projection, TransactorError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RayforceAdapterError {
    #[error("path contains interior NUL byte: {0}")]
    NulPath(String),
    #[error("rayforce allocation failed while building `{0}` table")]
    Allocation(&'static str),
    #[error("ray_sym_init failed with code {0}")]
    SymInit(i32),
    #[error("rayforce returned an error object while reading `{0}`")]
    RayforceRead(&'static str),
    #[error("ray_splay_save failed for {dir} with code {code}")]
    SplaySave { dir: PathBuf, code: i32 },
    #[error("ray_sym_save failed for {path} with code {code}")]
    SymSave { path: PathBuf, code: i32 },
    #[error("ray_sym_load failed for {path} with code {code}")]
    SymLoad { path: PathBuf, code: i32 },
    #[error("missing `{column}` column in `{table}` table")]
    MissingColumn {
        table: &'static str,
        column: &'static str,
    },
    #[error("invalid tx id `{0}` in persisted tx log")]
    InvalidTxId(i64),
    #[error("invalid actor kind `{0}` in persisted tx log")]
    InvalidActorKind(String),
    #[error("invalid JSON in `{field}`: {source}")]
    InvalidJson {
        field: &'static str,
        source: serde_json::Error,
    },
    #[error("invalid UTF-8 in `{field}`: {source}")]
    InvalidUtf8 {
        field: &'static str,
        source: std::string::FromUtf8Error,
    },
    #[error("create directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("replace {to} with {from}: {source}")]
    Rename {
        from: PathBuf,
        to: PathBuf,
        source: std::io::Error,
    },
}

/// On-disk Rayforce layout for an application tx log.
#[derive(Clone, Debug)]
pub struct TxLogLayout {
    root: PathBuf,
}

impl TxLogLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn tx_table_dir(&self) -> PathBuf {
        self.root.join("tx")
    }

    pub fn datom_table_dir(&self) -> PathBuf {
        self.root.join("datom")
    }

    pub fn sym_path(&self) -> PathBuf {
        self.root.join("sym.ray")
    }

    pub fn ensure(&self) -> Result<(), RayforceAdapterError> {
        std::fs::create_dir_all(&self.root).map_err(|source| RayforceAdapterError::CreateDir {
            path: self.root.clone(),
            source,
        })
    }
}

/// `ray-transactor` projection that materializes committed envelopes into
/// Rayforce splayed tables.
///
/// The current implementation rebuilds the `tx` and `datom` tables on every
/// append. That is intentionally simple and mirrors the early Rayforce app
/// persistence path; applications can later swap in incremental table updates
/// without changing the transactor boundary.
pub struct SplayedTxLogProjection {
    layout: TxLogLayout,
    envelopes: Vec<CommitEnvelope>,
}

impl SplayedTxLogProjection {
    pub fn new(layout: TxLogLayout) -> Result<Self, RayforceAdapterError> {
        ensure_sym_init()?;
        layout.ensure()?;
        Ok(Self {
            layout,
            envelopes: Vec::new(),
        })
    }

    pub fn open(layout: TxLogLayout) -> Result<Self, RayforceAdapterError> {
        ensure_sym_init()?;
        layout.ensure()?;
        let envelopes = load_envelopes(&layout)?;
        Ok(Self { layout, envelopes })
    }

    pub fn layout(&self) -> &TxLogLayout {
        &self.layout
    }

    pub fn envelopes(&self) -> &[CommitEnvelope] {
        &self.envelopes
    }

    pub fn rebuild(&self) -> Result<(), RayforceAdapterError> {
        ensure_sym_init()?;
        save_txs(
            &self.layout.tx_table_dir(),
            &self.layout.sym_path(),
            &self.envelopes,
        )?;
        save_datoms(
            &self.layout.datom_table_dir(),
            &self.layout.sym_path(),
            &self.envelopes,
        )?;
        let sym_path = self.layout.sym_path();
        let c_sym = path_to_cstring(&sym_path)?;
        let err = unsafe { ffi::ray_sym_save(c_sym.as_ptr()) };
        if err != ffi::RAY_OK {
            return Err(RayforceAdapterError::SymSave {
                path: sym_path,
                code: err,
            });
        }
        Ok(())
    }
}

pub fn load_envelopes(layout: &TxLogLayout) -> Result<Vec<CommitEnvelope>, RayforceAdapterError> {
    ensure_sym_init()?;
    let Some(tx_table) = TableReader::open("tx", &layout.tx_table_dir(), &layout.sym_path())?
    else {
        return Ok(Vec::new());
    };
    let datom_table = TableReader::open("datom", &layout.datom_table_dir(), &layout.sym_path())?;

    let tx_ids = tx_table.i64_col("tx_id")?;
    let tx_times = tx_table.sym_col("tx_time")?;
    let actors = tx_table.sym_col("actor")?;
    let actor_kinds = tx_table.sym_col("actor_kind")?;
    let commands = tx_table.sym_col("command")?;
    let idempotency = tx_table.sym_col("idempotency_key")?;
    let metadata = tx_table.sym_col("metadata_json")?;

    let mut datoms_by_tx: std::collections::BTreeMap<u64, Vec<Datom>> =
        std::collections::BTreeMap::new();
    if let Some(datoms) = datom_table {
        let datom_tx_ids = datoms.i64_col("tx_id")?;
        let entities = datoms.sym_col("entity")?;
        let attrs = datoms.sym_col("attr")?;
        let value_json = datoms.sym_col("value_json")?;
        let added = datoms.i64_col("added")?;
        for idx in 0..datoms.rows() {
            let tx_id = to_tx_id(&datom_tx_ids, idx)?;
            let value = serde_json::from_str::<Value>(&value_json.get(idx, "value_json")?)
                .map_err(|source| RayforceAdapterError::InvalidJson {
                    field: "value_json",
                    source,
                })?;
            datoms_by_tx.entry(tx_id.0).or_default().push(Datom {
                entity: ray_datom::value::EntityId::new(entities.get(idx, "entity")?),
                attr: attrs.get(idx, "attr")?,
                value,
                tx: tx_id,
                added: added.get(idx, "added")? != 0,
            });
        }
    }

    let mut envelopes = Vec::with_capacity(tx_table.rows() as usize);
    for idx in 0..tx_table.rows() {
        let tx_id = to_tx_id(&tx_ids, idx)?;
        let actor_kind = parse_actor_kind(actor_kinds.get(idx, "actor_kind")?)?;
        let metadata =
            serde_json::from_str::<serde_json::Value>(&metadata.get(idx, "metadata_json")?)
                .map_err(|source| RayforceAdapterError::InvalidJson {
                    field: "metadata_json",
                    source,
                })?;
        let mut tx = Tx::new(
            tx_id,
            tx_times.get(idx, "tx_time")?,
            PrincipalId::new(actors.get(idx, "actor")?),
            actor_kind,
            commands.get(idx, "command")?,
        )
        .with_metadata(metadata);
        let idem = idempotency.get(idx, "idempotency_key")?;
        if !idem.is_empty() {
            tx = tx.with_idempotency_key(idem);
        }
        envelopes.push(CommitEnvelope::new(
            tx,
            datoms_by_tx.remove(&tx_id.0).unwrap_or_default(),
        ));
    }
    Ok(envelopes)
}

impl Projection for SplayedTxLogProjection {
    fn append_envelope(&mut self, envelope: &CommitEnvelope) -> Result<(), TransactorError> {
        self.envelopes.push(envelope.clone());
        self.rebuild()
            .map_err(|err| TransactorError::Projection(err.to_string()))
    }
}

struct RayObj {
    ptr: *mut ffi::ray_t,
}

impl RayObj {
    fn from_raw(ptr: *mut ffi::ray_t, label: &'static str) -> Result<Self, RayforceAdapterError> {
        if ptr.is_null() {
            return Err(RayforceAdapterError::Allocation(label));
        }
        Ok(Self { ptr })
    }

    fn as_ptr(&self) -> *mut ffi::ray_t {
        self.ptr
    }
}

impl Drop for RayObj {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { ffi::ray_release(self.ptr) };
        }
    }
}

struct TableReader {
    label: &'static str,
    table: RayObj,
}

impl TableReader {
    fn open(
        label: &'static str,
        dir: &Path,
        sym_path: &Path,
    ) -> Result<Option<Self>, RayforceAdapterError> {
        if !dir.exists() {
            return Ok(None);
        }
        if sym_path.exists() {
            let c_sym = path_to_cstring(sym_path)?;
            let err = unsafe { ffi::ray_sym_load(c_sym.as_ptr()) };
            if err != ffi::RAY_OK {
                return Err(RayforceAdapterError::SymLoad {
                    path: sym_path.to_path_buf(),
                    code: err,
                });
            }
        }
        let c_dir = path_to_cstring(dir)?;
        let c_sym = path_to_cstring(sym_path)?;
        let ptr = unsafe { ffi::ray_read_splayed(c_dir.as_ptr(), c_sym.as_ptr()) };
        if ptr.is_null() || ray_is_error(ptr) {
            return Err(RayforceAdapterError::RayforceRead(label));
        }
        Ok(Some(Self {
            label,
            table: RayObj { ptr },
        }))
    }

    fn rows(&self) -> i64 {
        unsafe { ffi::ray_table_nrows(self.table.as_ptr()) }
    }

    fn col(&self, name: &'static str) -> Result<*mut ffi::ray_t, RayforceAdapterError> {
        let name_id = sym_find(name);
        if name_id < 0 {
            return Err(RayforceAdapterError::MissingColumn {
                table: self.label,
                column: name,
            });
        }
        let col = unsafe { ffi::ray_table_get_col(self.table.as_ptr(), name_id) };
        if col.is_null() || ray_is_error(col) {
            return Err(RayforceAdapterError::MissingColumn {
                table: self.label,
                column: name,
            });
        }
        Ok(col)
    }

    fn i64_col(&self, name: &'static str) -> Result<I64Column, RayforceAdapterError> {
        Ok(I64Column(self.col(name)?))
    }

    fn sym_col(&self, name: &'static str) -> Result<SymColumn, RayforceAdapterError> {
        Ok(SymColumn(self.col(name)?))
    }
}

struct I64Column(*mut ffi::ray_t);

impl I64Column {
    fn get(&self, idx: i64, _field: &'static str) -> Result<i64, RayforceAdapterError> {
        Ok(unsafe { ffi::ray_vec_get_i64(self.0, idx) })
    }
}

struct SymColumn(*mut ffi::ray_t);

impl SymColumn {
    fn get(&self, idx: i64, field: &'static str) -> Result<String, RayforceAdapterError> {
        let sym_id = unsafe { ffi::ray_vec_get_sym_id(self.0, idx) };
        sym_to_string(sym_id, field)
    }
}

fn save_txs(
    dir: &Path,
    sym_path: &Path,
    envelopes: &[CommitEnvelope],
) -> Result<(), RayforceAdapterError> {
    let mut builder = TableBuilder::new(7, "tx")?;
    let tx_ids: Vec<i64> = envelopes.iter().map(|e| e.tx.tx_id.0 as i64).collect();
    let tx_times: Vec<String> = envelopes.iter().map(|e| e.tx.tx_time.0.clone()).collect();
    let actors: Vec<String> = envelopes.iter().map(|e| e.tx.actor.0.clone()).collect();
    let actor_kinds: Vec<String> = envelopes
        .iter()
        .map(|e| actor_kind_str(e.tx.actor_kind).to_string())
        .collect();
    let commands: Vec<String> = envelopes.iter().map(|e| e.tx.command.clone()).collect();
    let idempotency: Vec<String> = envelopes
        .iter()
        .map(|e| e.tx.idempotency_key.clone().unwrap_or_default())
        .collect();
    let metadata: Vec<String> = envelopes
        .iter()
        .map(|e| e.tx.metadata.to_string())
        .collect();

    builder.add_i64_col("tx_id", &tx_ids)?;
    builder.add_sym_col("tx_time", &tx_times)?;
    builder.add_sym_col("actor", &actors)?;
    builder.add_sym_col("actor_kind", &actor_kinds)?;
    builder.add_sym_col("command", &commands)?;
    builder.add_sym_col("idempotency_key", &idempotency)?;
    builder.add_sym_col("metadata_json", &metadata)?;
    save_table(builder.finish()?, dir, sym_path)
}

fn save_datoms(
    dir: &Path,
    sym_path: &Path,
    envelopes: &[CommitEnvelope],
) -> Result<(), RayforceAdapterError> {
    let datoms: Vec<&Datom> = envelopes.iter().flat_map(|e| e.datoms.iter()).collect();
    let mut builder = TableBuilder::new(6, "datom")?;
    let tx_ids: Vec<i64> = datoms.iter().map(|d| d.tx.0 as i64).collect();
    let entities: Vec<String> = datoms.iter().map(|d| d.entity.0.clone()).collect();
    let attrs: Vec<String> = datoms.iter().map(|d| d.attr.clone()).collect();
    let kinds: Vec<String> = datoms.iter().map(|d| d.value.kind().to_string()).collect();
    let values: Vec<String> = datoms.iter().map(|d| value_json(&d.value)).collect();
    let added: Vec<i64> = datoms.iter().map(|d| i64::from(d.added)).collect();

    builder.add_i64_col("tx_id", &tx_ids)?;
    builder.add_sym_col("entity", &entities)?;
    builder.add_sym_col("attr", &attrs)?;
    builder.add_sym_col("value_kind", &kinds)?;
    builder.add_sym_col("value_json", &values)?;
    builder.add_i64_col("added", &added)?;
    save_table(builder.finish()?, dir, sym_path)
}

struct TableBuilder {
    ptr: *mut ffi::ray_t,
    label: &'static str,
}

impl TableBuilder {
    fn new(ncols: usize, label: &'static str) -> Result<Self, RayforceAdapterError> {
        let ptr = unsafe { ffi::ray_table_new(ncols as i64) };
        if ptr.is_null() {
            return Err(RayforceAdapterError::Allocation(label));
        }
        Ok(Self { ptr, label })
    }

    fn add_i64_col(&mut self, name: &str, values: &[i64]) -> Result<(), RayforceAdapterError> {
        unsafe {
            let mut col = ffi::ray_vec_new(ffi::RAY_I64, values.len() as i64);
            if col.is_null() {
                return Err(RayforceAdapterError::Allocation(self.label));
            }
            for value in values {
                col = ffi::ray_vec_append(col, value as *const i64 as *const _);
                if col.is_null() {
                    return Err(RayforceAdapterError::Allocation(self.label));
                }
            }
            self.ptr = ffi::ray_table_add_col(self.ptr, sym_intern(name), col);
            ffi::ray_release(col);
        }
        if self.ptr.is_null() {
            return Err(RayforceAdapterError::Allocation(self.label));
        }
        Ok(())
    }

    fn add_sym_col(&mut self, name: &str, values: &[String]) -> Result<(), RayforceAdapterError> {
        unsafe {
            let mut col = ffi::ray_vec_new(ffi::RAY_SYM, values.len() as i64);
            if col.is_null() {
                return Err(RayforceAdapterError::Allocation(self.label));
            }
            for value in values {
                let sym = sym_intern(value);
                col = ffi::ray_vec_append(col, &sym as *const i64 as *const _);
                if col.is_null() {
                    return Err(RayforceAdapterError::Allocation(self.label));
                }
            }
            self.ptr = ffi::ray_table_add_col(self.ptr, sym_intern(name), col);
            ffi::ray_release(col);
        }
        if self.ptr.is_null() {
            return Err(RayforceAdapterError::Allocation(self.label));
        }
        Ok(())
    }

    fn finish(mut self) -> Result<RayObj, RayforceAdapterError> {
        let ptr = self.ptr;
        self.ptr = std::ptr::null_mut();
        RayObj::from_raw(ptr, self.label)
    }
}

impl Drop for TableBuilder {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { ffi::ray_release(self.ptr) };
        }
    }
}

fn save_table(table: RayObj, dir: &Path, sym_path: &Path) -> Result<(), RayforceAdapterError> {
    let new_dir = dir.with_extension("new");
    let old_dir = dir.with_extension("old");

    if new_dir.exists() {
        let _ = std::fs::remove_dir_all(&new_dir);
    }
    std::fs::create_dir_all(&new_dir).map_err(|source| RayforceAdapterError::CreateDir {
        path: new_dir.clone(),
        source,
    })?;

    let c_dir = path_to_cstring(&new_dir)?;
    let c_sym = path_to_cstring(sym_path)?;
    let err = unsafe { ffi::ray_splay_save(table.as_ptr(), c_dir.as_ptr(), c_sym.as_ptr()) };
    if err != ffi::RAY_OK {
        return Err(RayforceAdapterError::SplaySave {
            dir: new_dir,
            code: err,
        });
    }

    if dir.exists() {
        if old_dir.exists() {
            let _ = std::fs::remove_dir_all(&old_dir);
        }
        std::fs::rename(dir, &old_dir).map_err(|source| RayforceAdapterError::Rename {
            from: dir.to_path_buf(),
            to: old_dir.clone(),
            source,
        })?;
    }
    std::fs::rename(&new_dir, dir).map_err(|source| RayforceAdapterError::Rename {
        from: new_dir.clone(),
        to: dir.to_path_buf(),
        source,
    })?;
    if old_dir.exists() {
        let _ = std::fs::remove_dir_all(&old_dir);
    }
    Ok(())
}

fn path_to_cstring(path: &Path) -> Result<CString, RayforceAdapterError> {
    let text = path.to_string_lossy().to_string();
    CString::new(text.clone()).map_err(|_| RayforceAdapterError::NulPath(text))
}

fn ensure_sym_init() -> Result<(), RayforceAdapterError> {
    let err = unsafe { ffi::ray_sym_init() };
    if err != ffi::RAY_OK {
        return Err(RayforceAdapterError::SymInit(err));
    }
    Ok(())
}

fn ray_is_error(ptr: *mut ffi::ray_t) -> bool {
    unsafe { ffi::ray_obj_type(ptr) == ffi::RAY_ERROR }
}

fn sym_intern(value: &str) -> i64 {
    unsafe { ffi::ray_sym_intern(value.as_ptr() as *const _, value.len()) }
}

fn sym_find(value: &str) -> i64 {
    unsafe { ffi::ray_sym_find(value.as_ptr() as *const _, value.len()) }
}

fn sym_to_string(sym_id: i64, field: &'static str) -> Result<String, RayforceAdapterError> {
    let atom = unsafe { ffi::ray_sym_str(sym_id) };
    if atom.is_null() || ray_is_error(atom) {
        return Err(RayforceAdapterError::RayforceRead(field));
    }
    let ptr = unsafe { ffi::ray_str_ptr(atom) };
    let len = unsafe { ffi::ray_str_len(atom) };
    let bytes = unsafe { slice::from_raw_parts(ptr as *const u8, len) };
    String::from_utf8(bytes.to_vec())
        .map_err(|source| RayforceAdapterError::InvalidUtf8 { field, source })
}

fn actor_kind_str(kind: ActorKind) -> &'static str {
    match kind {
        ActorKind::Human => "human",
        ActorKind::Worker => "worker",
        ActorKind::Adapter => "adapter",
        ActorKind::System => "system",
    }
}

fn parse_actor_kind(value: String) -> Result<ActorKind, RayforceAdapterError> {
    match value.as_str() {
        "human" => Ok(ActorKind::Human),
        "worker" => Ok(ActorKind::Worker),
        "adapter" => Ok(ActorKind::Adapter),
        "system" => Ok(ActorKind::System),
        _ => Err(RayforceAdapterError::InvalidActorKind(value)),
    }
}

fn to_tx_id(column: &I64Column, idx: i64) -> Result<TxId, RayforceAdapterError> {
    let tx_id = column.get(idx, "tx_id")?;
    if tx_id < 0 {
        return Err(RayforceAdapterError::InvalidTxId(tx_id));
    }
    Ok(TxId(tx_id as u64))
}

fn value_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_datom::datom_store::Datom;
    use ray_datom::tx::{ActorKind, PrincipalId, Tx, TxId};
    use ray_datom::value::{EntityId, Value};
    use ray_transactor::Projection;

    #[test]
    fn projection_roundtrips_envelopes_from_splayed_tables() {
        let root =
            std::env::temp_dir().join(format!("rayforce-adapter-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let layout = TxLogLayout::new(&root);
        let mut projection = SplayedTxLogProjection::new(layout.clone()).unwrap();
        let envelope = CommitEnvelope::new(
            Tx::new(
                TxId(1),
                "2026-05-17T00:00:00Z",
                PrincipalId::new("principal/alice"),
                ActorKind::Human,
                "task.create",
            )
            .with_idempotency_key("idem-1")
            .with_metadata(serde_json::json!({"source": "test"})),
            vec![Datom::add(
                EntityId::new("task/T1"),
                "task/title",
                Value::str("T1"),
                TxId(1),
            )],
        );

        projection.append_envelope(&envelope).unwrap();
        let reopened = SplayedTxLogProjection::open(layout).unwrap();
        let loaded = reopened.envelopes();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].tx.tx_id, TxId(1));
        assert_eq!(loaded[0].tx.idempotency_key.as_deref(), Some("idem-1"));
        assert_eq!(loaded[0].tx.metadata["source"], "test");
        assert_eq!(loaded[0].datoms.len(), 1);
        assert_eq!(loaded[0].datoms[0].entity, EntityId::new("task/T1"));

        let _ = std::fs::remove_dir_all(&root);
    }
}
