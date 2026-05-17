use libc::{c_char, c_int, c_void};

#[repr(C)]
pub struct ray_t {
    _private: [u8; 0],
}

#[allow(non_camel_case_types)]
pub type ray_err_t = c_int;

pub const RAY_OK: ray_err_t = 0;
pub const RAY_I64: i8 = 5;
pub const RAY_SYM: i8 = 12;
pub const RAY_ERROR: i8 = 127;

extern "C" {
    pub fn ray_release(v: *mut ray_t);
    pub fn ray_obj_type(v: *mut ray_t) -> i8;
    pub fn ray_str_ptr(s: *mut ray_t) -> *const c_char;
    pub fn ray_str_len(s: *mut ray_t) -> usize;
    pub fn ray_sym_init() -> ray_err_t;
    pub fn ray_sym_find(s: *const c_char, len: usize) -> i64;
    pub fn ray_sym_intern(s: *const c_char, len: usize) -> i64;
    pub fn ray_sym_str(id: i64) -> *mut ray_t;
    pub fn ray_sym_save(path: *const c_char) -> ray_err_t;
    pub fn ray_sym_load(path: *const c_char) -> ray_err_t;
    pub fn ray_table_get_col(tbl: *mut ray_t, name_id: i64) -> *mut ray_t;
    pub fn ray_table_nrows(tbl: *mut ray_t) -> i64;
    pub fn ray_table_new(ncols: i64) -> *mut ray_t;
    pub fn ray_table_add_col(tbl: *mut ray_t, name_id: i64, col: *mut ray_t) -> *mut ray_t;
    pub fn ray_vec_new(typ: i8, capacity: i64) -> *mut ray_t;
    pub fn ray_vec_append(vec: *mut ray_t, elem: *const c_void) -> *mut ray_t;
    pub fn ray_vec_get_i64(vec: *mut ray_t, idx: i64) -> i64;
    pub fn ray_vec_get_sym_id(vec: *mut ray_t, idx: i64) -> i64;
    pub fn ray_splay_save(
        tbl: *mut ray_t,
        dir: *const c_char,
        sym_path: *const c_char,
    ) -> ray_err_t;
    pub fn ray_read_splayed(dir: *const c_char, sym_path: *const c_char) -> *mut ray_t;
}
