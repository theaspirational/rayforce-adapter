# rayforce-adapter

`rayforce-adapter` persists `ray-transactor` commits as Rayforce splayed tables.

The adapter keeps the storage boundary small:

- `TxLogLayout` defines the on-disk Rayforce table layout.
- `SplayedTxLogProjection` implements `ray_transactor::Projection`.
- Committed tx metadata and datoms are rebuilt into Rayforce `tx` and `datom`
  tables after each commit.

The crate links against the Rayforce C library. Set `RAYFORCE_DIR` to a local
Rayforce checkout, or place this crate beside a `rayforce` checkout.

## License

MIT
