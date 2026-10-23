# dpu_io

An experimental DPU write I/O project for the output stage of LSM-tree compaction.

The host submits write, range sync, and sync-and-close requests through a frontend API. The DPU transfers data using DOCA DMA and writes it to a pool of pre-created SST files using io_uring. The goal is to overlap data generation on the host, DMA transfers, and file I/O, reducing the write I/O work performed by the host.

## How it works

```text
Host                                DPU
FrontendQp                          Backend
    |                                   |
Request ring buffer -- DOCA DMA -->  Request parsing
                                        |
                                    UringEngine
                                        |
                                    SST file pool
                                        |
Response ring buffer <-- DOCA DMA -- File completion results
```

- DOCA Comch carries initialization, queue state, and file reclamation messages; DMA transfers request data and responses.
- Host request buffer space can be reclaimed after DMA completes; DPU request buffer space is reclaimed after the corresponding I/O completes.
- `Frontend` manages connections and QPs. Each `FrontendQp` is used by a single calling thread.
- SST files are grouped by capacity, with logical file numbers mapped to slots. `delete_files()` releases slots for reuse.
