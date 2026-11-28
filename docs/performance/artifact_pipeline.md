# Artifact pipeline measurements

The [budget ledger](lifecycle_budgets.md#recorded-revision-2-results) summarizes
the acceptance results and reproduction commands. The complete machine-readable
report below retains the warm-up, every measured sample, source revisions,
environment settings and distribution definitions.

```json
{
  "budget_revision": 2,
  "build": {
    "admin_features": [
      "embedded-migrations"
    ],
    "default_features": true,
    "locked": true,
    "probe_features": [
      "integration-test-support",
      "embedded-migrations"
    ],
    "profile": "release"
  },
  "host": {
    "database_cpu_limit": null,
    "database_memory_limit": null,
    "database_peak_rss": null,
    "logical_cpus": 12,
    "memory_bytes": 34359738368,
    "model": "Mac14,12",
    "temporary_filesystem": "APFS",
    "worker_cpu_limit": null,
    "worker_memory_limit": null
  },
  "issue": "https://github.com/hubuum/hubuum/issues/508",
  "limits": {
    "block_bytes": 1048576,
    "blocks": 1024,
    "deployment_resident_bytes": 8589934592,
    "manifest_bytes": 262144,
    "maximum_encoded_row_bytes": 4194304,
    "minimum_slow_upload_mib_per_second": 0.8,
    "worker_resident_bytes": 4294967296,
    "worker_rss_bytes": 536870912
  },
  "maximum_rows_and_outputs": {
    "export_workers": [
      {
        "build": "release",
        "database": {
          "connection_and_query_wall_ms": [
            71.42245800059754,
            70.70520799607038,
            65.69595901237335,
            80.65912500023842,
            67.2223750007106
          ],
          "max_connections": "100",
          "server_cpu_limit": null,
          "server_memory_limit": null,
          "server_peak_rss": null,
          "shared_buffers": "128MB",
          "version": "18.6",
          "work_mem": "4MB"
        },
        "deployment_resident_limit": 8589934592,
        "elapsed_seconds": 573.7483344170178,
        "logical_cpus": 12,
        "modes": [
          "export",
          "export"
        ],
        "output_limit": 1072693248,
        "pool_acquire_timeout_ms": 30000,
        "profile": "artifact-remote-v2",
        "rss_acceptance_bytes": 536870912,
        "sample": 0,
        "source_commit": "6c1187916dac097345b409e826a68ec1fc3854c4",
        "statement_timeout_ms": 30000,
        "system": "macOS-27.2-arm64-arm-64bit-Mach-O",
        "worker_cpu_limit": null,
        "worker_memory_limit": null,
        "worker_pool_size": 8,
        "workers": [
          {
            "deployment_peak_reserved_bytes": 8582595150,
            "mode": "export",
            "native_worker_plus_child_high_water_rss": 446627840,
            "rate": 0,
            "row_bytes": 4194304,
            "rows": 255,
            "slots": 2,
            "streams": [
              {
                "bytes": 1069338784,
                "download_ms": 63938,
                "kind": "json",
                "producer_ms": 61140,
                "publication_ms": 357,
                "slot": 0,
                "upload_mib_per_second": 8.332183489561418,
                "upload_ms": 122393,
                "verification_ms": 64961
              },
              {
                "bytes": 1069286655,
                "download_ms": 64233,
                "kind": "template",
                "producer_ms": 58535,
                "publication_ms": 1516,
                "slot": 1,
                "upload_mib_per_second": 8.275320704299759,
                "upload_ms": 123228,
                "verification_ms": 65265
              }
            ],
            "worker_and_children_peak_rss": 335659008
          },
          {
            "deployment_peak_reserved_bytes": 8582595150,
            "mode": "export",
            "native_worker_plus_child_high_water_rss": 438337536,
            "rate": 0,
            "row_bytes": 4194304,
            "rows": 255,
            "slots": 2,
            "streams": [
              {
                "bytes": 1069338780,
                "download_ms": 63961,
                "kind": "json",
                "producer_ms": 55536,
                "publication_ms": 1742,
                "slot": 0,
                "upload_mib_per_second": 8.23103812056138,
                "upload_ms": 123897,
                "verification_ms": 67755
              },
              {
                "bytes": 1069286655,
                "download_ms": 63811,
                "kind": "template",
                "producer_ms": 54119,
                "publication_ms": 1895,
                "slot": 1,
                "upload_mib_per_second": 8.195380693960063,
                "upload_ms": 124430,
                "verification_ms": 68129
              }
            ],
            "worker_and_children_peak_rss": 320356352
          }
        ]
      }
    ],
    "mixed_workers": [
      {
        "build": "release",
        "database": {
          "connection_and_query_wall_ms": [
            68.83541701245122,
            66.05633301660419,
            66.86462499783374,
            65.23887498769909,
            62.1171249949839
          ],
          "max_connections": "100",
          "server_cpu_limit": null,
          "server_memory_limit": null,
          "server_peak_rss": null,
          "shared_buffers": "128MB",
          "version": "18.6",
          "work_mem": "4MB"
        },
        "deployment_resident_limit": 8589934592,
        "elapsed_seconds": 415.44362812498,
        "logical_cpus": 12,
        "modes": [
          "export",
          "backup"
        ],
        "output_limit": 1072693248,
        "pool_acquire_timeout_ms": 30000,
        "profile": "artifact-remote-v2",
        "rss_acceptance_bytes": 536870912,
        "sample": 0,
        "source_commit": "3ffc7ebcb5e4d16f308afbf13d96d7af906410c9",
        "statement_timeout_ms": 30000,
        "system": "macOS-27.2-arm64-arm-64bit-Mach-O",
        "worker_cpu_limit": null,
        "worker_memory_limit": null,
        "worker_pool_size": 8,
        "workers": [
          {
            "deployment_peak_reserved_bytes": 8582595027,
            "mode": "export",
            "native_worker_plus_child_high_water_rss": 449789952,
            "rate": 0,
            "row_bytes": 4194304,
            "rows": 127,
            "slots": 2,
            "streams": [
              {
                "bytes": 532572704,
                "download_ms": 43838,
                "kind": "json",
                "producer_ms": 31774,
                "publication_ms": 325,
                "slot": 0,
                "upload_mib_per_second": 12.793151543382487,
                "upload_ms": 39701,
                "verification_ms": 39803
              },
              {
                "bytes": 532546687,
                "download_ms": 44068,
                "kind": "template",
                "producer_ms": 29059,
                "publication_ms": 1774,
                "slot": 1,
                "upload_mib_per_second": 12.91221360382219,
                "upload_ms": 39333,
                "verification_ms": 40352
              }
            ],
            "worker_and_children_peak_rss": 413237248
          },
          {
            "deployment_peak_reserved_bytes": 8582595027,
            "mode": "backup",
            "native_worker_plus_child_high_water_rss": 437157888,
            "rate": 0,
            "row_bytes": 4194304,
            "rows": 127,
            "slots": 2,
            "streams": [
              {
                "bytes": 1061086443,
                "download_ms": 70725,
                "kind": "backup",
                "producer_ms": 61943,
                "publication_ms": 280,
                "slot": 0,
                "upload_mib_per_second": 11.622038453841329,
                "upload_ms": 87070,
                "verification_ms": 49672
              },
              {
                "bytes": 532546687,
                "download_ms": 41286,
                "kind": "template",
                "producer_ms": 29097,
                "publication_ms": 343,
                "slot": 1,
                "upload_mib_per_second": 11.001800092696277,
                "upload_ms": 46163,
                "verification_ms": 42353
              }
            ],
            "worker_and_children_peak_rss": 388775936
          }
        ]
      }
    ]
  },
  "measured_date": "2026-10-29",
  "notes": [
    "Repository tests ran concurrently in separate disposable databases on the same PostgreSQL server, with local compilation during parts of qualification. Shared server resources were not isolated; the reported throughput reflects that contention and does not establish dedicated-host latency.",
    "Slow measurements precede two narrow compatibility fixes: restoring the hydration progress event and mapping oversized template output to the existing payload-too-large error. Maximum-size runs include those fixes. The mixed bulk run also includes the later correction that accepts backup format 12 at the restore boundary; that guard is outside the measured creation/download path. All records pin their source revision.",
    "Initial diagnostic series used the general 20-connection test fixture despite declaring eight, and a later download hit its two-second pool acquisition timeout. Those results are excluded. The qualifying driver constructs an eight-connection pool with explicit 30-second acquisition and statement timeouts.",
    "CPU and memory limits are not imposed; RSS thresholds are asserted measurements.",
    "RSS includes the worker and renderer; native high-water sum is conservative.",
    "Resident accounting includes payload, encoded manifest, replay/output spools and pending cleanup; database page, index and WAL allocation is separate.",
    "Database timings include process startup, connection setup and query execution, not pure RTT.",
    "Each run uses a fresh database and two fresh worker processes with two concurrent production paths each.",
    "The resource probe executes task executors and production download streams, without a full HTTP server or global metrics exporter.",
    "Slow-transfer distributions exclude warm-up; bulk cases are resource/integrity qualification, not latency distributions.",
    "These results qualify artifact-remote-v2, not the original constrained lifecycle-shared-v1 host or the 64 GiB format ceiling."
  ],
  "producer_settings": {
    "export_stage_timeout_ms": 3600000,
    "renderer_deadline_seconds": 5,
    "renderer_heap_bytes": 134217728,
    "template_fuel": 10000000,
    "template_max_objects": 10000
  },
  "profile": "artifact-remote-v2",
  "schema_version": 1,
  "slow_transfer": {
    "summary": {
      "backup/backup": {
        "download_ms": {
          "maximum": 134509,
          "median": 134421,
          "minimum": 134343,
          "p95_nearest_rank": 134509
        },
        "producer_ms": {
          "maximum": 37637,
          "median": 31293,
          "minimum": 30131,
          "p95_nearest_rank": 37637
        },
        "publication_ms": {
          "maximum": 269,
          "median": 264,
          "minimum": 258,
          "p95_nearest_rank": 269
        },
        "upload_mib_per_second": {
          "maximum": 0.9270730266395488,
          "median": 0.9074218189793999,
          "minimum": 0.8858658468538488,
          "p95_nearest_rank": 0.9270730266395488
        },
        "upload_ms": {
          "maximum": 143716,
          "median": 140302,
          "minimum": 137328,
          "p95_nearest_rank": 143716
        },
        "verification_ms": {
          "maximum": 139140,
          "median": 133634,
          "minimum": 133481,
          "p95_nearest_rank": 139140
        }
      },
      "backup/template": {
        "download_ms": {
          "maximum": 73983,
          "median": 67200,
          "minimum": 67161,
          "p95_nearest_rank": 73983
        },
        "producer_ms": {
          "maximum": 21154,
          "median": 15680,
          "minimum": 14722,
          "p95_nearest_rank": 21154
        },
        "publication_ms": {
          "maximum": 1600,
          "median": 321,
          "minimum": 303,
          "p95_nearest_rank": 1600
        },
        "upload_mib_per_second": {
          "maximum": 0.9315123765395592,
          "median": 0.8989224022615396,
          "minimum": 0.87449129773191,
          "p95_nearest_rank": 0.9315123765395592
        },
        "upload_ms": {
          "maximum": 72043,
          "median": 70085,
          "minimum": 67633,
          "p95_nearest_rank": 72043
        },
        "verification_ms": {
          "maximum": 67069,
          "median": 66645,
          "minimum": 66548,
          "p95_nearest_rank": 67069
        }
      },
      "export/json": {
        "download_ms": {
          "maximum": 72780,
          "median": 67366,
          "minimum": 67315,
          "p95_nearest_rank": 72780
        },
        "producer_ms": {
          "maximum": 21966,
          "median": 16163,
          "minimum": 14564,
          "p95_nearest_rank": 21966
        },
        "publication_ms": {
          "maximum": 1095,
          "median": 309,
          "minimum": 288,
          "p95_nearest_rank": 1095
        },
        "upload_mib_per_second": {
          "maximum": 0.9331560827670243,
          "median": 0.9024301111641978,
          "minimum": 0.8856985890658338,
          "p95_nearest_rank": 0.9331560827670243
        },
        "upload_ms": {
          "maximum": 71357,
          "median": 70034,
          "minimum": 67728,
          "p95_nearest_rank": 71357
        },
        "verification_ms": {
          "maximum": 67160,
          "median": 66523,
          "minimum": 66389,
          "p95_nearest_rank": 67160
        }
      },
      "export/template": {
        "download_ms": {
          "maximum": 72520,
          "median": 67133,
          "minimum": 67117,
          "p95_nearest_rank": 72520
        },
        "producer_ms": {
          "maximum": 21813,
          "median": 16119,
          "minimum": 14634,
          "p95_nearest_rank": 21813
        },
        "publication_ms": {
          "maximum": 519,
          "median": 315,
          "minimum": 291,
          "p95_nearest_rank": 519
        },
        "upload_mib_per_second": {
          "maximum": 0.9324222854721981,
          "median": 0.9037580915578828,
          "minimum": 0.8799756482735984,
          "p95_nearest_rank": 0.9324222854721981
        },
        "upload_ms": {
          "maximum": 71594,
          "median": 69710,
          "minimum": 67567,
          "p95_nearest_rank": 71594
        },
        "verification_ms": {
          "maximum": 66965,
          "median": 66299,
          "minimum": 66212,
          "p95_nearest_rank": 66965
        }
      }
    },
    "warmup_and_samples": [
      {
        "build": "release",
        "database": {
          "connection_and_query_wall_ms": [
            58.60275001032278,
            57.90404201252386,
            59.57658299303148,
            57.96162500337232,
            57.662541992613114
          ],
          "max_connections": "100",
          "server_cpu_limit": null,
          "server_memory_limit": null,
          "server_peak_rss": null,
          "shared_buffers": "128MB",
          "version": "18.6",
          "work_mem": "4MB"
        },
        "deployment_resident_limit": 8589934592,
        "elapsed_seconds": 580.7567720829975,
        "logical_cpus": 12,
        "modes": [
          "export",
          "backup"
        ],
        "output_limit": 134217728,
        "pool_acquire_timeout_ms": 30000,
        "profile": "artifact-remote-v2",
        "rss_acceptance_bytes": 536870912,
        "sample": -1,
        "source_commit": "e9efba53f867becbbf31c3cc853981302e23fecd",
        "statement_timeout_ms": 30000,
        "system": "macOS-27.2-arm64-arm-64bit-Mach-O",
        "worker_cpu_limit": null,
        "worker_memory_limit": null,
        "worker_pool_size": 8,
        "workers": [
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "export",
            "native_worker_plus_child_high_water_rss": 72466432,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 66270836,
                "download_ms": 67284,
                "kind": "json",
                "producer_ms": 15082,
                "publication_ms": 290,
                "slot": 0,
                "upload_mib_per_second": 0.8788140917177082,
                "upload_ms": 71916,
                "verification_ms": 66488
              },
              {
                "bytes": 66061312,
                "download_ms": 67067,
                "kind": "template",
                "producer_ms": 14897,
                "publication_ms": 311,
                "slot": 1,
                "upload_mib_per_second": 0.8777810118359272,
                "upload_ms": 71773,
                "verification_ms": 66325
              }
            ],
            "worker_and_children_peak_rss": 65208320
          },
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "backup",
            "native_worker_plus_child_high_water_rss": 76316672,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 133497457,
                "download_ms": 134550,
                "kind": "backup",
                "producer_ms": 32198,
                "publication_ms": 278,
                "slot": 0,
                "upload_mib_per_second": 0.9077323716004375,
                "upload_ms": 140254,
                "verification_ms": 133503
              },
              {
                "bytes": 66061312,
                "download_ms": 67184,
                "kind": "template",
                "producer_ms": 14705,
                "publication_ms": 319,
                "slot": 1,
                "upload_mib_per_second": 0.8751958958463569,
                "upload_ms": 71985,
                "verification_ms": 66902
              }
            ],
            "worker_and_children_peak_rss": 69304320
          }
        ]
      },
      {
        "build": "release",
        "database": {
          "connection_and_query_wall_ms": [
            64.45395799528342,
            64.3489999929443,
            63.00066701078322,
            61.69149999914225,
            77.84066699969117
          ],
          "max_connections": "100",
          "server_cpu_limit": null,
          "server_memory_limit": null,
          "server_peak_rss": null,
          "shared_buffers": "128MB",
          "version": "18.6",
          "work_mem": "4MB"
        },
        "deployment_resident_limit": 8589934592,
        "elapsed_seconds": 572.0596825420071,
        "logical_cpus": 12,
        "modes": [
          "export",
          "backup"
        ],
        "output_limit": 134217728,
        "pool_acquire_timeout_ms": 30000,
        "profile": "artifact-remote-v2",
        "rss_acceptance_bytes": 536870912,
        "sample": 0,
        "source_commit": "e9efba53f867becbbf31c3cc853981302e23fecd",
        "statement_timeout_ms": 30000,
        "system": "macOS-27.2-arm64-arm-64bit-Mach-O",
        "worker_cpu_limit": null,
        "worker_memory_limit": null,
        "worker_pool_size": 8,
        "workers": [
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "export",
            "native_worker_plus_child_high_water_rss": 74252288,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 66270837,
                "download_ms": 67366,
                "kind": "json",
                "producer_ms": 21966,
                "publication_ms": 309,
                "slot": 0,
                "upload_mib_per_second": 0.9331560827670243,
                "upload_ms": 67728,
                "verification_ms": 66494
              },
              {
                "bytes": 66061312,
                "download_ms": 67127,
                "kind": "template",
                "producer_ms": 21813,
                "publication_ms": 326,
                "slot": 1,
                "upload_mib_per_second": 0.9324222854721981,
                "upload_ms": 67567,
                "verification_ms": 66299
              }
            ],
            "worker_and_children_peak_rss": 67272704
          },
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "backup",
            "native_worker_plus_child_high_water_rss": 78610432,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 133497445,
                "download_ms": 134509,
                "kind": "backup",
                "producer_ms": 37637,
                "publication_ms": 264,
                "slot": 0,
                "upload_mib_per_second": 0.9270730266395488,
                "upload_ms": 137328,
                "verification_ms": 133687
              },
              {
                "bytes": 66061312,
                "download_ms": 67185,
                "kind": "template",
                "producer_ms": 21154,
                "publication_ms": 321,
                "slot": 1,
                "upload_mib_per_second": 0.9315123765395592,
                "upload_ms": 67633,
                "verification_ms": 66575
              }
            ],
            "worker_and_children_peak_rss": 71598080
          }
        ]
      },
      {
        "build": "release",
        "database": {
          "connection_and_query_wall_ms": [
            78.33304199448321,
            70.43470798816998,
            68.07787499565165,
            66.81991599907633,
            66.79366699245293
          ],
          "max_connections": "100",
          "server_cpu_limit": null,
          "server_memory_limit": null,
          "server_peak_rss": null,
          "shared_buffers": "128MB",
          "version": "18.6",
          "work_mem": "4MB"
        },
        "deployment_resident_limit": 8589934592,
        "elapsed_seconds": 562.1399627920036,
        "logical_cpus": 12,
        "modes": [
          "export",
          "backup"
        ],
        "output_limit": 134217728,
        "pool_acquire_timeout_ms": 30000,
        "profile": "artifact-remote-v2",
        "rss_acceptance_bytes": 536870912,
        "sample": 1,
        "source_commit": "e9efba53f867becbbf31c3cc853981302e23fecd",
        "statement_timeout_ms": 30000,
        "system": "macOS-27.2-arm64-arm-64bit-Mach-O",
        "worker_cpu_limit": null,
        "worker_memory_limit": null,
        "worker_pool_size": 8,
        "workers": [
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "export",
            "native_worker_plus_child_high_water_rss": 74596352,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 66270832,
                "download_ms": 67315,
                "kind": "json",
                "producer_ms": 16163,
                "publication_ms": 288,
                "slot": 0,
                "upload_mib_per_second": 0.9024301111641978,
                "upload_ms": 70034,
                "verification_ms": 66607
              },
              {
                "bytes": 66061312,
                "download_ms": 67117,
                "kind": "template",
                "producer_ms": 16119,
                "publication_ms": 308,
                "slot": 1,
                "upload_mib_per_second": 0.9037580915578828,
                "upload_ms": 69710,
                "verification_ms": 66364
              }
            ],
            "worker_and_children_peak_rss": 67633152
          },
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "backup",
            "native_worker_plus_child_high_water_rss": 82214912,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 133497445,
                "download_ms": 134343,
                "kind": "backup",
                "producer_ms": 31293,
                "publication_ms": 269,
                "slot": 0,
                "upload_mib_per_second": 0.9142376960587404,
                "upload_ms": 139256,
                "verification_ms": 133482
              },
              {
                "bytes": 66061312,
                "download_ms": 67161,
                "kind": "template",
                "producer_ms": 15680,
                "publication_ms": 319,
                "slot": 1,
                "upload_mib_per_second": 0.8989224022615396,
                "upload_ms": 70085,
                "verification_ms": 66888
              }
            ],
            "worker_and_children_peak_rss": 75202560
          }
        ]
      },
      {
        "build": "release",
        "database": {
          "connection_and_query_wall_ms": [
            74.67466700472869,
            69.57979100116063,
            67.61908299813513,
            65.67837500188034,
            65.82595899817534
          ],
          "max_connections": "100",
          "server_cpu_limit": null,
          "server_memory_limit": null,
          "server_peak_rss": null,
          "shared_buffers": "128MB",
          "version": "18.6",
          "work_mem": "4MB"
        },
        "deployment_resident_limit": 8589934592,
        "elapsed_seconds": 599.7788421659934,
        "logical_cpus": 12,
        "modes": [
          "export",
          "backup"
        ],
        "output_limit": 134217728,
        "pool_acquire_timeout_ms": 30000,
        "profile": "artifact-remote-v2",
        "rss_acceptance_bytes": 536870912,
        "sample": 2,
        "source_commit": "e9efba53f867becbbf31c3cc853981302e23fecd",
        "statement_timeout_ms": 30000,
        "system": "macOS-27.2-arm64-arm-64bit-Mach-O",
        "worker_cpu_limit": null,
        "worker_memory_limit": null,
        "worker_pool_size": 8,
        "workers": [
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "export",
            "native_worker_plus_child_high_water_rss": 73711616,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 66270844,
                "download_ms": 67342,
                "kind": "json",
                "producer_ms": 14893,
                "publication_ms": 289,
                "slot": 0,
                "upload_mib_per_second": 0.8952208539812068,
                "upload_ms": 70598,
                "verification_ms": 67160
              },
              {
                "bytes": 66061312,
                "download_ms": 67133,
                "kind": "template",
                "producer_ms": 14824,
                "publication_ms": 291,
                "slot": 1,
                "upload_mib_per_second": 0.8799756482735984,
                "upload_ms": 71594,
                "verification_ms": 66965
              }
            ],
            "worker_and_children_peak_rss": 66715648
          },
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "backup",
            "native_worker_plus_child_high_water_rss": 76021760,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 133497457,
                "download_ms": 134421,
                "kind": "backup",
                "producer_ms": 30131,
                "publication_ms": 266,
                "slot": 0,
                "upload_mib_per_second": 0.8858658468538488,
                "upload_ms": 143716,
                "verification_ms": 133634
              },
              {
                "bytes": 66061312,
                "download_ms": 67367,
                "kind": "template",
                "producer_ms": 15135,
                "publication_ms": 303,
                "slot": 1,
                "upload_mib_per_second": 0.8826509458579095,
                "upload_ms": 71377,
                "verification_ms": 67069
              }
            ],
            "worker_and_children_peak_rss": 69042176
          }
        ]
      },
      {
        "build": "release",
        "database": {
          "connection_and_query_wall_ms": [
            77.74945900018793,
            69.1550420015119,
            68.3289589942433,
            68.23879199509975,
            68.8652499957243
          ],
          "max_connections": "100",
          "server_cpu_limit": null,
          "server_memory_limit": null,
          "server_peak_rss": null,
          "shared_buffers": "128MB",
          "version": "18.6",
          "work_mem": "4MB"
        },
        "deployment_resident_limit": 8589934592,
        "elapsed_seconds": 560.7763654580049,
        "logical_cpus": 12,
        "modes": [
          "export",
          "backup"
        ],
        "output_limit": 134217728,
        "pool_acquire_timeout_ms": 30000,
        "profile": "artifact-remote-v2",
        "rss_acceptance_bytes": 536870912,
        "sample": 3,
        "source_commit": "e9efba53f867becbbf31c3cc853981302e23fecd",
        "statement_timeout_ms": 30000,
        "system": "macOS-27.2-arm64-arm-64bit-Mach-O",
        "worker_cpu_limit": null,
        "worker_memory_limit": null,
        "worker_pool_size": 8,
        "workers": [
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "export",
            "native_worker_plus_child_high_water_rss": 65748992,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 66270839,
                "download_ms": 67387,
                "kind": "json",
                "producer_ms": 14564,
                "publication_ms": 1095,
                "slot": 0,
                "upload_mib_per_second": 0.9266299696648875,
                "upload_ms": 68205,
                "verification_ms": 66389
              },
              {
                "bytes": 66061312,
                "download_ms": 67285,
                "kind": "template",
                "producer_ms": 14634,
                "publication_ms": 519,
                "slot": 1,
                "upload_mib_per_second": 0.9163645119707349,
                "upload_ms": 68751,
                "verification_ms": 66212
              }
            ],
            "worker_and_children_peak_rss": 58458112
          },
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "backup",
            "native_worker_plus_child_high_water_rss": 70959104,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 133497457,
                "download_ms": 134378,
                "kind": "backup",
                "producer_ms": 30734,
                "publication_ms": 261,
                "slot": 0,
                "upload_mib_per_second": 0.9074218189793999,
                "upload_ms": 140302,
                "verification_ms": 133481
              },
              {
                "bytes": 66061312,
                "download_ms": 67200,
                "kind": "template",
                "producer_ms": 14722,
                "publication_ms": 1600,
                "slot": 1,
                "upload_mib_per_second": 0.9030456039919731,
                "upload_ms": 69765,
                "verification_ms": 66645
              }
            ],
            "worker_and_children_peak_rss": 63979520
          }
        ]
      },
      {
        "build": "release",
        "database": {
          "connection_and_query_wall_ms": [
            63.14533299882896,
            63.161707992549054,
            63.315916995634325,
            62.29137499758508,
            60.39825000334531
          ],
          "max_connections": "100",
          "server_cpu_limit": null,
          "server_memory_limit": null,
          "server_peak_rss": null,
          "shared_buffers": "128MB",
          "version": "18.6",
          "work_mem": "4MB"
        },
        "deployment_resident_limit": 8589934592,
        "elapsed_seconds": 579.3827829579968,
        "logical_cpus": 12,
        "modes": [
          "export",
          "backup"
        ],
        "output_limit": 134217728,
        "pool_acquire_timeout_ms": 30000,
        "profile": "artifact-remote-v2",
        "rss_acceptance_bytes": 536870912,
        "sample": 4,
        "source_commit": "e9efba53f867becbbf31c3cc853981302e23fecd",
        "statement_timeout_ms": 30000,
        "system": "macOS-27.2-arm64-arm-64bit-Mach-O",
        "worker_cpu_limit": null,
        "worker_memory_limit": null,
        "worker_pool_size": 8,
        "workers": [
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "export",
            "native_worker_plus_child_high_water_rss": 76890112,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 66270836,
                "download_ms": 72780,
                "kind": "json",
                "producer_ms": 16385,
                "publication_ms": 318,
                "slot": 0,
                "upload_mib_per_second": 0.8856985890658338,
                "upload_ms": 71357,
                "verification_ms": 66523
              },
              {
                "bytes": 66061312,
                "download_ms": 72520,
                "kind": "template",
                "producer_ms": 16262,
                "publication_ms": 315,
                "slot": 1,
                "upload_mib_per_second": 0.886251727636558,
                "upload_ms": 71087,
                "verification_ms": 66253
              }
            ],
            "worker_and_children_peak_rss": 69599232
          },
          {
            "deployment_peak_reserved_bytes": 1074790867,
            "mode": "backup",
            "native_worker_plus_child_high_water_rss": 75628544,
            "rate": 1048576,
            "row_bytes": 65536,
            "rows": 1024,
            "slots": 2,
            "streams": [
              {
                "bytes": 133497451,
                "download_ms": 134435,
                "kind": "backup",
                "producer_ms": 31388,
                "publication_ms": 258,
                "slot": 0,
                "upload_mib_per_second": 0.9026736409841312,
                "upload_ms": 141040,
                "verification_ms": 139140
              },
              {
                "bytes": 66061312,
                "download_ms": 73983,
                "kind": "template",
                "producer_ms": 15690,
                "publication_ms": 324,
                "slot": 1,
                "upload_mib_per_second": 0.87449129773191,
                "upload_ms": 72043,
                "verification_ms": 66548
              }
            ],
            "worker_and_children_peak_rss": 68616192
          }
        ]
      }
    ]
  },
  "source_commits": {
    "maximum_exports": "6c1187916dac097345b409e826a68ec1fc3854c4",
    "maximum_mixed": "3ffc7ebcb5e4d16f308afbf13d96d7af906410c9",
    "slow_transfer": "e9efba53f867becbbf31c3cc853981302e23fecd"
  },
  "toolchain": {
    "python": "Python 3.15.2",
    "rust": "rustc 1.99.1 (58cf8f9c3 2026-10-13)"
  }
}
```
